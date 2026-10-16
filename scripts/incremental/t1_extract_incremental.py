import sys
import logging
import scripts.common.config as cfg
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from databricks.sdk.runtime import dbutils


# --------------------------------------------------
# Logging Configuration
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def extract_incremental():

    # --------------------------------------------------
    # 1. Get run_id
    # --------------------------------------------------
    if len(sys.argv) < 2:
        raise ValueError("run_id argument is required.")

    run_id = sys.argv[1]

    spark = (
        SparkSession.builder
        .appName("extract_incremental")
        .getOrCreate()
    )

    try:
        logging.info(f"Starting extract incremental data for run_id={run_id}")

        # --------------------------------------------------
        # 2. Get RDS credentials
        # --------------------------------------------------
        logging.info( "Retrieving credentials from Databricks Secrets...")

        rds_endpoint  = dbutils.secrets.get(scope="fema_scope", key="rds_endpoint")
        rds_username  = dbutils.secrets.get(scope="fema_scope", key="rds_username")
        rds_password  = dbutils.secrets.get(scope="fema_scope", key="rds_password")

        # --------------------------------------------------
        # 3. JDBC connection
        # --------------------------------------------------
        jdbc_url = (
            f"jdbc:postgresql://{rds_endpoint}/fema_flood_db"
        )
        connection_properties = {
            "user": rds_username,
            "password": rds_password,
            "driver": "org.postgresql.Driver"
        }

        # --------------------------------------------------
        # 4. Get run watermarks
        # --------------------------------------------------
        logging.info("Reading run watermarks...")

        run_df = (
            spark.table(cfg.RUN_TABLE)
            .filter(
                (col("run_id") == run_id) &
                (col("pipeline_name") == cfg.PIPELINE_NAME)
            )
            .select(
                "start_watermark",
                "high_watermark"
            )
        )

        run_rows = run_df.take(2)

        if not run_rows:
            raise Exception( f"No pipeline_run found for run_id={run_id}, pipeline_name={cfg.PIPELINE_NAME}")

        if len(run_rows) > 1:
            raise Exception(f"Multiple pipeline_run records found for run_id={run_id}, pipeline_name={cfg.PIPELINE_NAME}")

        start_watermark = run_rows[0]["start_watermark"]
        high_watermark = run_rows[0]["high_watermark"]

        if start_watermark is None:
            raise Exception("start_watermark is NULL.")

        if high_watermark is None:
            raise Exception("high_watermark is NULL.")

        logging.info(f"Run window: {start_watermark} -> {high_watermark}")

        # --------------------------------------------------
        # 5. Check whether new data exists
        # --------------------------------------------------

        rows_read = 0

        if high_watermark <= start_watermark:
            logging.info("No new or changed data found.")

        else:
            # --------------------------------------------------
            # 6. Build incremental query
            # --------------------------------------------------
            incremental_query = (
                f"""
                (   SELECT *
                    FROM {cfg.SOURCE_TABLE}
                    WHERE source_updated_at > '{start_watermark}'
                    AND source_updated_at <= '{high_watermark}'
                ) AS incremental_data
                """
            )

            # --------------------------------------------------
            # 7. Read incremental data from RDS
            # --------------------------------------------------
            logging.info("Starting incremental extract from RDS...")

            df = (
                spark.read
                .format("jdbc")
                .option("url", jdbc_url)
                .option("user", connection_properties["user"])
                .option("password",connection_properties["password"])
                .option("driver", connection_properties["driver"])
                .option("dbtable", incremental_query)
                .load()
            )

            # --------------------------------------------------
            # 8. Count extracted rows
            # --------------------------------------------------
            rows_read = df.count()
            logging.info( f"Incremental rows read = {rows_read}.")

            if rows_read == 0:
                logging.info( "No rows found in incremental window." )

            else:
                # --------------------------------------------------
                # 9. Append to Bronze
                # --------------------------------------------------
                (df.write
                    .format("delta")
                    .mode("append")
                    .saveAsTable(cfg.BRONZE_TABLE)
                )

                logging.info(f"Successfully appended {rows_read} rows to Bronze.")
                logging.info(f"Incremental window: "f"{start_watermark} -> {high_watermark}")

        # --------------------------------------------------
        # 10. Update rows_read in RUN_TABLE
        # --------------------------------------------------
        logging.info("Updating rows_read in RUN_TABLE...")

        rows_read_df = (
            spark.createDataFrame(
                [(  run_id,
                    cfg.PIPELINE_NAME,
                    rows_read
                )],
                """
                run_id STRING,
                pipeline_name STRING,
                rows_read BIGINT
                """
            )
        )

        pipeline_run_delta = DeltaTable.forName(spark, cfg.RUN_TABLE)

        (
            pipeline_run_delta.alias("target")
            .merge(
                rows_read_df.alias("source"),
                """
                target.run_id = source.run_id AND 
                target.pipeline_name = source.pipeline_name
                """
            )
            .whenMatchedUpdate(
                set={"rows_read": "source.rows_read"}
            )
            .execute()
        )
        
        logging.info(f"Successfully updated rows_read in RUN_TABLE.")

    except Exception as e:
         logging.error( f"Unexpected error occurred!: {e}")
         raise

    finally:
        spark.stop()
        logging.info("Spark session stopped.")


if __name__ == "__main__":
    extract_incremental()