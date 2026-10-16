import sys
import logging
from datetime import datetime
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


def start_run():

    # --------------------------------------------------
    # 1. Get run_id and run_type
    # --------------------------------------------------
    if len(sys.argv) < 3:
        raise ValueError("run_id and run_type arguments are required.")

    run_id = sys.argv[1]
    run_type = sys.argv[2].upper()

    if run_type not in ("INITIAL", "INCREMENTAL"):
        raise ValueError(f"Invalid run_type: {run_type}. Expected INITIAL or INCREMENTAL.")

    spark = (
        SparkSession.builder
        .appName("start_run")
        .getOrCreate()
    )

    try:
        logging.info(f"Starting pipeline run: run_id={run_id}, run_type={run_type}.")

        # --------------------------------------------------
        # 2. Prevent repeated Initial Load
        # --------------------------------------------------
        if run_type == "INITIAL":

            initial_run_df = (
                spark.table(cfg.RUN_TABLE)
                .filter(
                    (col("pipeline_name") == cfg.PIPELINE_NAME) &
                    (col("run_type") == "INITIAL") &
                    (col("status") == "SUCCESS")
                )
                .limit(1)
            )

            if initial_run_df.count() > 0:
                raise Exception(
                    "Initial load has already completed successfully. "
                    "Repeated INITIAL run is not allowed."
            )

        # --------------------------------------------------
        # 3. Get RDS credentials
        # --------------------------------------------------
        logging.info( "Retrieving credentials from Databricks Secrets...")

        rds_endpoint  = dbutils.secrets.get(scope="fema_scope", key="rds_endpoint")
        rds_username  = dbutils.secrets.get(scope="fema_scope", key="rds_username")
        rds_password  = dbutils.secrets.get(scope="fema_scope", key="rds_password")

        # --------------------------------------------------
        # 4. JDBC connection
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
        # 5. Get last watermark (Based on run_type)
        # --------------------------------------------------
        logging.info(f"Determining start_watermark for {run_type} load...")

        if run_type == "INITIAL":
            start_watermark = datetime.strptime(
                cfg.INITIAL_START_WATERMARK,
                "%Y-%m-%d %H:%M:%S"
            )
            logging.info(f"Initial load: setting start_watermark to {cfg.INITIAL_START_WATERMARK}")
            
        elif run_type == "INCREMENTAL":
            logging.info("Reading last watermark from WATERMARK_TABLE...")
            
            watermark_df = (
                spark.table(cfg.WATERMARK_TABLE)
                .filter(
                    (col("pipeline_name") == cfg.PIPELINE_NAME) &
                    (col("source_table") == cfg.SOURCE_TABLE)
                )
                .select("last_watermark")
            )

            watermark_rows = watermark_df.take(2)

            if not watermark_rows:
                raise Exception(f"No watermark found for {cfg.PIPELINE_NAME} pipeline, source_table={cfg.SOURCE_TABLE}")

            if len(watermark_rows) > 1:
                raise Exception(f"Multiple watermark records found for {cfg.PIPELINE_NAME} pipeline, source_table={cfg.SOURCE_TABLE}")

            start_watermark = watermark_rows[0]["last_watermark"]

            if start_watermark is None:
                raise Exception("last_watermark is NULL. Incremental load cannot start.")

        logging.info( f"start_watermark = {start_watermark}")

        # --------------------------------------------------
        # 6. Get high watermark from RDS
        # --------------------------------------------------
        logging.info("Getting high watermark from RDS...")

        high_watermark_df = (
            spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("user",connection_properties["user"])
            .option("password",connection_properties["password"])
            .option("driver",connection_properties["driver"])
            .option("query",
                f"""
                SELECT MAX(source_updated_at) AS high_watermark
                FROM {cfg.SOURCE_TABLE}
                """
            )
            .load()
        )

        high_watermark = (high_watermark_df.collect()[0]["high_watermark"])

        if high_watermark is None:
            raise Exception("high_watermark is NULL. No source data available.")

        logging.info( f"high_watermark = {high_watermark}")

        # --------------------------------------------------
        # 7. Create pipeline_run record
        # --------------------------------------------------
        run_df = (
            spark.createDataFrame(
                [(  run_id,
                    cfg.PIPELINE_NAME,
                    run_type,
                    start_watermark,
                    high_watermark,
                    "RUNNING",
                    None,
                    None,
                    None,
                    None,
                    None
                )],
                """
                run_id STRING,
                pipeline_name STRING,
                run_type STRING,
                start_watermark TIMESTAMP,
                high_watermark TIMESTAMP,
                status STRING,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                rows_read BIGINT,
                rows_written BIGINT,
                error_message STRING
                """
            )
            .withColumn("started_at", current_timestamp())
        )

        # --------------------------------------------------
        # 8. Insert pipeline_run
        # --------------------------------------------------
        pipeline_run_delta = DeltaTable.forName(spark, cfg.RUN_TABLE)

        (
            pipeline_run_delta.alias("target")
            .merge(
                run_df.alias("source"),
                """
                target.run_id = source.run_id AND 
                target.pipeline_name = source.pipeline_name
                """
            )
            .whenNotMatchedInsert(values={
                "run_id": "source.run_id",
                "pipeline_name": "source.pipeline_name",
                "run_type": "source.run_type",
                "start_watermark": "source.start_watermark",
                "high_watermark": "source.high_watermark",
                "status": "source.status",
                "started_at": "source.started_at",
                "completed_at": "source.completed_at",
                "rows_read": "source.rows_read",
                "rows_written": "source.rows_written",
                "error_message": "source.error_message"
            })
            .execute()
        )

        logging.info(f"Pipeline run {run_id} initialized successfully." )
        logging.info(f"Run window: {start_watermark} -> {high_watermark}")

    except Exception as e:
        logging.error(f"Failed to initialize pipeline run: {e}.")
        raise

    finally:
        spark.stop()
        logging.info("Spark session stopped.")

if __name__ == "__main__":
    start_run()