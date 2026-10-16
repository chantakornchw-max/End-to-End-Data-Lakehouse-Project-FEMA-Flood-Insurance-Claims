import sys  
import logging
import scripts.common.config as cfg
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from databricks.sdk.runtime import dbutils


# --------------------------------------------------
# Logging Configuration
# --------------------------------------------------
logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s'
)


def extract_initial():

    # --------------------------------------------------
    # 1. Get argv
    # --------------------------------------------------
    if len(sys.argv) < 3:
        raise ValueError("run_id and bucket_name arguments are required.")

    run_id = sys.argv[1]
    bucket_name = sys.argv[2]
    table_name = cfg.SOURCE_TABLE
    bronze_output_path = f"s3://{bucket_name}/bronze/raw_data/"

    spark = (
        SparkSession.builder
        .appName("extract_initial")
        .getOrCreate()
    )

    try:
        logging.info(f"Starting extract initial data for run_id = {run_id}.") 

        # --------------------------------------------------
        # 2. Get RDS credentials
        # --------------------------------------------------
        logging.info("Retrieving credentials from Databricks Secrets...")
        
        rds_endpoint  = dbutils.secrets.get(scope="fema_scope", key="rds_endpoint")
        rds_username  = dbutils.secrets.get(scope="fema_scope", key="rds_username")
        rds_password  = dbutils.secrets.get(scope="fema_scope", key="rds_password")

        # --------------------------------------------------
        # 3. JDBC connection
        # --------------------------------------------------
        jdbc_url = f"jdbc:postgresql://{rds_endpoint}/{cfg.DATABASE_NAME}"
        connection_properties = {
            "user": rds_username, 
            "password": rds_password,
            "driver": "org.postgresql.Driver" 
        }

        # --------------------------------------------------
        # 4. Extract data from RDS
        # --------------------------------------------------
        logging.info("Starting extract raw data from RDS...")

        df = (spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table_name)
            .option("user", connection_properties["user"])
            .option("password", connection_properties["password"])
            .option("driver", connection_properties["driver"])
            .option("partitionColumn", "row_num")
            .option("lowerBound", "1")
            .option("upperBound", "3000000")
            .option("numPartitions", "10")
            .load()
        )

        # --------------------------------------------------
        # 5. Count extracted rows
        # --------------------------------------------------
        rows_read = df.count()
        logging.info( f"Initial rows read = {rows_read}.")

        if rows_read == 0:
            raise Exception("No rows found in RDS. Initial load cannot continue.")

        else:
            # --------------------------------------------------
            # 6. Write data to Bronze table
            # --------------------------------------------------
            logging.info("Writing data to Bronze table...")

            (df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .option("path", bronze_output_path) 
                .saveAsTable(cfg.BRONZE_TABLE) 
            )

            logging.info(f"Successfully overwritten Bronze table '{cfg.BRONZE_TABLE}' at {bronze_output_path}")

        # --------------------------------------------------
        # 7. Update rows_read in RUN_TABLE
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

        logging.info(f"Successfully updated rows_read = {rows_read} in RUN_TABLE.")

    except Exception as e:
        logging.error(f"Unexpected error occurred!: {e}")
        raise 

    finally:
        spark.stop()
        logging.info("Spark session stopped.")


if __name__ == "__main__":
    extract_initial()