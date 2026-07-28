import sys  
import logging
from pyspark.sql import SparkSession
from databricks.sdk.runtime import dbutils


def ingest_from_rds_to_s3():

    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    spark = (SparkSession.builder
        .appName("ingest_from_rds_to_s3")
        .getOrCreate()
    )

    bucket_name = sys.argv[1]
    table_name = "public.raw_fema_claims"
    bronze_output_path = f"s3://{bucket_name}/bronze/raw_data/"

    try:
        logging.info("Retrieving credentials from Databricks Secrets...")
        
        rds_endpoint  = dbutils.secrets.get(scope="fema_scope", key="rds_endpoint")
        rds_username  = dbutils.secrets.get(scope="fema_scope", key="rds_username")
        rds_password  = dbutils.secrets.get(scope="fema_scope", key="rds_password")

        jdbc_url = f"jdbc:postgresql://{rds_endpoint}/fema_flood_db"
        connection_properties = {
            "user": rds_username, 
            "password": rds_password,
            "driver": "org.postgresql.Driver" 
        }

        logging.info("Starting ingest raw data from RDS...")

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

        (df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("path", bronze_output_path) 
            .saveAsTable("default.bronze_raw_fema_claims") 
        )

        logging.info(f"Successfully uploaded data to {bronze_output_path}")

    except Exception as e:
        logging.error(f"Unexpected error occurred!: {e}")
        raise e 

    finally:
        spark.stop()

if __name__ == "__main__":
    ingest_from_rds_to_s3()