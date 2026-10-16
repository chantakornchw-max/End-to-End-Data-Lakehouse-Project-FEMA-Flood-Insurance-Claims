import sys
import logging
import scripts.common.config as cfg
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


# --------------------------------------------------
# Logging Configuration
# --------------------------------------------------
logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s'
)


def update_watermark():

    # --------------------------------------------------
    # 1. Get run_id
    # --------------------------------------------------
    if len(sys.argv) < 2:
        raise ValueError("run_id argument is required.")

    run_id = sys.argv[1]

    spark = (
        SparkSession.builder
        .appName("update_watermark")
        .getOrCreate()
    )

    try:
        logging.info(f"Starting watermark update for run_id={run_id}")

        # --------------------------------------------------
        # 2. Get pipeline run result
        # --------------------------------------------------
        logging.info("Reading pipeline run status...")

        run_df = (
            spark.table(cfg.RUN_TABLE)
            .filter(
                (col("run_id") == run_id) &
                (col("pipeline_name") == cfg.PIPELINE_NAME)
            )
            .select(
                "high_watermark",
                "status"
            )
        )

        run_rows = run_df.take(2)

        if not run_rows:
            raise Exception( f"No pipeline_run found for run_id={run_id}, pipeline_name={cfg.PIPELINE_NAME}")
        
        if len(run_rows) > 1:
            raise Exception(f"Multiple pipeline_run records found for run_id={run_id}, pipeline_name={cfg.PIPELINE_NAME}")

        high_watermark = run_rows[0]["high_watermark"]
        run_status = run_rows[0]["status"]

        # --------------------------------------------------
        # 3. Safety checks
        # --------------------------------------------------
        if run_status != "SUCCESS":
            raise Exception(f"Run {run_id} is not successful (status={run_status}). Watermark will not be updated.")

        if high_watermark is None:
            raise Exception(f"Run {run_id} has null high_watermark.")

        logging.info(f"Run status = {run_status}, high_watermark = {high_watermark}")

        # --------------------------------------------------
        # 4. Prepare watermark update
        # --------------------------------------------------
        update_df = (
            spark.createDataFrame(
                [
                    (
                        cfg.PIPELINE_NAME,
                        cfg.SOURCE_TABLE,
                        "source_updated_at",
                        high_watermark,
                        run_id
                    )
                ],
                """
                pipeline_name STRING,
                source_table STRING,
                watermark_column STRING,
                last_watermark TIMESTAMP,
                last_run_id STRING
                """
            )
            .withColumn("updated_at", current_timestamp())
        )

        # --------------------------------------------------
        # 5. Update watermark table
        # --------------------------------------------------
        logging.info("Updating pipeline watermark...")

        watermark_delta = DeltaTable.forName(spark, cfg.WATERMARK_TABLE)

        (
            watermark_delta.alias("target")
            .merge(
                update_df.alias("source"),
                """
                target.pipeline_name = source.pipeline_name AND 
                target.source_table = source.source_table
                """
            )
            .whenMatchedUpdate(
                condition="""
                    source.last_watermark >= target.last_watermark
                """,
                set={
                    "watermark_column": "source.watermark_column",
                    "last_watermark": "source.last_watermark",
                    "last_run_id": "source.last_run_id",
                    "updated_at": "source.updated_at"
                }
            )
            .whenNotMatchedInsert(
                values={
                    "pipeline_name": "source.pipeline_name",
                    "source_table": "source.source_table",
                    "watermark_column": "source.watermark_column",
                    "last_watermark": "source.last_watermark",
                    "last_run_id": "source.last_run_id",
                    "updated_at": "source.updated_at"
                }
            )
            .execute()
        )

        logging.info(f"Watermark updated successfully: {high_watermark}")

    except Exception as e:
        logging.error(f"Failed to update watermark: {e}")
        raise

    finally:
        spark.stop()
        logging.info("Spark session stopped.")


if __name__ == "__main__":
    update_watermark()