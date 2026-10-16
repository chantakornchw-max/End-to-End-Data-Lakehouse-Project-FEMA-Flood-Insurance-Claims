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
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def finalize_run():

    # --------------------------------------------------
    # 1. Get run_id and status
    # --------------------------------------------------
    if len(sys.argv) < 3:
        raise ValueError("run_id and status arguments are required.")

    run_id = sys.argv[1]
    run_status = sys.argv[2].upper()

    if run_status not in ("SUCCESS", "FAILED"):
        raise ValueError(f"Invalid run status: {run_status}. Expected SUCCESS or FAILED.")

    logging.info( f"Finalizing pipeline run: run_id={run_id}, status={run_status}")

    spark = (
        SparkSession.builder
        .appName("finalize_run")
        .getOrCreate()
    )

    try:
        # --------------------------------------------------
        # 2. Get current pipeline_run record
        # --------------------------------------------------
        run_df = (
            spark.table(cfg.RUN_TABLE)
            .filter(
                (col("run_id") == run_id) &
                (col("pipeline_name") == cfg.PIPELINE_NAME)
            )
            .select("status")
        )

        run_rows = run_df.take(2)

        if not run_rows:
            raise Exception(f"No pipeline_run found for run_id={run_id}, pipeline_name={cfg.PIPELINE_NAME}")

        if len(run_rows) > 1:
            raise Exception(f"Multiple pipeline_run records found for run_id={run_id}, pipeline_name={cfg.PIPELINE_NAME}")

        current_status = run_rows[0]["status"]

        logging.info(f"Current pipeline status = {current_status}")

        if current_status != "RUNNING": 
            logging.warning(f"Pipeline run {run_id} is already finalized, with status={current_status}.")
            return

        # --------------------------------------------------
        # 3. Prepare final update
        # --------------------------------------------------
        final_run_df = (
            spark.createDataFrame(
                [(  run_id,
                    cfg.PIPELINE_NAME,
                    run_status
                )],
                """
                run_id STRING,
                pipeline_name STRING,
                status STRING
                """
            )
            .withColumn("completed_at", current_timestamp())
        )

        # --------------------------------------------------
        # 4. Update pipeline_run
        # --------------------------------------------------
        pipeline_run_delta = DeltaTable.forName(spark, cfg.RUN_TABLE)

        ( pipeline_run_delta.alias("target")
            .merge(
                final_run_df.alias("source"),
                """
                target.run_id = source.run_id AND 
                target.pipeline_name = source.pipeline_name
                """
            )
            .whenMatchedUpdate(
                condition="target.status = 'RUNNING'",
                set={
                    "status": "source.status",
                    "completed_at": "source.completed_at"
                }
            )
            .execute()
        )

        logging.info(f"Pipeline run {run_id} finalized successfully, with status={run_status}")

    except Exception as e:
        logging.error(f"Failed to finalize pipeline run: {e}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    finalize_run()