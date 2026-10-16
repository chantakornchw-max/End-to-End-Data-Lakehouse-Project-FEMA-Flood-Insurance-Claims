import sys
import logging
import scripts.common.config as cfg
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lpad, split, when, abs, year, current_timestamp, row_number, sum as spark_sum, coalesce, lit
from pyspark.sql.types import StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.window import Window


# --------------------------------------------------
# Logging Configuration
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def merge_to_silver():

    # --------------------------------------------------
    # 1. Get run_id
    # --------------------------------------------------
    if len(sys.argv) < 2:
        raise ValueError("run_id argument is required.")

    run_id = sys.argv[1]

    spark = (
        SparkSession.builder
        .appName("merge_to_silver")
        .getOrCreate()
    )

    try:
        logging.info(f"Starting Merge Bronze to Silver for run_id={run_id}")

        # --------------------------------------------------
        # 2. Get run watermarks
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
            raise Exception(f"No pipeline_run found for run_id={run_id}, pipeline_name={cfg.PIPELINE_NAME}")

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
        # 3. Read only current incremental batch from Bronze
        # --------------------------------------------------
        logging.info("Reading incremental data from Bronze...")

        bronze_df = (
            spark.table(cfg.BRONZE_TABLE)
            .filter(
                (col("source_updated_at") > start_watermark) &
                (col("source_updated_at") <= high_watermark)
            )
        )

        # --------------------------------------------------
        # 4. Clean and transform
        # --------------------------------------------------
        logging.info("Starting clean process...")

        cleaned_df = (
            bronze_df

            # Select columns, Change type, and Rename
            .select(
                col("id").cast(StringType()).alias("claim_id"),
                col("floodEvent").cast(StringType()).alias("flood_event"),
                col("dateOfLoss").cast(TimestampType()).alias("date_of_loss"),
                col("originalConstructionDate").cast(TimestampType()).alias("original_construction_date"),
                col("state").cast(StringType()).alias("state"),
                lpad(split( col("reportedZipCode").cast(StringType()), r"[.-]")[0], 5 ,"0").alias("reported_zip_code"),
                col("latitude").cast(DoubleType()).alias("latitude"),
                col("longitude").cast(DoubleType()).alias("longitude"),
                col("ratedFloodZone").cast(StringType()).alias("rated_flood_zone"),
                col("occupancyType").cast(DoubleType()).cast(IntegerType()).alias("occupancy_type"),
                col("primaryResidenceIndicator").cast(DoubleType()).cast(IntegerType()).alias("primary_residence_indicator"),
                col("elevationDifference").cast(DoubleType()).alias("elevation_difference"),
                col("waterDepth").cast(DoubleType()).alias("water_depth"),
                col("buildingPropertyValue").cast(DoubleType()).alias("building_property_value"),
                col("totalBuildingInsuranceCoverage").cast(DoubleType()).alias("total_building_insurance_coverage"),
                col("buildingDamageAmount").cast(DoubleType()).alias("building_damage_amount"),
                col("buildingReplacementCost").cast(DoubleType()).alias("building_replacement_cost"),
                col("netBuildingPaymentAmount").cast(DoubleType()).alias("net_building_payment_amount"),
                col("totalContentsInsuranceCoverage").cast(DoubleType()).alias("total_contents_insurance_coverage"),
                col("contentsDamageAmount").cast(DoubleType()).alias("contents_damage_amount"),
                col("netContentsPaymentAmount").cast(DoubleType()).alias("net_contents_payment_amount"),
                col("buildingDeductibleCode").cast(StringType()).alias("building_deductible_code"),
                col("causeOfDamage") .cast(StringType()).alias("cause_of_damage"),
                when(col("nonPaymentReasonBuilding").cast(StringType()).rlike(r"^\d+(\.0)?$"),
                    lpad(split(col("nonPaymentReasonBuilding").cast(StringType()), r"\." )[0], 2, "0")
                )
                .otherwise(col("nonPaymentReasonBuilding").cast(StringType()))
                .alias("non_payment_reason_building"),
                col("source_updated_at").cast(TimestampType()).alias("source_updated_at")
            )

            # Handling NULL values
            .withColumn("year_of_loss", year(col("date_of_loss")))
            .fillna("Unknown",
                subset=[
                    "flood_event",
                    "state",
                    "reported_zip_code",
                    "rated_flood_zone",
                    "building_deductible_code",
                    "cause_of_damage"
                ]
            )
            .fillna("Not Applicable", subset=["non_payment_reason_building"])
            .fillna(0.0,
                subset=[
                    "building_property_value",
                    "total_building_insurance_coverage",
                    "building_damage_amount",
                    "building_replacement_cost",
                    "net_building_payment_amount",
                    "total_contents_insurance_coverage",
                    "contents_damage_amount",
                    "net_contents_payment_amount"
                ]
            )

            # Handling negative values
            .withColumn("building_property_value", abs(col("building_property_value")))
            .withColumn("total_building_insurance_coverage", abs(col("total_building_insurance_coverage")))
            .withColumn("building_replacement_cost", abs(col("building_replacement_cost")))
            .withColumn("net_building_payment_amount", abs(col("net_building_payment_amount")))
            .withColumn("total_contents_insurance_coverage", abs(col("total_contents_insurance_coverage")))
            .withColumn("contents_damage_amount", abs(col("contents_damage_amount")))
            .withColumn("net_contents_payment_amount", abs(col("net_contents_payment_amount")))
        )

        # --------------------------------------------------
        # 5. Handle Deduplicate - keep latest record
        # --------------------------------------------------
        window_spec = (
            Window
            .partitionBy(col("claim_id"))
            .orderBy(
                col("source_updated_at").desc()
            )
        )

        cleaned_df = (
                cleaned_df
                .withColumn("row_num", row_number().over(window_spec))
                .filter(col("row_num") == 1)
                .drop("row_num")
                .withColumn( "processed_at", current_timestamp())
        )

        # --------------------------------------------------
        # 6. Check Data Quality 
        # --------------------------------------------------
        logging.info("Checking Silver data quality...")
        
        cleaned_dq = (
            cleaned_df
            .agg(
                coalesce(
                    spark_sum(
                        when(col("claim_id").isNull(), 1)
                        .otherwise(0)
                    ), 
                    lit(0)
                ).alias("claim_id_null"),

                coalesce(
                    spark_sum(
                        when(col("date_of_loss").isNull(), 1)
                        .otherwise(0)
                    ), 
                    lit(0)
                ).alias("date_of_loss_null"),

                coalesce(
                    spark_sum(
                        when(col("year_of_loss").isNull(), 1)
                        .otherwise(0)
                    ), 
                    lit(0)
                ).alias("year_of_loss_null"),

                coalesce(
                    spark_sum(
                        when(col("building_damage_amount") < 0, 1)
                        .otherwise(0)
                    ), 
                    lit(0)
                ).alias("negative_building_damage"),

                coalesce(
                    spark_sum(
                        when(col("water_depth").isNotNull() & (col("water_depth") < 0), 1)
                        .otherwise(0)
                    ),
                    lit(0)
                ).alias("invalid_water_depth"),
                
                coalesce(
                    spark_sum(
                        when(col("latitude").isNotNull() & ((col("latitude") < -90) | (col("latitude") > 90)), 1)
                        .otherwise(0)
                    ),
                    lit(0)
                ).alias("invalid_latitude"),

                coalesce(
                    spark_sum(
                        when(col("longitude").isNotNull() & ((col("longitude") < -180) | (col("longitude") > 180)), 1)
                        .otherwise(0)
                    ),
                    lit(0)
                ).alias("invalid_longitude"),

                coalesce(
                    spark_sum(
                        when(col("year_of_loss").isNotNull() & ((col("year_of_loss") < 1970) | (col("year_of_loss") > 2100)), 1)
                        .otherwise(0)
                    ),
                    lit(0)
                ).alias("invalid_year_of_loss")
            )
            .collect()[0]
        )

        silver_violations = {
            "claim_id_null": cleaned_dq["claim_id_null"],
            "date_of_loss_null": cleaned_dq["date_of_loss_null"],
            "year_of_loss_null": cleaned_dq["year_of_loss_null"],
            "negative_building_damage": cleaned_dq["negative_building_damage"],
            "invalid_water_depth": cleaned_dq["invalid_water_depth"],
            "invalid_latitude": cleaned_dq["invalid_latitude"],
            "invalid_longitude": cleaned_dq["invalid_longitude"],
            "invalid_year_of_loss": cleaned_dq["invalid_year_of_loss"]
        }

        # --------------------------------------------------
        # 7. Check violations
        # --------------------------------------------------
        logging.info("Silver Data Quality results:")
        
        for rule, count in silver_violations.items():
            logging.info(f"{rule} = {count}") 
        
        failed_rules = {
            rule: count
            for rule, count in silver_violations.items()
            if count > 0
        }

        if failed_rules:
            logging.error(f"Data Quality FAILED: {failed_rules}")
            raise Exception("Data Quality check failed.")

        # --------------------------------------------------
        # 8. MERGE into Silver
        # --------------------------------------------------
        logging.info("Merging data into Silver...")

        silver_delta = DeltaTable.forName(spark, cfg.SILVER_TABLE)

        (
            silver_delta.alias("target")
            .merge(
                cleaned_df.alias("source"), 
                "target.claim_id = source.claim_id"
            )
            .whenMatchedUpdateAll(
                condition= "source.source_updated_at > target.source_updated_at"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        # Get MERGE metrics
        merge_metrics = (
            silver_delta
            .history(1)
            .select("operationMetrics")
            .collect()[0]["operationMetrics"]
        )

        rows_inserted = int(
            merge_metrics.get("numTargetRowsInserted", 0)
        )

        rows_updated = int(
            merge_metrics.get("numTargetRowsUpdated", 0)
        )

        rows_written = rows_inserted + rows_updated

        logging.info(f"Silver MERGE completed: "
            f"inserted={rows_inserted}, "
            f"updated={rows_updated}, "
            f"rows_written={rows_written}"
        )

        # --------------------------------------------------
        # 9. Update rows_written in RUN_TABLE
        # --------------------------------------------------
        logging.info("Updating rows_written in RUN_TABLE...")

        rows_written_df = (
            spark.createDataFrame(
                [
                    (
                        run_id,
                        cfg.PIPELINE_NAME,
                        rows_written
                    )
                ],
                """
                run_id STRING,
                pipeline_name STRING,
                rows_written BIGINT
                """
            )
        )

        pipeline_run_delta = DeltaTable.forName(spark, cfg.RUN_TABLE)

        (
            pipeline_run_delta.alias("target")
            .merge(
                rows_written_df.alias("source"),
                """
                target.run_id = source.run_id AND 
                target.pipeline_name = source.pipeline_name
                """
            )
            .whenMatchedUpdate(
                set={
                    "rows_written": "source.rows_written"
                }
            )
            .execute()
        )
        
        logging.info(f"Successfully updated rows_written = {rows_written}")

    except Exception as e:
        logging.error( f"Unexpected error occurred!: {e}")
        raise

    finally:
        spark.stop()
        logging.info("Spark session stopped.")


if __name__ == "__main__":
    merge_to_silver()