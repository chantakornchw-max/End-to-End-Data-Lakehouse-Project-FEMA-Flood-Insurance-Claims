import sys
import logging
import scripts.common.config as cfg
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, lit, when, abs, round, sum as spark_sum
from pyspark.sql.types import IntegerType


# --------------------------------------------------
# Logging Configuration
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def merge_fact_table():

    # --------------------------------------------------
    # 1. Get run_id
    # --------------------------------------------------
    if len(sys.argv) < 2:
        raise ValueError("run_id argument is required.")

    run_id = sys.argv[1]

    spark = (
        SparkSession.builder
        .appName("merge_fact_table")
        .getOrCreate()
    )

    try:
        logging.info(f"Starting Merge Silver to Fact for run_id={run_id}")

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
        # 3. Read incremental data from Silver
        # --------------------------------------------------
        logging.info( "Reading incremental data from Silver...")

        silver_df = (
            spark.table(cfg.SILVER_TABLE)
            .filter(
                (col("source_updated_at") > start_watermark) &
                (col("source_updated_at") <= high_watermark)
            )
        )

        # --------------------------------------------------
        # 4. Check extracted rows
        # --------------------------------------------------
        rows_read = silver_df.count()

        logging.info(f"Incremental Fact rows = {rows_read}")

        if rows_read == 0:
            logging.info("No changed records found for Fact table.")
            return

        # --------------------------------------------------
        # 5. Create derived columns   
        # --------------------------------------------------
        logging.info("Creating derived Fact columns...")

        derived_df = (
            silver_df
            .withColumn("raw_age", col("year_of_loss") - year(col("original_construction_date")))
            .withColumn("building_age_at_loss",
                when( col("raw_age") < 0, lit(None).cast(IntegerType()))
                .otherwise(col("raw_age"))
            )
            .drop("raw_age")

            .withColumn("total_damage_amount", round(col("building_damage_amount") + col("contents_damage_amount"), 2))
            .withColumn("total_net_payment", round(col("net_building_payment_amount") + col("net_contents_payment_amount"), 2))         
            .withColumn("coverage_percentage", 
                when(col("total_damage_amount") > 0, round((col("total_net_payment") / col("total_damage_amount")) * 100, 2))
                .otherwise(0.0)
            )

            .withColumn("is_underinsured", 
                when(col("total_building_insurance_coverage") < col("building_replacement_cost"), 1)
                .otherwise(0))
        )

        # --------------------------------------------------
        # 6. Define DQ rules
        # --------------------------------------------------
        
        invalid_building_age = (
            (
                (col("original_construction_date").isNotNull()) &
                (col("year_of_loss").isNotNull()) &
                (year(col("original_construction_date")) <= col("year_of_loss")) &
                (
                    col("building_age_at_loss").isNull() |
                    (
                        col("building_age_at_loss") !=
                        (col("year_of_loss") - year(col("original_construction_date")))
                    )
                )
            )
            |
            (
                (col("original_construction_date").isNotNull()) &
                (col("year_of_loss").isNotNull()) &
                (year(col("original_construction_date")) > col("year_of_loss")) &
                col("building_age_at_loss").isNotNull()
            )
        )

        invalid_total_damage = (
            abs(
                col("total_damage_amount") - round(col("building_damage_amount") + col("contents_damage_amount"), 2)
            ) > 0.01
        )

        invalid_total_payment = (
            abs(
                col("total_net_payment") - round(col("net_building_payment_amount") + col("net_contents_payment_amount"), 2)
            ) > 0.01
        )

        invalid_coverage_percentage = (
            (
                (col("total_damage_amount") > 0) &
                (abs(
                        col("coverage_percentage") -
                        round((col("total_net_payment") / col("total_damage_amount")) * 100, 2)
                    ) > 0.01
                )
            )
            |
            (
                (col("total_damage_amount") <= 0) &
                (col("coverage_percentage") != 0)
            )
        )

        invalid_underinsured_flag = (
            (
                (col("is_underinsured") == 1) & (col("total_building_insurance_coverage") >= col("building_replacement_cost"))
            )
            |
            (
                (col("is_underinsured") == 0) &  (col("total_building_insurance_coverage") < col("building_replacement_cost"))
            )
        )

        # --------------------------------------------------
        # 7. Run Data Quality checks
        # --------------------------------------------------
        logging.info("Checking Fact data quality...")

        fact_dq = (
            derived_df
            .agg(
                spark_sum(
                    when(invalid_building_age, 1).otherwise(0)
                ).alias("invalid_building_age"),

                spark_sum(
                    when(invalid_total_damage, 1).otherwise(0)
                ).alias("invalid_total_damage"),

                spark_sum(
                    when(invalid_total_payment, 1).otherwise(0)
                ).alias("invalid_total_payment"),

                spark_sum(
                    when(invalid_coverage_percentage, 1).otherwise(0)
                ).alias("invalid_coverage_percentage"),

                spark_sum(
                    when(invalid_underinsured_flag, 1).otherwise(0)
                ).alias("invalid_underinsured_flag")
            )
            .collect()[0]
        )

        fact_violations = {
            "invalid_building_age": fact_dq["invalid_building_age"],
            "invalid_total_damage": fact_dq["invalid_total_damage"],
            "invalid_total_payment": fact_dq["invalid_total_payment"],
            "invalid_coverage_percentage": fact_dq["invalid_coverage_percentage"],
            "invalid_underinsured_flag": fact_dq["invalid_underinsured_flag"]
        }

        # --------------------------------------------------
        # 8. Check violations
        # --------------------------------------------------
        logging.info("Fact Data Quality results:")

        for rule, count in fact_violations.items():
            logging.info(f"{rule} = {count}") 
                
        failed_rules = {
            rule: count
            for rule, count in fact_violations.items()
            if count > 0
        }

        if failed_rules:
            logging.error(f"Data Quality FAILED: {failed_rules}")
            raise Exception("Data Quality check failed.")

        # --------------------------------------------------
        # 9. Select Fact columns
        # --------------------------------------------------
        fact_df = (
            derived_df
            .select(
                col("claim_id"),
                col("flood_event"),
                col("date_of_loss"),
                col("year_of_loss"),
                col("building_age_at_loss"),
                col("state"),
                col("reported_zip_code"),
                col("latitude"),
                col("longitude"),
                col("rated_flood_zone"),
                col("occupancy_type"),
                col("primary_residence_indicator"),
                col("elevation_difference"),
                col("water_depth"),
                col("building_property_value"),
                col("total_building_insurance_coverage"),
                col("building_damage_amount"),
                col("building_replacement_cost"),
                col("net_building_payment_amount"),
                col("total_contents_insurance_coverage"),
                col("contents_damage_amount"),
                col("net_contents_payment_amount"),
                col("building_deductible_code"),
                col("cause_of_damage"),
                col("non_payment_reason_building"),
                col("total_damage_amount"),
                col("total_net_payment"),
                col("coverage_percentage"),
                col("is_underinsured")
            )
        )

        # --------------------------------------------------
        # 10. MERGE into Fact
        # --------------------------------------------------
        logging.info("Merging incremental data into Fact table...")

        fact_delta = DeltaTable.forName(spark, cfg.FACT_TABLE)

        (
            fact_delta.alias("target")
            .merge(
                fact_df.alias("source"), "target.claim_id = source.claim_id"
            )

            .whenMatchedUpdate(
                set={
                    "flood_event": "source.flood_event",
                    "date_of_loss": "source.date_of_loss",
                    "year_of_loss": "source.year_of_loss",
                    "building_age_at_loss": "source.building_age_at_loss",
                    "state": "source.state",
                    "reported_zip_code": "source.reported_zip_code",
                    "latitude": "source.latitude",
                    "longitude": "source.longitude",
                    "rated_flood_zone": "source.rated_flood_zone",
                    "occupancy_type": "source.occupancy_type",
                    "primary_residence_indicator": "source.primary_residence_indicator",
                    "elevation_difference": "source.elevation_difference",
                    "water_depth": "source.water_depth",
                    "building_property_value": "source.building_property_value",
                    "total_building_insurance_coverage": "source.total_building_insurance_coverage",
                    "building_damage_amount": "source.building_damage_amount",
                    "building_replacement_cost": "source.building_replacement_cost",
                    "net_building_payment_amount": "source.net_building_payment_amount",
                    "total_contents_insurance_coverage": "source.total_contents_insurance_coverage",
                    "contents_damage_amount": "source.contents_damage_amount",
                    "net_contents_payment_amount": "source.net_contents_payment_amount",
                    "building_deductible_code": "source.building_deductible_code",
                    "cause_of_damage": "source.cause_of_damage",
                    "non_payment_reason_building": "source.non_payment_reason_building",
                    "total_damage_amount": "source.total_damage_amount",
                    "total_net_payment": "source.total_net_payment",
                    "coverage_percentage": "source.coverage_percentage",
                    "is_underinsured": "source.is_underinsured"
                }
            )

            .whenNotMatchedInsert(
                values={
                    "claim_id": "source.claim_id",
                    "flood_event": "source.flood_event",
                    "date_of_loss": "source.date_of_loss",
                    "year_of_loss": "source.year_of_loss",
                    "building_age_at_loss": "source.building_age_at_loss",
                    "state": "source.state",
                    "reported_zip_code": "source.reported_zip_code",
                    "latitude": "source.latitude",
                    "longitude": "source.longitude",
                    "rated_flood_zone": "source.rated_flood_zone",
                    "occupancy_type": "source.occupancy_type",
                    "primary_residence_indicator": "source.primary_residence_indicator",
                    "elevation_difference": "source.elevation_difference",
                    "water_depth": "source.water_depth",
                    "building_property_value": "source.building_property_value",
                    "total_building_insurance_coverage": "source.total_building_insurance_coverage",
                    "building_damage_amount": "source.building_damage_amount",
                    "building_replacement_cost": "source.building_replacement_cost",
                    "net_building_payment_amount": "source.net_building_payment_amount",
                    "total_contents_insurance_coverage": "source.total_contents_insurance_coverage",
                    "contents_damage_amount": "source.contents_damage_amount",
                    "net_contents_payment_amount": "source.net_contents_payment_amount",
                    "building_deductible_code": "source.building_deductible_code",
                    "cause_of_damage": "source.cause_of_damage",
                    "non_payment_reason_building": "source.non_payment_reason_building",
                    "total_damage_amount": "source.total_damage_amount",
                    "total_net_payment": "source.total_net_payment",
                    "coverage_percentage": "source.coverage_percentage",
                    "is_underinsured": "source.is_underinsured"
                }
            )
            .execute()
        )

        logging.info(f"Successfully merged {rows_read} rows into Fact table.")

    except Exception as e:
        logging.error(f"Unexpected error occurred!: {e}")
        raise

    finally:
        spark.stop()
        logging.info("Spark session stopped.")


if __name__ == "__main__":
    merge_fact_table()