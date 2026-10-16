import sys
import logging
import scripts.common.config as cfg
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, lit, when, abs, round, sum as spark_sum
from pyspark.sql.types import IntegerType


# --------------------------------------------------
# Logging Configuration
# --------------------------------------------------
logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s'
)


def create_fact_table():

    # --------------------------------------------------
    # 1. Get argv
    # --------------------------------------------------
    if len(sys.argv) < 3:
        raise ValueError("run_id and bucket_name arguments are required.")

    run_id = sys.argv[1]
    bucket_name = sys.argv[2]
    gold_output_path = f"s3://{bucket_name}/gold/fact/"

    spark = (
        SparkSession.builder
        .appName("create_fact_table")
        .getOrCreate()
    )

    try:
        logging.info(f"Starting Create Fact Table (Initial Load) for run_id = {run_id}.")

        # --------------------------------------------------
        # 2. Read data from Silver
        # --------------------------------------------------
        logging.info("Reading data from Silver...")

        silver_df = spark.table(cfg.SILVER_TABLE)
        
        # --------------------------------------------------
        # 3. Create derived columns   
        # --------------------------------------------------
        logging.info("Creating derived columns ...")

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
        # 4. Define DQ rules
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
        # 5. Run Data Quality checks
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
        # 6. Check violations
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
        # 7. Sort columns
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
        # 8. Write fact data to Gold
        # --------------------------------------------------
        (fact_df
            .repartition("year_of_loss")
            .write
            .format("delta") 
            .partitionBy("year_of_loss")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("path", gold_output_path) 
            .saveAsTable(cfg.GOLD_FACT_TABLE) 
        )

        logging.info(f"Successfully overwritten Fact table '{cfg.GOLD_FACT_TABLE}' at {gold_output_path}")

    except Exception as e:
        logging.error(f"Unexpected error occurred!: {e}")
        raise 

    finally:
        spark.stop()
        logging.info("Spark session stopped.")


if __name__ == "__main__":
    create_fact_table()