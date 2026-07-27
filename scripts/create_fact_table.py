import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def create_fact_table():

    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    spark = (SparkSession.builder
        .appName("silver_to_gold_fact")
        .getOrCreate()
    )

    bucket_name = sys.argv[1]
    silver_input_path = f"s3://{bucket_name}/silver/cleaned_data/fact/"
    gold_output_path = f"s3://{bucket_name}/gold/fact/"

    try:
        logging.info("Starting load Fact Table from Silver to Gold...")

        df_silver = spark.read.parquet(silver_input_path)

        enriched_df = (df_silver
            .withColumn("building_age_at_loss", col("year_of_loss") - year(col("original_construction_date")))
            .withColumn("total_damage_amount", round(col("building_damage_amount") + col("contents_damage_amount"), 2))
            .withColumn("total_net_payment", round(col("net_building_payment_amount") + col("net_contents_payment_amount"), 2))         
            .withColumn("coverage_percentage", when(col("total_damage_amount") > 0, round((col("total_net_payment") / col("total_damage_amount")) * 100, 2)).otherwise(0.0))
            .withColumn("is_underinsured", when(col("total_building_insurance_coverage") < col("building_replacement_cost"), 1).otherwise(0))
        )

        gold_df = (enriched_df
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

        (gold_df
            .write
            .format("delta") 
            .partitionBy("year_of_loss")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("path", gold_output_path) 
            .saveAsTable("default.gold_fact_fema_claims") 
        )

        logging.info(f"Successfully loaded Fact table to {gold_output_path}")

    except Exception as e:
        logging.error(f"Unexpected error occurred!: {e}")
        raise e

    finally:
        spark.stop()

if __name__ == "__main__":
    create_fact_table()