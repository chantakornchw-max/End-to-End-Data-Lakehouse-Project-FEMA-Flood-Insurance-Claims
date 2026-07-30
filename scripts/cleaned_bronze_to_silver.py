import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def cleaned_bronze_to_silver():

    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    spark = (SparkSession.builder
        .appName("cleaned_bronze_to_silver")
        .getOrCreate()
    )

    bucket_name = sys.argv[1]
    bronze_input_path = f"s3://{bucket_name}/bronze/raw_data/"
    silver_output_path = f"s3://{bucket_name}/silver/cleaned_data/" 

    try:
        logging.info("Starting clean process...")

        cleaned_df = (
            spark.read.format("delta").load(bronze_input_path)

            # Select columns, Change type, and Rename
            .select(
                col("id").cast(StringType()).alias("claim_id"),
                col("floodEvent").cast(StringType()).alias("flood_event"),
                
                col("dateOfLoss").cast(TimestampType()).alias("date_of_loss"),
                col("yearOfLoss").cast(DoubleType()).cast(IntegerType()).alias("year_of_loss"),
                col("originalConstructionDate").cast(TimestampType()).alias("original_construction_date"),
                
                col("state").cast(StringType()).alias("state"),
                lpad(split(col("reportedZipCode").cast(StringType()), r"[\.\-]")[0], 5, "0").alias("reported_zip_code"), 
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
                col("causeOfDamage").cast(StringType()).alias("cause_of_damage"),
                when(col("nonPaymentReasonBuilding").cast(StringType()).rlike(r"^\d+(\.0)?$"), 
                    lpad(split(col("nonPaymentReasonBuilding").cast(StringType()), r"\.")[0], 2, "0")
                )
                .otherwise(col("nonPaymentReasonBuilding").cast(StringType()))
                .alias("non_payment_reason_building")
            )

            # Handling NULL values
            .dropna(subset=['claim_id', 'date_of_loss'])
            .withColumn("year_of_loss", year(col("date_of_loss")))
            .fillna("Unknown", subset=[
                "flood_event", 
                "state", 
                "reported_zip_code",
                "rated_flood_zone", 
                "building_deductible_code", 
                "cause_of_damage"
            ])
            .fillna("Not Applicable", subset=["non_payment_reason_building"])
            .fillna(0.0, subset=[
                "building_property_value",
                "total_building_insurance_coverage",
                "building_damage_amount",
                "building_replacement_cost",
                "net_building_payment_amount",
                "total_contents_insurance_coverage",
                "contents_damage_amount",
                "net_contents_payment_amount"
            ])

            # Handling negative values
            .withColumn("building_property_value", abs(col("building_property_value")))
            .withColumn("total_building_insurance_coverage", abs(col("total_building_insurance_coverage")))
            .withColumn("building_damage_amount", abs(col("building_damage_amount")))
            .withColumn("building_replacement_cost", abs(col("building_replacement_cost")))
            .withColumn("net_building_payment_amount", abs(col("net_building_payment_amount")))
            .withColumn("total_contents_insurance_coverage", abs(col("total_contents_insurance_coverage")))
            .withColumn("contents_damage_amount", abs(col("contents_damage_amount")))
            .withColumn("net_contents_payment_amount", abs(col("net_contents_payment_amount")))
            .withColumn("water_depth", abs(col("water_depth")))

            # Handling duplicate and and timestamps
            .dropDuplicates(["claim_id"])
            .withColumn("processed_at", current_timestamp())
        )

        (cleaned_df
            .repartition("year_of_loss")
            .write
            .format("delta")
            .partitionBy("year_of_loss")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("path", silver_output_path) 
            .saveAsTable("fema_prod.silver.cleaned_fema_claims") 
        )
        logging.info(f"Successfully cleaned and uploaded data to {silver_output_path}")

    except Exception as e:
        logging.error(f"Unexpected error occurred!: {e}")
        raise e

    finally:
        spark.stop()


if __name__ == "__main__":
    cleaned_bronze_to_silver()