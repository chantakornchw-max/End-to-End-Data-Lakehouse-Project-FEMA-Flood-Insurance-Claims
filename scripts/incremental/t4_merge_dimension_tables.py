import sys
import logging
import scripts.common.config as cfg
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# --------------------------------------------------
# Logging Configuration
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def merge_dimension(spark, df, table_name, merge_key):

    logging.info(f"Merging dimension table: {table_name}")

    dimension_delta = DeltaTable.forName(spark, table_name)

    (dimension_delta.alias("target")
        .merge(
            df.alias("source"),
            f"target.{merge_key} = source.{merge_key}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    logging.info(f"Successfully merged dimension table: {table_name}")


def merge_dimension_tables():

    # --------------------------------------------------
    # 1. Get run_id
    # --------------------------------------------------
    if len(sys.argv) < 2:
        raise ValueError("run_id argument is required.")

    run_id = sys.argv[1]

    spark = (
        SparkSession.builder
        .appName("merge_dimension_tables")
        .getOrCreate()
    )

    try:
        # --------------------------------------------------
        # 2. Merge Dimension Tables
        # --------------------------------------------------
        logging.info(f"Starting merge Dimension Tables for run_id={run_id}.")

        # ==========================================
        # Dimension 1: dim_occupancy_type
        # ==========================================
        occupancy_data = [
            (1, "Single Family Residence"),
            (2, "2 to 4 Unit Residential Building"),
            (3, "Residential Building with > 4 Units"),
            (4, "Non-Residential Building"),
            (6, "Non-Residential - Business"),
            (11, "Single Family Residential (Excl. Mobile Home)"),
            (12, "Residential Non-Condo (2-4 Units)"),
            (13, "Residential Non-Condo (5+ Units)"),
            (14, "Residential Mobile/Manufactured Home"),
            (15, "Residential Condo Association"),
            (16, "Single Residential Unit within Multi-Unit Building"),
            (17, "Non-Residential Mobile/Manufactured Home"),
            (18, "Non-Residential Building (General)"),
            (19, "Non-Residential Unit within Multi-Unit Building")
        ]

        occupancy_schema = StructType([
            StructField("occupancy_type", IntegerType(), False),
            StructField("occupancy_type_description", StringType(), False)
        ])
                
        dim_occupancy_df = spark.createDataFrame(occupancy_data, occupancy_schema)

        merge_dimension(
            spark,
            dim_occupancy_df,
            cfg.DIM_OCCUPANCY_TABLE,
            "occupancy_type"
        )

        # ==========================================
        # Dimension 2: dim_primary_residence
        # ==========================================
        primary_residence_data = [
            (1, "Yes (Primary Residence)"),
            (0, "No (Not a Primary Residence)")
        ]

        primary_residence_schema = StructType([
            StructField("primary_residence_indicator", IntegerType(), False),
            StructField("primary_residence_description", StringType(), False)
        ])

        dim_primary_residence_df = spark.createDataFrame(primary_residence_data, primary_residence_schema)

        merge_dimension(
            spark,
            dim_primary_residence_df,
            cfg.DIM_PRIMARY_RESIDENCE_TABLE,
            "primary_residence_indicator"
        )

        # ==========================================
        # Dimension 3: dim_building_deductible
        # ==========================================
        deductible_data = [
            ("0", "$500"),
            ("1", "$1,000"),
            ("2", "$2,000"),
            ("3", "$3,000"),
            ("4", "$4,000"),
            ("5", "$5,000"),
            ("9", "$750"),
            ("A", "$10,000"),
            ("B", "$15,000"),
            ("C", "$20,000"),
            ("D", "$25,000"),
            ("E", "$50,000"),
            ("F", "$1,250"),
            ("G", "$1,500"),
            ("H", "$200 (Group Policies Only)")
        ]

        deductible_schema = StructType([
            StructField("building_deductible_code", StringType(), False),
            StructField("building_deductible_description", StringType(), False)
        ])
        
        dim_deductible_df = spark.createDataFrame(deductible_data, deductible_schema)

        merge_dimension(
            spark,
            dim_deductible_df,
            cfg.DIM_BUILDING_DEDUCTIBLE_TABLE,
            "building_deductible_code"
        )

        # ==========================================
        # Dimension 4: dim_cause_of_damage
        # ==========================================
        cause_data = [
            ("0", "Other causes"),
            ("1", "Tidal water overflow"),
            ("2", "Stream, river, or lake overflow"),
            ("3", "Alluvial fan overflow"),
            ("4", "Accumulation of rainfall or snowmelt"),
            ("7", "Erosion-demolition"),
            ("8", "Erosion-removal"),
            ("9", "Earth movement, landslide, subsidence"),
            ("A", "Closed basin lake"),
            ("B", "Expedited (No site inspection)"),
            ("C", "Expedited (Follow-up site inspection)"),
            ("D", "Expedited (Remote Adjustment)")
        ]

        cause_schema = StructType([
            StructField("cause_of_damage", StringType(), False),
            StructField("cause_of_damage_description", StringType(), False)
        ])
        
        dim_cause_df = spark.createDataFrame(cause_data, cause_schema)

        merge_dimension(
            spark,
            dim_cause_df,
            cfg.DIM_CAUSE_OF_DAMAGE_TABLE,
            "cause_of_damage"
        )

        # ==========================================
        # Dimension 5: dim_non_payment_reason
        # ==========================================
        non_payment_data = [
            ("01", "Claim denied (less than deductible)"),
            ("02", "Seepage"),
            ("03", "Backup drains"),
            ("04", "Shrubs not covered"),
            ("05", "Sea wall"),
            ("06", "Not actual flood"),
            ("07", "Loss in progress"),
            ("08", "Failure to pursue claim"),
            ("09", "Debris removal only"),
            ("10", "Fire"),
            ("11", "Fence damage"),
            ("12", "Hydrostatic pressure"),
            ("13", "Drainage clogged"),
            ("14", "Boat piers"),
            ("15", "Not insured (damage before policy)"),
            ("16", "Not insured (wind damage)"),
            ("17", "Erosion type not in flood definition"),
            ("18", "Landslide"),
            ("19", "Mudflow type not in flood definition"),
            ("20", "No demonstrable damage"),
            ("97", "Other"),
            ("98", "Error-delete claim"),
            ("99", "Erroneous assignment"),
            ("Not Applicable", "Paid or N/A")
        ]

        non_payment_schema = StructType([
            StructField("non_payment_reason_building", StringType(), False),
            StructField("non_payment_reason_description", StringType(), False)
        ])
        
        dim_non_payment_df = spark.createDataFrame(non_payment_data, non_payment_schema)

        merge_dimension(
            spark,
            dim_non_payment_df,
            cfg.DIM_NON_PAYMENT_REASON_TABLE,
            "non_payment_reason_building"
        )

        # ==========================================
        # Dimension 6: dim_is_underinsured
        # ==========================================
        underinsured_data = [
            (0, "Adequately Insured"),
            (1, "Underinsured")
        ]

        underinsured_schema = StructType([
                    StructField("is_underinsured", IntegerType(), False),
                    StructField("insurance_coverage_status", StringType(), False)
                ])
        
        dim_underinsured_df = spark.createDataFrame(underinsured_data, underinsured_schema)

        merge_dimension(
            spark,
            dim_underinsured_df,
            cfg.DIM_IS_UNDERINSURED_TABLE,
            "is_underinsured"
        )

        logging.info("All Dimension Tables merged successfully.")

    except Exception as e:
        logging.error(f"Unexpected error occurred!: {e}")
        raise

    finally:
        spark.stop()
        logging.info("Spark session stopped.")


if __name__ == "__main__":
    merge_dimension_tables()