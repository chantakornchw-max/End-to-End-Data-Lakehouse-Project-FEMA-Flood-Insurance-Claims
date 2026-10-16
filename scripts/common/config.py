CATALOG_NAME = "fema_dev"

PIPELINE_NAME = "fema_claims"
SOURCE_TABLE = "public.raw_fema_claims"
DATABASE_NAME = "fema_flood_db"

INITIAL_START_WATERMARK = "1950-01-01 00:00:00"

RUN_TABLE = f"{CATALOG_NAME}.metadata.pipeline_run"
WATERMARK_TABLE = f"{CATALOG_NAME}.metadata.pipeline_watermark"

BRONZE_TABLE = f"{CATALOG_NAME}.bronze.raw_fema_claims"
SILVER_TABLE = f"{CATALOG_NAME}.silver.cleaned_fema_claims"

GOLD_FACT_TABLE = f"{CATALOG_NAME}.gold.fact_fema_claims"
DIM_OCCUPANCY_TABLE = f"{CATALOG_NAME}.gold.dim_occupancy_type"
DIM_PRIMARY_RESIDENCE_TABLE = f"{CATALOG_NAME}.gold.dim_primary_residence"
DIM_BUILDING_DEDUCTIBLE_TABLE = f"{CATALOG_NAME}.gold.dim_building_deductible"
DIM_CAUSE_OF_DAMAGE_TABLE = f"{CATALOG_NAME}.gold.dim_cause_of_damage"
DIM_NON_PAYMENT_REASON_TABLE = f"{CATALOG_NAME}.gold.dim_non_payment_reason"
DIM_IS_UNDERINSURED_TABLE = f"{CATALOG_NAME}.gold.dim_is_underinsured"