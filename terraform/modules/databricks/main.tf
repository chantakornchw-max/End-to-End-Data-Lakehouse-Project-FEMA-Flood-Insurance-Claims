terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

# ==========================================
# External Location
# ==========================================
resource "databricks_external_location" "s3" {
  name            = var.external_location_name
  url             = "s3://${var.s3_bucket_name}/"
  credential_name = var.storage_credential_name

  skip_validation = var.skip_validation
}

# ==========================================
# Unity Catalog
# ==========================================
resource "databricks_catalog" "fema" {
  name    = var.catalog_name
  comment = "Main Catalog for FEMA Data Lakehouse"

  storage_root = "s3://${var.s3_bucket_name}/managed_catalog/"

  depends_on = [
    databricks_external_location.s3
  ]
}

# ==========================================
# Schemas
# ==========================================
resource "databricks_schema" "metadata" {
  catalog_name = databricks_catalog.fema.name
  name         = "metadata"
  comment      = "Stores pipeline execution metadata, including run history and incremental-load watermarks."
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.fema.name
  name         = "bronze"
  comment      = "Raw data layer ingested from RDS"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.fema.name
  name         = "silver"
  comment      = "Cleaned and validated data layer"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.fema.name
  name         = "gold"
  comment      = "Business aggregated metrics and Star Schema"
}