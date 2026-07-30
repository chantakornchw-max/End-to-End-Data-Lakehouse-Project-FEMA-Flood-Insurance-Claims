# ===============================================
# Create Databricks Storage Credential
# ===============================================
resource "databricks_storage_credential" "fema_s3_cred_pro" {
  name = "fema_s3_cred_pro"

  aws_iam_role {
    role_arn = aws_iam_role.databricks_s3_role.arn 
  }

  depends_on = [
    aws_iam_role_policy_attachment.attach_s3_access
  ]
}


# ===============================================
# Create Databricks External Location
# ===============================================
resource "databricks_external_location" "fema_s3_location" {
  name = "fema_s3_bucket"
  url  = "s3://${aws_s3_bucket.fema_data_lake.id}/" 
  credential_name = databricks_storage_credential.fema_s3_cred_pro.id
  skip_validation = true 
}


# ===============================================
# Create Catalog and Schemas
# ===============================================
resource "databricks_catalog" "fema_catalog" {
  name    = "fema_prod"
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.fema_catalog.name
  name         = "bronze"
  comment      = "Raw data layer ingested from RDS"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.fema_catalog.name
  name         = "silver"
  comment      = "Cleaned and validated data layer"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.fema_catalog.name
  name         = "gold"
  comment      = "Business aggregated metrics and Star Schema"
}