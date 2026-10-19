# ==========================================
# Provider Requirements
# ==========================================
terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}


# ==========================================
# Databricks Storage Credential
# ==========================================
resource "databricks_storage_credential" "s3" {
  name = var.credential_name

  aws_iam_role {
    role_arn = var.role_arn
  }

  skip_validation = true
  comment = "Storage credential for FEMA Data Lakehouse"
}