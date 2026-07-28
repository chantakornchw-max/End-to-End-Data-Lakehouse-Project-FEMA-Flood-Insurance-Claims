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