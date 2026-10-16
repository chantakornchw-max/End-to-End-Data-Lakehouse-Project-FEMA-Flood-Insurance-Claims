# ==========================================
# Provider Requirements
# ==========================================
terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }

    databricks = {
      source = "databricks/databricks"
    }
  }
}


# ==========================================
# AWS Account Information
# ==========================================
data "aws_caller_identity" "current" {}


# ==========================================
# Databricks IAM Trust Policy
# ==========================================
data "databricks_aws_unity_catalog_assume_role_policy" "this" {
  aws_account_id = data.aws_caller_identity.current.account_id
  role_name      = var.role_name

  external_id = var.storage_credential_external_id
}


# ==========================================
# Databricks S3 Access Policy
# ==========================================
data "databricks_aws_unity_catalog_policy" "this" {
  aws_account_id = data.aws_caller_identity.current.account_id
  bucket_name    = var.s3_bucket_name
  role_name      = var.role_name
}


# ==========================================
# IAM Role
# ==========================================
resource "aws_iam_role" "databricks_s3_role" {
  name               = var.role_name
  assume_role_policy = data.databricks_aws_unity_catalog_assume_role_policy.this.json
}


# ==========================================
# IAM Policy
# ==========================================
resource "aws_iam_policy" "databricks_s3_policy" {
  name   = var.policy_name
  policy = data.databricks_aws_unity_catalog_policy.this.json
}


# ==========================================
# Attach Policy to Role
# ==========================================
resource "aws_iam_role_policy_attachment" "attach_s3_access" {
  role       = aws_iam_role.databricks_s3_role.name
  policy_arn = aws_iam_policy.databricks_s3_policy.arn
}