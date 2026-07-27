# ==========================================
# AWS Variables
# ==========================================
variable "aws_access_key" {
  type        = string
  description = "AWS Access Key"
  sensitive   = true 
}

variable "aws_secret_access_key" {
  type        = string
  description = "AWS Secret Access Key"
  sensitive   = true 
}

variable "aws_region" {
  type        = string
  description = "AWS Region"
  default     = "ap-southeast-1" 
}

variable "s3_bucket" {
  type        = string
  description = "S3 Bucket Name"
  default     = "fema-flood-claims-bucket-2026-pro" 
}


# ==========================================
# RDS Variables
# ==========================================
variable "rds_username" {
  type        = string
  description = "RDS Username"
  default     = "postgres"
}

variable "rds_password" {
  type        = string
  description = "RDS Password"
  sensitive   = true 
}


# ==========================================
# Databricks Variables
# ==========================================
variable "databricks_account_id" {
  description = "Databricks Account UUID (External ID)"
  type        = string
  sensitive   = true
}

variable "databricks_aws_control_plane_id" {
  description = "AWS Account ID of Databricks Control Plane"
  type        = string
  sensitive   = true
}