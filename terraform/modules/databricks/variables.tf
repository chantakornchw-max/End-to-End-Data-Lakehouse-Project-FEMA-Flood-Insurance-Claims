# ==========================================
# S3
# ==========================================
variable "s3_bucket_name" {
  description = "S3 bucket used by FEMA Data Lakehouse"
  type        = string
}

# ==========================================
# Storage Credential
# ==========================================
variable "storage_credential_name" {
  description = "Name of the existing Databricks storage credential"
  type        = string
}

# ==========================================
# External Location and Catalog
# ==========================================
variable "external_location_name" {
  description = "Name of the Databricks external location"
  type        = string
}

variable "catalog_name" {
  description = "Name of the Unity Catalog catalog"
  type        = string
}

variable "skip_validation" {
  description = "Whether to skip external location validation"
  type        = bool
  default     = false
}

# ==========================================
# RDS Secrets
# ==========================================
variable "rds_endpoint" {
  description = "RDS endpoint"
  type        = string
}

variable "rds_username" {
  description = "RDS username"
  type        = string
}

variable "rds_password" {
  description = "RDS password"
  type        = string
  sensitive   = true
}