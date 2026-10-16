variable "credential_name" {
  description = "Name of the Databricks storage credential"
  type        = string
}

variable "role_arn" {
  description = "Precomputed AWS IAM role ARN for Unity Catalog"
  type        = string
}