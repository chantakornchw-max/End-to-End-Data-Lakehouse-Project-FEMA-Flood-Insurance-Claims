output "role_arn" {
  description = "ARN of the Databricks IAM role"
  value       = aws_iam_role.databricks_s3_role.arn
}

output "role_name" {
  description = "Name of the Databricks IAM role"
  value       = aws_iam_role.databricks_s3_role.name
}

output "policy_arn" {
  description = "ARN of the S3 access policy"
  value       = aws_iam_policy.databricks_s3_policy.arn
}