data "aws_caller_identity" "current" {}

# ===============================================
# Create IAM Role for Databricks
# ===============================================
resource "aws_iam_role" "databricks_s3_role" {
  name = "fema-databricks-s3-access-role-pro"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${var.databricks_aws_control_plane_id}:root"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.databricks_account_id
          }
        }
      },
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/fema-databricks-s3-access-role-pro"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}


# ===============================================
# Create AWS S3 Bucket 
# ===============================================
resource "aws_s3_bucket" "fema_data_lake" {
  bucket        = var.s3_bucket
  force_destroy = true
}


resource "aws_s3_bucket_public_access_block" "fema_data_lake_security" {
  bucket = aws_s3_bucket.fema_data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


# ===============================================
# Custom IAM Policy & Attachment 
# ===============================================
resource "aws_iam_policy" "databricks_s3_policy" {
  name        = "fema-databricks-s3-policy-pro"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]

        Resource = [
          aws_s3_bucket.fema_data_lake.arn,
          "${aws_s3_bucket.fema_data_lake.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_s3_access" {
  role       = aws_iam_role.databricks_s3_role.name
  policy_arn = aws_iam_policy.databricks_s3_policy.arn
}