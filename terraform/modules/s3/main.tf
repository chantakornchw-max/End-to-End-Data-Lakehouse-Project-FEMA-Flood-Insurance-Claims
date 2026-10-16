resource "aws_s3_bucket" "fema_data_lake" {
  bucket        = var.bucket_name
  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_public_access_block" "fema_data_lake_security" {
  bucket                  = aws_s3_bucket.fema_data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}