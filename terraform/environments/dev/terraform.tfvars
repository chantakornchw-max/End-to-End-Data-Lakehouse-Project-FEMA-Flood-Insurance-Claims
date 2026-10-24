# ==========================================
# AWS
# ==========================================
aws_region       = "ap-southeast-1"
s3_bucket_name   = "fema-flood-claims-bucket-2026-dev"
force_destroy_s3 = false


# ==========================================
# VPC
# ==========================================
vpc_cidr             = "10.10.0.0/16"
public_subnet_1_cidr = "10.10.1.0/24"
public_subnet_2_cidr = "10.10.2.0/24"


# ==========================================
# RDS
# ==========================================
rds_engine_version    = "15"
rds_instance_class    = "db.t3.micro"
rds_allocated_storage = 20
rds_db_name           = "fema_flood_db"
rds_username          = "postgres"

rds_publicly_accessible = true
rds_skip_final_snapshot = true

rds_ingress_cidr_blocks = [
  "13.213.212.4/32",
  "13.214.1.96/28",
  "47.128.12.0/24"
]