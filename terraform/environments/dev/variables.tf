# ==========================================
# AWS
# ==========================================
variable "aws_region" {
  type    = string
  default = "ap-southeast-1"
}

variable "s3_bucket_name" {
  description = "Globally unique S3 bucket name for DEV"
  type        = string
}

variable "force_destroy_s3" {
  description = "Allow destroying a non-empty S3 bucket"
  type        = bool
  default     = true
}


# ==========================================
# VPC
# ==========================================
variable "vpc_cidr" {
  type = string
}

variable "public_subnet_1_cidr" {
  type = string
}

variable "public_subnet_2_cidr" {
  type = string
}


# ==========================================
# RDS
# ==========================================
variable "rds_engine_version" {
  type    = string
  default = "15"
}

variable "rds_instance_class" {
  type    = string
  default = "db.t3.micro"
}

variable "rds_allocated_storage" {
  type    = number
  default = 20
}

variable "rds_db_name" {
  type    = string
  default = "fema_flood_db"
}

variable "rds_username" {
  type    = string
  default = "postgres"
}

variable "rds_password" {
  type      = string
  sensitive = true
}

variable "rds_ingress_cidr_blocks" {
  description = "Approved IPv4 CIDRs allowed to connect to PostgreSQL"
  type        = list(string)
}

variable "rds_publicly_accessible" {
  description = "Whether the RDS instance has a public endpoint"
  type        = bool
  default     = false
}

variable "rds_skip_final_snapshot" {
  type    = bool
  default = true
}