# ==========================================
# General
# ==========================================
variable "name_prefix" {
  description = "Prefix used for naming RDS resources"
  type        = string
}


# ==========================================
# Network
# ==========================================
variable "vpc_id" {
  description = "VPC ID where RDS will be created"
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for the RDS DB subnet group"
  type        = list(string)
}

variable "ingress_cidr_blocks" {
  description = "CIDR blocks allowed to connect to PostgreSQL"
  type        = list(string)
}


# ==========================================
# PostgreSQL
# ==========================================
variable "engine_version" {
  description = "PostgreSQL engine version"
  type        = string
  default     = "15"
}

variable "instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.micro"
}

variable "allocated_storage" {
  description = "Allocated storage in GB"
  type        = number
  default     = 20
}

variable "db_name" {
  description = "Database name"
  type        = string
}

variable "username" {
  description = "Database username"
  type        = string
}

variable "password" {
  description = "Database password"
  type        = string
  sensitive   = true
}

variable "publicly_accessible" {
  description = "Whether the RDS instance is publicly accessible"
  type        = bool
}

variable "skip_final_snapshot" {
  description = "Whether to skip the final snapshot when destroying RDS"
  type        = bool
  default     = true
}