# ==========================================
# General
# ==========================================
variable "name_prefix" {
  description = "Prefix used for naming VPC resources"
  type        = string
}


# ==========================================
# VPC
# ==========================================
variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
}


# ==========================================
# Public Subnets
# ==========================================
variable "public_subnet_1_cidr" {
  description = "CIDR block for public subnet 1"
  type        = string
}

variable "public_subnet_2_cidr" {
  description = "CIDR block for public subnet 2"
  type        = string
}