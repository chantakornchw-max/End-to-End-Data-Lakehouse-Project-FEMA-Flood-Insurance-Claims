# ==========================================
# VPC 
# ==========================================
resource "aws_vpc" "databricks_vpc" {
  cidr_block           = "10.0.0.0/16" 
  enable_dns_hostnames = true          
  enable_dns_support   = true          

  tags = {
    Name = "fema-databricks-vpc"
  }
}


# ==========================================
# Internet Gateway
# ==========================================
resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.databricks_vpc.id

  tags = {
    Name = "fema-databricks-igw"
  }
}


# ==========================================
# Subnets
# ==========================================
data "aws_availability_zones" "available" {} 

resource "aws_subnet" "public_subnet_1" {
  vpc_id                  = aws_vpc.databricks_vpc.id
  cidr_block              = "10.0.1.0/24" 
  availability_zone       = data.aws_availability_zones.available.names[0] 
  map_public_ip_on_launch = true 

  tags = {
    Name = "fema-databricks-public-subnet-1"
  }
}

resource "aws_subnet" "public_subnet_2" {
  vpc_id                  = aws_vpc.databricks_vpc.id
  cidr_block              = "10.0.2.0/24" 
  availability_zone       = data.aws_availability_zones.available.names[1] 
  map_public_ip_on_launch = true

  tags = {
    Name = "fema-databricks-public-subnet-2"
  }
}


# ==========================================
# Route Table
# ==========================================
resource "aws_route_table" "public_rt" {
  vpc_id = aws_vpc.databricks_vpc.id

  route {
    cidr_block = "0.0.0.0/0" 
    gateway_id = aws_internet_gateway.igw.id 
  }

  tags = {
    Name = "fema-databricks-public-rt"
  }
}

resource "aws_route_table_association" "public_assoc_1" {
  subnet_id      = aws_subnet.public_subnet_1.id
  route_table_id = aws_route_table.public_rt.id
}

resource "aws_route_table_association" "public_assoc_2" {
  subnet_id      = aws_subnet.public_subnet_2.id
  route_table_id = aws_route_table.public_rt.id
}


# ==========================================
# Security Group
# ==========================================
resource "aws_security_group" "databricks_sg" {
  name        = "fema-databricks-cluster-sg"
  description = "Security Group for Databricks Clusters"
  vpc_id      = aws_vpc.databricks_vpc.id

  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "fema-databricks-cluster-sg"
  }
}


# ==========================================
# DB Subnet Group (Bridge for RDS)
# ==========================================
resource "aws_db_subnet_group" "rds_subnet_group" {
  name       = "fema-rds-subnet-group"
  subnet_ids = [aws_subnet.public_subnet_1.id, aws_subnet.public_subnet_2.id]

  tags = {
    Name = "FEMA RDS Subnet Group"
  }
}