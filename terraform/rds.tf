# ==========================================
# Create Security Group for RDS
# ==========================================
resource "aws_security_group" "rds_sg" {
  name        = "fema_rds_security_group"
  description = "Allow inbound traffic for PostgreSQL"
  vpc_id      = aws_vpc.databricks_vpc.id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"

    # NOTE: Set to 0.0.0.0/0 for Portfolio/PoC to allow Databricks Serverless access.
    # In enterprise production, this MUST be restricted to specific VPCs or Databricks NAT IPs.
    cidr_blocks = ["0.0.0.0/0"] 
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ==========================================
# PostgreSQL Configurations
# ==========================================
resource "aws_db_instance" "fema_postgres_db" {
  identifier             = "fema-flood-db"           
  engine                 = "postgres"                
  engine_version         = "15"                    
  instance_class         = "db.t3.micro"              
  allocated_storage      = 20                        
  db_name                = "fema_flood_db"            
  username               = var.rds_username           
  password               = var.rds_password          
  vpc_security_group_ids = [aws_security_group.rds_sg.id] 
  db_subnet_group_name   = aws_db_subnet_group.rds_subnet_group.name
  
  # NOTE: Set to true for Databricks Serverless connectivity in this PoC. 
  # Should be false (Private Subnet) in actual production.
  publicly_accessible    = true                      
  skip_final_snapshot    = true                      
}