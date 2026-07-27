resource "databricks_secret_scope" "fema_scope" {
  name = "fema_scope"
}

resource "databricks_secret" "rds_endpoint" {
  key          = "rds_endpoint"
  string_value = aws_db_instance.fema_postgres_db.endpoint 
  scope        = databricks_secret_scope.fema_scope.name
}

resource "databricks_secret" "rds_username" {
  key          = "rds_username"
  string_value = var.rds_username
  scope        = databricks_secret_scope.fema_scope.name
}

resource "databricks_secret" "rds_password" {
  key          = "rds_password"
  string_value = var.rds_password
  scope        = databricks_secret_scope.fema_scope.name
}