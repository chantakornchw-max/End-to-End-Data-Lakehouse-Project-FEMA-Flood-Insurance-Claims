resource "databricks_secret_scope" "fema_scope" {
  name = "${var.catalog_name}_scope"
}

resource "databricks_secret" "rds_endpoint" {
  key          = "rds_endpoint"
  string_value = var.rds_endpoint
  scope        = databricks_secret_scope.fema_scope.name
}

resource "databricks_secret" "rds_username" {
  key          = "rds_username"
  string_value = var.rds_username
  scope        = databricks_secret_scope.fema_scope.name
}

resource "databricks_secret" "rds_password" {
  key                     = "rds_password"
  string_value_wo         = var.rds_password
  string_value_wo_version = 1
  scope                   = databricks_secret_scope.fema_scope.name
}