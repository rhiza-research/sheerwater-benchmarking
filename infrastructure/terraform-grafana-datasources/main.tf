########################################################
# This file is used to configure the shared datasources for the prod and ephemeral grafana instances
# 
# This should only be run in the default workspace. 
# The ephemeral workspaces will import data from the output of this module.
# 
# It is used to create/configure:
# - postgres datasources
# - influx datasources
########################################################

terraform {
  backend "gcs" {
    bucket = "rhiza-terraform-state"
    prefix = "sheerwater-benchmarking-config"
  }

  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.4.0"
    }

    postgresql = {
      source = "cyrilgdn/postgresql"
      version = "1.23.0"
    }
  }
}

provider "google" {
  project = "sheerwater"
}

data "google_secret_manager_secret_version" "postgres_admin_password" {
  secret = "sheerwater-postgres-admin-password"
}

data "google_secret_manager_secret_version" "grafana_admin_password" {
  secret = "sheerwater-grafana-admin-password"
}


locals {
  is_default_workspace = terraform.workspace == "default"
}

output "workspace_warning" {
  value = local.is_default_workspace ? "Currently using the 'default' workspace." : "WARNING: You are currently using the '${terraform.workspace}' workspace, not 'default'."
}

resource "null_resource" "warn_non_default" {
  count = local.is_default_workspace ? 0 : 1 # Only create if not default

  provisioner "local-exec" {
    command = "echo 'WARNING: You are currently using the \"${terraform.workspace}\" workspace, not \"default\". This resource should only be run in the default workspace.' >&2"
  }
}


provider "postgresql" {
  host = "sheerwater-benchmarking-postgres"
  port = 5432
  database = "postgres"
  username = "postgres"
  password = data.google_secret_manager_secret_version.postgres_admin_password.secret_data
  sslmode = "disable"
  connect_timeout = 15
}

# Gcloud secrets for Single sign on
data "google_secret_manager_secret_version" "postgres_read_password" {
  secret = "sheerwater-postgres-read-password"
}

# Gcloud secrets for Single sign on
data "google_secret_manager_secret_version" "tahmo_influx_read_password" {
  secret = "tahmo-influx-read-password"
}

resource "postgresql_role" "read" {
  name = "read"
  password = data.google_secret_manager_secret_version.postgres_read_password.secret_data
  login = true
}

resource "postgresql_grant" "readonly_public" {
  database = "postgres"
  role = postgresql_role.read.name
  schema = "public"
  object_type = "table"
  privileges = ["SELECT"]
}

# Create postgres users and grant them permissions
resource "random_password" "postgres_tahmo_password" {
  length = 16
  special = true
}

resource "google_secret_manager_secret" "postgres_tahmo_password" {
  secret_id = "sheerwater-postgres-tahmo-password"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "postgres_tahmo_password" {
  secret = google_secret_manager_secret.postgres_tahmo_password.id
  secret_data = random_password.postgres_tahmo_password.result
}

resource "postgresql_role" "tahmo" {
  name = "tahmo"
  password = google_secret_manager_secret_version.postgres_tahmo_password.secret_data
  login = true
}

resource "postgresql_grant" "tahmo_read" {
  database = "postgres"
  role = postgresql_role.tahmo.name
  schema = "public"
  object_type = "table"
  privileges = ["SELECT"]
}


resource "postgresql_grant" "readonly_public_terracotta" {
  database = "terracotta"
  role = postgresql_role.read.name
  schema = "public"
  object_type = "table"
  privileges = ["SELECT"]
}

resource "postgresql_default_privileges" "read_only_default_admin" {
  database = "postgres"
  role = postgresql_role.read.name
  schema = "public"
  owner = "postgres"
  object_type = "table"
  privileges = ["SELECT"]
}

resource "postgresql_default_privileges" "tahmo_default_admin" {
  database = "postgres"
  role = postgresql_role.tahmo.name
  schema = "public"
  owner = "postgres"
  object_type = "table"
  privileges = ["SELECT"]
}

resource "postgresql_default_privileges" "read_only_default" {
  database = "postgres"
  role = postgresql_role.read.name
  schema = "public"
  owner = "write"
  object_type = "table"
  privileges = ["SELECT"]
}

resource "postgresql_default_privileges" "tahmo_default" {
  database = "postgres"
  role = postgresql_role.tahmo.name
  schema = "public"
  owner = "write"
  object_type = "table"
  privileges = ["SELECT"]
}

resource "postgresql_default_privileges" "read_only_default_terracotta" {
  database = "terracotta"
  role = postgresql_role.read.name
  schema = "public"
  owner = "write"
  object_type = "table"
  privileges = ["SELECT"]
}

resource "postgresql_default_privileges" "read_only_default_admin_terracotta" {
  database = "terracotta"
  role = postgresql_role.read.name
  schema = "public"
  owner = "postgres"
  object_type = "table"
  privileges = ["SELECT"]
}


resource "random_password" "postgres_write_password" {
  length = 16
  special = true
}

resource "google_secret_manager_secret" "postgres_write_password" {
  secret_id = "sheerwater-postgres-write-password"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "postgres_write_password" {
  secret = google_secret_manager_secret.postgres_write_password.id
  secret_data = random_password.postgres_write_password.result
}

resource "postgresql_role" "write" {
  name = "write"
  password = random_password.postgres_write_password.result
  login = true
  create_database = true
}

resource "postgresql_grant" "write_public" {
  database = "postgres"
  role = postgresql_role.write.name
  schema = "public"
  object_type = "table"
  privileges = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"]
}

resource "postgresql_grant" "write_public_terracotta" {
  database = "terracotta"
  role = postgresql_role.write.name
  schema = "public"
  object_type = "table"
  privileges = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"]
}

resource "postgresql_grant" "write_public_terracottads" {
  database = "terracotta"
  role = postgresql_role.write.name
  schema = "public"
  object_type = "table"
  objects = ["datasets"]
  privileges = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"]
}

resource "postgresql_grant" "write_schema_public" {
  database = "postgres"
  role = postgresql_role.write.name
  schema = "public"
  object_type = "schema"
  privileges = ["CREATE"]
}

resource "postgresql_grant" "public_schema_public" {
  database = "postgres"
  role = "public"
  schema = "public"
  object_type = "schema"
  privileges = ["CREATE", "USAGE"]
}

resource "postgresql_grant" "write_database_public" {
  database = "postgres"
  role = postgresql_role.write.name
  schema = "public"
  object_type = "database"
  privileges = ["CREATE"]
}
