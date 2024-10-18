terraform {
  backend "gcs" {
    bucket  = "rhiza-terraform-state"
    prefix  = "sheerwater-benchmarking-config"
  }

  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.4.0"
    }

    grafana = {
      source = "grafana/grafana"
      version = "3.7.0"
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
 secret   = "sheerwater-postgres-admin-password"
}

data "google_secret_manager_secret_version" "grafana_admin_password" {
 secret   = "sheerwater-grafana-admin-password"
}

provider "postgresql" {
  host            = "sheerwater-benchmarking-postgres"
  port            = 5432
  database        = "postgres"
  username        = "postgres"
  password        = "${data.google_secret_manager_secret_version.postgres_admin_password.secret_data}"
  sslmode         = "disable"
  connect_timeout = 15
}

provider "grafana" {
  url  = "https://benchmarks.sheerwater.rhizaresearch.org/"
  auth = "admin:${data.google_secret_manager_secret_version.grafana_admin_password.secret_data}"
}

# Gcloud secrets for Single sign on
data "google_secret_manager_secret_version" "postgres_read_password" {
 secret   = "sheerwater-postgres-read-password"
}

resource "postgresql_role" "read" {
  name = "read"
  password = "${data.google_secret_manager_secret_version.postgres_read_password.secret_data}"
  login = true
}

resource postgresql_grant "readonly_public" {
  database    = "postgres"
  role        = postgresql_role.read.name
  schema      = "public"
  object_type = "table"
  privileges  = ["SELECT"]
}

resource postgresql_grant "readonly_public_terracotta" {
  database    = "terracotta"
  role        = postgresql_role.read.name
  schema      = "public"
  object_type = "table"
  privileges  = ["SELECT"]
}

resource "postgresql_default_privileges" "read_only_default_admin" {
  database = "postgres"
  role        = postgresql_role.read.name
  schema   = "public"
  owner       = "postgres"
  object_type = "table"
  privileges  = ["SELECT"]
}

resource "postgresql_default_privileges" "read_only_default" {
  database = "postgres"
  role        = postgresql_role.read.name
  schema   = "public"
  owner       = "write"
  object_type = "table"
  privileges  = ["SELECT"]
}

resource "postgresql_default_privileges" "read_only_default_terracotta" {
  database = "terracotta"
  role        = postgresql_role.read.name
  schema   = "public"
  owner       = "write"
  object_type = "table"
  privileges  = ["SELECT"]
}

resource "postgresql_default_privileges" "read_only_default_admin_terracotta" {
  database = "terracotta"
  role        = postgresql_role.read.name
  schema   = "public"
  owner       = "postgres"
  object_type = "table"
  privileges  = ["SELECT"]
}


resource "random_password" "postgres_write_password" {
  length           = 16
  special          = true
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
  password = "${random_password.postgres_write_password.result}"
  login = true
  create_database = true
}

resource postgresql_grant "write_public" {
  database    = "postgres"
  role        = postgresql_role.write.name
  schema      = "public"
  object_type = "table"
  privileges  = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"]
}

resource postgresql_grant "write_schema_public" {
  database    = "postgres"
  role        = postgresql_role.write.name
  schema      = "public"
  object_type = "schema"
  privileges  = ["CREATE"]
}

resource postgresql_grant "write_database_public" {
  database    = "postgres"
  role        = postgresql_role.write.name
  schema      = "public"
  object_type = "database"
  privileges  = ["CREATE"]
}




# Gcloud secrets for Single sign on
data "google_secret_manager_secret_version" "sheerwater_oauth_client_id" {
 secret   = "sheerwater-oauth-client-id"
}

data "google_secret_manager_secret_version" "sheerwater_oauth_client_secret" {
 secret   = "sheerwater-oauth-client-secret"
}

# Enable googel oauth
resource "grafana_sso_settings" "google_sso_settings" {
  provider_name = "google"
  oauth2_settings {
    name            = "Google"
    client_id = "${data.google_secret_manager_secret_version.sheerwater_oauth_client_id.secret_data}"
    client_secret = "${data.google_secret_manager_secret_version.sheerwater_oauth_client_secret.secret_data}"
    allow_sign_up   = true
    auto_login      = false
    #allow_assign_grafana_admin = true
    scopes          = "openid email profile"
    allowed_domains = "rhizaresearch.org"
    skip_org_role_sync = true
    use_pkce        = true
  }
}

resource "grafana_organization_preferences" "light_preference" {
  theme      = "light"
  timezone   = "utc"
  week_start = "sunday"
}

# Connect grafana to the read user with a datasource
resource "grafana_data_source" "postgres" {
  type                = "grafana-postgresql-datasource"
  name                = "postgres"
  url                 = "postgres:5432"
  username                = "read"
  
  secure_json_data_encoded = jsonencode({
    password = "${data.google_secret_manager_secret_version.postgres_read_password.secret_data}"
  })

  json_data_encoded = jsonencode({
    database = "postgres"
    sslmode = "disable"
    postgresVersion = 1500
    timescaledb = false
  })
}

# Eventually create dashboards
