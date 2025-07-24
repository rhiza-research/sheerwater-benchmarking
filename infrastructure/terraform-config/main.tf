########################################################
# This file is used to configure the prod and ephemeral grafana instances based on workspace name
# 
# Workspace naming convention:
# - the "default" workspace configures the prod instance at benchmarks.sheerwater.rhizaresearch.org
# - any other workspace is an ephemeral instance and should be named with the format "grafana-pr-<pr_number>"
# 
# It is used to create/configure:
# - sso settings
# - orgs (with preferences)
# - datasources
# - dashboards
########################################################

terraform {
  backend "gcs" {
    bucket  = "rhiza-terraform-state"
    prefix  = "sheerwater-benchmarking-config"
  }

  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.45.0"
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

locals {
  is_prod = terraform.workspace == "default"
  grafana_url  = terraform.workspace == "default" ? "https://benchmarks.sheerwater.rhizaresearch.org" : "https://${terraform.workspace}.dev.sheerwater.rhizaresearch.org"
  
  # Postgres connection URL - different for prod vs ephemeral
  # TODO: this url should be built from other resource values 
  # postgres = ?
  # sheerwater-benchmarking = infrastructure.terraform-config.sheerwater_k8s_namespace
  # svc.cluster.local = ?
  # port = ?
  postgres_url = terraform.workspace == "default" ? "postgres:5432" : "postgres.sheerwater-benchmarking.svc.cluster.local:5432"
}

// if workspace == default then url =  "https://benchmarks.sheerwater.rhizaresearch.org/"
// else url = "https://${terraform.workspace}.dev.sheerwater.rhizaresearch.org/"
provider "grafana" {
  url  = local.grafana_url
  #auth = "admin:${data.google_secret_manager_secret_version.grafana_admin_password.secret_data}"
  # TODO: for now I have to use the default password because 
  # the correct password is not working. It is being set but I think 
  # there is some urlencoding happening somewhere breaking the password.
  auth = "admin:admin"

}

output "grafana_url" {
  value = local.grafana_url
}

# Gcloud secrets for postgres read user
data "google_secret_manager_secret_version" "postgres_read_password" {
 secret   = "sheerwater-postgres-read-password"
}

# Gcloud secrets for influx read user
data "google_secret_manager_secret_version" "tahmo_influx_read_password" {
 secret   = "tahmo-influx-read-password"
}



# Gcloud secrets for Single sign on
data "google_secret_manager_secret_version" "sheerwater_oauth_client_id" {
 secret   = "sheerwater-oauth-client-id"
}

data "google_secret_manager_secret_version" "sheerwater_oauth_client_secret" {
 secret   = "sheerwater-oauth-client-secret"
}

# Enable google oauth
resource "grafana_sso_settings" "google_sso_settings" {
  count = local.is_prod ? 1 : 0 # only enable for the prod instance because sso doesnt work for ephemeral instances
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

  lifecycle {
    ignore_changes = [home_dashboard_uid,]
  }
}

# Connect grafana to the read user with a datasource
resource "grafana_data_source" "postgres" {
  type                = "grafana-postgresql-datasource"
  name                = "postgres"
  url                 = local.postgres_url
  username                = "read"
  uid = "bdz3m3xs99p1cf"
  
  secure_json_data_encoded = jsonencode({
    password = "${data.google_secret_manager_secret_version.postgres_read_password.secret_data}"
  })

  json_data_encoded = jsonencode({
    database = "postgres"
    sslmode = "disable"
    postgresVersion = 1500
    timescaledb = true
  })
}

resource "grafana_organization" "tahmo" {
  name         = "TAHMO"
  admin_user   = "admin"

  lifecycle {
    ignore_changes = [admins, viewers, editors]
  }
}

resource "grafana_organization_preferences" "light_preference_tahmo" {
  theme      = "light"
  timezone   = "utc"
  week_start = "sunday"

  lifecycle {
    ignore_changes = [home_dashboard_uid,]
  }

  org_id = grafana_organization.tahmo.id
}

# Connect grafana to the read user with a datasource
resource "grafana_data_source" "postgres_tahmo" {
  type                = "grafana-postgresql-datasource"
  name                = "postgres"
  url                 = local.postgres_url
  username                = "read"
  uid = "cegueq2crd3wge"
  
  secure_json_data_encoded = jsonencode({
    password = "${data.google_secret_manager_secret_version.postgres_read_password.secret_data}"
  })

  json_data_encoded = jsonencode({
    database = "postgres"
    sslmode = "disable"
    postgresVersion = 1500
    timescaledb = true
  })

  org_id = grafana_organization.tahmo.id
}

# Connect grafana to the read user with a datasource
resource "grafana_data_source" "influx_tahmo" {
  type                = "influxdb"
  name                = "influx"
  url                 = "https://heavy-d24620b1.influxcloud.net:8086"
  basic_auth_enabled  = true
  basic_auth_username = "RhizaResearch"
  database_name       = "TAHMO"
  uid = "eepjuov1zfi0wb"
  
  secure_json_data_encoded = jsonencode({
    basicAuthPassword = "${data.google_secret_manager_secret_version.tahmo_influx_read_password.secret_data}"
  })

  json_data_encoded = jsonencode({
    dbname = "TAHMO"
    basicAuthPassword = "${data.google_secret_manager_secret_version.tahmo_influx_read_password.secret_data}"
    authType          = "default"
    query_language    = "SQL"
  })

  org_id = grafana_organization.tahmo.id
}

# other tahmodatasource uid = 'cer8o24n0lfy8b'





# FIXME: dont hardcode users here. This if we can't use oauth this should at least be dynamic in some way.
data "google_secret_manager_secret_version" "grafana_tristan_password" {
 secret   = "grafana-tristan-password"
}

resource "grafana_user" "tristan" {
  count = local.is_prod ? 0 : 1 # only enable for the ephemeral workspaces, prod uses SSO
  email    = "tristan@rhizaresearch.org"
  name     = "Tristan"
  login    = "tristan"
  password = "${data.google_secret_manager_secret_version.grafana_tristan_password.secret_data}"
  is_admin = true
}



# Create dashboards
resource "grafana_dashboard" "dashboards" {
  # only create dashboards for the ephemeral workspaces (for now)
  for_each = local.is_prod ? [] : fileset("${path.module}/../../dashboards/build", "*.json")
  config_json = file("${path.module}/../../dashboards/build/${each.value}")
}