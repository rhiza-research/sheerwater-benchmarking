########################################################
# This file is used to configure the prod and ephemeral grafana instances based on workspace name
# 
# Workspace naming convention:
# - the "default" workspace configures the prod instance at dashboards.rhizaresearch.org
# - any other workspace is an ephemeral instance and should be named with the format "grafana-pr-<pr_number>"
# 
# It is used to create/configure:
# - sso settings
# - orgs (with preferences)
# - datasources
# - dashboards
########################################################

terraform {
  required_version = ">= 1.5.7"

  backend "gcs" {
    bucket = "rhiza-terraform-state"
    prefix = "sheerwater-config"
  }

  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.45.0"
    }

    grafana = {
      source = "grafana/grafana"
      version = "4.5.1"
    }

    external = {
      source = "hashicorp/external"
      version = "~> 2.0"
    }

    local = {
      source = "hashicorp/local"
      version = "~> 2.0"
    }
  }
}

provider "google" {
  project = "sheerwater"
}

data "google_secret_manager_secret_version" "grafana_ephemeral_admin_password" {
  secret = "grafana-ephemeral-admin-password"
  project = "rhiza-shared"
}

data "google_secret_manager_secret_version" "grafana_admin_password" {
  secret = "grafana-admin-password"
  project = "rhiza-shared"
}

locals {
  is_prod = terraform.workspace == "default"

  # git repo name
  repo_name = "sheerwater-benchmarking"

  # Extract PR number from workspace name "grafana-pr-<pr_number>"
  pr_number = local.is_prod ? "" : element(split("-", terraform.workspace), length(split("-", terraform.workspace)) - 1)

  # Base URLs
  # - prod:      https://dashboards.rhizaresearch.org
  # - ephemeral: https://dev.shared.rhizaresearch.org/sheerwater-benchmarking/<pr_number>
  grafana_url = local.is_prod ? "https://dashboards.rhizaresearch.org" : "https://dev.shared.rhizaresearch.org/${local.repo_name}/${local.pr_number}"
  grafana_auth = local.is_prod ? "admin:${data.google_secret_manager_secret_version.grafana_admin_password.secret_data}" : "admin:${data.google_secret_manager_secret_version.grafana_ephemeral_admin_password.secret_data}"

  # Postgres connection URL - different for prod vs ephemeral - ephemeral using a different namespace so use the full address
  postgres_url = terraform.workspace == "default" ? "postgres:5432" : "postgres.shared.rhizaresearch.org:5432"

  home_dashboard_uid = "ee4mze492j0n4d"
}
provider "grafana" {
  # Base URLs
  # - prod:      https://dashboards.rhizaresearch.org
  # - ephemeral: https://dev.shared.rhizaresearch.org/sheerwater-benchmarking/<pr_number>
  url = local.grafana_url
  auth = local.grafana_auth
}

# Gcloud secrets for postgres read user
data "google_secret_manager_secret_version" "postgres_read_password" {
  secret = "postgres-read-password"
}

resource "grafana_organization" "org" {
  name = "SheerWater"

  lifecycle {
    ignore_changes = [admins, viewers, editors]
    prevent_destroy = true
  }
}

resource "grafana_organization_preferences" "preferences" {
  theme = "light"
  timezone = "utc"
  week_start = "sunday"
  # only set the home dashboard uid on the first run
  home_dashboard_uid = local.home_dashboard_uid
  org_id = grafana_organization.org.id

  #lifecycle {
  #  ignore_changes = [home_dashboard_uid, ]
  #}

  depends_on = [grafana_organization.org, grafana_dashboard.dashboards]

}

# Connect grafana to the read user with a datasource
resource "grafana_data_source" "postgres" {
  type = "grafana-postgresql-datasource"
  name = "postgres"
  url = local.postgres_url
  username = "sheerwater_read"
  uid = "bdz3m3xs99p1cf"

  secure_json_data_encoded = jsonencode({
    password = data.google_secret_manager_secret_version.postgres_read_password.secret_data
  })

  json_data_encoded = jsonencode({
    database = "postgres"
    sslmode = "disable"
    postgresVersion = 1500
    timescaledb = true
  })

  org_id = grafana_organization.org.id

  depends_on = [grafana_organization.org]

}

resource "local_file" "datasource_config" {
  content = jsonencode({
    (grafana_data_source.postgres.type) : grafana_data_source.postgres.uid
  })
  filename = "${path.module}/../../dashboards/datasource_config.json"
}
