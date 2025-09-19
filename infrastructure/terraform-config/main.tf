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

  # Postgres connection URL - different for prod vs ephemeral
  # TODO: this url should be built from other resource values 
  # postgres = ?
  # sheerwater-benchmarking = infrastructure.terraform-config.sheerwater_k8s_namespace
  # svc.cluster.local = ?
  # port = ?
  postgres_url = terraform.workspace == "default" ? "postgres:5432" : "postgres.shared.rhizaresearch.org:5432"

  # Collect dashboards and key them by UID so filename changes don't disturb state
  dashboard_files = fileset("${path.module}/../../dashboards/build", "*.json")
  dashboards_parsed = { for f in local.dashboard_files : f => jsondecode(file("${path.module}/../../dashboards/build/${f}")) }
  dashboards_by_uid = {
    for f, d in local.dashboards_parsed :
    d.uid => "${path.module}/../../dashboards/build/${f}"
    if try(d.uid, "") != ""
  }
  home_dashboard_uid = "ee4mze492j0n4d"
}
provider "grafana" {
  # Base URLs
  # - prod:      https://dashboards.rhizaresearch.org
  # - ephemeral: https://dev.shared.rhizaresearch.org/sheerwater-benchmarking/<pr_number>
  url = local.grafana_url
  auth = local.grafana_auth
  # TODO: for now I have to use the default password because 
  # the correct password is not working. It is being set but I think 
  # there is some urlencoding happening somewhere breaking the password.
  #auth = "admin:admin"
}

output "grafana_url" {
  value = local.grafana_url
}

# Gcloud secrets for postgres read user
data "google_secret_manager_secret_version" "postgres_read_password" {
  secret = "postgres-read-password" 
}

# handled via import.sh
# import {
#   to = grafana_organization.benchmarking
#   id = local.is_prod ? "1" : "1"
#   # in prod, the benchmarking org is the main org (id 1)
#   # in ephemeral, the benchmarking org is the main org (id 1)
# }

resource "grafana_organization" "benchmarking" {
  name = "SheerWater"
  # Note: changing the name will disable anonymous access because they are given access by org name.
  #name = "benchmarking"
  #admin_user = "admin"

  lifecycle {
    ignore_changes = [admins, viewers, editors]
    prevent_destroy = true
  }
}

# handled via import.sh
# import {
#   to = grafana_organization_preferences.light_preference_benchmarking
#   id = "1"
# }


resource "grafana_organization_preferences" "light_preference_benchmarking" {
  theme = "light"
  timezone = "utc"
  week_start = "sunday"
  # only set the home dashboard uid on the first run
  home_dashboard_uid = local.home_dashboard_uid
  org_id = grafana_organization.benchmarking.id

  #lifecycle {
  #  ignore_changes = [home_dashboard_uid, ]
  #}

  depends_on = [grafana_organization.benchmarking, grafana_dashboard.dashboards]

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

  org_id = grafana_organization.benchmarking.id

  depends_on = [grafana_organization.benchmarking]

}


# Create dashboards
resource "grafana_dashboard" "dashboards" {
  # only create dashboards for the ephemeral workspaces (for now)
  for_each = local.dashboards_by_uid
  config_json = file(each.value)
  message = "Modified by terraform from https://github.com/rhiza-research/${local.repo_name}/pull/${local.pr_number}"
  #is_starred = each.value == local.home_dashboard_uid

  overwrite = true
  org_id = grafana_organization.benchmarking.id

  depends_on = [grafana_data_source.postgres]
}


# resource "grafana_apps_dashboard_dashboard_v1beta1" "home" {
#   #name = "Home"
#   metadata {
#     uid = "ee4mze492j0n4d"
#     #folder_uid = "dashboards"
#   }
#   # options {
#   #   #overwrite = true
#   #   #path = "dashboards/home"
#   # }
#   spec {
#     json = file("${path.module}/../../dashboards/build/Home.json")
#     #tags = []
#     #title = "Home"
#   }
# }


# TODO: sheerwater.rhizaresearch.org should forward to dashboards.rhizaresearch.org/orgId=1 (or whatever the sheerwater-benchmarking orgId is in production)
# this would require something like an nginx proxy in the k8s cluster to forward the requests to the correct orgId

# resource "google_dns_managed_zone" "sheerwater" {
#   #depends_on = [google_project.sheerwater]
#   name = "sheerwater"
#   dns_name = "sheerwater.rhizaresearch.org."
#   project = "sheerwater"

#   description = "sheerwater dns zone"

#   lifecycle {
#     prevent_destroy = true
#   }
# }

# resource nginx proxy {
#  forward google_dns_managed_zone.sheerwater to dashboards.rhizaresearch.org/orgId=1
#}
