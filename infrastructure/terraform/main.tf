terraform {
  backend "gcs" {
    bucket  = "rhiza-terraform-state"
    prefix  = "sheerwater-benchmarking-state"
  }

  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.4.0"
    }

    helm = {
      source = "hashicorp/helm"
      version = "2.15.0"
    }

    kubernetes = {
      source = "hashicorp/kubernetes"
      version = "2.32.0"
    }
  }


}

provider "google" {
  project = "sheerwater"
}

data "google_client_config" "provider" {}

data "google_container_cluster" "rhiza_shared" {
  name     = "rhiza-cluster"
  location = "us-central1-a"
  project = "rhiza-shared"
}

# Connect to the kubernetes cluster
provider "kubernetes" {
  host  = "https://${data.google_container_cluster.rhiza_shared.endpoint}"
  token = data.google_client_config.provider.access_token
  cluster_ca_certificate = base64decode(
    data.google_container_cluster.rhiza_shared.master_auth[0].cluster_ca_certificate,
  )
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "gke-gcloud-auth-plugin"
  }
}

provider "helm" {
  kubernetes {
    host  = "https://${data.google_container_cluster.rhiza_shared.endpoint}"
    token = data.google_client_config.provider.access_token
    cluster_ca_certificate = base64decode(
      data.google_container_cluster.rhiza_shared.master_auth[0].cluster_ca_certificate,
    )
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "gke-gcloud-auth-plugin"
    }
  }
}

# Create the sheerwater namespace
resource "kubernetes_namespace" "sheerwater_benchmarking" {
  metadata {
    name = "sheerwater-benchmarking"
  }
}


#################
# Database
################

# Username and password secrets
resource "random_password" "db_admin_password" {
  length           = 16
  special          = true
}

# Persistent disk
resource "google_compute_disk" "sheerwater_benchmarking_db" {
  name  = "sheerwater-benchmarking-db"
  type  = "pd-balanced"
  zone  = "us-central1-a"
  size  = 20
  project = "rhiza-shared"
}

resource "google_compute_resource_policy" "db_snapshot_policy" {
  name = "sheerwater-benchmarking-db-snapshot-policy"
  region = "us-central1"
  snapshot_schedule_policy {
    schedule {
      daily_schedule {
        days_in_cycle = 1
        start_time = "04:00"
      }
    }
    retention_policy {
      max_retention_days    = 30
      on_source_disk_delete = "KEEP_AUTO_SNAPSHOTS"
    }
  }
  project = "rhiza-shared"
}

resource "google_compute_disk_resource_policy_attachment" "attachment" {
  name = google_compute_resource_policy.db_snapshot_policy.name
  disk = google_compute_disk.sheerwater_benchmarking_db.name
  zone = "us-central1-a"
  project = "rhiza-shared"
}


#################
# Grafana
#################

# Gcloud secrets for Single sign on
data "google_secret_manager_secret_version" "sheerwater_oauth_client_id" {
 secret   = "sheerwater-oauth-client-id"
}

data "google_secret_manager_secret_version" "sheerwater_oauth_client_secret" {
 secret   = "sheerwater-oauth-client-secret"
}


# Persistent disk
resource "google_compute_disk" "sheerwater_benchmarking_grafana" {
  name  = "sheerwater-benchmarking-grafana"
  type  = "pd-balanced"
  zone  = "us-central1-a"
  size  = 10
  project = "rhiza-shared"
}

# SMTP secrets for inviting users
data "google_secret_manager_secret_version" "sheerwater_sendgrid_api_key" {
 secret   = "sheerwater-sendgrid-api-key"
}

# grafana password
resource "random_password" "grafana_admin_password" {
  length           = 16
  special          = true
}

# Create a domain name and IP address
resource "google_compute_global_address" "grafana_address" {
  name = "sheerwater-benchmarking-grafana-address"
  project = "rhiza-shared"
}

resource "google_dns_record_set" "grafana_recordset" {
  managed_zone = "sheerwater"
  name = "benchmarks.sheerwater.rhizaresearch.org."
  type = "A"
  rrdatas = [google_compute_global_address.grafana_address.address]
  ttl = 300
}



################
## Helm Release
################

# Now the helm release to release all of the kubernetes manifest
locals {
  chart_values = {
    grafana = {
      admin_password = "${random_password.grafana_admin_password.result}"
      smtp_password = "${data.google_secret_manager_secret_version.sheerwater_sendgrid_api_key.secret_data}"
      google_oauth = {
        client_id = "${data.google_secret_manager_secret_version.sheerwater_oauth_client_id.secret_data}"
        client_secret = "${data.google_secret_manager_secret_version.sheerwater_oauth_client_secret.secret_data}"
      }
      pv = {
        name = "${google_compute_disk.sheerwater_benchmarking_grafana.name}"
        size = "${google_compute_disk.sheerwater_benchmarking_grafana.size}"
      }
      domain_name = "${trimsuffix(google_dns_record_set.grafana_recordset.name, ".")}"
      ip_name = "${google_compute_global_address.grafana_address.name}"
    }
    postgres = {
      pv = {
        name = "${google_compute_disk.sheerwater_benchmarking_db.name}"
        size = "${google_compute_disk.sheerwater_benchmarking_db.size}"
      }
      admin_password = "${random_password.db_admin_password.result}"
    }
  }
}


# Now monitor the deployed resources with uptime robot
resource "helm_release" "sheerwater_benchmarking" {
  name = "sheerwater-benchmarking"
  chart = "../helm/sheerwater-benchmarking"
  namespace = "sheerwater-benchmarking"
  values = [yamlencode(local.chart_values)]
}
