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

data "google_container_cluster" "rhiza-shared" {
  name     = "rhiza-cluster"
  location = "us-central1-a"
}

# Connect to the kubernetes cluster
provider "kubernetes" {
  host  = "https://${data.google_container_cluster.rhiza-shared.endpoint}"
  token = data.google_client_config.provider.access_token
  cluster_ca_certificate = base64decode(
    data.google_container_cluster.rhiza-shared.master_auth[0].cluster_ca_certificate,
  )
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "gke-gcloud-auth-plugin"
  }
}

provider "helm" {
  kubernetes {
    host  = "https://${data.google_container_cluster.rhiza-shared.endpoint}"
    token = data.google_client_config.provider.access_token
    cluster_ca_certificate = base64decode(
      data.google_container_cluster.rhiza-shared.master_auth[0].cluster_ca_certificate,
    )
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "gke-gcloud-auth-plugin"
    }
  }
}

# Create the sheerwater namespace
resource "kubernetes_namespace" "sheerwater-benchmarking" {
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

resource "random_password" "db_write_password" {
  length           = 16
  special          = true
}

resource "random_password" "db_read_password" {
  length           = 16
  special          = true
}


# Persistent disk
resource "google_compute_disk" "sheerwater-benchmarking-db" {
  name  = "sheerwater-benchmarking-db"
  type  = "pd-balanced"
  zone  = "us-central1-a"
  size  = "20GiB"
}

# Create a snapshot backup policy for the database
resource "google_compute_snapshot" "sheerwater-benchmarking-db-snapshot" {
  name        = "sheerwater-benchmarking-db-snapshot"
  zone        = "us-central1-a"
  storage_locations = ["us-central1"]


}

resource "google_compute_resource_policy" "db-snapshot-policy" {
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
}

resource "google_compute_disk_resource_policy_attachment" "attachment" {
  name = google_compute_resource_policy.db-snapshot-policy.name
  disk = google_compute_disk.sheerwater-benchmarking-db.id
  zone = "us-central1-a"
}


#################
# Grafana
#################

# Gcloud secrets for Single sign on
data "google_secret_manager_secret_version" "sheerwater-oauth-client-id" {
 secret   = "sheerwater-oauth-client-id"
}

data "google_secret_manager_secret_version" "sheerwater-oauth-client-secret" {
 secret   = "sheerwater-oauth-client-secret"
}


# Persistent disk
resource "google_compute_disk" "sheerwater-benchmarking-grafana" {
  name  = "sheerwater-benchmarking-grafana"
  type  = "pd-balanced"
  zone  = "us-central1-a"
  size  = "10GiB"
}

# SMTP secrets for inviting users
data "google_secret_manager_secret_version" "sheerwater-sendgrid-api-key" {
 secret   = "sheerwater-sendgrid-api-key"
}

# Create a domain name and IP address




#################
# Tile Server
################


# Now the helm release to release all of the kubernetes manifest

# Now monitor the deployed resources with uptime robot
