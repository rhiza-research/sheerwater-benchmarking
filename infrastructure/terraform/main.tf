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

# Create a kubernetes service account in the namespace
resource "kubernetes_service_account" "sheerwater_sa" {
  metadata {
    name = "sheerwater-sa"
    namespace = "sheerwater-benchmarking"
  }
}

# Bind the service account to a cloud storage reader policy
resource "google_project_iam_binding" "project" {
  project = "sheerwater"
  role    = "roles/storage.objectViewer"

  members = [
    "principal://iam.googleapis.com/projects/730596460290/locations/global/workloadIdentityPools/rhiza-shared.svc.id.goog/subject/ns/sheerwater-benchmarking/sa/sheerwater-sa",
  ]
}


#################
# Database
################

# Username and password secrets
resource "random_password" "db_admin_password" {
  length           = 16
  special          = true
}

resource "google_secret_manager_secret" "db_admin_password" {
  secret_id = "sheerwater-postgres-admin-password"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "db_admin_password" {
  secret = google_secret_manager_secret.db_admin_password.id
  secret_data = random_password.db_admin_password.result
}

# Create postgres users and grant them permissions
resource "random_password" "postgres_read_password" {
  length           = 16
  special          = true
}

resource "google_secret_manager_secret" "postgres_read_password" {
  secret_id = "sheerwater-postgres-read-password"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "postgres_read_password" {
  secret = google_secret_manager_secret.postgres_read_password.id
  secret_data = random_password.postgres_read_password.result
}

# Persistent disk
resource "google_compute_disk" "sheerwater_benchmarking_db" {
  name  = "sheerwater-benchmarking-db"
  type  = "pd-balanced"
  zone  = "us-central1-a"
  size  = 20
  project = "rhiza-shared"

  # Mark as deprecated
  lifecycle {
    prevent_destroy = true
  }
}

# Persistent disk
resource "google_compute_disk" "sheerwater_benchmarking_terracotta" {
  name  = "sheerwater-benchmarking-terracotta"
  type  = "pd-ssd"
  zone  = "us-central1-a"
  size  = 50
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
  disk = google_compute_disk.sheerwater_benchmarking_db_ssd.name
  zone = "us-central1-a"
  project = "rhiza-shared"
}

# Create new disk from the specific snapshot
resource "google_compute_disk" "sheerwater_benchmarking_db_ssd" {
  name     = "sheerwater-benchmarking-db-ssd"
  type     = "pd-ssd"
  zone     = "us-central1-a"
  size     = 200
  project  = "rhiza-shared"
  snapshot = "projects/rhiza-shared/global/snapshots/sheerwater-benchmar-us-central1-a-20250217055130-5p7cuzqi"

  # Prevent accidental deletion
  lifecycle {
    prevent_destroy = true
  }
}


### New timescale disk and policy
resource "google_compute_resource_policy" "timescaledb_snapshot_policy" {
  name = "sheerwater-benchmarking-timescaledb-snapshot-policy"
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

resource "google_compute_disk_resource_policy_attachment" "timescale_attachment" {
  name = google_compute_resource_policy.timescaledb_snapshot_policy.name
  disk = google_compute_disk.sheerwater_benchmarking_timescaledb_ssd.name
  zone = "us-central1-a"
  project = "rhiza-shared"
}

# Create new disk from the specific snapshot
resource "google_compute_disk" "sheerwater_benchmarking_timescaledb_ssd" {
  name     = "sheerwater-benchmarking-timescaledb-ssd"
  type     = "pd-ssd"
  zone     = "us-central1-a"
  size     = 200
  project  = "rhiza-shared"

  # Prevent accidental deletion
  lifecycle {
    prevent_destroy = true
  }
}


#################
# Grafana
#################


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

resource "google_secret_manager_secret" "grafana_admin_password" {
  secret_id = "sheerwater-grafana-admin-password"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "grafana_admin_password" {
  secret = google_secret_manager_secret.grafana_admin_password.id
  secret_data = random_password.grafana_admin_password.result
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

# Create a domain name and IP address
resource "google_compute_global_address" "terracotta_address" {
  name = "sheerwater-benchmarking-terracotta-address"
  project = "rhiza-shared"
}

resource "google_dns_record_set" "terracotta_recordset" {
  managed_zone = "sheerwater"
  name = "terracotta.sheerwater.rhizaresearch.org."
  type = "A"
  rrdatas = [google_compute_global_address.terracotta_address.address]
  ttl = 300
}

# Create a regional IP Address
resource "google_compute_address" "postgres_address" {
  name = "sheerwater-benchmarking-postgres-address"
  project = "rhiza-shared"
  region = "us-central1"
}

# Set a DNS record for that IP Address
resource "google_dns_record_set" "resource-recordset" {
  managed_zone = "sheerwater"
  name         = "postgres.sheerwater.rhizaresearch.org."
  type         = "A"
  rrdatas = [google_compute_address.postgres_address.address]
  ttl          = 300
}

# TODO: move this to the infrastructure repo to be the with rest of the argocd resources
resource "google_compute_global_address" "argocd_address" {
  name = "sheerwater-benchmarking-argocd-address"
  project = "rhiza-shared"
}

resource "google_dns_record_set" "argocd_recordset" {
  managed_zone = "sheerwater"
  name = "argocd.sheerwater.rhizaresearch.org."
  type = "A"
  rrdatas = [google_compute_global_address.argocd_address.address]
  ttl = 300
}



################
## Helm Release
################

# Now the helm release to release all of the kubernetes manifest
resource "helm_release" "sheerwater_benchmarking" {
  name = "sheerwater-benchmarking"
  chart = "../helm/sheerwater-benchmarking"
  namespace = "sheerwater-benchmarking"

  # load the default values from the helm chart
  values = [
    file("../helm/sheerwater-benchmarking/values.yaml")
  ]

  # set any dynamic values based on the terraform resources

  # Grafana settings
  set {
    name = "grafana.pv.name"
    value = google_compute_disk.sheerwater_benchmarking_grafana.name
  }
  set {
    name = "grafana.pv.size"
    value = google_compute_disk.sheerwater_benchmarking_grafana.size
  }
  set {
    name = "grafana.domain_name"
    value = trimsuffix(google_dns_record_set.grafana_recordset.name, ".")
  }
  set {
    name = "grafana.ip_name"
    value = google_compute_global_address.grafana_address.name
  }
  set_sensitive {
    name = "grafana.admin_password"
    value = random_password.grafana_admin_password.result
  }
  set_sensitive {
    name = "grafana.smtp.password"
    value = data.google_secret_manager_secret_version.sheerwater_sendgrid_api_key.secret_data
  }

  # Timescale settings
  set {
    name = "timescale.pv.name"
    value = google_compute_disk.sheerwater_benchmarking_timescaledb_ssd.name
  }
  set {
    name = "timescale.pv.size"
    value = google_compute_disk.sheerwater_benchmarking_timescaledb_ssd.size
  }
  set {
    name = "timescale.externalIP"
    value = google_compute_address.postgres_address.address
  }
  set_sensitive {
    name = "timescale.admin_password"
    value = random_password.db_admin_password.result
  }

  # Terracotta settings
  set {
    name = "terracotta.sql_user"
    value = "read"
  }
  set {
    name = "terracotta.domain_name"
    value = trimsuffix(google_dns_record_set.terracotta_recordset.name, ".")
  }
  set {
    name = "terracotta.ip_name"
    value = google_compute_global_address.terracotta_address.name
  }
  set {
    name = "terracotta.pv.name"
    value = google_compute_disk.sheerwater_benchmarking_terracotta.name
  }
  set {
    name = "terracotta.pv.size"
    value = google_compute_disk.sheerwater_benchmarking_terracotta.size
  }
  set_sensitive {
    name = "terracotta.sql_password"
    value = random_password.postgres_read_password.result
  }
}
