terraform {
  required_version = ">= 1.5.7"
  required_providers {

    google = {
      source = "hashicorp/google"
      version = "7.3.0"
    }

    postgresql = {
      source = "cyrilgdn/postgresql"
      version = "1.23.0"
    }
  }
}

resource "google_project" "project" {
  name = "sheerwater"
  project_id = "sheerwater"
  org_id = "636990752070" # Rhiza org id
  billing_account = "010A80-225887-239CE0"
  lifecycle {
    prevent_destroy = true
  }
}

################################################
# Sheerwater: Postgres roles and grants
################################################

################################################
# Read role for postgres
################################################

module "random_password_postgres_read" {
  source = "git@github.com:rhiza-research/infrastructure.git//terraform/modules/random-gsm-secret?ref=argocd"
  random_password_secret_name = "postgres-read-password"
  project = google_project.project.project_id
}

resource "postgresql_role" "read" {
  name = "sheerwater_read"
  password = module.random_password_postgres_read.random_password_value
  login = true
}

################################################
# Write role for postgres
################################################

module "random_password_postgres_write" {
  source = "git@github.com:rhiza-research/infrastructure.git//terraform/modules/random-gsm-secret?ref=argocd"
  random_password_secret_name = "postgres-write-password"
  project = google_project.project.project_id
}

resource "postgresql_role" "write" {
  name = "sheerwater_write"
  password = module.random_password_postgres_read.random_password_value
  login = true
  create_database = true
}

################################################
# Postgres: Read role grants
################################################
resource "postgresql_grant" "readonly_public" {
  database = "postgres"
  role = postgresql_role.read.name
  schema = "public"
  object_type = "table"
  privileges = ["SELECT"]
}

resource "postgresql_default_privileges" "readonly_default" {
  database = "postgres"
  role = postgresql_role.read.name
  schema = "public"
  owner = postgresql_role.write.name
  object_type = "table"
  privileges = ["SELECT"]
}

################################################
# Postgres: Write role grants
################################################
resource "postgresql_grant" "write_schema_public" {
  database = "postgres"
  role = postgresql_role.write.name
  schema = "public"
  object_type = "schema"
  privileges = ["CREATE"]
}

resource "postgresql_grant" "write_public" {
  database = "postgres"
  role = postgresql_role.write.name
  schema = "public"
  object_type = "table"
  privileges = [
    "DELETE",
    "INSERT",
    "REFERENCES",
    "SELECT",
    "TRIGGER",
    "TRUNCATE",
    "UPDATE",
  ]
}

################################################
# Terracotta: Read role grants
################################################
resource "postgresql_grant" "readonly_public_terracotta" {
  database = var.terracotta_database_name
  role = postgresql_role.read.name
  schema = "public"
  object_type = "table"
  privileges = ["SELECT"]
}

################################################
# Terracotta: Write role grants
################################################
resource "postgresql_grant" "write_public_terracottads" {
  database = var.terracotta_database_name
  role = postgresql_role.write.name
  schema = "public"
  object_type = "table"
  objects = ["datasets"]
  privileges = [
    "DELETE",
    "INSERT",
    "REFERENCES",
    "SELECT",
    "TRIGGER",
    "UPDATE",
  ]
}

### Access for the github action to deploy terraform

# tflint-ignore: terraform_naming_convention
resource "google_project_iam_member" "access-terraform-state" {
  project = google_project.project.project_id
  role = "roles/secretmanager.secretAccessor"
  member = "serviceAccount:${var.service_account_email}"
}

# tflint-ignore: terraform_naming_convention
resource "google_project_iam_member" "view-secrets" {
  project = google_project.project.project_id
  role = "roles/secretmanager.viewer"
  member = "serviceAccount:${var.service_account_email}"
}
