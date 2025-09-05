terraform {
  required_providers {
    null = {
      source = "hashicorp/null"
      version = "3.2.4"
    }

    random = {
      source = "hashicorp/random"
      version = "3.7.2"
    }

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



# data "google_secret_manager_secret_version" "postgres_admin_password" {
#   secret = "sheerwater-postgres-admin-password"
# }







################################################
# Sheerwater: Postgres roles and grants
################################################

# TODO: why are the read and write credentials handled differently
################################################
# Read role for postgres
################################################

data "google_secret_manager_secret_version" "postgres_read_password" {
  secret = "sheerwater-postgres-read-password"
}

resource "postgresql_role" "read" {
  name = "read"
  password = data.google_secret_manager_secret_version.postgres_read_password.secret_data
  login = true
}

################################################
# Write role for postgres
################################################

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


################################################
# Postgres: Public grants
################################################
resource "postgresql_grant" "public_schema_public" {
  database = "postgres"
  role = "public"
  schema = "public"
  object_type = "schema"
  privileges = ["CREATE", "USAGE"]
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

resource "postgresql_default_privileges" "read_only_default_admin" {
  database = "postgres"
  role = postgresql_role.read.name
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

################################################
# Postgres: Write role grants
################################################
resource "postgresql_grant" "write_database_public" {
  database = "postgres"
  role = postgresql_role.write.name
  schema = "public"
  object_type = "database"
  privileges = ["CREATE"]
}

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
  privileges = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"]
}

################################################
# Terracotta: Read role grants
################################################
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

resource "postgresql_grant" "readonly_public_terracotta" {
  database = "terracotta"
  role = postgresql_role.read.name
  schema = "public"
  object_type = "table"
  privileges = ["SELECT"]
}

################################################
# Terracotta: Write role grants
################################################
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
