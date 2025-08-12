

# get github_token from kubernetes_secret
data "kubernetes_secret" "github_token" {
  metadata {
    name = "github-token"
  }
}

# Get Grafana admin password from Google Secret Manager
data "google_secret_manager_secret_version" "grafana_admin_password" {
  secret = "sheerwater-grafana-admin-password"
  project = "sheerwater"
}

# Google OAuth client secrets from Secret Manager
data "google_secret_manager_secret_version" "google_oauth_client_id" {
  secret  = "sheerwater-oauth-client-id"
  project = "sheerwater"
}

data "google_secret_manager_secret_version" "google_oauth_client_secret" {
  secret  = "sheerwater-oauth-client-secret"
  project = "sheerwater"
}

resource "terraform_data" "argocd_helm_release_updated" {
  input = data.terraform_remote_state.shared_state.outputs.argocd_helm_release
}

# Install ApplicationSet for ephemeral Grafana
resource "helm_release" "grafana_applicationset" {
  depends_on = [data.terraform_remote_state.shared_state, data.kubernetes_secret.github_token]

  name = "grafana-applicationset"
  chart = "../../charts/applicationset"
  namespace = data.terraform_remote_state.shared_state.outputs.argocd_namespace

  lifecycle {
    replace_triggered_by = [terraform_data.argocd_helm_release_updated]
  }

  values = [
    yamlencode({
      github = {
        org = "rhiza-research"
        repo = "sheerwater-benchmarking"
        tokenSecret = {
          name = data.kubernetes_secret.github_token.metadata[0].name
          key = "token"
        }
      }

      # Google OAuth client settings (override via TF vars/secrets)
                oauth = {
            google = {
              client_id = data.google_secret_manager_secret_version.google_oauth_client_id.secret_data
              client_secret = data.google_secret_manager_secret_version.google_oauth_client_secret.secret_data
            }
          }

      # domain is passed to the applicationset here from the infrastructure repo dns record output
      domain = data.terraform_remote_state.shared_state.outputs.grafana_dev_domain

      applicationSet = {
        name = "ephemeral-grafana"
        namespace = data.terraform_remote_state.shared_state.outputs.argocd_namespace
        pollingInterval = 300 # 5 minutes
      }

      ephemeral = {
        # Set admin password from Google Secret Manager
        admin_password = data.google_secret_manager_secret_version.grafana_admin_password.secret_data

        ingress = {
          enabled = true
          className = "nginx"
          annotations = {}
          tls = {
            enabled = true
            secretName = "wildcard-dev-sheerwater-tls"
          }
        }
        resources = {
          limits = {
            cpu = "500m"
            memory = "512Mi"
          }
          requests = {
            cpu = "200m"
            memory = "256Mi"
          }
        }
      }

      syncPolicy = {
        automated = {
          prune = true
          selfHeal = true
        }
        syncOptions = [
          "CreateNamespace = true",
          "PrunePropagationPolicy = foreground",
          "PruneLast = true"
        ]
        retry = {
          limit = 5
          backoff = {
            duration = "5s"
            factor = 2
            maxDuration = "3m"
          }
        }
      }

      # Optional: Enable notifications
      notifications = {
        enabled = false # Set to true if you want PR comments
        github = {
          authType = "token" # or "app" for GitHub App
        }
      }

      # Optional: Enable webhooks
      webhook = {
        enabled = false # Set to true for real-time PR detection
        host = "argocd-webhook.${data.terraform_remote_state.shared_state.outputs.grafana_dev_domain}"
      }
    })
  ]
}
