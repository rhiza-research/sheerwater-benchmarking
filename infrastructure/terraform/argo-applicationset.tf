
# get the "helm_release" "argocd" and "kubernetes_namespace" "argocd"  managed resources from state
data "terraform_remote_state" "shared_state" {
  backend = "gcs"
  config = {
    bucket = "rhiza-terraform-state"
    prefix = "state"
  }
}


# get github_token from kubernetes_secret
data "kubernetes_secret" "github_token" {
  metadata {
    name = "github-token"
  }
}

# TODO: setup grafana_domain dns record


resource "terraform_data" "argocd_helm_release_updated" {
  input = data.terraform_remote_state.shared_state.outputs.argocd_helm_release
}

# Install ApplicationSet for ephemeral Grafana
resource "helm_release" "grafana_applicationset" {
  depends_on = [data.terraform_remote_state.shared_state, data.kubernetes_secret.github_token]

  name      = "grafana-applicationset"
  chart     = "../../charts/applicationset"
  namespace = data.terraform_remote_state.shared_state.outputs.argocd_namespace

  lifecycle {
    replace_triggered_by = [terraform_data.argocd_helm_release_updated]
  }

  values = [
    yamlencode({
      github = {
        org  = "rhiza-research"
        repo = "sheerwater-benchmarking"
        chartBranch = var.grafana_chart_branch  # Set to "main", "develop", etc. to use a specific branch
        tokenSecret = {
          name = data.kubernetes_secret.github_token.metadata[0].name
          key  = "token"
        }
      }
      
      domain = var.grafana_domain
      
      applicationSet = {
        name      = "ephemeral-grafana"
        namespace = data.terraform_remote_state.shared_state.outputs.argocd_namespace
        pollingInterval = 300  # 5 minutes
      }
      
      ephemeral = {
        ingress = {
          enabled     = true
          className   = "nginx"
          annotations = {
            "nginx.ingress.kubernetes.io/rewrite-target" = "/"
            "cert-manager.io/cluster-issuer" = "letsencrypt-prod"
          }
          tls = {
            enabled = true
          }
        }
        resources = {
          limits = {
            cpu    = "500m"
            memory = "512Mi"
          }
          requests = {
            cpu    = "200m"
            memory = "256Mi"
          }
        }
      }
      
      syncPolicy = {
        automated = {
          prune    = true
          selfHeal = true
        }
        syncOptions = [
          "CreateNamespace=true",
          "PrunePropagationPolicy=foreground",
          "PruneLast=true"
        ]
        retry = {
          limit = 5
          backoff = {
            duration    = "5s"
            factor      = 2
            maxDuration = "3m"
          }
        }
      }
      
      # Optional: Enable notifications
      notifications = {
        enabled = false  # Set to true if you want PR comments
        github = {
          authType = "token"  # or "app" for GitHub App
        }
      }
      
      # Optional: Enable webhooks
      webhook = {
        enabled = false  # Set to true for real-time PR detection
        host    = "argocd-webhook.${var.grafana_domain}"
      }
    })
  ]
} 
