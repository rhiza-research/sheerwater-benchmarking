# ArgoCD Configuration Variables


variable "github_org" {
  description = "GitHub organization or username"
  type        = string
  default     = "rhiza-research"
}

variable "grafana_repo_name" {
  description = "Name of the repository containing Grafana charts"
  type        = string
  default     = "sheerwater-benchmarking"
}

variable "argocd_domain" {
  description = "Domain for ArgoCD UI"
  type        = string
  default     = "argocd.sheerwater.rhizaresearch.org"
}

variable "grafana_domain" {
  description = "Base domain for ephemeral Grafana instances"
  type        = string
  default     = "dev.sheerwater.rhizaresearch.org"
}

variable "grafana_chart_branch" {
  description = "Branch to use for Grafana helm chart (null = use PR branch)"
  type        = string
  default     = null
} 