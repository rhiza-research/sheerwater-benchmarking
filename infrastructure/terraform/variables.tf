# ArgoCD Configuration Variables

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