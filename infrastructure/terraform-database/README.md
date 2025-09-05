# Sheerwater Benchmarking Database Config

This terraform module is used to configure the shared databases for the prod and ephemeral grafana instances for the Sheerwater Benchmarking repo.

It is used to create/configure:
- postgres users and roles
- postgres grants
- postgres default privileges

This module is imported and executed by the (private) `rhiza-research/infrastructure.git//infrastructure/terraform/30-database-config` module. 

This module is executed in the `default` terraform workspace.

