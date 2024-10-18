The infrastructure for sheerwater benchmarking

./docker - any custom docker containers we may need for this project
./helm - the helm chart to deploy all the kubernetes manifests
./terraform - all of the terraform infrastructure. This includes cloud resources and the deployed of k8s services
./terraform-config - the configuration of deployed services (postgres users, grafana dashboards, etc)
./dashboards - contains grafana dashboards
