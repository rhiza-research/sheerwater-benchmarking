# Sheerwater Benchmarking Infrastructure

This directory contains the infrastructure configuration for the Sheerwater Benchmarking project. The actual deployment and management is handled by ArgoCD ApplicationSets configured in the [rhiza-research/infrastructure](https://github.com/rhiza-research/infrastructure) repository.

## Directory Structure

- **./terraform-config** - Terraform configurations for Grafana instances (organizations, datasources, dashboards, etc.)
- **./terraform-database** - Database configuration module imported by infrastructure repo's `terraform/modules/rhiza-shared/database_config.tf`

## Deployment Workflow

1. **Pull Requests**: PRs labeled with `PR-env` trigger ephemeral environment creation via ArgoCD ApplicationSets
2. **Database Config**: The `terraform-database/` module is executed by the infrastructure repository

## Important Notes

- Infrastructure changes should be coordinated with the main infrastructure repository
- Database configurations in `terraform-database/` are imported and managed centrally

## Operational Notes

### Updating a disk resource

After updating a disk resource in the infrastructure repository, the PVC and PV will need to be deleted. The following commands can be executed to delete the PVC and PV. In this example we are deleting the TimescaleDB PVC and PV.

```bash
# Connect to the cluster
gcloud container clusters get-credentials rhiza-cluster --zone us-central1-a --project rhiza-shared

# Get disk names from Terraform outputs (in infrastructure repo)
cd /path/to/infrastructure/terraform/20-gke-cluster
GRAFANA_DISK=$(terraform output -raw rhiza_shared.grafana_disk_name)
TIMESCALE_DISK=$(terraform output -raw rhiza_shared.timescale_disk_name)
TERRACOTTA_DISK=$(terraform output -raw rhiza_shared.terracotta_disk_name)
NAMESPACE=$(terraform output -raw rhiza_shared.namespace)

# List all PVCs to identify the correct ones
kubectl get pvc -A

# Delete TimescaleDB PVC (example)
pvc=$TIMESCALE_DISK
namespace=$NAMESPACE

kubectl patch pvc $pvc -p '{"metadata":{"finalizers":null}}' --namespace $namespace
kubectl delete persistentvolumeclaim $pvc --namespace $namespace --grace-period=0 --force

# List all PVs to identify the correct one
kubectl get pv

# Delete the corresponding PV (naming pattern: <disk-name>-pv-<namespace>)
pv=${TIMESCALE_DISK}-pv-${NAMESPACE}

kubectl patch pv $pv -p '{"metadata":{"finalizers":null}}'
kubectl delete persistentvolume $pv --grace-period=0 --force
```

**Note**: Disk resources are managed in the [rhiza-research/infrastructure](https://github.com/rhiza-research/infrastructure) repository under `terraform/modules/rhiza-shared/`. Use the Terraform outputs to get the exact disk names:
- `terraform output rhiza_shared.grafana_disk_name` - Production Grafana disk
- `terraform output rhiza_shared.timescale_disk_name` - TimescaleDB disk
- `terraform output rhiza_shared.terracotta_disk_name` - Terracotta disk
