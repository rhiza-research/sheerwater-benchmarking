The infrastructure for sheerwater benchmarking

./docker - any custom docker containers we may need for this project
./helm - the helm chart to deploy all the kubernetes manifests
./terraform - all of the terraform infrastructure. This includes cloud resources and the deployed of k8s services
./terraform-config - the configuration of deployed services (postgres users, grafana dashboards, etc)
./dashboards - contains grafana dashboards


# Notes:

## Updating a disk resource

After updating a disk resource the pvc and pv will need to be deleted. The following commands can be executed in the gcloud shell to delete the pvc and pv. In this example we are deleting the postgres pvc and pv.

```bash
# in gcloud shell
gcloud container clusters get-credentials rhiza-cluster --zone us-central1-a --project rhiza-shared

# list pvc
# kubectl get pvc -A
# get the pvc name and namespace you want to delete
pvc=sheerwater-benchmarking-postgres
namespace=sheerwater-benchmarking

# delete pvc
kubectl patch pvc $pvc -p '{"metadata":{"finalizers":null}}' --namespace $namespace 
kubectl delete persistentvolumeclaim $pvc --namespace $namespace --grace-period=0 --force

# list pv
# kubectl get pv
# get the pv name you want to delete
pv=sheerwater-benchmarking-postgres-pv-sheerwater-benchmarking

# delete pv
kubectl patch pv $pv -p '{"metadata":{"finalizers":null}}'
kubectl delete persistentvolume $pv --grace-period=0 --force
```
