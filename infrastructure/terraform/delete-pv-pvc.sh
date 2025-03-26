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
