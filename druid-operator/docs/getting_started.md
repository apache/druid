# Installation

The Helm chart is available at the [DataInfra chart repository](https://charts.datainfra.io).

The operator can be deployed in one of the following modes:
- namespace scope (default)
- cluster scope

### Add the Helm repository
```shell
helm repo add datainfra https://charts.datainfra.io
helm repo update
```

### Cluster Scope Installation 
`NOTE:` the default installation restrics the reconciliation on the default and kube-system namespaces 
```bash
# Install Druid operator using Helm
helm -n druid-operator-system upgrade -i --create-namespace cluster-druid-operator datainfra/druid-operator

# ... or generate manifest.yaml to install using other means:
helm -n druid-operator-system template --create-namespace cluster-druid-operator datainfra/druid-operator > manifest.yaml
```

### Custom Namespaces Installation
```bash
# Install Druid operator using Helm
kubectl create ns mynamespace
helm -n druid-operator-system upgrade -i --create-namespace --set env.WATCH_NAMESPACE="mynamespace" namespaced-druid-operator datainfra/druid-operator

# Override the default namespace DENY_LIST
helm -n druid-operator-system upgrade -i --create-namespace --set env.DENY_LIST="kube-system" namespaced-druid-operator datainfra/druid-operator

# ... or generate manifest.yaml to install using other means:
helm -n druid-operator-system template --set env.WATCH_NAMESPACE=""  namespaced-druid-operator datainfra/druid-operator --create-namespace > manifest.yaml
```

### Uninstall
```bash
# To avoid destroying existing clusters, helm will not uninstall its CRD. For 
# complete cleanup annotation needs to be removed first:
kubectl annotate crd druids.druid.apache.org helm.sh/resource-policy-

# This will uninstall operator
helm -n druid-operator-system  uninstall cluster-druid-operator
```

## Deploy a sample Druid cluster
Bellow is an example spec to deploy a tiny Druid cluster.  
For full details on spec please see [Druid CRD API reference](api_specifications/druid.md) 
or the [Druid API code](../apis/druid/v1alpha1/druid_types.go).

```bash
# Deploy single node zookeeper
kubectl apply -f examples/tiny-cluster-zk.yaml

# Deploy druid cluster spec
# NOTE: add a namespace when applying the cluster if you installed the operator with the default DENY_LIST
kubectl apply -f examples/tiny-cluster.yaml
```

`NOTE:` the above tiny-cluster only works on a single node kubernetes cluster (e.g. typical k8s cluster setup for dev 
using kind or minikube) as it uses local disk as "deep storage". Other example specs in the `examples/` directory use distributed "deep storage" and therefore expect to be deployed into a k8s cluster with s3-compatible storage. To bootstrap your k8s cluster with s3-compatible storage, you can run `make helm-minio-install`. See the [Makefile](../Makefile) for more details.


## Debugging Problems

```bash
# get druid-operator pod name
kubectl get po | grep druid-operator

# check druid-operator pod logs
kubectl logs <druid-operator pod name>

# check the druid spec
kubectl describe druids tiny-cluster

# check if druid cluster is deployed
kubectl get svc | grep tiny
kubectl get cm | grep tiny
kubectl get sts | grep tiny
```
