# Deploying Druid On KIND

- In this tutorial, we are going to deploy an Apache Druid cluster on KIND.
- This tutorial can easily run on your local machine.

## Prerequisites
To follow this tutorial you will need:

- The [KIND CLI](https://kind.sigs.k8s.io/) installed.
- The KUBECTL CLI installed.
- Docker up and Running.

## Install Kind Cluster
Create kind cluster on your machine.

```kind create cluster --name druid```

## Install Druid Operator

- Add Helm Repo
```
helm repo add datainfra https://charts.datainfra.io
helm repo update
```

- Install Operator 
```
# Install Druid operator using Helm
helm -n druid-operator-system upgrade -i --create-namespace cluster-druid-operator datainfra/druid-operator
```

## Apply Druid Customer Resource

- This druid CR runs druid without zookeeper, using druid k8s extension.
- MM less deployment.
- Derby for metadata.
- Minio for deepstorage.

- Run ```make  helm-minio-install ```. This will deploy minio using minio operator.

- Once the minio pod is up and running in druid namespace, apply the druid CR.
- ```kubectl apply -f tutorials/druid-on-kind/druid-mmless.yaml -n druid```

Here's a view of the druid namespace.

```
NAMESPACE            NAME                                               READY   STATUS    RESTARTS        AGE
druid                druid-tiny-cluster-brokers-5ddcb655cf-plq6x        1/1     Running   0               2d
druid                druid-tiny-cluster-cold-0                          1/1     Running   0               2d
druid                druid-tiny-cluster-coordinators-846df8f545-9qrsw   1/1     Running   1               2d
druid                druid-tiny-cluster-hot-0                           1/1     Running   0               2d
druid                druid-tiny-cluster-routers-5c9677bf9d-qk9q7        1/1     Running   0               2d
druid                myminio-ss-0-0                                     2/2     Running   0               2d

```

## Access Router Console

- Port forward router
- ```kubectl port-forward svc/druid-tiny-cluster-routers 8088 -n druid```
