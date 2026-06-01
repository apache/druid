---
id: kubernetes
title: "Kubernetes"
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

Consider this an [EXPERIMENTAL](../experimental.md) feature mostly because it has not been tested yet on a wide variety of long running Druid clusters.

Apache Druid Extension to enable using Kubernetes API Server for node discovery and leader election. This extension allows Druid cluster deployment on Kubernetes without Zookeeper. It allows running multiple Druid clusters within same Kubernetes Cluster, See `clusterIdentifier` config below.


## Configuration

To use this extension please make sure to  [include](../../configuration/extensions.md#loading-extensions) `druid-kubernetes-extensions` in the extensions load list.

This extension works together with HTTP-based segment and task management in Druid. Consequently, following configurations must be set on all Druid nodes.

`druid.zk.service.enabled=false`
`druid.serverview.type=http`
`druid.indexer.runner.type=httpRemote`
`druid.discovery.type=k8s`

For node discovery, each Druid process running inside a pod "announces" itself by adding labels and annotations to the pod spec. A pod is discoverable by other Druid processes when it has the required labels and annotations and Kubernetes considers the container ready. Without a readiness probe, Kubernetes marks a container as ready the moment the process starts — see [Readiness Probes](#readiness-probes) for why you should configure one.

Each Druid process needs to be aware of its own pod name and namespace, which it reads from environment variables `POD_NAME` and `POD_NAMESPACE`. These variable names can be changed (see configuration below), but every pod must have its own name and namespace available as environment variables.

Additionally, this extension has the following configuration.

### Properties
|Property|Possible Values|Description|Default|required|
|--------|---------------|-----------|-------|--------|
|`druid.discovery.k8s.clusterIdentifier`|`string that matches [a-z0-9][a-z0-9-]*[a-z0-9]`|Unique identifier for this Druid cluster in Kubernetes e.g. us-west-prod-druid.|None|Yes|
|`druid.discovery.k8s.podNameEnvKey`|`Pod Env Variable`|Pod Env variable whose value is that pod's name.|POD_NAME|No|
|`druid.discovery.k8s.podNamespaceEnvKey`|`Pod Env Variable`|Pod Env variable whose value is that pod's kubernetes namespace.|POD_NAMESPACE|No|
|`druid.discovery.k8s.leaseDuration`|`Duration`|Lease duration used by Leader Election algorithm. Candidates wait for this time before taking over previous Leader.|PT60S|No|
|`druid.discovery.k8s.renewDeadline`|`Duration`|Lease renewal period used by Leader.|PT17S|No|
|`druid.discovery.k8s.retryPeriod`|`Duration`|Retry wait used by Leader Election algorithm on failed operations.|PT5S|No|

### Readiness Probes

:::info
Readiness probe configuration directly affects discovery behavior. If a probe is too aggressive (low timeout, low failure threshold), a pod under heavy load could temporarily fail its probe, be removed from discovery, and shift its load onto other pods — potentially causing a cascade. To avoid this, tune your probes to tolerate brief periods of high load.
:::

This extension uses Kubernetes container readiness, in addition to labels and annotations, to decide whether a pod is available for service discovery.

You should configure [readiness probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/) on all Druid pods. Without a readiness probe, Kubernetes marks a container as ready the moment the process starts, which may be before the Druid service is fully initialized and able to handle requests.

A container may become unready if:
* The process is killed or crashes. It will be immediately marked as unready without waiting for the readiness probe to cross its failure threshold.
* The process is alive but not healthy. It will be marked as unready after it fails its readiness probe a configured number of times (`failureThreshold`).

Once marked as unready, a container must pass its readiness probe `successThreshold` times (default: 1) before it is considered ready again and re-enters discovery.

#### Recommendations

The `/status/ready` endpoint is a good candidate for readiness checks, as it indicates whether the Druid node is ready to serve requests. Services use this endpoint to decide if they should announce themselves, so it is a natural choice for the readiness probe. However, you can choose a different endpoint if it better suits your needs.

For Druid processes that have long startup times, consider using a [startup probe](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-startup-probes) so that the readiness probe does not run (and fail) during initialization.

Baseline readiness probe configuration for Druid might look like this. Replace the port value with the port your Druid process listens on (e.g., `8888` for the Router, `8081` for the Broker):

```yaml
readinessProbe:
  httpGet:
    path: /status/ready
    port: 8888
  periodSeconds: 10
  failureThreshold: 3
  timeoutSeconds: 10
```

With this configuration, a pod must fail its readiness check 3 times in a row (30 seconds) before it is marked as not ready. Adjust these values based on your workload and tolerance for routing to temporarily unhealthy pods.

### Gotchas

- The label/annotation path in each pod spec must exist, which is easily satisfied if there is at least one label or annotation in the pod spec already.
- All Druid pods belonging to one Druid cluster must be inside the same Kubernetes namespace.
- All Druid pods need permissions to add labels to their own pod, list and watch other pods, and create and read ConfigMaps for leader election. Assuming the `default` service account is used by Druid pods, you might need to add the following (or similar) Kubernetes Role and RoleBinding.

```
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: druid-cluster
rules:
- apiGroups:
  - ""
  resources:
  - pods
  - configmaps
  verbs:
  - '*'
---
kind: RoleBinding
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: druid-cluster
subjects:
- kind: ServiceAccount
  name: default
roleRef:
  kind: Role
  name: druid-cluster
  apiGroup: rbac.authorization.k8s.io
```
