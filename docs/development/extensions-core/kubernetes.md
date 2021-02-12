---
id: druid-kubernetes
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

To use this extension please make sure to  [include](../../development/extensions.md#loading-extensions) `druid-kubernetes-extensions` as an extension.

This extension works together with HTTP based segment and task management in Druid. Consequently, following configurations must be set on all Druid nodes.

`druid.zk.service.enabled=false`
`druid.serverview.type=http`
`druid.coordinator.loadqueuepeon.type=http`
`druid.indexer.runner.type=httpRemote`
`druid.discovery.type=k8s`

For Node Discovery, Each Druid process running inside a pod "announces" itself by adding few "labels" and "annotations" in the pod spec. Druid process needs to be aware of pod name and namespace which it reads from environment variables `POD_NAME` and `POD_NAMESPACE`. These variable names can be changed, see configuration below. But in the end, each pod needs to have self pod name and namespace added as environment variables.

Additionally, this extension has following configuration.

### Properties
|Property|Possible Values|Description|Default|required|
|--------|---------------|-----------|-------|--------|
|`druid.discovery.k8s.clusterIdentifier`|`string that matches [a-z0-9][a-z0-9-]*[a-z0-9]`|Unique identifier for this Druid cluster in Kubernetes e.g. us-west-prod-druid.|None|Yes|
|`druid.discovery.k8s.podNameEnvKey`|`Pod Env Variable`|Pod Env variable whose value is that pod's name.|POD_NAME|No|
|`druid.discovery.k8s.podNamespaceEnvKey`|`Pod Env Variable`|Pod Env variable whose value is that pod's kubernetes namespace.|POD_NAMESPACE|No|
|`druid.discovery.k8s.coordinatorLeaderElectionConfigMapNamespace`|`k8s namespace`|Leader election algorithm requires creating a ConfigMap resource in a namespace. This MUST only be provided if different coordinator pods run in different namespaces, such setup is discouraged however.|coordinator pod's namespace|No|
|`druid.discovery.k8s.overlordLeaderElectionConfigMapNamespace`|`k8s namespace`|Leader election algorithm requires creating a ConfigMap resource in a namespace. This MUST only be provided if different overlord pods run in different namespaces, such setup is discouraged however.|overlord pod's namespace|No|
|`druid.discovery.k8s.leaseDuration`|`Duration`|Lease duration used by Leader Election algorithm. Candidates wait for this time before taking over previous Leader.|PT60S|No|
|`druid.discovery.k8s.renewDeadline`|`Duration`|Lease renewal period used by Leader.|PT17S|No|
|`druid.discovery.k8s.retryPeriod`|`Duration`|Retry wait used by Leader Election algorithm on failed operations.|PT5S|No|

### Gotchas

- Label/Annotation path in each pod spec MUST EXIST, which is easily satisfied if there is at least one label/annotation in the pod spec already. This limitation may be removed in future.
- Druid Pods need permissions to be able to add labels to self-pod, List and Watch other Pods, create ConfigMap for leader election. Assuming, "default" service account is used by Druid pods, you might need to add following or something similar Kubernetes Role and Role Binding.

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