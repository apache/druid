---
id: k8s-jobs
title: "MM-less Druid in K8s"
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

Consider this an [EXPERIMENTAL](../experimental.md) feature mostly because it has not been tested yet on a wide variety of long-running Druid clusters.

Apache Druid Extension to enable using Kubernetes for launching and managing tasks instead of the Middle Managers.  This extension allows you to launch tasks as K8s jobs removing the need for your middle manager.  

## How it works

It takes the podSpec of your `Overlord`pod and creates a kubernetes job from this podSpec.  Thus if you have sidecars such as splunk, hubble, istio it can optionally launch a task as a k8s job.  All jobs are natively restorable, they are decopled from the druid deployment, thus restarting pods or doing upgrades has no affect on tasks in flight.  They will continue to run and when the overlord comes back up it will start tracking them again.  

## Configuration

To use this extension please make sure to  [include](../extensions.md#loading-extensions)`druid-kubernetes-overlord-extensions` in the extensions load list for your overlord process.

The extension uses the task queue to limit how many concurrent tasks (k8s jobs) are in flight so it is required you have a reasonable value for `druid.indexer.queue.maxSize`.  Additionally set the variable `druid.indexer.runner.namespace` to the namespace in which you are running druid.

Other configurations required are: 
`druid.indexer.runner.type: k8s` and `druid.indexer.task.enableTaskLevelLogPush: true`

You can add optional labels to your k8s jobs / pods if you need them by using the following configuration: 
`druid.indexer.runner.labels: '{"key":"value"}'`

All other configurations you had for the middle manager tasks must be moved under the overlord with one caveat, you must specify javaOpts as an array: 
`druid.indexer.runner.javaOptsArray`, `druid.indexer.runner.javaOpts` is no longer supported.

If you are running without a middle manager you need to also use `druid.processing.intermediaryData.storage.type=deepstore`

Additional Configuration

### Properties
|Property|Possible Values|Description|Default|required|
|--------|---------------|-----------|-------|--------|
|`druid.indexer.runner.debugJobs`|`boolean`|Clean up k8s jobs after tasks complete.|False|No|
|`druid.indexer.runner.sidecarSupport`|`boolean`|If your overlord pod has sidecars, this will attempt to start the task with the same sidecars as the overlord pod.|False|No|
|`druid.indexer.runner.kubexitImage`|`String`|Used kubexit project to help shutdown sidecars when the main pod completes.  Otherwise jobs with sidecars never terminate.|karlkfi/kubexit:latest|No|
|`druid.indexer.runner.maxTaskDuration`|`Duration`|Max time a task is allowed to run for before getting killed|4H|No|
|`druid.indexer.runner.taskCleanupDelay`|`Duration`|How long do jobs stay around before getting reaped from k8s|2D|No|
|`druid.indexer.runner.taskCleanupInterval`|`Duration`|How often to check for jobs to be reaped|10m|No|
|`druid.indexer.runner.k8sjobLaunchTimeout`|`Duration`|How long to wait to launch a k8s task before marking it as failed, on a resource constrained cluster it may take some time.|1H|No|
|`druid.indexer.runner.javaOptsArray`|`Duration`|java opts for the task.|-Xmx1g|No|
|`druid.indexer.runner.graceTerminationPeriodSeconds`|`Long`|Number of seconds you want to wait after a sigterm for container lifecycle hooks to complete.  Keep at a smaller value if you want tasks to hold locks for shorter periods.|30s (k8s default)|No|

### Gotchas

- You must have in your role the abiliity to launch jobs.  
- All Druid Pods belonging to one Druid cluster must be inside same kubernetes namespace.

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
  - jobs
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
