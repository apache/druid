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

# Features

- [Deny List in Operator](#deny-list-in-operator)
- [Reconcile Time in Operator](#reconcile-time-in-operator)
- [Finalizer in Druid CR](#finalizer-in-druid-cr)
- [Deletion of Orphan PVCs](#deletion-of-orphan-pvcs)
- [Rolling Deploy](#rolling-deploy)
- [Force Delete of Sts Pods](#force-delete-of-sts-pods)
- [Horizontal Scaling of Druid Pods](#horizontal-scaling-of-druid-pods)
- [Volume Expansion of Druid Pods Running As StatefulSets](#volume-expansion-of-druid-pods-running-as-statefulsets)
- [Add Additional Containers to Druid Pods](#add-additional-containers-to-druid-pods)
- [Default Yet Configurable Probes](#default-yet-configurable-probes)


## Deny List in Operator
There may be use cases where we want the operator to watch all namespaces except a few 
(might be due to security, testing flexibility, etc. reasons).  
Druid operator supports such cases - in the chart, edit `env.DENY_LIST` to be a comma-seperated list.  
For example: "default,kube-system"

## Reconcile Time in Operator
As per operator pattern, *the druid operator reconciles every 10s* (default reconciliation time) to make sure 
the desired state (in that case, the druid CR's spec) is in sync with the current state.  
The reconciliation time can be adjusted - in the chart, add `env.RECONCILE_WAIT` to be a duration
in seconds.  
Examples: "10s", "30s", "120s"

## Finalizer in Druid CR
The Druid operator supports provisioning of StatefulSets and Deployments. When a StatefulSet is created, 
a PVC is created along. When the Druid CR is deleted, the StatefulSet controller does not delete the PVC's 
associated with it.  
In case the PVC data is important and you wish to reclaim it, you can enable: `DisablePVCDeletionFinalizer: true`
in the Druid CR.  
The default behavior is to trigger finalizers and pre-delete hooks that will be executed. They will first clean up the 
StatefulSet and then the PVCs referenced to it. That means that after a 
deletion of a Druid CR, any PVCs provisioned by a StatefulSet will be deleted.

## Deletion of Orphan PVCs
There are some use-cases (the most popular is horizontal auto-scaling) where a StatefulSet scales down. In that case,
the statefulSet will terminate its owned pods but nit their attached PVCs which left orphaned and unused.  
The operator support the ability to auto delete these PVCs. This can be enabled by setting `deleteOrphanPvc: true`.
⚠️ This feature is enabled by default.

## Rolling Deploy
The operator supports Apache Druid's recommended rolling updates. It will do incremental updates in the order
specified in Druid's [documentation](https://druid.apache.org/docs/latest/operations/rolling-updates.html).  
In case any of the node goes in pending/crashing state during an update, the operator halts the update and does
not continue with the update - this will require a manual intervention.  
Default updates are done in parallel. Since cluster creation does not require a rolling update, they will be done
in parallel anyway. To enable this feature, set `rollingDeploy: true` in the Druid CR.
⚠️ This feature is enabled by default.

## Force Delete of Sts Pods
During upgradeS, if THE StatefulSet is set to `OrderedReady` - the StatefulSet controller will not recover from 
crash-loopback state. The issues is referenced [here](https://github.com/kubernetes/kubernetes/issues/67250). 
Documentation reference: [doc](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#forced-rollback)
The operator solves this by using the `forceDeleteStsPodOnError` key, the operator will delete the sts pod if its in 
crash-loopback state.  
Example scenario: During upgrade, user rolls out a faulty configuration causing the historical pod going in crashing 
state. Then, the user rolls out a valid configuration - the new configuration will not be applied unless user manually 
delete the pods. To solve this scenario, the operator will delete the pod automatically without user intervention.  

```
NOTE: User must be aware of this feature, there might be cases where crash-loopback might be caused due probe failure, 
fault image etc, the operator will keep on deleting on each re-concile loop. Default Behavior is True.
```

## Horizontal Scaling of Druid Pods
The operator supports the `HPA autosaling/v2` specification in the `nodeSpec` for druid nodes. In case an HPA deployed, 
the HPA controller maintains the replica count/state for the particular workload referenced.  
Refer to `examples.md` for HPA configuration. 

```
NOTE: This option in currently prefered to scale only brokers using HPA. In order to scale Middle Managers with HPA, 
its recommended not to use HPA. Refer to these discussions which have adderessed the issues in details:
```
1. <https://github.com/apache/druid/issues/8801#issuecomment-664020630>
2. <https://github.com/apache/druid/issues/8801#issuecomment-664648399>

## Volume Expansion of Druid Pods Running As StatefulSets
```
NOTE: This feature has been tested only on cloud environments and storage classes which have supported volume expansion.
This feature uses cascade=orphan strategy to make sure that only the StatefulSet is deleted and recreated and pods 
are not deleted.
```
Druid Nodes (specifically historical nodes) run as StatefulSets. Each StatefulSet replica has a PVC attached. The 
`NodeSpec` in Druid CR has the key `volumeClaimTemplates` where users can define the PVC's storage class as well 
as size. Currently, in Kubernetes, in case a user wants to increase the size in the node, the StatefulSets cannot 
be directly updated. The Druid operator can perform a seamless update of the StatefulSet, and patch the 
PVCs with the desired size defined in the druid CR. Behind the scenes, the operator performs a cascade deletion of the 
StatefulSet, and patches the PVC. Cascade deletion has no affect to the pods running (queries are served and no 
downtime is experienced).  
While enabling this feature, the operator will check if volume expansion is supported in the storage class mentioned 
in the druid CR, only then will it perform expansion. 
This feature is disabled by default. To enable it set `scalePvcSts: true` in the Druid CR.
By default, this feature is disabled.
```
IMPORTANT: Shrinkage of pvc's isnt supported - desiredSize cannot be less than currentSize as well as counts. 
```

## Add Additional Containers to Druid Pods
The operator supports adding additional containers to run along with the druid pods. This helps support co-located, 
co-managed helper processes for the primary druid application. This can be used for init containers, sidecars, 
proxies etc.  
To enable this features users just need to add new containers to the `AdditionalContainers` in the Druid spec API.
There are two scopes you can add additional container:
  - Cluster scope: Under `spec.additionalContainer` which means that additional containers will be common to all the nodes.
  - Node scope: Under `spec.nodes[NODE_TYPE].additionalContainer` which means that additional containers will be common to all the pods whithin a specific node group.

## Default Yet Configurable Probes
The operator create the Deployments and StatefulSets with a default set of probes for each druid components.
These probes can be overriden by adding one of the probes in the `DruidSpec` (global) or under the
`NodeSpec` (component-scope).

This feature is enabled by default.

:warning: Disable this feature by settings : `defaultProbes: false` if you have the `kubernetes-overlord-extensions` enabled also named [middle manager less druid in k8s](https://druid.apache.org/docs/latest/development/extensions-contrib/k8s-jobs/)
more details are described here: https://github.com/datainfrahq/druid-operator/issues/97#issuecomment-1687048907

All the probes definitions are documented bellow:

<details>

<summary>Coordinator, Overlord, MiddleManager, Router and Indexer probes</summary>

```yaml
  livenessProbe:
    failureThreshold: 10
    httpGet:
      path: /status/health
      port: $druid.port
    initialDelaySeconds: 5
    periodSeconds: 10
    successThreshold: 1
    timeoutSeconds: 5
  readinessProbe:
    failureThreshold: 10
    httpGet:
      path: /status/health
      port: $druid.port
    initialDelaySeconds: 5
    periodSeconds: 10
    successThreshold: 1
    timeoutSeconds: 5
  startupProbe:
    failureThreshold: 10
    httpGet:
      path: /status/health
      port: $druid.port
    initialDelaySeconds: 5
    periodSeconds: 10
    successThreshold: 1
    timeoutSeconds: 5
```

</details>

<details>

<summary>Broker probes </summary>

  ```yaml
      livenessProbe:
        failureThreshold: 10
        httpGet:
          path: /status/health
          port: $druid.port
        initialDelaySeconds: 5
        periodSeconds: 10
        successThreshold: 1
        timeoutSeconds: 5
      readinessProbe:
        failureThreshold: 20
        httpGet:
          path: /druid/broker/v1/readiness
          port: $druid.port
        initialDelaySeconds: 5
        periodSeconds: 10
        successThreshold: 1
        timeoutSeconds: 5
      startupProbe:
        failureThreshold: 20
        httpGet:
          path: /druid/broker/v1/readiness
          port: $druid.port
        initialDelaySeconds: 5
        periodSeconds: 10
        successThreshold: 1
        timeoutSeconds: 5
  ```
</details>

<details>

<summary>Historical probes</summary>

```yaml
  livenessProbe:
    failureThreshold: 10
    httpGet:
      path: /status/health
      port: $druid.port
    initialDelaySeconds: 5
    periodSeconds: 10
    successThreshold: 1
    timeoutSeconds: 5
  readinessProbe:
    failureThreshold: 20
    httpGet:
      path: /druid/historical/v1/readiness
      port: $druid.port
    initialDelaySeconds: 5
    periodSeconds: 10
    successThreshold: 1
    timeoutSeconds: 5
  startUpProbe:
    failureThreshold: 20
    httpGet:
      path: /druid/historical/v1/readiness
      port: $druid.port
    initialDelaySeconds: 180
    periodSeconds: 30
    successThreshold: 1
    timeoutSeconds: 10
```

</details>

## Dynamic Configurations

The Druid operator now supports specifying dynamic configurations directly within the Druid manifest. This feature allows for fine-tuned control over Druid's behavior at runtime by adjusting configurations dynamically.


### Overlord Dynamic Configurations

Usage: Add overlord dynamic configurations under the middlemanagers section within the nodes element of the Druid manifest.

<details>

<summary>Overlord Dynamic Configurations</summary>

```yaml
spec:
  nodes:
    middlemanagers:
      dynamicConfig:
        type: default
        selectStrategy:
          type: fillCapacityWithCategorySpec
          workerCategorySpec:
            categoryMap: {}
            strong: true
        autoScaler: null
```

</details>

### Coordinator Dynamic Configurations

Adjust coordinator settings to optimize data balancing and segment management.

Usage: Include coordinator dynamic configurations in the coordinator section within the nodes element of the Druid manifest.

Ensure all parameters are supported for the operator to properly configure dynamic configurations.

<details>

<summary>Overlord Dynamic Configurations</summary>

```yaml
spec:
  nodes:
    coordinators:
      dynamicConfig:
        millisToWaitBeforeDeleting: 900000
        mergeBytesLimit: 524288000
        mergeSegmentsLimit: 100
        maxSegmentsToMove: 5
        replicantLifetime: 15
        replicationThrottleLimit: 10
        balancerComputeThreads: 1
        killDataSourceWhitelist: []
        killPendingSegmentsSkipList: []
        maxSegmentsInNodeLoadingQueue: 100
        decommissioningNodes: []
        pauseCoordination: false
        replicateAfterLoadTimeout: false
        useRoundRobinSegmentAssignment: true
```

</details>

## nativeSpec Ingestion Configuration

The `nativeSpec` feature in the Druid Ingestion Operator provides a flexible and robust way to define ingestion specifications directly within Kubernetes manifests using YAML format. This enhancement allows users to leverage Kubernetes-native formats, facilitating easier integration with Kubernetes tooling and practices while offering a more readable and maintainable configuration structure.

### Key Benefits

* **Kubernetes-Native Integration:** By using YAML, the `nativeSpec` aligns with Kubernetes standards, enabling seamless integration with Kubernetes-native tools and processes, such as kubectl, Helm, and GitOps workflows.
* **Improved Readability and Maintainability:** YAML's human-readable format makes it easier for operators and developers to understand and modify ingestion configurations without deep JSON knowledge or tools.
* **Enhanced Configuration Management:** Leveraging YAML facilitates the use of environment-specific configurations and overrides, making it easier to manage configurations across different stages of deployment (e.g., development, staging, production).

### Usage

Specifying nativeSpec in Kubernetes Manifests

To use `nativeSpec`, define your ingestion specifications in YAML format under the `nativeSpec` field in the Druid Ingestion Custom Resource Definition (CRD). This field supercedes the traditional JSON `spec` field, providing a more integrated approach to configuration management.

<details>

<summary>nativeSpec Example</summary>

```yaml
apiVersion: druid.apache.org/v1alpha1
kind: DruidIngestion
metadata:
  labels:
    app.kubernetes.io/name: druidingestion
    app.kubernetes.io/instance: druidingestion-sample
  name: kafka-1
spec:
  suspend: false
  druidCluster: example-cluster
  ingestion:
    type: kafka
    nativeSpec:
      type: kafka
      spec:
        dataSchema:
          dataSource: metrics-kafka-1
          timestampSpec:
            column: timestamp
            format: auto
          dimensionsSpec:
            dimensions: []
            dimensionExclusions:
            - timestamp
            - value
          metricsSpec:
          - name: count
            type: count
          - name: value_sum
            fieldName: value
            type: doubleSum
          - name: value_min
            fieldName: value
            type: doubleMin
          - name: value_max
            fieldName: value
            type: doubleMax
          granularitySpec:
            type: uniform
            segmentGranularity: HOUR
            queryGranularity: NONE
        ioConfig:
          topic: metrics
          inputFormat:
            type: json
          consumerProperties:
            bootstrap.servers: localhost:9092
          taskCount: 1
          replicas: 1
          taskDuration: PT1H
        tuningConfig:
          type: kafka
          maxRowsPerSegment: 5000000

```

</details>

## Set Rules and Compaction in DruidIngestion

### Rules

Rules in Druid define automated behaviors such as data retention, load balancing, or replication. They can be configured in the Rules section of the `DruidIngestion` CRD.

<details>

<summary>Rules Example</summary>

```yaml
apiVersion: druid.apache.org/v1alpha1
kind: DruidIngestion
metadata:
  name: example-druid-ingestion
spec:
  ingestion:
    type: native-batch
    rules:
      - type: "loadForever"
        tieredReplicants:
          _default_tier: 2
      - type: "dropByPeriod"
        period: "P7D"
```

</details>

### Compaction

Compaction in Druid helps optimize data storage and query performance by merging smaller data segments into larger ones. The compaction configuration can be specified in the Compaction section of the DruidIngestion CRD.

The Druid Operator ensures accurate application of compaction settings by:

1. Retrieving Current Settings: It performs a GET request on the Druid API to fetch existing compaction settings.

2. Comparing and Updating: If there is a discrepancy between current and desired settings specified in the Kubernetes CRD manifest, the operator updates Druid with the desired configuration.

3. Ensuring Accuracy: This method ensures settings are correctly applied, addressing cases where Druid might return a 200 HTTP status code without saving the changes.

<details>

<summary>Compaction Example</summary>

```yaml
apiVersion: druid.apache.org/v1alpha1
kind: DruidIngestion
metadata:
  name: example-druid-ingestion
spec:
  ingestion:
    type: native-batch
    compaction:
      ioConfig:
        type: "index_parallel"
        inputSpec:
          type: "dataSource"
          dataSource: "my-data-source"
      tuningConfig:
        maxNumConcurrentSubTasks: 4
      granularitySpec:
        segmentGranularity: "day"
        queryGranularity: "none"
        rollup: false
      taskPriority: "high"
      taskContext: '{"priority": 75}'
```

</details>
