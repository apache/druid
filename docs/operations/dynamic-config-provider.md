---
id: dynamic-config-provider
title: "Dynamic Config Providers"
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

Druid relies on dynamic config providers to supply multiple related sets of credentials, secrets, and configurations within a Druid extension. Dynamic config providers are intended to eventually replace [PasswordProvider](./password-provider.md).

By default, Druid includes an environment variable dynamic config provider that supports Kafka consumer configuration in [Kafka ingestion](../ingestion/kafka-ingestion.md).

To develop a custom extension of the `DynamicConfigProvider` interface that is registered at Druid process startup, see [Adding a new DynamicConfigProvider implementation](../development/modules.md#adding-a-new-dynamicconfigprovider-implementation).

## Environment variable dynamic config provider

You can use the environment variable dynamic config provider (`EnvironmentVariableDynamicConfigProvider`) to store passwords or other sensitive information using system environment variables instead of plain text configuration.

The environment variable dynamic config provider uses the following syntax:

```json
druid.dynamic.config.provider={"type": "environment","variables":{"secret1": "SECRET1_VAR","secret2": "SECRET2_VAR"}}
```

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`|String|dynamic config provider type|Yes: `environment`|
|`variables`|Map|environment variables that store the configuration information|Yes|

When using the environment variable config provider, consider the following:
- If you manually specify a configuration key-value pair and use the dynamic config provider for the same key, Druid uses the value from the dynamic config provider.
- If an environment variable is not set, Druid omits its key from the resolved configuration, as if the key had not been listed under `variables`.
- For use in a supervisor spec, environment variables must be available to the system user that runs the Overlord service and that runs the Peon service.

The following example shows how to configure environment variables to store the SSL key and truststore passwords for Kafka.

On the Overlord and Peon machines, set the following environment variables for the system user that runs the Druid services:

```
export SSL_KEY_PASSWORD=mysecretkeypassword
export SSL_KEYSTORE_PASSWORD=mysecretkeystorepassword
export SSL_TRUSTSTORE_PASSWORD=mysecrettruststorepassword
```

When you define the consumer properties in the supervisor spec, use the dynamic config provider to refer to the environment variables:
```
...
   "consumerProperties": {
        "bootstrap.servers": "localhost:9092",
        "ssl.keystore.location": "/opt/kafka/config/kafka01.keystore.jks",
        "ssl.truststore.location": "/opt/kafka/config/kafka.truststore.jks",
        "druid.dynamic.config.provider": {
          "type": "environment",
          "variables": {
            "ssl.key.password": "SSL_KEY_PASSWORD",
            "ssl.keystore.password": "SSL_KEYSTORE_PASSWORD",
            "ssl.truststore.password": "SSL_TRUSTSTORE_PASSWORD"
          }
        }
      },
...
```
When connecting to Kafka, Druid replaces the environment variables with their corresponding values.

## Kubernetes node label dynamic config provider

The [`druid-kubernetes-extensions`](../development/extensions-core/kubernetes.md) extension provides a dynamic config provider (`K8sNodeLabelDynamicConfigProvider`) that reads configuration values from the labels of the Kubernetes node that the Druid process runs on.

Use it for configuration that depends on where a process happens to be scheduled rather than on the spec. An ingestion task is a short-lived pod that can land on any node, so a value such as the availability zone of the node cannot be written into a supervisor spec; it has to be read where the task ends up.

The Kubernetes node label dynamic config provider uses the following syntax:

```json
druid.dynamic.config.provider={"type": "k8sNodeLabel","labels":{"property1": "example.com/label-one","property2": "example.com/label-two"}}
```

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`|String|dynamic config provider type|Yes: `k8sNodeLabel`|
|`labels`|Map|configuration keys to resolve, each mapped to the name of the node label that holds its value|Yes|
|`nodeNameVariable`|String|environment variable that holds the name of the node the process runs on|No (default: `HOST_NODE_NAME`)|

### Prerequisites

Include `druid-kubernetes-extensions` in the [extensions load list](../configuration/extensions.md#loading-extensions) of every service that resolves the configuration. For a supervisor spec, that is the Overlord and the Peon services.

Each pod must be able to name the node it runs on. The downward API supplies the node name, which you expose as the environment variable named by `nodeNameVariable`:

```yaml
env:
  - name: HOST_NODE_NAME
    valueFrom:
      fieldRef:
        fieldPath: spec.nodeName
```

The pod also needs permission to read that node. Nodes are cluster-scoped, so this requires a ClusterRole granting `get` on `nodes`, bound to the service account the Druid pods use:

```yaml
kind: ClusterRole
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: druid-node-reader
rules:
- apiGroups:
  - ""
  resources:
  - nodes
  verbs:
  - get
---
kind: ClusterRoleBinding
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: druid-node-reader
subjects:
- kind: ServiceAccount
  name: default
  namespace: druid
roleRef:
  kind: ClusterRole
  name: druid-node-reader
  apiGroup: rbac.authorization.k8s.io
```

The provider reads the node through the in-cluster API server, using the service account credentials that the kubelet projects into the pod.

### Behavior

When using the Kubernetes node label config provider, consider the following:

- Lookups fail open. If the environment variable is not set, if the pod has no service account credentials, if the API server refuses or does not answer within five seconds, or if the node does not carry the label, Druid logs a warning and omits the key from the resolved configuration, as if the key had not been listed under `labels`. The consumer then applies its own default for that key. Outside Kubernetes this means every key is omitted.
- Druid reads the node once per process and keeps its labels until the process restarts, so a node relabeled afterwards is not picked up. That suits an ingestion task, which is shorter lived than the labels it reads. A lookup that failed is not kept, so a transient error is retried the next time the configuration is resolved.
- A label with an empty value counts as unset. Kubernetes allows a label to mark a node without carrying a value, and there is nothing to configure a property with.

### Example: rack-aware Kafka consumers

Kafka's [KIP-392](https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A+Allow+consumers+to+fetch+from+closest+replica) lets a consumer fetch from the closest replica instead of the partition leader. A consumer opts in by setting `client.rack` to a value that matches the `broker.rack` of the brokers it should prefer. For ingestion tasks, this avoids paying for cross-zone traffic on every fetch.

The rack of a task is the zone of whichever node it was scheduled onto, which the node already carries as a label. Which label depends on the cloud provider, because the managed Kafka services identify zones differently:

```
...
   "consumerProperties": {
        "bootstrap.servers": "localhost:9092",
        "druid.dynamic.config.provider": {
          "type": "k8sNodeLabel",
          "labels": {
            "client.rack": "topology.k8s.aws/zone-id"
          }
        }
      },
...
```

On Amazon EKS, use `topology.k8s.aws/zone-id`. It holds the availability zone ID, such as `usw2-az1`, which is what Amazon MSK reports as `broker.rack`. The zone ID is stable across accounts, whereas the zone name is not: `us-west-2a` refers to a different physical zone in different accounts.

On Google Kubernetes Engine, use `topology.kubernetes.io/zone` instead. It holds the zone name, such as `us-central1-a`, which is what Google Cloud Managed Service for Apache Kafka reports as `broker.rack`.

If the label is missing, `client.rack` stays unset and the consumer fetches from the leader, exactly as it would without this provider.
