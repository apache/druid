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

The [`druid-kubernetes-extensions`](../development/extensions-core/kubernetes.md) extension provides a dynamic config provider (`K8sNodeLabelDynamicConfigProvider`) that reads configuration values from the labels of the Kubernetes node that the Druid process runs on. Use it for values that depend on Kubernetes runtime information, for example where a process was scheduled, which cannot be written into a spec ahead of time.

The Kubernetes node label dynamic config provider uses the following syntax:

```json
druid.dynamic.config.provider={"type": "k8sNodeLabel","labels":{"property1": "example.com/label-one","property2": "example.com/label-two"}}
```

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`|String|dynamic config provider type|Yes: `k8sNodeLabel`|
|`labels`|Map|configuration keys to resolve, each mapped to the name of the node label that holds its value|Yes|
|`nodeNameVariable`|String|environment variable that holds the name of the node the process runs on|No (default: `HOST_NODE_NAME`)|

You can use it anywhere Druid accepts a dynamic config provider, for example Kafka [`consumerProperties`](../ingestion/kafka-ingestion.md), Iceberg [`catalogProperties`](../development/extensions-contrib/iceberg.md), and Schema Registry [`config` and `headers`](../ingestion/data-formats.md).

### Prerequisites

Include `druid-kubernetes-extensions` in the [extensions load list](../configuration/extensions.md#loading-extensions) of every service that resolves the spec. For a supervisor spec, that is the Overlord and the Peon services.

Each pod must know the node it runs on. Expose the node name from the downward API as the environment variable named by `nodeNameVariable`, or set `nodeNameVariable` to a variable your pod template already provides:

```yaml
env:
  - name: HOST_NODE_NAME
    valueFrom:
      fieldRef:
        fieldPath: spec.nodeName
```

Nodes are cluster-scoped, so the service account the Druid pods run as needs a ClusterRole granting `get` on `nodes`:

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
  name: default        # the service account your Druid pods use
  namespace: druid     # their namespace
roleRef:
  kind: ClusterRole
  name: druid-node-reader
  apiGroup: rbac.authorization.k8s.io
```

### Behavior

- If the node name variable is unset, the API server cannot be reached or refuses the request, or the node does not carry the label, Druid logs a warning and omits the key. The consumer then applies its own default. A label with an empty value counts as missing. As with other dynamic config providers, a resolved value takes precedence over the same key specified directly.
- Druid reads the node once per process and keeps its labels until the process restarts. A failed lookup is not kept, so it is retried the next time the configuration is resolved.

### Examples

Kafka consumers fetch from the nearest replica when `client.rack` matches a broker's `broker.rack` ([KIP-392](https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A+Allow+consumers+to+fetch+from+closest+replica)). On EKS, `topology.k8s.aws/zone-id` holds the zone ID that MSK uses for `broker.rack`; on GKE, `topology.kubernetes.io/zone` holds the zone name that Google Cloud Managed Service for Apache Kafka uses:

```json
"consumerProperties": {
  "bootstrap.servers": "localhost:9092",
  "druid.dynamic.config.provider": {
    "type": "k8sNodeLabel",
    "labels": { "client.rack": "topology.k8s.aws/zone-id" }
  }
}
```

An Iceberg REST catalog can read from S3 in the task's own region by taking `client.region` from the node's region label:

```json
"icebergCatalog": {
  "type": "rest",
  "catalogUri": "https://iceberg-rest.example.com",
  "catalogProperties": {
    "druid.dynamic.config.provider": {
      "type": "k8sNodeLabel",
      "labels": { "client.region": "topology.kubernetes.io/region" }
    }
  }
}
```
