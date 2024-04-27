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