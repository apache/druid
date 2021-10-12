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

Dynamic config providers are Apache Druid's core mechanism to supply multiple related sets of credentials, secrets, and configurations with a Druid extension mechanism. It is intened as the eventual replacement of  [PasswordProvider](./password-provider.md).

Druid comes with an environment variable dynamic config provider that supports:
- Kafka consumer configuration in [Kafka ingestion](../development/extensions-core/kafka-ingestion.md)
- Kinesis consumer configuration in [Kafka ngestion](../development/extensions-core/kafka-ingestion.md)


To develop a custom extension of the `DynamicConfigProvider` interface that is registered at Druid process startup, see [Adding a new DynamicConfigProvider implementation](../development/modules.md#adding-a-new-dynamicconfigprovider-implementation).

## Environment variable dynamic config provider

You can use the environment variable dynamic config provider (`EnvironmentVariableDynamicConfigProvider`) to  store passwords or other sensitive information using system environment variables instead of plain text configuration. For example:

```json
druid.some.config.dynamicConfigProvider={"type": "environment","variables":{"secret1": "SECRET1_VAR","secret2": "SECRET2_VAR"}}
```
The values are described below.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`|String|dynamic config provider type|Yes: `environment`|
|`variables`|Map|environment variables to get information from|Yes|

