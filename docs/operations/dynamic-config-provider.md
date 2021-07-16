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

Druid's core mechanism of supplying multiple related set of credentials/secrets/configurations via Druid extension mechanism. Currently, it is only supported for providing Kafka Consumer configuration in [Kafka Ingestion](../development/extensions-core/kafka-ingestion.md).

Eventually this will replace [PasswordProvider](./password-provider.md) 


Users can create custom extension of the `DynamicConfigProvider` interface that is registered at Druid process startup.

For more information, see [Adding a new DynamicConfigProvider implementation](../development/modules.md#adding-a-new-dynamicconfigprovider-implementation).

## Environment variable dynamic config provider

`EnvironmentVariableDynamicConfigProvider` can be used to avoid exposing credentials or other secret information in the configuration files using environment variables. An example to use this `configProvider` is:
```json
druid.some.config.dynamicConfigProvider={"type": "environment","variables":{"secret1": "SECRET1_VAR","secret2": "SECRET2_VAR"}}
```
The values are described below.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`|String|dynamic config provider type|Yes: `environment`|
|`variables`|Map|environment variables to get information from|Yes|

