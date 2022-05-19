---
id: password-provider
title: "Password providers"
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


Passwords help secure Apache Druid systems such as the metadata store and the keystore that contains server certificates, and so on.

These passwords have corresponding runtime properties associated with them, for example `druid.metadata.storage.connector.password` corresponds to the metadata store password.

By default users can directly set the passwords in plaintext for runtime properties. For example, `druid.metadata.storage.connector.password=pwd` sets the password to be used by Druid to connect to the metadata store to `pwd`. Alternatively, users can can set passwords as environment variables.

Environment variable passwords allow users to avoid exposing passwords in the `runtime.properties` file. 

You can set an environment variable password as in the following example: 

```json
druid.metadata.storage.connector.password={ "type": "environment", "variable": "METADATA_STORAGE_PASSWORD" }
```

The values are described below.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`|String|password provider type|Yes: `environment`|
|`variable`|String|environment variable to read password from|Yes|

Another option that provides even greater control is to securely fetch passwords at runtime using a custom extension of the `PasswordProvider` interface that is registered at Druid process startup.

For more information, see [Adding a new Password Provider implementation](../development/modules.md#adding-a-new-password-provider-implementation).

To use this implementation, simply set the relevant password runtime property similarly to how was shown for the environment variable password: 

```json
druid.metadata.storage.connector.password={ "type": "<registered_password_provider_name>", "<jackson_property>": "<value>", ... }
```
