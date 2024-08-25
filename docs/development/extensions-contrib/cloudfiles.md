---
id: cloudfiles
title: "Rackspace Cloud Files"
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


To use this Apache Druid extension, [include](../../configuration/extensions.md#loading-extensions) `druid-cloudfiles-extensions` in the extensions load list.

## Deep Storage

[Rackspace Cloud Files](http://www.rackspace.com/cloud/files/) is another option for deep storage. This requires some additional Druid configuration.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.storage.type`|cloudfiles||Must be set.|
|`druid.storage.region`||Rackspace Cloud Files region.|Must be set.|
|`druid.storage.container`||Rackspace Cloud Files container name.|Must be set.|
|`druid.storage.basePath`||Rackspace Cloud Files base path to use in the container.|Must be set.|
|`druid.storage.operationMaxRetries`||Number of tries before cancel a Rackspace operation.|10|
|`druid.cloudfiles.userName`||Rackspace Cloud username|Must be set.|
|`druid.cloudfiles.apiKey`||Rackspace Cloud API key.|Must be set.|
|`druid.cloudfiles.provider`|rackspace-cloudfiles-us,rackspace-cloudfiles-uk|Name of the provider depending on the region.|Must be set.|
|`druid.cloudfiles.useServiceNet`|true,false|Whether to use the internal service net.|true|
