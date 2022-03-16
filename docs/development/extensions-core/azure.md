---
id: azure
title: "Microsoft Azure"
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


To use this Apache Druid extension, [include](../../development/extensions.md#loading-extensions) `druid-azure-extensions` in the extensions load list.

## Deep Storage

[Microsoft Azure Storage](http://azure.microsoft.com/en-us/services/storage/) is another option for deep storage. This requires some additional Druid configuration.

|Property|Description|Possible Values|Default|
|--------|---------------|-----------|-------|
|`druid.storage.type`|azure||Must be set.|
|`druid.azure.account`||Azure Storage account name.|Must be set.|
|`druid.azure.key`||Azure Storage account key.|Optional. Either set key or sharedAccessStorageToken but not both.|
|`druid.azure.sharedAccessStorageToken`||Azure Shared Storage access token|Optional. Either set key or sharedAccessStorageToken but not both.| 
|`druid.azure.container`||Azure Storage container name.|Must be set.|
|`druid.azure.prefix`|A prefix string that will be prepended to the blob names for the segments published to Azure deep storage| |""|
|`druid.azure.protocol`|the protocol to use|http or https|https|
|`druid.azure.maxTries`|Number of tries before canceling an Azure operation.| |3|
|`druid.azure.maxListingLength`|maximum number of input files matching a given prefix to retrieve at a time| |1024|

See [Azure Services](http://azure.microsoft.com/en-us/pricing/free-trial/) for more information.
