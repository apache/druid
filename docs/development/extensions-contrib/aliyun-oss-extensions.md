---
id: aliyun-oss
title: "Aliyun OSS"
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


To use this Apache Druid extension, make sure to [include](../../development/extensions.md#loading-extensions) `aliyun-oss-extensions` extension.

## Deep Storage

[Aliyun](https://www.aliyun.com) is the 3rd largest cloud infrastructure provider in the world. It provides its own storage solution known as OSS, [Object Storage Service](https://www.aliyun.com/product/oss). 

To use aliyun OSS as deep storage, first config as below

|Property|Description|Possible Values|Default|
|--------|---------------|-----------|-------|
|`druid.oss.accessKey`|the `AccessKey ID` of your account which can be used to access the bucket| |Must be set.|
|`druid.oss.secretKey`|the `AccessKey Secret` of your account which can be used to access the bucket| |Must be set. |
|`druid.oss.endpoint`|the endpoint url of your OSS storage| |Must be set.|

if you want to use OSS as deep storage, use the configurations below

|Property|Description|Possible Values|Default|
|--------|---------------|-----------|-------|
|`druid.storage.type`| Global deep storage provider. Must be set to `oss` to make use of this extension. | oss |Must be set.|
|`druid.storage.oss.bucket`|storage bucket name.| | Must be set.|
|`druid.storage.oss.prefix`|a prefix string prepended to the file names for the segments published to aliyun OSS deep storage| druid/segments | |

To save index logs to OSS, apply the configurations below:

|Property|Description|Possible Values|Default|
|--------|---------------|-----------|-------|
|`druid.indexer.logs.type`| Global deep storage provider. Must be set to `oss` to make use of this extension. | oss |Must be set.|
|`druid.indexer.logs.oss.bucket`|the bucket used to keep logs| |Must be set.|
|`druid.indexer.logs.oss.prefix`|a prefix string prepended to the log files.| | |
