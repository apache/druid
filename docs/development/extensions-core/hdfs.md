---
id: hdfs
title: "HDFS"
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


To use this Apache Druid (incubating) extension, make sure to [include](../../development/extensions.md#loading-extensions) `druid-hdfs-storage` as an extension.

## Deep Storage

### Configuration

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.storage.type`|hdfs||Must be set.|
|`druid.storage.storageDirectory`||Directory for storing segments.|Must be set.|
|`druid.hadoop.security.kerberos.principal`|`druid@EXAMPLE.COM`| Principal user name |empty|
|`druid.hadoop.security.kerberos.keytab`|`/etc/security/keytabs/druid.headlessUser.keytab`|Path to keytab file|empty|

If you are using the Hadoop indexer, set your output directory to be a location on Hadoop and it will work.
If you want to eagerly authenticate against a secured hadoop/hdfs cluster you must set `druid.hadoop.security.kerberos.principal` and `druid.hadoop.security.kerberos.keytab`, this is an alternative to the cron job method that runs `kinit` command periodically.

## Google Cloud Storage

The HDFS extension can also be used for GCS as deep storage.

### Configuration

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.storage.type`|hdfs||Must be set.|
|`druid.storage.storageDirectory`||gs://bucket/example/directory|Must be set.|

All services that need to access GCS need to have the [GCS connector jar](https://cloud.google.com/hadoop/google-cloud-storage-connector#manualinstallation) in their class path. One option is to place this jar in <druid>/lib/ and <druid>/extensions/druid-hdfs-storage/

Tested with Druid 0.9.0, Hadoop 2.7.2 and gcs-connector jar 1.4.4-hadoop2.
