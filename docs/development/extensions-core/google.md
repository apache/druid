---
id: google
title: "Google Cloud Storage"
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

## Google Cloud Storage Extension

This extension allows you to do 2 things:
* [Ingest data](#reading-data-from-google-cloud-storage) from files stored in Google Cloud Storage.
* Write segments to [deep storage](#deep-storage) in GCS.

To use this Apache Druid extension, make sure to [include](../../development/extensions.md#loading-extensions) `druid-google-extensions` extension.

### Required Configuration

To configure connectivity to google cloud, run druid processes with `GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account_keyfile` in the environment.

### Reading data from Google Cloud Storage

The [Google Cloud Storage input source](../../ingestion/native-batch.md#google-cloud-storage-input-source) is supported by the [Parallel task](../../ingestion/native-batch.md#parallel-task)
to read objects directly from Google Cloud Storage. If you use the [Hadoop task](../../ingestion/hadoop.md),
you can read data from Google Cloud Storage by specifying the paths in your [`inputSpec`](../../ingestion/hadoop.md#inputspec).

Objects can also be read directly from Google Cloud Storage via the [StaticGoogleBlobStoreFirehose](../../ingestion/native-batch.md#staticgoogleblobstorefirehose)

### Deep Storage

Deep storage can be written to Google Cloud Storage either via this extension or the [druid-hdfs-storage extension](../extensions-core/hdfs.md).

#### Configuration

To configure connectivity to google cloud, run druid processes with `GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account_keyfile` in the environment.

|Property|Description|Possible Values|Default|
|--------|---------------|-----------|-------|
|`druid.storage.type`|google||Must be set.|
|`druid.google.bucket`||Google Storage bucket name.|Must be set.|
|`druid.google.prefix`|A prefix string that will be prepended to the blob names for the segments published to Google deep storage| |""|
|`druid.google.maxListingLength`|maximum number of input files matching a given prefix to retrieve at a time| |1024|
