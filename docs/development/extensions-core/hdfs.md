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

### Configuration for HDFS

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.storage.type`|hdfs||Must be set.|
|`druid.storage.storageDirectory`||Directory for storing segments.|Must be set.|
|`druid.hadoop.security.kerberos.principal`|`druid@EXAMPLE.COM`| Principal user name |empty|
|`druid.hadoop.security.kerberos.keytab`|`/etc/security/keytabs/druid.headlessUser.keytab`|Path to keytab file|empty|

If you are using the Hadoop indexer, set your output directory to be a location on Hadoop and it will work.
If you want to eagerly authenticate against a secured hadoop/hdfs cluster you must set `druid.hadoop.security.kerberos.principal` and `druid.hadoop.security.kerberos.keytab`, this is an alternative to the cron job method that runs `kinit` command periodically.

### Configuration for Google Cloud Storage

The HDFS extension can also be used for GCS as deep storage.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.storage.type`|hdfs||Must be set.|
|`druid.storage.storageDirectory`||gs://bucket/example/directory|Must be set.|

All services that need to access GCS need to have the [GCS connector jar](https://cloud.google.com/hadoop/google-cloud-storage-connector#manualinstallation) in their class path. One option is to place this jar in <druid>/lib/ and <druid>/extensions/druid-hdfs-storage/

Tested with Druid 0.9.0, Hadoop 2.7.2 and gcs-connector jar 1.4.4-hadoop2.

<a name="firehose"></a>

## Native batch ingestion

This firehose ingests events from a predefined list of S3 objects.
This firehose is _splittable_ and can be used by [native parallel index tasks](../../ingestion/native-batch.md#parallel-task).
Since each split represents an HDFS file, each worker task of `index_parallel` will read an object.

Sample spec:

```json
"firehose" : {
    "type" : "hdfs",
    "paths": "/foo/bar,/foo/baz"
}
```

This firehose provides caching and prefetching features. During native batch indexing, a firehose can be read twice if
`intervals` are not specified, and, in this case, caching can be useful. Prefetching is preferred when direct scanning
of files is slow.

|Property|Description|Default|
|--------|-----------|-------|
|type|This should be `hdfs`.|none (required)|
|paths|HDFS paths. Can be either a JSON array or comma-separated string of paths. Wildcards like `*` are supported in these paths.|none (required)|
|maxCacheCapacityBytes|Maximum size of the cache space in bytes. 0 means disabling cache. Cached files are not removed until the ingestion task completes.|1073741824|
|maxFetchCapacityBytes|Maximum size of the fetch space in bytes. 0 means disabling prefetch. Prefetched files are removed immediately once they are read.|1073741824|
|prefetchTriggerBytes|Threshold to trigger prefetching s3 objects.|maxFetchCapacityBytes / 2|
|fetchTimeout|Timeout for fetching each file.|60000|
|maxFetchRetry|Maximum number of retries for fetching each file.|3|
