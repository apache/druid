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


To use this Apache Druid (incubating) extension, make sure to [include](../../development/extensions.md#loading-extensions) `druid-google-extensions` extension.

## Deep Storage

Deep storage can be written to Google Cloud Storage either via this extension or the [druid-hdfs-storage extension](../extensions-core/hdfs.md).

### Configuration

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.storage.type`|google||Must be set.|
|`druid.google.bucket`||GCS bucket name.|Must be set.|
|`druid.google.prefix`||GCS prefix.|No-prefix|


<a name="input-source"></a>

## Google cloud storage batch ingestion input source

This extension also provides an input source for Druid native batch ingestion to support reading objects directly from Google Cloud Storage. Objects can be specified as list of Google Cloud Storage URI strings. The Google Cloud Storage input source is splittable and can be used by [native parallel index tasks](../../ingestion/native-batch.md#parallel-task), where each worker task of `index_parallel` will read a single object.

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "google",
        "uris": ["gs://foo/bar/file.json", "gs://bar/foo/file2.json"]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "google",
        "prefixes": ["gs://foo/bar", "gs://bar/foo"]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```


```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "google",
        "objects": [
          { "bucket": "foo", "path": "bar/file1.json"},
          { "bucket": "bar", "path": "foo/file2.json"}
        ]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be `google`.|N/A|yes|
|uris|JSON array of URIs where Google Cloud Storage objects to be ingested are located.|N/A|`uris` or `prefixes` must be set|
|prefixes|JSON array of URI prefixes for the locations of Google Cloud Storage objects to be ingested.|N/A|`uris` or `prefixes` must be set|
|objects|JSON array of Google Cloud Storage objects to be ingested.|N/A|`uris` or `prefixes` or `objects` must be set|


Google Cloud Storage object:

|property|description|default|required?|
|--------|-----------|-------|---------|
|bucket|Name of the Google Cloud Storage bucket|N/A|yes|
|path|The path where data is located.|N/A|yes|

## Firehose

<a name="firehose"></a>

#### StaticGoogleBlobStoreFirehose

This firehose ingests events, similar to the StaticS3Firehose, but from an Google Cloud Store.

As with the S3 blobstore, it is assumed to be gzipped if the extension ends in .gz

This firehose is _splittable_ and can be used by [native parallel index tasks](../../ingestion/native-batch.md#parallel-task).
Since each split represents an object in this firehose, each worker task of `index_parallel` will read an object.

Sample spec:

```json
"firehose" : {
    "type" : "static-google-blobstore",
    "blobs": [
        {
          "bucket": "foo",
          "path": "/path/to/your/file.json"
        },
        {
          "bucket": "bar",
          "path": "/another/path.json"
        }
    ]
}
```

This firehose provides caching and prefetching features. In IndexTask, a firehose can be read twice if intervals or
shardSpecs are not specified, and, in this case, caching can be useful. Prefetching is preferred when direct scan of objects is slow.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be `static-google-blobstore`.|N/A|yes|
|blobs|JSON array of Google Blobs.|N/A|yes|
|maxCacheCapacityBytes|Maximum size of the cache space in bytes. 0 means disabling cache. Cached files are not removed until the ingestion task completes.|1073741824|no|
|maxFetchCapacityBytes|Maximum size of the fetch space in bytes. 0 means disabling prefetch. Prefetched files are removed immediately once they are read.|1073741824|no|
|prefetchTriggerBytes|Threshold to trigger prefetching Google Blobs.|maxFetchCapacityBytes / 2|no|
|fetchTimeout|Timeout for fetching a Google Blob.|60000|no|
|maxFetchRetry|Maximum retry for fetching a Google Blob.|3|no|

Google Blobs:

|property|description|default|required?|
|--------|-----------|-------|---------|
|bucket|Name of the Google Cloud bucket|N/A|yes|
|path|The path where data is located.|N/A|yes|
