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


To use this Apache Druid (incubating) extension, make sure to [include](../../development/extensions.md#loading-extensions) `druid-cloudfiles-extensions` extension.

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

## Firehose

<a name="firehose"></a>

#### StaticCloudFilesFirehose

This firehose ingests events, similar to the StaticAzureBlobStoreFirehose, but from Rackspace's Cloud Files.

Data is newline delimited, with one JSON object per line and parsed as per the `InputRowParser` configuration.

The storage account is shared with the one used for Rackspace's Cloud Files deep storage functionality, but blobs can be in a different region and container.

As with the Azure blobstore, it is assumed to be gzipped if the extension ends in .gz

This firehose is _splittable_ and can be used by [native parallel index tasks](../../ingestion/native-batch.md#parallel-task).
Since each split represents an object in this firehose, each worker task of `index_parallel` will read an object.

Sample spec:

```json
"firehose" : {
    "type" : "static-cloudfiles",
    "blobs": [
        {
          "region": "DFW"
          "container": "container",
          "path": "/path/to/your/file.json"
        },
        {
          "region": "ORD"
          "container": "anothercontainer",
          "path": "/another/path.json"
        }
    ]
}
```
This firehose provides caching and prefetching features. In IndexTask, a firehose can be read twice if intervals or
shardSpecs are not specified, and, in this case, caching can be useful. Prefetching is preferred when direct scan of objects is slow.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be `static-cloudfiles`.|N/A|yes|
|blobs|JSON array of Cloud Files blobs.|N/A|yes|
|maxCacheCapacityBytes|Maximum size of the cache space in bytes. 0 means disabling cache.|1073741824|no|
|maxCacheCapacityBytes|Maximum size of the cache space in bytes. 0 means disabling cache. Cached files are not removed until the ingestion task completes.|1073741824|no|
|maxFetchCapacityBytes|Maximum size of the fetch space in bytes. 0 means disabling prefetch. Prefetched files are removed immediately once they are read.|1073741824|no|
|fetchTimeout|Timeout for fetching a Cloud Files object.|60000|no|
|maxFetchRetry|Maximum retry for fetching a Cloud Files object.|3|no|

Cloud Files Blobs:

|property|description|default|required?|
|--------|-----------|-------|---------|
|container|Name of the Cloud Files container|N/A|yes|
|path|The path where data is located.|N/A|yes|
