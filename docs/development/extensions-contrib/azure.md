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


To use this Apache Druid (incubating) extension, make sure to [include](../../development/extensions.md#loading-extensions) `druid-azure-extensions` extension.

## Deep Storage

[Microsoft Azure Storage](http://azure.microsoft.com/en-us/services/storage/) is another option for deep storage. This requires some additional Druid configuration.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.storage.type`|azure||Must be set.|
|`druid.azure.account`||Azure Storage account name.|Must be set.|
|`druid.azure.key`||Azure Storage account key.|Must be set.|
|`druid.azure.container`||Azure Storage container name.|Must be set.|
|`druid.azure.protocol`|http or https||https|
|`druid.azure.maxTries`||Number of tries before cancel an Azure operation.|3|

See [Azure Services](http://azure.microsoft.com/en-us/pricing/free-trial/) for more information.

## Firehose

<a name="firehose"></a>

#### StaticAzureBlobStoreFirehose

This firehose ingests events, similar to the StaticS3Firehose, but from an Azure Blob Store.

Data is newline delimited, with one JSON object per line and parsed as per the `InputRowParser` configuration.

The storage account is shared with the one used for Azure deep storage functionality, but blobs can be in a different container.

As with the S3 blobstore, it is assumed to be gzipped if the extension ends in .gz

This firehose is _splittable_ and can be used by [native parallel index tasks](../../ingestion/native-batch.md#parallel-task).
Since each split represents an object in this firehose, each worker task of `index_parallel` will read an object.

Sample spec:

```json
"firehose" : {
    "type" : "static-azure-blobstore",
    "blobs": [
        {
          "container": "container",
          "path": "/path/to/your/file.json"
        },
        {
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
|type|This should be `static-azure-blobstore`.|N/A|yes|
|blobs|JSON array of [Azure blobs](https://msdn.microsoft.com/en-us/library/azure/ee691964.aspx).|N/A|yes|
|maxCacheCapacityBytes|Maximum size of the cache space in bytes. 0 means disabling cache. Cached files are not removed until the ingestion task completes.|1073741824|no|
|maxFetchCapacityBytes|Maximum size of the fetch space in bytes. 0 means disabling prefetch. Prefetched files are removed immediately once they are read.|1073741824|no|
|prefetchTriggerBytes|Threshold to trigger prefetching Azure objects.|maxFetchCapacityBytes / 2|no|
|fetchTimeout|Timeout for fetching an Azure object.|60000|no|
|maxFetchRetry|Maximum retry for fetching an Azure object.|3|no|

Azure Blobs:

|property|description|default|required?|
|--------|-----------|-------|---------|
|container|Name of the azure [container](https://azure.microsoft.com/en-us/documentation/articles/storage-dotnet-how-to-use-blobs/#create-a-container)|N/A|yes|
|path|The path where data is located.|N/A|yes|
