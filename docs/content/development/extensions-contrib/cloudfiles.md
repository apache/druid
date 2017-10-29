---
layout: doc_page
---

# Rackspace Cloud Files

## Deep Storage

[Rackspace Cloud Files](http://www.rackspace.com/cloud/files/) is another option for deep storage. This requires some additional druid configuration.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.storage.type`|cloudfiles||Must be set.|
|`druid.storage.region`||Rackspace Cloud Files region.|Must be set.|
|`druid.storage.container`||Rackspace Cloud Files container name.|Must be set.|
|`druid.storage.basePath`||Rackspace Cloud Files base path to use in the container.|Must be set.|
|`druid.storage.operationMaxRetries`||Number of tries before cancel a Rackspace operation.|10|
|`druid.cloudfiles.userName`||Rackspace Cloud username|Must be set.|
|`druid.cloudfiles.apiKey`||Rackspace Cloud api key.|Must be set.|
|`druid.cloudfiles.provider`|rackspace-cloudfiles-us,rackspace-cloudfiles-uk|Name of the provider depending on the region.|Must be set.|
|`druid.cloudfiles.useServiceNet`|true,false|Whether to use the internal service net.|true|

## Firehose

#### StaticCloudFilesFirehose

This firehose ingests events, similar to the StaticAzureBlobStoreFirehose, but from Rackspace's Cloud Files.

Data is newline delimited, with one JSON object per line and parsed as per the `InputRowParser` configuration.

The storage account is shared with the one used for Racksapce's Cloud Files deep storage functionality, but blobs can be in a different region and container.

As with the Azure blobstore, it is assumed to be gzipped if the extension ends in .gz

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
