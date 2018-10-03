---
layout: doc_page
---

# Google Cloud Storage

To use this extension, make sure to [include](../../operations/including-extensions.html) `druid-google-extensions` extension.

## Deep Storage

Deep storage can be written to Google Cloud Storage either via this extension or the [druid-hdfs-storage extension](../extensions-core/hdfs.html).

### Configuration

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.storage.type`|google||Must be set.|
|`druid.google.bucket`||GCS bucket name.|Must be set.|
|`druid.google.prefix`||GCS prefix.|Must be set.|


## Firehose

#### StaticGoogleBlobStoreFirehose

This firehose ingests events, similar to the StaticS3Firehose, but from an Google Cloud Store.

As with the S3 blobstore, it is assumed to be gzipped if the extension ends in .gz

This firehose is _splittable_ and can be used by [native parallel index tasks](../../ingestion/native_tasks.html#parallel-index-task).
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

