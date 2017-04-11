---
layout: doc_page
---

# Google Cloud Storage

To use this extension, make sure to [include](../../operations/including-extensions.html) `druid-google-extensions` extension.

## Deep Storage

[Google Cloud Storage](https://cloud.google.com/storage/) is another option for deep storage. This requires some additional druid configuration.

|Property|Description|Default|Required?|
|--------|-----------|-------|---------|
|bucket|Name of the Google Cloud bucket|N/A|yes|
|prefix|The path where data is located.|N/A|yes|

## Firehose

#### StaticGoogleBlobStoreFirehose

This firehose ingests events, similar to the StaticS3Firehose, but from an Google Cloud Store.

As with the S3 blobstore, it is assumed to be gzipped if the extension ends in .gz

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

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be "static-google-blobstore".|N/A|yes|
|blobs|JSON array of Google Blobs.|N/A|yes|

Google Blobs:

|property|description|default|required?|
|--------|-----------|-------|---------|
|bucket|Name of the Google Cloud bucket|N/A|yes|
|path|The path where data is located.|N/A|yes|

