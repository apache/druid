---
layout: doc_page
---

# Microsoft Azure

To use this extension, make sure to [include](../../operations/including-extensions.html) `druid-azure-extensions` extension.

## Deep Storage

[Microsoft Azure Storage](http://azure.microsoft.com/en-us/services/storage/) is another option for deep storage. This requires some additional druid configuration.

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

#### StaticAzureBlobStoreFirehose

This firehose ingests events, similar to the StaticS3Firehose, but from an Azure Blob Store.

Data is newline delimited, with one JSON object per line and parsed as per the `InputRowParser` configuration.

The storage account is shared with the one used for Azure deep storage functionality, but blobs can be in a different container.

As with the S3 blobstore, it is assumed to be gzipped if the extension ends in .gz

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

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be "static-azure-blobstore".|N/A|yes|
|blobs|JSON array of [Azure blobs](https://msdn.microsoft.com/en-us/library/azure/ee691964.aspx).|N/A|yes|

Azure Blobs:

|property|description|default|required?|
|--------|-----------|-------|---------|
|container|Name of the azure [container](https://azure.microsoft.com/en-us/documentation/articles/storage-dotnet-how-to-use-blobs/#create-a-container)|N/A|yes|
|path|The path where data is located.|N/A|yes|
