---
layout: doc_page
---

# HDFS

Make sure to [include](../../operations/including-extensions.html) `druid-hdfs-storage` as an extension.

## Deep Storage 

### Configuration

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.storage.type`|hdfs||Must be set.|
|`druid.storage.storageDirectory`||Directory for storing segments.|Must be set.|

If you are using the Hadoop indexer, set your output directory to be a location on Hadoop and it will work
