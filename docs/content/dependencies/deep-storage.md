---
layout: doc_page
---
# Deep Storage
Deep storage is where segments are stored.  It is a storage mechanism that Druid does not provide.  This deep storage infrastructure defines the level of durability of your data, as long as Druid nodes can see this storage infrastructure and get at the segments stored on it, you will not lose data no matter how many Druid nodes you lose.  If segments disappear from this storage layer, then you will lose whatever data those segments represented.

## Local Mount

A local mount can be used for storage of segments as well.  This allows you to use just your local file system or anything else that can be mount locally like NFS, Ceph, etc.  This is the default deep storage implementation.

In order to use a local mount for deep storage, you need to set the following configuration in your common configs.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.storage.type`|local||Must be set.|
|`druid.storage.storageDirectory`||Directory for storing segments.|Must be set.|

Note that you should generally set `druid.storage.storageDirectory` to something different from `druid.segmentCache.locations` and `druid.segmentCache.infoDir`.

If you are using the Hadoop indexer in local mode, then just give it a local file as your output directory and it will work.

## S3-compatible

S3-compatible deep storage is basically either S3 or something like Google Storage which exposes the same API as S3.

S3 configuration parameters are

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.s3.accessKey`||S3 access key.|Must be set.|
|`druid.s3.secretKey`||S3 secret key.|Must be set.|
|`druid.storage.bucket`||Bucket to store in.|Must be set.|
|`druid.storage.baseKey`||Base key prefix to use, i.e. what directory.|Must be set.|

## HDFS

In order to use hdfs for deep storage, you need to set the following configuration in your common configs.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.storage.type`|hdfs||Must be set.|
|`druid.storage.storageDirectory`||Directory for storing segments.|Must be set.|

If you are using the Hadoop indexer, set your output directory to be a location on Hadoop and it will work
