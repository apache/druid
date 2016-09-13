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
|`druid.hadoop.security.kerberos.principal`|`druid@EXAMPLE.COM`| Principal user name |empty|
|`druid.hadoop.security.kerberos.keytab`|`/etc/security/keytabs/druid.headlessUser.keytab`|Path to keytab file|empty|

If you are using the Hadoop indexer, set your output directory to be a location on Hadoop and it will work.
If you want to eagerly authenticate against a secured hadoop/hdfs cluster you must set `druid.hadoop.security.kerberos.principal` and `druid.hadoop.security.kerberos.keytab`, this is an alternative to the cron job method that runs `kinit` command periodically.  

## Google Cloud Storage

The HDFS extension can also be used for GCS as deep storage.

### Configuration

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.storage.type`|hdfs||Must be set.|
|`druid.storage.storageDirectory`||gs://bucket/example/directory|Must be set.|

All services that need to access GCS need to have the [GCS connector jar](https://cloud.google.com/hadoop/google-cloud-storage-connector#manualinstallation) in their class path. One option is to place this jar in <druid>/lib/ and <druid>/extensions/druid-hdfs-storage/

Tested with Druid 0.9.0, Hadoop 2.7.2 and gcs-connector jar 1.4.4-hadoop2.

