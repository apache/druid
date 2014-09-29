---
layout: doc_page
---
# Deep Storage
Deep storage is where segments are stored.  It is a storage mechanism that Druid does not provide.  This deep storage infrastructure defines the level of durability of your data, as long as Druid nodes can see this storage infrastructure and get at the segments stored on it, you will not lose data no matter how many Druid nodes you lose.  If segments disappear from this storage layer, then you will lose whatever data those segments represented.

The currently supported types of deep storage follow. Other deep-storage options, such as [Cassandra](http://planetcassandra.org/blog/post/cassandra-as-a-deep-storage-mechanism-for-druid-real-time-analytics-engine/), have been developed by members of the community.

## S3-compatible

S3-compatible deep storage is basically either S3 or something like riak-cs which exposes the same API as S3.  This is the default deep storage implementation.

S3 configuration parameters are

```
druid.s3.accessKey=<S3 access key>
druid.s3.secretKey=<S3 secret_key>
druid.storage.bucket=<bucket to store in>
druid.storage.baseKey=<base key prefix to use, i.e. what directory>
```

## HDFS

As of 0.4.0, HDFS can be used for storage of segments as well.  

In order to use hdfs for deep storage, you need to set the following configuration on your real-time nodes.

```
druid.storage.type=hdfs
druid.storage.storageDirectory=<directory for storing segments>
```

If you are using the Hadoop indexer, set your output directory to be a location on Hadoop and it will work


## Local Mount

A local mount can be used for storage of segments as well.  This allows you to use just your local file system or anything else that can be mount locally like NFS, Ceph, etc.

In order to use a local mount for deep storage, you need to set the following configuration on your real-time nodes.

```
druid.storage.type=local
druid.storage.storageDirectory=<directory for storing segments>
```

Note that you should generally set `druid.storage.storageDirectory` to something different from `druid.segmentCache.locations` and `druid.segmentCache.infoDir`.

If you are using the Hadoop indexer in local mode, then just give it a local file as your output directory and it will work.


## Cassandra

[Apache Cassandra](http://www.datastax.com/what-we-offer/products-services/datastax-enterprise/apache-cassandra) can also be leveraged for deep storage.  This requires some additional druid configuration as well as setting up the necessary schema within a Cassandra keystore.

For more information on using Cassandra as deep storage, see [Cassandra Deep Storage](Cassandra-Deep-Storage.html).

