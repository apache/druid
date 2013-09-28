---
layout: doc_page
---
This describes the basic server configuration that is loaded by all the server processes; the same file is loaded by all. See also the json "specFile" descriptions in [Realtime](Realtime.html) and [Batch-ingestion](Batch-ingestion.html).

JVM Configuration Best Practices
================================

There are three JVM parameters that we set on all of our processes:

1.  `-Duser.timezone=UTC` This sets the default timezone of the JVM to UTC. We always set this and do not test with other default timezones, so local timezones might work, but they also might uncover weird and interesting bugs
2.  `-Dfile.encoding=UTF-8` This is similar to timezone, we test assuming UTF-8. Local encodings might work, but they also might result in weird and interesting bugs
3.  `-Djava.io.tmpdir=<a path>` Various parts of the system that interact with the file system do it via temporary files, these files can get somewhat large. Many production systems are setup to have small (but fast) `/tmp` directories, these can be problematic with Druid so we recommend pointing the JVM’s tmp directory to something with a little more meat.

Basic Service Configuration
===========================

Configuration of the various nodes is done via Java properties. These can either be provided as `-D` system properties on the java command line or they can be passed in via a file called `runtime.properties` that exists on the classpath. Note: as a future item, I’d like to consolidate all of the various configuration into a yaml/JSON based configuration files.

The periodic time intervals (like "PT1M") are [ISO8601 intervals](http://en.wikipedia.org/wiki/ISO_8601#Time_intervals)

An example runtime.properties is as follows:

```
# S3 access
com.metamx.aws.accessKey=<S3 access key>
com.metamx.aws.secretKey=<S3 secret_key>

# thread pool size for servicing queries
druid.client.http.connections=30

# JDBC connection string for metadata database
druid.database.connectURI=
druid.database.user=user
druid.database.password=password
# time between polling for metadata database
druid.database.poll.duration=PT1M
druid.database.segmentTable=prod_segments

# Path on local FS for storage of segments; dir will be created if needed
druid.paths.indexCache=/tmp/druid/indexCache
# Path on local FS for storage of segment metadata; dir will be created if needed
druid.paths.segmentInfoCache=/tmp/druid/segmentInfoCache

druid.request.logging.dir=/tmp/druid/log

druid.server.maxSize=300000000000

# ZK quorum IPs
druid.zk.service.host=
# ZK path prefix for Druid-usage of zookeeper, Druid will create multiple paths underneath this znode
druid.zk.paths.base=/druid
# ZK path for discovery, the only path not to default to anything
druid.zk.paths.discoveryPath=/druid/discoveryPath

# the host:port as advertised to clients
druid.host=someHostOrIPaddrWithPort
# the port on which to listen, this port should line up with the druid.host value
druid.port=8080

com.metamx.emitter.logging=true
com.metamx.emitter.logging.level=debug

druid.processing.formatString=processing_%s
druid.processing.numThreads=3


druid.computation.buffer.size=100000000

# S3 dest for realtime indexer
druid.pusher.s3.bucket=
druid.pusher.s3.baseKey=

druid.bard.cache.sizeInBytes=40000000
druid.master.merger.service=blah_blah
```

Configuration groupings
-----------------------

### S3 Access

These properties are for connecting with S3 and using it to pull down segments. In the future, we plan on being able to use other deep storage file systems as well, like HDFS. The file system is actually only accessed by the [Compute](Compute.html), [Realtime](Realtime.html) and [Indexing service](Indexing service.html) nodes.

|Property|Description|Default|
|--------|-----------|-------|
|`com.metamx.aws.accessKey`|The access key to use to access S3.|none|
|`com.metamx.aws.secretKey`|The secret key to use to access S3.|none|
|`druid.pusher.s3.bucket`|The bucket to store segments, this is used by Realtime and the Indexing service.|none|
|`druid.pusher.s3.baseKey`|The base key to use when storing segments, this is used by Realtime and the Indexing service|none|

### JDBC connection

These properties specify the jdbc connection and other configuration around the "segments table" database. The only processes that connect to the DB with these properties are the [Master](Master.html) and [Indexing service](Indexing-service.html). This is tested on MySQL.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.database.connectURI`|The jdbc connection uri|none|
|`druid.database.user`|The username to connect with|none|
|`druid.database.password`|The password to connect with|none|
|`druid.database.poll.duration`|The duration between polls the Master does for updates to the set of active segments. Generally defines the amount of lag time it can take for the master to notice new segments|PT1M|
|`druid.database.segmentTable`|The table to use to look for segments.|none|
|`druid.database.ruleTable`|The table to use to look for segment load/drop rules.|none|
|`druid.database.configTable`|The table to use to look for configs.|none|

### Master properties

|Property|Description|Default|
|--------|-----------|-------|
|`druid.master.period`|The run period for the master. The master’s operates by maintaining the current state of the world in memory and periodically looking at the set of segments available and segments being served to make decisions about whether any changes need to be made to the data topology. This property sets the delay between each of these runs|PT60S|
|`druid.master.removedSegmentLifetime`|When a node disappears, the master can provide a grace period for how long it waits before deciding that the node really isn’t going to come back and it really should declare that all segments from that node are no longer available. This sets that grace period in number of runs of the master.|1|
|`druid.master.startDelay`|The operation of the Master works on the assumption that it has an up-to-date view of the state of the world when it runs, the current ZK interaction code, however, is written in a way that doesn’t allow the Master to know for a fact that it’s done loading the current state of the world. This delay is a hack to give it enough time to believe that it has all the data|PT600S|

### Zk properties

See [ZooKeeper](ZooKeeper.html) for a description of these properties.

### Service properties

These are properties that define various service/HTTP server aspects

|Property|Description|Default|
|--------|-----------|-------|
|`druid.client.http.connections`|Size of connection pool for the Broker to connect to compute nodes. If there are more queries than this number that all need to speak to the same node, then they will queue up.|none|
|`druid.paths.indexCache`|Segments assigned to a compute node are first stored on the local file system and then served by the compute node. This path defines where that local cache resides. Directory will be created if needed|none|
|`druid.paths.segmentInfoCache`|Compute nodes keep track of the segments they are serving so that when the process is restarted they can reload the same segments without waiting for the master to reassign. This path defines where this metadata is kept. Directory will be created if needed|none|
|`druid.http.numThreads`|The number of HTTP worker threads.|10|
|`druid.http.maxIdleTimeMillis`|The amount of time a connection can remain idle before it is terminated|300000 (5 min)|
|`druid.request.logging.dir`|Compute, Realtime and Broker nodes maintain request logs of all of the requests they get (interacton is via POST, so normal request logs don’t generally capture information about the actual query), this specifies the directory to store the request logs in|none|
|`druid.host`|The host for the current node. This is used to advertise the current processes location as reachable from another node and should generally be specified such that `http://${druid.host}/` could actually talk to this process|none|
|`druid.port`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|none|
|`druid.processing.formatString`|Realtime and Compute nodes use this format string to name their processing threads.|none|
|`druid.processing.numThreads`|The number of processing threads to have available for parallel processing of segments. Our rule of thumb is `num_cores - 1`, this means that even under heavy load there will still be one core available to do background tasks like talking with ZK and pulling down segments.|none|
|`druid.computation.buffer.size`|This specifies a buffer size for the storage of intermediate results. The computation engine in both the Compute and Realtime nodes will use a scratch buffer of this size to do all of their intermediate computations off-heap. Larger values allow for more aggregations in a single pass over the data while smaller values can require more passes depending on the query that is being executed.|1073741824 (1GB)|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|none|
|`druid.bard.cache.sizeInBytes`|The Broker (called Bard internally) instance has the ability to store results of queries in an in-memory cache. This specifies the number of bytes to use for that cache|none|

### Compute Properties

These are properties that the compute nodes use

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.maxSize`|The maximum number of bytes worth of segment that the node wants assigned to it. This is not a limit that the compute nodes actually enforce, they just publish it to the master and trust the master to do the right thing|none|
|`druid.server.type`|Specifies the type of the node. This is published via ZK and depending on the value the node will be treated specially by the Master/Broker. Allowed values are "realtime" or "historical". This is a configuration parameter because the plan is to allow for a more configurable cluster composition. At the current time, all realtime nodes should just be "realtime" and all compute nodes should just be "compute"|none|

### Emitter Properties

The Druid servers emit various metrics and alerts via something we call an [Emitter](Emitter.html). There are two emitter implementations included with the code, one that just logs to log4j and one that does POSTs of JSON events to a server. More information can be found on the [Emitter](Emitter.html) page. The properties for using the logging emitter are described below.

|Property|Description|Default|
|--------|-----------|-------|
|`com.metamx.emitter.logging`|Set to "true" to use the logging emitter|none|
|`com.metamx.emitter.logging.level`|Sets the level to log at|debug|
|`com.metamx.emitter.logging.class`|Sets the class to log at|com.metamx.emiter.core.LoggingEmitter|

### Realtime Properties

|Property|Description|Default|
|--------|-----------|-------|
|`druid.realtime.specFile`|The file with realtime specifications in it. See [Realtime](Realtime.html).|none|

