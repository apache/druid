---
layout: doc_page
---

# Configuring Druid

This describes the basic server configuration that is loaded by all Druid server processes; the same file is loaded by all. See also the JSON "specFile" descriptions in [Realtime](Realtime.html) and [Batch-ingestion](Batch-ingestion.html).

## JVM Configuration Best Practices

There are three JVM parameters that we set on all of our processes:

1.  `-Duser.timezone=UTC` This sets the default timezone of the JVM to UTC. We always set this and do not test with other default timezones, so local timezones might work, but they also might uncover weird and interesting bugs.
2.  `-Dfile.encoding=UTF-8` This is similar to timezone, we test assuming UTF-8. Local encodings might work, but they also might result in weird and interesting bugs.
3.  `-Djava.io.tmpdir=<a path>` Various parts of the system that interact with the file system do it via temporary files, and these files can get somewhat large. Many production systems are set up to have small (but fast) `/tmp` directories, which can be problematic with Druid so we recommend pointing the JVM’s tmp directory to something with a little more meat.

## Modules

As of Druid v0.6, most core Druid functionality has been compartmentalized into modules. There are a set of default modules that may apply to any node type, and there are specific modules for the different node types. Default modules are __lazily instantiated__. Each module has its own set of configuration. 

This page describes the configuration of the default modules. Node-specific configuration is discussed on each node's respective page. In addition, you can add custom modules to [extend Druid](Modules.html).

Configuration of the various modules is done via Java properties. These can either be provided as `-D` system properties on the java command line or they can be passed in via a file called `runtime.properties` that exists on the classpath.

Note: as a future item, we’d like to consolidate all of the various configuration into a yaml/JSON based configuration file.

### Emitter Module

The Druid servers emit various metrics and alerts via something we call an Emitter. There are two emitter implementations included with the code, one that just logs to log4j ("logging", which is used by default if no emitter is specified) and one that does POSTs of JSON events to a server ("http"). The properties for using the logging emitter are described below.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.emitter`|Setting this value to either "logging" or "http" will instantialize one of the emitter modules.|logging|


#### Logging Emitter Module

|Property|Description|Default|
|--------|-----------|-------|
|`druid.emitter.logging.loggerClass`|Choices: HttpPostEmitter, LoggingEmitter, NoopServiceEmitter, ServiceEmitter. The class used for logging.|LoggingEmitter|
|`druid.emitter.logging.logLevel`|Choices: debug, info, warn, error. The log level at which message are logged.|info|

#### Http Emitter Module

|Property|Description|Default|
|--------|-----------|-------|
|`druid.emitter.http.timeOut`|The timeout for data reads.|PT5M|
|`druid.emitter.http.flushMillis`|How often to internal message buffer is flushed (data is sent).|60000|
|`druid.emitter.http.flushCount`|How many messages can the internal message buffer hold before flushing (sending).|500|
|`druid.emitter.http.recipientBaseUrl`|The base URL to emit messages to. Druid will POST JSON to be consumed at the HTTP endpoint specified by this property.|none|

### Http Client Module

This is the HTTP client used by [Broker](Broker.html) nodes.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.broker.http.numConnections`|Size of connection pool for the Broker to connect to historical and real-time nodes. If there are more queries than this number that all need to speak to the same node, then they will queue up.|5|
|`druid.broker.http.readTimeout`|The timeout for data reads.|PT15M|

### Curator Module

Druid uses [Curator](http://curator.incubator.apache.org/) for all [Zookeeper](http://zookeeper.apache.org/) interactions.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.zk.service.host`|The ZooKeeper hosts to connect to. This is a REQUIRED property and therefore a host address must be supplied.|none|
|`druid.zk.service.sessionTimeoutMs`|ZooKeeper session timeout, in milliseconds.|30000|
|`druid.curator.compress`|Boolean flag for whether or not created Znodes should be compressed.|false|

### Announcer Module

The announcer module is used to announce and unannounce Znodes in ZooKeeper (using Curator).

#### ZooKeeper Paths

See [ZooKeeper](ZooKeeper.html).

#### Data Segment Announcer

Data segment announcers are used to announce segments.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.announcer.type`|Choices: legacy or batch. The type of data segment announcer to use.|legacy|

##### Single Data Segment Announcer

In legacy Druid, each segment served by a node would be announced as an individual Znode.

##### Batch Data Segment Announcer

In current Druid, multiple data segments may be announced under the same Znode.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.announcer.segmentsPerNode`|Each Znode contains info for up to this many segments.|50|
|`druid.announcer.maxBytesPerNode`|Max byte size for Znode.|524288|

### Druid Processing Module

This module contains query processing functionality.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.processing.buffer.sizeBytes`|This specifies a buffer size for the storage of intermediate results. The computation engine in both the Historical and Realtime nodes will use a scratch buffer of this size to do all of their intermediate computations off-heap. Larger values allow for more aggregations in a single pass over the data while smaller values can require more passes depending on the query that is being executed.|1073741824 (1GB)|
|`druid.processing.formatString`|Realtime and historical nodes use this format string to name their processing threads.|processing-%s|
|`druid.processing.numThreads`|The number of processing threads to have available for parallel processing of segments. Our rule of thumb is `num_cores - 1`, which means that even under heavy load there will still be one core available to do background tasks like talking with ZooKeeper and pulling down segments. If only one core is available, this property defaults to the value `1`.|Number of cores - 1 (or 1)|


### Metrics Module

The metrics module is used to track Druid metrics.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.monitoring.emissionPeriod`|How often metrics are emitted.|PT1m|
|`druid.monitoring.monitors`|Sets list of Druid monitors used by a node. Each monitor is specified as `com.metamx.metrics.<monitor-name>` (see below for names and more information). For example, you can specify monitors for a Broker with `druid.monitoring.monitors=["com.metamx.metrics.SysMonitor","com.metamx.metrics.JvmMonitor"]`.|none (no monitors)|

The following monitors are available:

* CacheMonitor &ndash; Emits metrics (to logs) about the segment results cache for Historical and Broker nodes. Reports typical cache statistics include hits, misses, rates, and size (bytes and number of entries), as well as timeouts and and errors.
* SysMonitor &ndash; This uses the [SIGAR library](http://www.hyperic.com/products/sigar) to report on various system activities and statuses.
* ServerMonitor &ndash; Reports statistics on Historical nodes.
* JvmMonitor &ndash; Reports JVM-related statistics.
* RealtimeMetricsMonitor &ndash; Reports statistics on Realtime nodes.

### Server Module

This module is used for Druid server nodes.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current node. This is used to advertise the current processes location as reachable from another node and should generally be specified such that `http://${druid.host}/` could actually talk to this process|none|
|`druid.port`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|none|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|none|

### Storage Node Module

This module is used by nodes that store data (Historical and Realtime).

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.maxSize`|The maximum number of bytes-worth of segments that the node wants assigned to it. This is not a limit that Historical nodes actually enforce, just a value published to the Coordinator node so it can plan accordingly.|0|
|`druid.server.tier`| A string to name the distribution tier that the storage node belongs to. Many of the [rules Coordinator nodes use](Rule-Configuration.html) to manage segments can be keyed on tiers. |  `_default_tier` |
|`druid.server.priority`|In a tiered architecture, the priority of the tier, thus allowing control over which nodes are queried. Higher numbers mean higher priority. The default (no priority) works for architecture with no cross replication (tiers that have no data-storage overlap). Data centers typically have equal priority. | 0 |


#### Segment Cache

Druid storage nodes maintain information about segments they have already downloaded, and a disk cache to store that data.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.segmentCache.locations`|Segments assigned to a Historical node are first stored on the local file system (in a disk cache) and then served by the Historical node. These locations define where that local cache resides. | none (no caching) |
|`druid.segmentCache.deleteOnRemove`|Delete segment files from cache once a node is no longer serving a segment.|true|
|`druid.segmentCache.dropSegmentDelayMillis`|How long a node delays before completely dropping segment.|5 minutes|
|`druid.segmentCache.infoDir`|Historical nodes keep track of the segments they are serving so that when the process is restarted they can reload the same segments without waiting for the Coordinator to reassign. This path defines where this metadata is kept. Directory will be created if needed.|${first_location}/info_dir|

### Jetty Server Module

Druid uses Jetty to serve HTTP requests.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.http.numThreads`|Number of threads for HTTP requests.|10|
|`druid.server.http.maxIdleTime`|The Jetty max idle time for a connection.|PT5m|

### Queryable Module

This module is used by all nodes that can serve queries.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.request.logging.type`|Choices: noop, file, emitter. How to log every request.|noop|

#### File Request Logging

Daily request logs are stored on disk.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.request.logging.dir`|Historical, Realtime and Broker nodes maintain request logs of all of the requests they get (interacton is via POST, so normal request logs don’t generally capture information about the actual query), this specifies the directory to store the request logs in|none|

#### Emitter Request Logging

Every request is emitted to some external location.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.request.logging.feed`|Feed name for requests.|none|

### Query Runner Factory Module

This module is required by nodes that can serve queries.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.chunkPeriod`|Long-interval queries (of any type) may be broken into shorter interval queries, reducing the impact on resources. Use ISO 8601 periods. For example, if this property is set to `P1M` (one month), then a query covering a year would be broken into 12 smaller queries. |0 (off)|

#### GroupBy Query Config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.groupBy.singleThreaded`|Run single threaded group By queries.|false|
|`druid.query.groupBy.maxIntermediateRows`|Maximum number of intermediate rows.|50000|
|`druid.query.groupBy.maxResults`|Maximum number of results.|500000|


#### Search Query Config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.search.maxSearchLimit`|Maximum number of search results to return.|1000|


### Discovery Module

The discovery module is used for service discovery.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.discovery.curator.path`|Services announce themselves under this ZooKeeper path.|/druid/discovery|


#### Indexing Service Discovery Module

This module is used to find the [Indexing Service](Indexing-Service.html) using Curator service discovery.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.selectors.indexing.serviceName`|The druid.service name of the indexing service Overlord node. To start the Overlord with a different name, set it with this property. |overlord|


### Server Inventory View Module

This module is used to read announcements of segments in ZooKeeper. The configs are identical to the Announcer Module.

### Database Connector Module

These properties specify the jdbc connection and other configuration around the database. The only processes that connect to the DB with these properties are the [Coordinator](Coordinator.html) and [Indexing service](Indexing-service.html). This is tested on MySQL.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.db.connector.user`|The username to connect with.|none|
|`druid.db.connector.password`|The password to connect with.|none|
|`druid.db.connector.createTables`|If Druid requires a table and it doesn't exist, create it?|true|
|`druid.db.connector.useValidationQuery`|Validate a table with a query.|false|
|`druid.db.connector.validationQuery`|The query to validate with.|SELECT 1|
|`druid.db.tables.base`|The base name for tables.|druid|
|`druid.db.tables.segmentTable`|The table to use to look for segments.|druid_segments|
|`druid.db.tables.ruleTable`|The table to use to look for segment load/drop rules.|druid_rules|
|`druid.db.tables.configTable`|The table to use to look for configs.|druid_config|
|`druid.db.tables.tasks`|Used by the indexing service to store tasks.|druid_tasks|
|`druid.db.tables.taskLog`|Used by the indexing service to store task logs.|druid_taskLog|
|`druid.db.tables.taskLock`|Used by the indexing service to store task locks.|druid_taskLock|

### Jackson Config Manager Module

The Jackson Config manager reads and writes config entries from the Druid config table using [Jackson](http://jackson.codehaus.org/).

|Property|Description|Default|
|--------|-----------|-------|
|`druid.manager.config.pollDuration`|How often the manager polls the config table for updates.|PT1m|


### DataSegment Pusher/Puller Module

This module is used to configure Druid deep storage. The configurations concern how to push and pull [Segments](Segments.html) from deep storage.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.storage.type`|Choices:local, noop, s3, hdfs, c*. The type of deep storage to use.|local|

#### Local Deep Storage

Local deep storage uses the local filesystem.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.storage.storageDirectory`|Directory on disk to use as deep storage.|/tmp/druid/localStorage|

#### Noop Deep Storage

This deep storage doesn't do anything. There are no configs.

#### S3 Deep Storage

This deep storage is used to interface with Amazon's S3.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.storage.bucket`|S3 bucket name.|none|
|`druid.storage.baseKey`|S3 object key prefix for storage.|none|
|`druid.storage.disableAcl`|Boolean flag for ACL.|false|
|`druid.storage.archiveBucket`|S3 bucket name for archiving when running the indexing-service *archive task*.|none|
|`druid.storage.archiveBaseKey`|S3 object key prefix for archiving.|none|

#### AWS Module

This module is used to interact with S3.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.s3.accessKey`|The access key to use to access S3.|none|
|`druid.s3.secretKey`|The secret key to use to access S3.|none|


#### HDFS Deep Storage

This deep storage is used to interface with HDFS.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.storage.storageDirectory`|HDFS directory to use as deep storage.|none|

#### Cassandra Deep Storage

This deep storage is used to interface with Cassandra.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.storage.host`|Cassandra host.|none|
|`druid.storage.keyspace`|Cassandra key space.|none|

### Task Log Module

This module is used to configure the [Indexing Service](Indexing-Service.html) task logs.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.type`|Choices:noop, s3, file. Where to store task logs|file|

#### File Task Logs

Store task logs in the local filesystem.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.directory`|Local filesystem path.|log|

#### S3 Task Logs

Store task logs in S3.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.s3Bucket`|S3 bucket name.|none|
|`druid.indexer.logs.s3Prefix`|S3 key prefix.|none|

#### Noop Task Logs

No task logs are actually stored.

### Firehose Module

The Firehose module lists all available firehoses. There are no configurations.
