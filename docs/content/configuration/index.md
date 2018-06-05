---
layout: doc_page
---

# Configuring Druid

This describes the common configuration shared by all Druid nodes. These configurations can be defined in the `common.runtime.properties` file.

## JVM Configuration Best Practices

There are four JVM parameters that we set on all of our processes:

1.  `-Duser.timezone=UTC` This sets the default timezone of the JVM to UTC. We always set this and do not test with other default timezones, so local timezones might work, but they also might uncover weird and interesting bugs. To issue queries in a non-UTC timezone, see [query granularities](../querying/granularities.html#period-granularities)
2.  `-Dfile.encoding=UTF-8` This is similar to timezone, we test assuming UTF-8. Local encodings might work, but they also might result in weird and interesting bugs.
3.  `-Djava.io.tmpdir=<a path>` Various parts of the system that interact with the file system do it via temporary files, and these files can get somewhat large. Many production systems are set up to have small (but fast) `/tmp` directories, which can be problematic with Druid so we recommend pointing the JVM’s tmp directory to something with a little more meat.
4.  `-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager` This allows log4j2 to handle logs for non-log4j2 components (like jetty) which use standard java logging.

### Extensions

Many of Druid's external dependencies can be plugged in as modules. Extensions can be provided using the following configs:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.extensions.directory`|The root extension directory where user can put extensions related files. Druid will load extensions stored under this directory.|`extensions` (This is a relative path to Druid's working directory)|
|`druid.extensions.hadoopDependenciesDir`|The root hadoop dependencies directory where user can put hadoop related dependencies files. Druid will load the dependencies based on the hadoop coordinate specified in the hadoop index task.|`hadoop-dependencies` (This is a relative path to Druid's working directory|
|`druid.extensions.loadList`|A JSON array of extensions to load from extension directories by Druid. If it is not specified, its value will be `null` and Druid will load all the extensions under `druid.extensions.directory`. If its value is empty list `[]`, then no extensions will be loaded at all. It is also allowed to specify absolute path of other custom extensions not stored in the common extensions directory.|null|
|`druid.extensions.searchCurrentClassloader`|This is a boolean flag that determines if Druid will search the main classloader for extensions.  It defaults to true but can be turned off if you have reason to not automatically add all modules on the classpath.|true|
|`druid.extensions.useExtensionClassloaderFirst`|This is a boolean flag that determines if Druid extensions should prefer loading classes from their own jars rather than jars bundled with Druid. If false, extensions must be compatible with classes provided by any jars bundled with Druid. If true, extensions may depend on conflicting versions.|false|
|`druid.extensions.hadoopContainerDruidClasspath`|Hadoop Indexing launches hadoop jobs and this configuration provides way to explicitly set the user classpath for the hadoop job. By default this is computed automatically by druid based on the druid process classpath and set of extensions. However, sometimes you might want to be explicit to resolve dependency conflicts between druid and hadoop.|null|
|`druid.extensions.addExtensionsToHadoopContainer`|Only applicable if `druid.extensions.hadoopContainerDruidClasspath` is provided. If set to true, then extensions specified in the loadList are added to hadoop container classpath. Note that when `druid.extensions.hadoopContainerDruidClasspath` is not provided then extensions are always added to hadoop container classpath.|false|

### Modules

|Property|Description|Default|
|--------|-----------|-------|
|`druid.modules.excludeList`|A JSON array of canonical class names (e. g. `"io.druid.somepackage.SomeModule"`) of module classes which shouldn't be loaded, even if they are found in extensions specified by `druid.extensions.loadList`, or in the list of core modules specified to be loaded on a particular Druid node type. Useful when some useful extension contains some module, which shouldn't be loaded on some Druid node type because some dependencies of that module couldn't be satisfied.|[]|

### Zookeeper
We recommend just setting the base ZK path and the ZK service host, but all ZK paths that Druid uses can be overwritten to absolute paths.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.zk.paths.base`|Base Zookeeper path.|`/druid`|
|`druid.zk.service.host`|The ZooKeeper hosts to connect to. This is a REQUIRED property and therefore a host address must be supplied.|none|

#### Zookeeper Behavior

|Property|Description|Default|
|--------|-----------|-------|
|`druid.zk.service.sessionTimeoutMs`|ZooKeeper session timeout, in milliseconds.|`30000`|
|`druid.zk.service.compress`|Boolean flag for whether or not created Znodes should be compressed.|`true`|
|`druid.zk.service.acl`|Boolean flag for whether or not to enable ACL security for ZooKeeper. If ACL is enabled, zNode creators will have all permissions.|`false`|

#### Path Configuration
Druid interacts with ZK through a set of standard path configurations. We recommend just setting the base ZK path, but all ZK paths that Druid uses can be overwritten to absolute paths.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.zk.paths.base`|Base Zookeeper path.|`/druid`|
|`druid.zk.paths.propertiesPath`|Zookeeper properties path.|`${druid.zk.paths.base}/properties`|
|`druid.zk.paths.announcementsPath`|Druid node announcement path.|`${druid.zk.paths.base}/announcements`|
|`druid.zk.paths.liveSegmentsPath`|Current path for where Druid nodes announce their segments.|`${druid.zk.paths.base}/segments`|
|`druid.zk.paths.loadQueuePath`|Entries here cause historical nodes to load and drop segments.|`${druid.zk.paths.base}/loadQueue`|
|`druid.zk.paths.coordinatorPath`|Used by the coordinator for leader election.|`${druid.zk.paths.base}/coordinator`|
|`druid.zk.paths.servedSegmentsPath`|@Deprecated. Legacy path for where Druid nodes announce their segments.|`${druid.zk.paths.base}/servedSegments`|

The indexing service also uses its own set of paths. These configs can be included in the common configuration.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.zk.paths.indexer.base`|Base zookeeper path for |`${druid.zk.paths.base}/indexer`|
|`druid.zk.paths.indexer.announcementsPath`|Middle managers announce themselves here.|`${druid.zk.paths.indexer.base}/announcements`|
|`druid.zk.paths.indexer.tasksPath`|Used to assign tasks to middle managers.|`${druid.zk.paths.indexer.base}/tasks`|
|`druid.zk.paths.indexer.statusPath`|Parent path for announcement of task statuses.|`${druid.zk.paths.indexer.base}/status`|

If `druid.zk.paths.base` and `druid.zk.paths.indexer.base` are both set, and none of the other `druid.zk.paths.*` or `druid.zk.paths.indexer.*` values are set, then the other properties will be evaluated relative to their respective `base`.
For example, if `druid.zk.paths.base` is set to `/druid1` and `druid.zk.paths.indexer.base` is set to `/druid2` then `druid.zk.paths.announcementsPath` will default to `/druid1/announcements` while `druid.zk.paths.indexer.announcementsPath` will default to `/druid2/announcements`.

The following path is used for service discovery. It is **not** affected by `druid.zk.paths.base` and **must** be specified separately.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.discovery.curator.path`|Services announce themselves under this ZooKeeper path.|`/druid/discovery`|

### Exhibitor

[Exhibitor](https://github.com/Netflix/exhibitor/wiki) is a supervisor system for ZooKeeper.
Exhibitor can dynamically scale-up/down the cluster of ZooKeeper servers.
Druid can update self-owned list of ZooKeeper servers through Exhibitor without restarting.
That is, it allows Druid to keep the connections of Exhibitor-supervised ZooKeeper servers.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.exhibitor.service.hosts`|A JSON array which contains the hostnames of Exhibitor instances. Please specify this property if you want to use Exhibitor-supervised cluster.|none|
|`druid.exhibitor.service.port`|The REST port used to connect to Exhibitor.|`8080`|
|`druid.exhibitor.service.restUriPath`|The path of the REST call used to get the server set.|`/exhibitor/v1/cluster/list`|
|`druid.exhibitor.service.useSsl`|Boolean flag for whether or not to use https protocol.|`false`|
|`druid.exhibitor.service.pollingMs`|How ofter to poll the exhibitors for the list|`10000`|

Note that `druid.zk.service.host` is used as a backup in case an Exhibitor instance can't be contacted and therefore should still be set.

### Startup Logging

All nodes can log debugging information on startup.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.startup.logging.logProperties`|Log all properties on startup (from common.runtime.properties, runtime.properties, and the JVM command line).|false|
|`druid.startup.logging.maskProperties`|Masks sensitive properties (passwords, for example) containing theses words.|["password"]|

Note that some sensitive information may be logged if these settings are enabled.

### Request Logging

All nodes that can serve queries can also log the query requests they see.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.request.logging.type`|Choices: noop, file, emitter, slf4j, filtered, composing. How to log every query request.|noop|

Note that, you can enable sending all the HTTP requests to log by setting  "io.druid.jetty.RequestLog" to DEBUG level. See [Logging](../configuration/logging.html)

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

#### SLF4J Request Logging

Every request is logged via SLF4J. Queries are serialized into JSON in the log message regardless of the SJF4J format specification. They will be logged under the class `io.druid.server.log.LoggingRequestLogger`.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.request.logging.setMDC`|If MDC entries should be set in the log entry. Your logging setup still has to be configured to handle MDC to format this data|false|
|`druid.request.logging.setContextMDC`|If the druid query `context` should be added to the MDC entries. Has no effect unless `setMDC` is `true`|false|

MDC fields populated with `setMDC`:

|MDC field|Description|
|---------|-----------|
|`queryId`   |The query ID|
|`dataSource`|The datasource the query was against|
|`queryType` |The type of the query|
|`hasFilters`|If the query has any filters|
|`remoteAddr`|The remote address of the requesting client|
|`duration`  |The duration of the query interval|
|`resultOrdering`|The ordering of results|
|`descending`|If the query is a descending query|

#### Filtered Request Logging
Filtered Request Logger filters requests based on a configurable query/time threshold. Only request logs where query/time is above the threshold are emitted.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.request.logging.queryTimeThresholdMs`|Threshold value for query/time in milliseconds.|0 i.e no filtering|
|`druid.request.logging.delegate`|Delegate request logger to log requests.|none|

#### Composite Request Logging
Composite Request Logger emits request logs to multiple request loggers.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.request.logging.loggerProviders`|List of request loggers for emitting request logs.|none|


### Enabling Metrics

Druid nodes periodically emit metrics and different metrics monitors can be included. Each node can overwrite the default list of monitors.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.monitoring.emissionPeriod`|How often metrics are emitted.|PT1m|
|`druid.monitoring.monitors`|Sets list of Druid monitors used by a node. See below for names and more information. For example, you can specify monitors for a Broker with `druid.monitoring.monitors=["io.druid.java.util.metrics.SysMonitor","io.druid.java.util.metrics.JvmMonitor"]`.|none (no monitors)|

The following monitors are available:

|Name|Description|
|----|-----------|
|`io.druid.client.cache.CacheMonitor`|Emits metrics (to logs) about the segment results cache for Historical and Broker nodes. Reports typical cache statistics include hits, misses, rates, and size (bytes and number of entries), as well as timeouts and and errors.|
|`io.druid.java.util.metrics.SysMonitor`|This uses the [SIGAR library](http://www.hyperic.com/products/sigar) to report on various system activities and statuses.|
|`io.druid.server.metrics.HistoricalMetricsMonitor`|Reports statistics on Historical nodes.|
|`io.druid.java.util.metrics.JvmMonitor`|Reports various JVM-related statistics.|
|`io.druid.java.util.metrics.JvmCpuMonitor`|Reports statistics of CPU consumption by the JVM.|
|`io.druid.java.util.metrics.CpuAcctDeltaMonitor`|Reports consumed CPU as per the cpuacct cgroup.|
|`io.druid.java.util.metrics.JvmThreadsMonitor`|Reports Thread statistics in the JVM, like numbers of total, daemon, started, died threads.|
|`io.druid.segment.realtime.RealtimeMetricsMonitor`|Reports statistics on Realtime nodes.|
|`io.druid.server.metrics.EventReceiverFirehoseMonitor`|Reports how many events have been queued in the EventReceiverFirehose.|
|`io.druid.server.metrics.QueryCountStatsMonitor`|Reports how many queries have been successful/failed/interrupted.|
|`io.druid.server.emitter.HttpEmittingMonitor`|Reports internal metrics of `http` or `parametrized` emitter (see below). Must not be used with another emitter type. See the description of the metrics here: https://github.com/druid-io/druid/pull/4973.|

### Emitting Metrics

The Druid servers [emit various metrics](../operations/metrics.html) and alerts via something we call an Emitter. There are three emitter implementations included with the code, a "noop" emitter, one that just logs to log4j ("logging", which is used by default if no emitter is specified) and one that does POSTs of JSON events to a server ("http"). The properties for using the logging emitter are described below.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.emitter`|Setting this value to "noop", "logging", "http" or "parametrized" will initialize one of the emitter modules. value "composing" can be used to initialize multiple emitter modules. |noop|

#### Logging Emitter Module

|Property|Description|Default|
|--------|-----------|-------|
|`druid.emitter.logging.loggerClass`|Choices: HttpPostEmitter, LoggingEmitter, NoopServiceEmitter, ServiceEmitter. The class used for logging.|LoggingEmitter|
|`druid.emitter.logging.logLevel`|Choices: debug, info, warn, error. The log level at which message are logged.|info|

#### Http Emitter Module

|Property|Description|Default|
|--------|-----------|-------|
|`druid.emitter.http.flushMillis`|How often the internal message buffer is flushed (data is sent).|60000|
|`druid.emitter.http.flushCount`|How many messages the internal message buffer can hold before flushing (sending).|500|
|`druid.emitter.http.basicAuthentication`|Login and password for authentification in "login:password" form, e. g. `druid.emitter.http.basicAuthentication=admin:adminpassword`|not specified = no authentification|
|`druid.emitter.http.flushTimeOut`|The timeout after which an event should be sent to the endpoint, even if internal buffers are not filled, in milliseconds.|not specified = no timeout|
|`druid.emitter.http.batchingStrategy`|The strategy of how the batch is formatted. "ARRAY" means `[event1,event2]`, "NEWLINES" means `event1\nevent2`, ONLY_EVENTS means `event1event2`.|ARRAY|
|`druid.emitter.http.maxBatchSize`|The maximum batch size, in bytes.|the minimum of (10% of JVM heap size divided by 2) or (5191680 (i. e. 5 MB))|
|`druid.emitter.http.batchQueueSizeLimit`|The maximum number of batches in emitter queue, if there are problems with emitting.|the maximum of (2) or (10% of the JVM heap size divided by 5MB)|
|`druid.emitter.http.minHttpTimeoutMillis`|If the speed of filling batches imposes timeout smaller than that, not even trying to send batch to endpoint, because it will likely fail, not being able to send the data that fast. Configure this depending based on emitter/successfulSending/minTimeMs metric. Reasonable values are 10ms..100ms.|0|
|`druid.emitter.http.recipientBaseUrl`|The base URL to emit messages to. Druid will POST JSON to be consumed at the HTTP endpoint specified by this property.|none, required config|

#### Http Emitter Module TLS Overrides

When emitting events to a TLS-enabled receiver, the Http Emitter will by default use an SSLContext obtained via the process described at [Druid's internal communication over TLS](../operations/tls-support.html#druids-internal-communication-over-tls), i.e., the same SSLContext that would be used for internal communications between Druid nodes.

In some use cases it may be desirable to have the Http Emitter use its own separate truststore configuration. For example, there may be organizational policies that prevent the TLS-enabled metrics receiver's certificate from being added to the same truststore used by Druid's internal HTTP client.

The following properties allow the Http Emitter to use its own truststore configuration when building its SSLContext.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.emitter.http.ssl.useDefaultJavaContext`|If set to true, the HttpEmitter will use `SSLContext.getDefault()`, the default Java SSLContext, and all other properties below are ignored.|false|
|`druid.emitter.http.ssl.trustStorePath`|The file path or URL of the TLS/SSL Key store where trusted root certificates are stored. If this is unspecified, the Http Emitter will use the same SSLContext as Druid's internal HTTP client, as described in the beginning of this section, and all other properties below are ignored.|null|
|`druid.emitter.http.ssl.trustStoreType`|The type of the key store where trusted root certificates are stored.|`java.security.KeyStore.getDefaultType()`|
|`druid.emitter.http.ssl.trustStoreAlgorithm`|Algorithm to be used by TrustManager to validate certificate chains|`javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm()`|
|`druid.emitter.http.ssl.trustStorePassword`|The [Password Provider](../../operations/password-provider.html) or String password for the Trust Store.|none|
|`druid.emitter.http.ssl.protocol`|TLS protocol to use.|"TLSv1.2"|

#### Parametrized Http Emitter Module

`druid.emitter.parametrized.httpEmitting.*` configs correspond to the configs of Http Emitter Modules, see above.
Except `recipientBaseUrl`. E. g. `druid.emitter.parametrized.httpEmitting.flushMillis`,
`druid.emitter.parametrized.httpEmitting.flushCount`, `druid.emitter.parametrized.httpEmitting.ssl.trustStorePath`, etc.

The additional configs are:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.emitter.parametrized.recipientBaseUrlPattern`|The URL pattern to send an event to, based on the event's feed. E. g. `http://foo.bar/{feed}`, that will send event to `http://foo.bar/metrics` if the event's feed is "metrics".|none, required config|

#### Composing Emitter Module

|Property|Description|Default|
|--------|-----------|-------|
|`druid.emitter.composing.emitters`|List of emitter modules to load e.g. ["logging","http"].|[]|

#### Graphite Emitter

To use graphite as emitter set `druid.emitter=graphite`. For configuration details please follow this [link](../development/extensions-contrib/graphite.html).


### Metadata Storage

These properties specify the jdbc connection and other configuration around the metadata storage. The only processes that connect to the metadata storage with these properties are the [Coordinator](../design/coordinator.html), [Indexing service](../design/indexing-service.html) and [Realtime Nodes](../design/realtime.html).

|Property|Description|Default|
|--------|-----------|-------|
|`druid.metadata.storage.type`|The type of metadata storage to use. Choose from "mysql", "postgresql", or "derby".|derby|
|`druid.metadata.storage.connector.connectURI`|The jdbc uri for the database to connect to|none|
|`druid.metadata.storage.connector.user`|The username to connect with.|none|
|`druid.metadata.storage.connector.password`|The [Password Provider](../operations/password-provider.html) or String password used to connect with.|none|
|`druid.metadata.storage.connector.createTables`|If Druid requires a table and it doesn't exist, create it?|true|
|`druid.metadata.storage.tables.base`|The base name for tables.|druid|
|`druid.metadata.storage.tables.segments`|The table to use to look for segments.|druid_segments|
|`druid.metadata.storage.tables.rules`|The table to use to look for segment load/drop rules.|druid_rules|
|`druid.metadata.storage.tables.config`|The table to use to look for configs.|druid_config|
|`druid.metadata.storage.tables.tasks`|Used by the indexing service to store tasks.|druid_tasks|
|`druid.metadata.storage.tables.taskLog`|Used by the indexing service to store task logs.|druid_taskLog|
|`druid.metadata.storage.tables.taskLock`|Used by the indexing service to store task locks.|druid_taskLock|
|`druid.metadata.storage.tables.supervisors`|Used by the indexing service to store supervisor configurations.|druid_supervisors|
|`druid.metadata.storage.tables.audit`|The table to use for audit history of configuration changes e.g. Coordinator rules.|druid_audit|

### Deep Storage

The configurations concern how to push and pull [Segments](../design/segments.html) from deep storage.

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
|`druid.s3.accessKey`|The access key to use to access S3.|none|
|`druid.s3.secretKey`|The secret key to use to access S3.|none|
|`druid.storage.bucket`|S3 bucket name.|none|
|`druid.storage.baseKey`|S3 object key prefix for storage.|none|
|`druid.storage.disableAcl`|Boolean flag for ACL.|false|
|`druid.storage.archiveBucket`|S3 bucket name for archiving when running the indexing-service *archive task*.|none|
|`druid.storage.archiveBaseKey`|S3 object key prefix for archiving.|none|

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

### Caching

You can enable caching of results at the broker, historical, or realtime level using following configurations.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.cache.type`|`local`, `memcached`, `hybrid`, `caffeine`|The type of cache to use for queries.|`caffeine`|
|<code>druid.(broker&#124;historical&#124;realtime).cache.unCacheable</code>|All druid query types|All query types to not cache.|["groupBy", "select"]|
|<code>druid.(broker&#124;historical&#124;realtime).cache.useCache</code>|true, false|Whether to use cache for getting query results.|false|
|<code>druid.(broker&#124;historical&#124;realtime).cache.populateCache</code>|true, false|Whether to populate cache.|false|

#### Local Cache

|Property|Description|Default|
|--------|-----------|-------|
|`druid.cache.sizeInBytes`|Maximum cache size in bytes. You must set this if you enabled populateCache/useCache, or else cache size of zero wouldn't really cache anything.|0|
|`druid.cache.initialSize`|Initial size of the hashtable backing the cache.|500000|
|`druid.cache.logEvictionCount`|If non-zero, log cache eviction every `logEvictionCount` items.|0|

#### Memcache

|Property|Description|Default|
|--------|-----------|-------|
|`druid.cache.expiration`|Memcached [expiration time](https://code.google.com/p/memcached/wiki/NewCommands#Standard_Protocol).|2592000 (30 days)|
|`druid.cache.timeout`|Maximum time in milliseconds to wait for a response from Memcached.|500|
|`druid.cache.hosts`|Comma separated list of Memcached hosts `<host:port>`.|none|
|`druid.cache.maxObjectSize`|Maximum object size in bytes for a Memcached object.|52428800 (50 MB)|
|`druid.cache.memcachedPrefix`|Key prefix for all keys in Memcached.|druid|

### Indexing Service Discovery

This config is used to find the [Indexing Service](../design/indexing-service.html) using Curator service discovery. Only required if you are actually running an indexing service.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.selectors.indexing.serviceName`|The druid.service name of the indexing service Overlord node. To start the Overlord with a different name, set it with this property. |druid/overlord|


### Coordinator Discovery

This config is used to find the [Coordinator](../design/coordinator.html) using Curator service discovery. This config is used by the realtime indexing nodes to get information about the segments loaded in the cluster.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.selectors.coordinator.serviceName`|The druid.service name of the coordinator node. To start the Coordinator with a different name, set it with this property. |druid/coordinator|


### Announcing Segments

You can configure how to announce and unannounce Znodes in ZooKeeper (using Curator). For normal operations you do not need to override any of these configs.

##### Batch Data Segment Announcer

In current Druid, multiple data segments may be announced under the same Znode.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.announcer.segmentsPerNode`|Each Znode contains info for up to this many segments.|50|
|`druid.announcer.maxBytesPerNode`|Max byte size for Znode.|524288|
|`druid.announcer.skipDimensionsAndMetrics`|Skip Dimensions and Metrics list from segment announcements. NOTE: Enabling this will also remove the dimensions and metrics list from coordinator and broker endpoints.|false|
|`druid.announcer.skipLoadSpec`|Skip segment LoadSpec from segment announcements. NOTE: Enabling this will also remove the loadspec from coordinator and broker endpoints.|false|

### JavaScript

Druid supports dynamic runtime extension through JavaScript functions. This functionality can be configured through
the following properties.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.javascript.enabled`|Set to "true" to enable JavaScript functionality. This affects the JavaScript parser, filter, extractionFn, aggregator, post-aggregator, router strategy, and worker selection strategy.|false|

<div class="note info">
JavaScript-based functionality is disabled by default. Please refer to the Druid <a href="../development/javascript.html">JavaScript programming guide</a> for guidelines about using Druid's JavaScript functionality, including instructions on how to enable it.
</div>

### Double Column storage

Prior to version 0.13.0 Druid's storage layer used a 32-bit float representation to store columns created by the 
doubleSum, doubleMin, and doubleMax aggregators at indexing time.
Starting from version 0.13.0 the default will be 64-bit floats for Double columns.
Using 64-bit representation for double column will lead to avoid precesion loss at the cost of doubling the storage size of such columns.
To keep the old format set the system-wide property `druid.indexing.doubleStorage=float`.
You can also use floatSum, floatMin and floatMax to use 32-bit float representation.
Support for 64-bit floating point columns was released in Druid 0.11.0, so if you use this feature then older versions of Druid will not be able to read your data segments.
|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexing.doubleStorage`|Set to "float" to use 32-bit double representation for double columns.|double|
