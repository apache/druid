---
layout: doc_page
---

# Configuration Reference

This page documents all of the configuration properties for each Druid service type.

## Table of Contents
  * [Recommended Configuration File Organization](#recommended-configuration-file-organization)
  * [Common configurations](#common-configurations)
    * [JVM Configuration Best Practices](#jvm-configuration-best-practices)
    * [Extensions](#extensions)
    * [Modules](#modules)
    * [Zookeeper](#zookeper)
    * [Exhibitor](#exhibitor)
    * [TLS](#tls)
    * [Authentication & Authorization](#authentication-and-authorization)
    * [Startup Logging](#startup-logging)
    * [Request Logging](#request-logging)
    * [Enabling Metrics](#enabling-metrics)
    * [Emitting Metrics](#emitting-metrics)
    * [Metadata Storage](#metadata-storage)
    * [Deep Storage](#deep-storage)
    * [Task Logging](#task-logging)
    * [Indexing Service Discovery](#indexing-service-discovery)
    * [Coordinator Discovery](#coordinator-discovery)
    * [Announcing Segments](#announcing-segments)
    * [JavaScript](#javascript)
    * [Double Column Storage](#double-column-storage)
  * [Coordinator](#coordinator)
    * [Static Configuration](#static-configuration)
        * [Node Config](#coordinator-node-config)
        * [Coordinator Operation](#coordinator-operation)
        * [Segment Management](#segment-management)
        * [Metadata Retrieval](#metadata-retrieval)
    * [Dynamic Configuration](#dynamic-configuration)
        * [Lookups](#lookups-dynamic-configuration)
        * [Compaction](#compaction-dynamic-configuration)
  * [Overlord](#overlord)
    * [Node Config](#overlord-node-config)
    * [Static Configuration](#overlord-static-configuration)
    * [Dynamic Configuration](#overlord-dynamic-configuration)
        * [Worker Select Strategy](#worker-select-strategy)
        * [Autoscaler](#autoscaler)
  * [MiddleManager & Peons](#middlemanager-and-peons)
    * [Node Config](#middlemanager-node-config)
    * [MiddleManger Configuration](#middlemanager-configuration)
    * [Peon Processing](#peon-processing)
    * [Peon Query Configuration](#peon-query-configuration)
    * [Caching](#peon-caching)
    * [Additional Peon Configuration](#additional-peon-configuration)
  * [Broker](#broker)
    * [Node Config](#broker-node-configs)
    * [Query Configuration](#broker-query-configuration)
    * [SQL](#sql)
    * [Caching](#broker-caching)
    * [Segment Discovery](#segment-discovery)
  * [Historical](#historical)
    * [Node Configuration](#historical-node-config)
    * [General Configuration](#historical-general-configuration)
    * [Query Configs](#historical-query-configs)
    * [Caching](#historical-caching)
  * [Caching](#cache-configuration)
  * [General Query Configuration](#general-query-configuration)
  * [Realtime nodes (Deprecated)](#realtime-nodes)
  
## Recommended Configuration File Organization

A recommended way of organizing Druid configuration files can be seen in the `conf` directory in the Druid package root, shown below:

```
$ ls -R conf
druid       tranquility

conf/druid:
_common       broker        coordinator   historical    middleManager overlord

conf/druid/_common:
common.runtime.properties log4j2.xml

conf/druid/broker:
jvm.config         runtime.properties

conf/druid/coordinator:
jvm.config         runtime.properties

conf/druid/historical:
jvm.config         runtime.properties

conf/druid/middleManager:
jvm.config         runtime.properties

conf/druid/overlord:
jvm.config         runtime.properties

conf/tranquility:
kafka.json  server.json
```

Each directory has a `runtime.properties` file containing configuration properties for the specific Druid service correponding to the directory (e.g., `historical`).

The `jvm.config` files contain JVM flags such as heap sizing properties for each service.

Common properties shared by all services are placed in `_common/common.runtime.properties`.
  

## Common Configurations

This describes the common configuration shared by all Druid nodes. These configurations can be defined in the `common.runtime.properties` file.

### JVM Configuration Best Practices

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

### TLS

#### General Configuration

|Property|Description|Default|
|--------|-----------|-------|
|`druid.enablePlaintextPort`|Enable/Disable HTTP connector.|`true`|
|`druid.enableTlsPort`|Enable/Disable HTTPS connector.|`false`|

Although not recommended but both HTTP and HTTPS connectors can be enabled at a time and respective ports are configurable using `druid.plaintextPort`
and `druid.tlsPort` properties on each node. Please see `Configuration` section of individual nodes to check the valid and default values for these ports.

#### Jetty Server TLS Configuration

Druid uses Jetty as an embedded web server. To get familiar with TLS/SSL in general and related concepts like Certificates etc.
reading this [Jetty documentation](http://www.eclipse.org/jetty/documentation/9.3.x/configuring-ssl.html) might be helpful.
To get more in depth knowledge of TLS/SSL support in Java in general, please refer to this [guide](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html).
The documentation [here](http://www.eclipse.org/jetty/documentation/9.3.x/configuring-ssl.html#configuring-sslcontextfactory)
can help in understanding TLS/SSL configurations listed below. This [document](http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html) lists all the possible
values for the below mentioned configs among others provided by Java implementation.

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.server.https.keyStorePath`|The file path or URL of the TLS/SSL Key store.|none|yes|
|`druid.server.https.keyStoreType`|The type of the key store.|none|yes|
|`druid.server.https.certAlias`|Alias of TLS/SSL certificate for the connector.|none|yes|
|`druid.server.https.keyStorePassword`|The [Password Provider](../operations/password-provider.html) or String password for the Key Store.|none|yes|

Following table contains non-mandatory advanced configuration options, use caution.

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.server.https.keyManagerFactoryAlgorithm`|Algorithm to use for creating KeyManager, more details [here](https://docs.oracle.com/javase/7/docs/technotes/guides/security/jsse/JSSERefGuide.html#KeyManager).|`javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm()`|no|
|`druid.server.https.keyManagerPassword`|The [Password Provider](../operations/password-provider.html) or String password for the Key Manager.|none|no|
|`druid.server.https.includeCipherSuites`|List of cipher suite names to include. You can either use the exact cipher suite name or a regular expression.|Jetty's default include cipher list|no|
|`druid.server.https.excludeCipherSuites`|List of cipher suite names to exclude. You can either use the exact cipher suite name or a regular expression.|Jetty's default exclude cipher list|no|
|`druid.server.https.includeProtocols`|List of exact protocols names to include.|Jetty's default include protocol list|no|
|`druid.server.https.excludeProtocols`|List of exact protocols names to exclude.|Jetty's default exclude protocol list|no|

#### Internal Client TLS Configuration (requires `simple-client-sslcontext` extension)

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.client.https.protocol`|SSL protocol to use.|`TLSv1.2`|no|
|`druid.client.https.trustStoreType`|The type of the key store where trusted root certificates are stored.|`java.security.KeyStore.getDefaultType()`|no|
|`druid.client.https.trustStorePath`|The file path or URL of the TLS/SSL Key store where trusted root certificates are stored.|none|yes|
|`druid.client.https.trustStoreAlgorithm`|Algorithm to be used by TrustManager to validate certificate chains|`javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm()`|no|
|`druid.client.https.trustStorePassword`|The [Password Provider](../../operations/password-provider.html) or String password for the Trust Store.|none|yes|

This [document](http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html) lists all the possible
values for the above mentioned configs among others provided by Java implementation.

### Authentication and Authorization

|Property|Type|Description|Default|Required|
|--------|-----------|--------|--------|--------|
|`druid.auth.authenticationChain`|JSON List of Strings|List of Authenticator type names|["allowAll"]|no|
|`druid.escalator.type`|String|Type of the Escalator that should be used for internal Druid communications. This Escalator must use an authentication scheme that is supported by an Authenticator in `druid.auth.authenticationChain`.|"noop"|no|
|`druid.auth.authorizers`|JSON List of Strings|List of Authorizer type names |["allowAll"]|no|
|`druid.auth.allowUnauthenticatedHttpOptions`|Boolean|If true, skip authentication checks for HTTP OPTIONS requests. This is needed for certain use cases, such as supporting CORS pre-flight requests. Note that disabling authentication checks for OPTIONS requests will allow unauthenticated users to determine what Druid endpoints are valid (by checking if the OPTIONS request returns a 200 instead of 404), so enabling this option may reveal information about server configuration, including information about what extensions are loaded (if those extensions add endpoints).|false|no|

For more information, please see [Authentication and Authorization](../design/auth.html).

For configuration options for specific auth extensions, please refer to the extension documentation.

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
|`io.druid.server.emitter.HttpEmitterMonitor`|Reports internal metrics of `http` or `parametrized` emitter (see below). Must not be used with another emitter type. See the description of the metrics here: https://github.com/druid-io/druid/pull/4973.|

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
|`druid.emitter.http.flushTimeOut|The timeout after which an event should be sent to the endpoint, even if internal buffers are not filled, in milliseconds.|not specified = no timeout|
|`druid.emitter.http.batchingStrategy`|The strategy of how the batch is formatted. "ARRAY" means `[event1,event2]`, "NEWLINES" means `event1\nevent2`, ONLY_EVENTS means `event1event2`.|ARRAY|
|`druid.emitter.http.maxBatchSize`|The maximum batch size, in bytes.|the minimum of (10% of JVM heap size divided by 2) or (5191680 (i. e. 5 MB))|
|`druid.emitter.http.batchQueueSizeLimit`|The maximum number of batches in emitter queue, if there are problems with emitting.|the maximum of (2) or (10% of the JVM heap size divided by 5MB)|
|`druid.emitter.http.minHttpTimeoutMillis`|If the speed of filling batches imposes timeout smaller than that, not even trying to send batch to endpoint, because it will likely fail, not being able to send the data that fast. Configure this depending based on emitter/successfulSending/minTimeMs metric. Reasonable values are 10ms..100ms.|0|
|`druid.emitter.http.recipientBaseUrl`|The base URL to emit messages to. Druid will POST JSON to be consumed at the HTTP endpoint specified by this property.|none, required config|

#### Parametrized Http Emitter Module

`druid.emitter.parametrized.httpEmitting.*` configs correspond to the configs of Http Emitter Modules, see above.
Except `recipientBaseUrl`. E. g. `druid.emitter.parametrized.httpEmitting.flushMillis`,
`druid.emitter.parametrized.httpEmitting.flushCount`, etc.

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

This deep storage is used to interface with Amazon's S3. Note that the `druid-s3-extensions` extension must be loaded.

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

This deep storage is used to interface with HDFS. Note that the `druid-hdfs-storage` extension must be loaded.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.storage.storageDirectory`|HDFS directory to use as deep storage.|none|

#### Cassandra Deep Storage

This deep storage is used to interface with Cassandra. Note that the `druid-cassandra-storage` extension must be loaded.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.storage.host`|Cassandra host.|none|
|`druid.storage.keyspace`|Cassandra key space.|none|

### Task Logging

If you are running the indexing service in remote mode, the task logs must be stored in S3, Azure Blob Store, Google Cloud Storage or HDFS.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.type`|Choices:noop, s3, azure, google, hdfs, file. Where to store task logs|file|

You can also configure the Overlord to automatically retain the task logs only for last x milliseconds by configuring following additional properties.
Caution: Automatic log file deletion typically works based on log file modification timestamp on the backing store, so large clock skews between druid nodes and backing store nodes might result in un-intended behavior.  

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.kill.enabled`|Boolean value for whether to enable deletion of old task logs. |false|
|`druid.indexer.logs.kill.durationToRetain`| Required if kill is enabled. In milliseconds, task logs to be retained created in last x milliseconds. |None|
|`druid.indexer.logs.kill.initialDelay`| Optional. Number of milliseconds after overlord start when first auto kill is run. |random value less than 300000 (5 mins)|
|`druid.indexer.logs.kill.delay`|Optional. Number of milliseconds of delay between successive executions of auto kill run. |21600000 (6 hours)|

#### File Task Logs

Store task logs in the local filesystem.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.directory`|Local filesystem path.|log|

#### S3 Task Logs

Store task logs in S3. Note that the `druid-s3-extensions` extension must be loaded.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.s3Bucket`|S3 bucket name.|none|
|`druid.indexer.logs.s3Prefix`|S3 key prefix.|none|

#### Azure Blob Store Task Logs
Store task logs in Azure Blob Store.

Note: The `druid-azure-extensions` extension must be loaded, and this uses the same storage account as the deep storage module for azure.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.container`|The Azure Blob Store container to write logs to|none|
|`druid.indexer.logs.prefix`|The path to prepend to logs|none|

#### Google Cloud Storage Task Logs
Store task logs in Google Cloud Storage.

Note: The `druid-google-extensions` extension must be loaded, and this uses the same storage settings as the deep storage module for google.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.bucket`|The Google Cloud Storage bucket to write logs to|none|
|`druid.indexer.logs.prefix`|The path to prepend to logs|none|

#### HDFS Task Logs

Store task logs in HDFS. Note that the `druid-hdfs-storage` extension must be loaded.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.directory`|The directory to store logs.|none|

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

Druid's storage layer uses a 32-bit float representation to store columns created by the 
doubleSum, doubleMin, and doubleMax aggregators at indexing time. To instead use 64-bit floats
for these columns, please set the system-wide property `druid.indexing.doubleStorage=double`.
This will become the default behavior in a future version of Druid.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexing.doubleStorage`|Set to "double" to use 64-bit double representation for double columns.|float|

## Coordinator

For general Coordinator Node information, see [here](../design/coordinator.html).

### Static Configuration

These coordinator static configurations can be defined in the `coordinator/runtime.properties` file.

#### Node Config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current node. This is used to advertise the current processes location as reachable from another node and should generally be specified such that `http://${druid.host}/` could actually talk to this process|InetAddress.getLocalHost().getCanonicalHostName()|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8081|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.enableTlsPort](../operations/tls-support.html) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative Integer.|8281|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|druid/coordinator|

#### Coordinator Operation

|Property|Description|Default|
|--------|-----------|-------|
|`druid.coordinator.period`|The run period for the coordinator. The coordinator’s operates by maintaining the current state of the world in memory and periodically looking at the set of segments available and segments being served to make decisions about whether any changes need to be made to the data topology. This property sets the delay between each of these runs.|PT60S|
|`druid.coordinator.period.indexingPeriod`|How often to send indexing tasks to the indexing service. Only applies if merge or conversion is turned on.|PT1800S (30 mins)|
|`druid.coordinator.startDelay`|The operation of the Coordinator works on the assumption that it has an up-to-date view of the state of the world when it runs, the current ZK interaction code, however, is written in a way that doesn’t allow the Coordinator to know for a fact that it’s done loading the current state of the world. This delay is a hack to give it enough time to believe that it has all the data.|PT300S|
|`druid.coordinator.merge.on`|Boolean flag for whether or not the coordinator should try and merge small segments into a more optimal segment size.|false|
|`druid.coordinator.conversion.on`|Boolean flag for converting old segment indexing versions to the latest segment indexing version.|false|
|`druid.coordinator.load.timeout`|The timeout duration for when the coordinator assigns a segment to a historical node.|PT15M|
|`druid.coordinator.kill.pendingSegments.on`|Boolean flag for whether or not the coordinator clean up old entries in the `pendingSegments` table of metadata store. If set to true, coordinator will check the created time of most recently complete task. If it doesn't exist, it finds the created time of the earlist running/pending/waiting tasks. Once the created time is found, then for all dataSources not in the `killPendingSegmentsSkipList` (see [Dynamic configuration](#dynamic-configuration)), coordinator will ask the overlord to clean up the entries 1 day or more older than the found created time in the `pendingSegments` table. This will be done periodically based on `druid.coordinator.period` specified.|false|
|`druid.coordinator.kill.on`|Boolean flag for whether or not the coordinator should submit kill task for unused segments, that is, hard delete them from metadata store and deep storage. If set to true, then for all whitelisted dataSources (or optionally all), coordinator will submit tasks periodically based on `period` specified. These kill tasks will delete all segments except for the last `durationToRetain` period. Whitelist or All can be set via dynamic configuration `killAllDataSources` and `killDataSourceWhitelist` described later.|false|
|`druid.coordinator.kill.period`|How often to send kill tasks to the indexing service. Value must be greater than `druid.coordinator.period.indexingPeriod`. Only applies if kill is turned on.|P1D (1 Day)|
|`druid.coordinator.kill.durationToRetain`| Do not kill segments in last `durationToRetain`, must be greater or equal to 0. Only applies and MUST be specified if kill is turned on. Note that default value is invalid.|PT-1S (-1 seconds)|
|`druid.coordinator.kill.maxSegments`|Kill at most n segments per kill task submission, must be greater than 0. Only applies and MUST be specified if kill is turned on. Note that default value is invalid.|0|
|`druid.coordinator.balancer.strategy`|Specify the type of balancing strategy that the coordinator should use to distribute segments among the historicals. `cachingCost` is logically equivalent to `cost` but is more CPU-efficient on large clusters and will replace `cost` in the future versions, users are invited to try it. Use `diskNormalized` to distribute segments among nodes so that the disks fill up uniformly and use `random` to randomly pick nodes to distribute segments.|`cost`|
|`druid.coordinator.loadqueuepeon.repeatDelay`|The start and repeat delay for the loadqueuepeon , which manages the load and drop of segments.|PT0.050S (50 ms)|
|`druid.coordinator.asOverlord.enabled`|Boolean value for whether this coordinator node should act like an overlord as well. This configuration allows users to simplify a druid cluster by not having to deploy any standalone overlord nodes. If set to true, then overlord console is available at `http://coordinator-host:port/console.html` and be sure to set `druid.coordinator.asOverlord.overlordService` also. See next.|false|
|`druid.coordinator.asOverlord.overlordService`| Required, if `druid.coordinator.asOverlord.enabled` is `true`. This must be same value as `druid.service` on standalone Overlord nodes and `druid.selectors.indexing.serviceName` on Middle Managers.|NULL|

#### Segment Management

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.announcer.type`|batch or http|Segment discovery method to use. "http" enables discovering segments using HTTP instead of zookeeper.|batch|
|`druid.coordinator.loadqueuepeon.type`|curator or http|Whether to use "http" or "curator" implementation to assign segment loads/drops to historical|curator|

##### Additional config when "http" loadqueuepeon is used
|Property|Description|Default|
|--------|-----------|-------|
|`druid.coordinator.loadqueuepeon.http.batchSize`|Number of segment load/drop requests to batch in one HTTP request. Note that it must be smaller than `druid.segmentCache.numLoadingThreads` config on historical node.|1|

#### Metadata Retrieval

|Property|Description|Default|
|--------|-----------|-------|
|`druid.manager.config.pollDuration`|How often the manager polls the config table for updates.|PT1m|
|`druid.manager.segments.pollDuration`|The duration between polls the Coordinator does for updates to the set of active segments. Generally defines the amount of lag time it can take for the coordinator to notice new segments.|PT1M|
|`druid.manager.rules.pollDuration`|The duration between polls the Coordinator does for updates to the set of active rules. Generally defines the amount of lag time it can take for the coordinator to notice rules.|PT1M|
|`druid.manager.rules.defaultTier`|The default tier from which default rules will be loaded from.|_default|
|`druid.manager.rules.alertThreshold`|The duration after a failed poll upon which an alert should be emitted.|PT10M|

### Dynamic Configuration

The coordinator has dynamic configuration to change certain behaviour on the fly. The coordinator uses a JSON spec object from the Druid [metadata storage](../dependencies/metadata-storage.html) config table. This object is detailed below:

It is recommended that you use the Coordinator Console to configure these parameters. However, if you need to do it via HTTP, the JSON object can be submitted to the coordinator via a POST request at:

```
http://<COORDINATOR_IP>:<PORT>/druid/coordinator/v1/config
```

Optional Header Parameters for auditing the config change can also be specified.

|Header Param Name| Description | Default |
|----------|-------------|---------|
|`X-Druid-Author`| author making the config change|""|
|`X-Druid-Comment`| comment describing the change being done|""|

A sample coordinator dynamic config JSON object is shown below:

```json
{
  "millisToWaitBeforeDeleting": 900000,
  "mergeBytesLimit": 100000000,
  "mergeSegmentsLimit" : 1000,
  "maxSegmentsToMove": 5,
  "replicantLifetime": 15,
  "replicationThrottleLimit": 10,
  "emitBalancingStats": false,
  "killDataSourceWhitelist": ["wikipedia", "testDatasource"]
}
```

Issuing a GET request at the same URL will return the spec that is currently in place. A description of the config setup spec is shown below.

|Property|Description|Default|
|--------|-----------|-------|
|`millisToWaitBeforeDeleting`|How long does the coordinator need to be active before it can start removing (marking unused) segments in metadata storage.|900000 (15 mins)|
|`mergeBytesLimit`|The maximum total uncompressed size in bytes of segments to merge.|524288000L|
|`mergeSegmentsLimit`|The maximum number of segments that can be in a single [append task](../ingestion/tasks.html).|100|
|`maxSegmentsToMove`|The maximum number of segments that can be moved at any given time.|5|
|`replicantLifetime`|The maximum number of coordinator runs for a segment to be replicated before we start alerting.|15|
|`replicationThrottleLimit`|The maximum number of segments that can be replicated at one time.|10|
|`emitBalancingStats`|Boolean flag for whether or not we should emit balancing stats. This is an expensive operation.|false|
|`killDataSourceWhitelist`|List of dataSources for which kill tasks are sent if property `druid.coordinator.kill.on` is true. This can be a list of comma-separated dataSources or a JSON array.|none|
|`killAllDataSources`|Send kill tasks for ALL dataSources if property `druid.coordinator.kill.on` is true. If this is set to true then `killDataSourceWhitelist` must not be specified or be empty list.|false|
|`killPendingSegmentsSkipList`|List of dataSources for which pendingSegments are _NOT_ cleaned up if property `druid.coordinator.kill.pendingSegments.on` is true. This can be a list of comma-separated dataSources or a JSON array.|none|
|`maxSegmentsInNodeLoadingQueue`|The maximum number of segments that could be queued for loading to any given server. This parameter could be used to speed up segments loading process, especially if there are "slow" nodes in the cluster (with low loading speed) or if too much segments scheduled to be replicated to some particular node (faster loading could be preferred to better segments distribution). Desired value depends on segments loading speed, acceptable replication time and number of nodes. Value 1000 could be a start point for a rather big cluster. Default value is 0 (loading queue is unbounded) |0|

To view the audit history of coordinator dynamic config issue a GET request to the URL -

```
http://<COORDINATOR_IP>:<PORT>/druid/coordinator/v1/config/history?interval=<interval>
```

default value of interval can be specified by setting `druid.audit.manager.auditHistoryMillis` (1 week if not configured) in coordinator runtime.properties

To view last <n> entries of the audit history of coordinator dynamic config issue a GET request to the URL -

```
http://<COORDINATOR_IP>:<PORT>/druid/coordinator/v1/config/history?count=<n>
```


#### Lookups Dynamic Configuration (EXPERIMENTAL)<a id="lookups-dynamic-configuration"></a>

These configuration options control the behavior of the Lookup dynamic configuration described in the [lookups page](../querying/lookups.html)

|Property|Description|Default|
|--------|-----------|-------|
|`druid.manager.lookups.hostDeleteTimeout`|How long to wait for a `DELETE` request to a particular node before considering the `DELETE` a failure|PT1s|
|`druid.manager.lookups.hostUpdateTimeout`|How long to wait for a `POST` request to a particular node before considering the `POST` a failure|PT10s|
|`druid.manager.lookups.deleteAllTimeout`|How long to wait for all `DELETE` requests to finish before considering the delete attempt a failure|PT10s|
|`druid.manager.lookups.updateAllTimeout`|How long to wait for all `POST` requests to finish before considering the attempt a failure|PT60s|
|`druid.manager.lookups.threadPoolSize`|How many nodes can be managed concurrently (concurrent POST and DELETE requests). Requests this limit will wait in a queue until a slot becomes available.|10|
|`druid.manager.lookups.period`|How many milliseconds between checks for configuration changes|30_000|

## Overlord

### Overlord Static Configuration

These overlord static configurations can be defined in the `overlord/runtime.properties` file.

#### Overlord Node Configs

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current node. This is used to advertise the current processes location as reachable from another node and should generally be specified such that `http://${druid.host}/` could actually talk to this process|InetAddress.getLocalHost().getCanonicalHostName()|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8090|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.enableTlsPort](../operations/tls-support.html) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative Integer.|8290|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|druid/overlord|

#### Overlord Operation

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.runner.type`|Choices "local" or "remote". Indicates whether tasks should be run locally or in a distributed environment.|local|
|`druid.indexer.storage.type`|Choices are "local" or "metadata". Indicates whether incoming tasks should be stored locally (in heap) or in metadata storage. Storing incoming tasks in metadata storage allows for tasks to be resumed if the overlord should fail.|local|
|`druid.indexer.storage.recentlyFinishedThreshold`|A duration of time to store task results.|PT24H|
|`druid.indexer.queue.maxSize`|Maximum number of active tasks at one time.|Integer.MAX_VALUE|
|`druid.indexer.queue.startDelay`|Sleep this long before starting overlord queue management. This can be useful to give a cluster time to re-orient itself after e.g. a widespread network issue.|PT1M|
|`druid.indexer.queue.restartDelay`|Sleep this long when overlord queue management throws an exception before trying again.|PT30S|
|`druid.indexer.queue.storageSyncRate`|Sync overlord state this often with an underlying task persistence mechanism.|PT1M|

The following configs only apply if the overlord is running in remote mode. For a description of local vs. remote mode, please see (../design/indexing-service.html#overlord-node).

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.runner.taskAssignmentTimeout`|How long to wait after a task as been assigned to a middle manager before throwing an error.|PT5M|
|`druid.indexer.runner.minWorkerVersion`|The minimum middle manager version to send tasks to. |"0"|
|`druid.indexer.runner.compressZnodes`|Indicates whether or not the overlord should expect middle managers to compress Znodes.|true|
|`druid.indexer.runner.maxZnodeBytes`|The maximum size Znode in bytes that can be created in Zookeeper.|524288|
|`druid.indexer.runner.taskCleanupTimeout`|How long to wait before failing a task after a middle manager is disconnected from Zookeeper.|PT15M|
|`druid.indexer.runner.taskShutdownLinkTimeout`|How long to wait on a shutdown request to a middle manager before timing out|PT1M|
|`druid.indexer.runner.pendingTasksRunnerNumThreads`|Number of threads to allocate pending-tasks to workers, must be at least 1.|1|
|`druid.indexer.runner.maxRetriesBeforeBlacklist`|Number of consecutive times the middle manager can fail tasks,  before the worker is blacklisted, must be at least 1|5|
|`druid.indexer.runner.workerBlackListBackoffTime`|How long to wait before a task is whitelisted again. This value should be greater that the value set for taskBlackListCleanupPeriod.|PT15M|
|`druid.indexer.runner.workerBlackListCleanupPeriod`|A duration after which the cleanup thread will startup to clean blacklisted workers.|PT5M|
|`druid.indexer.runner.maxPercentageBlacklistWorkers`|The maximum percentage of workers to blacklist, this must be between 0 and 100.|20|

There are additional configs for autoscaling (if it is enabled):

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.autoscale.strategy`|Choices are "noop" or "ec2". Sets the strategy to run when autoscaling is required.|noop|
|`druid.indexer.autoscale.doAutoscale`|If set to "true" autoscaling will be enabled.|false|
|`druid.indexer.autoscale.provisionPeriod`|How often to check whether or not new middle managers should be added.|PT1M|
|`druid.indexer.autoscale.terminatePeriod`|How often to check when middle managers should be removed.|PT5M|
|`druid.indexer.autoscale.originTime`|The starting reference timestamp that the terminate period increments upon.|2012-01-01T00:55:00.000Z|
|`druid.indexer.autoscale.workerIdleTimeout`|How long can a worker be idle (not a run task) before it can be considered for termination.|PT90M|
|`druid.indexer.autoscale.maxScalingDuration`|How long the overlord will wait around for a middle manager to show up before giving up.|PT15M|
|`druid.indexer.autoscale.numEventsToTrack`|The number of autoscaling related events (node creation and termination) to track.|10|
|`druid.indexer.autoscale.pendingTaskTimeout`|How long a task can be in "pending" state before the overlord tries to scale up.|PT30S|
|`druid.indexer.autoscale.workerVersion`|If set, will only create nodes of set version during autoscaling. Overrides dynamic configuration. |null|
|`druid.indexer.autoscale.workerPort`|The port that middle managers will run on.|8080|


### Overlord Dynamic Configuration

The overlord can dynamically change worker behavior.

The JSON object can be submitted to the overlord via a POST request at:

```
http://<OVERLORD_IP>:<port>/druid/indexer/v1/worker
```

Optional Header Parameters for auditing the config change can also be specified.

|Header Param Name| Description | Default |
|----------|-------------|---------|
|`X-Druid-Author`| author making the config change|""|
|`X-Druid-Comment`| comment describing the change being done|""|

A sample worker config spec is shown below:

```json
{
  "selectStrategy": {
    "type": "fillCapacity",
    "affinityConfig": {
      "affinity": {
        "datasource1": ["host1:port", "host2:port"],
        "datasource2": ["host3:port"]
      }
    }
  },
  "autoScaler": {
    "type": "ec2",
    "minNumWorkers": 2,
    "maxNumWorkers": 12,
    "envConfig": {
      "availabilityZone": "us-east-1a",
      "nodeData": {
        "amiId": "${AMI}",
        "instanceType": "c3.8xlarge",
        "minInstances": 1,
        "maxInstances": 1,
        "securityGroupIds": ["${IDs}"],
        "keyName": "${KEY_NAME}"
      },
      "userData": {
        "impl": "string",
        "data": "${SCRIPT_COMMAND}",
        "versionReplacementString": ":VERSION:",
        "version": null
      }
    }
  }
}
```

Issuing a GET request at the same URL will return the current worker config spec that is currently in place. The worker config spec list above is just a sample for EC2 and it is possible to extend the code base for other deployment environments. A description of the worker config spec is shown below.

|Property|Description|Default|
|--------|-----------|-------|
|`selectStrategy`|How to assign tasks to middle managers. Choices are `fillCapacity`, `equalDistribution`, and `javascript`.|equalDistribution|
|`autoScaler`|Only used if autoscaling is enabled. See below.|null|

To view the audit history of worker config issue a GET request to the URL -

```
http://<OVERLORD_IP>:<port>/druid/indexer/v1/worker/history?interval=<interval>
```

default value of interval can be specified by setting `druid.audit.manager.auditHistoryMillis` (1 week if not configured) in overlord runtime.properties.

To view last <n> entries of the audit history of worker config issue a GET request to the URL -

```
http://<OVERLORD_IP>:<port>/druid/indexer/v1/worker/history?count=<n>
```

#### Worker Select Strategy

Worker select strategies control how Druid assigns tasks to middleManagers.

##### Equal Distribution

Tasks are assigned to the middleManager with the most available capacity at the time the task begins running. This is
useful if you want work evenly distributed across your middleManagers.

|Property|Description|Default|
|--------|-----------|-------|
|`type`|`equalDistribution`.|required; must be `equalDistribution`|
|`affinityConfig`|[Affinity config](#affinity) object|null (no affinity)|

##### Fill Capacity

Tasks are assigned to the worker with the most currently-running tasks at the time the task begins running. This is
useful in situations where you are elastically auto-scaling middleManagers, since it will tend to pack some full and
leave others empty. The empty ones can be safely terminated.

Note that if `druid.indexer.runner.pendingTasksRunnerNumThreads` is set to _N_ > 1, then this strategy will fill _N_
middleManagers up to capacity simultaneously, rather than a single middleManager.

|Property|Description|Default|
|--------|-----------|-------|
|`type`|`fillCapacity`.|required; must be `fillCapacity`|
|`affinityConfig`|[Affinity config](#affinity) object|null (no affinity)|

##### Javascript

Allows defining arbitrary logic for selecting workers to run task using a JavaScript function.
The function is passed remoteTaskRunnerConfig, map of workerId to available workers and task to be executed and returns the workerId on which the task should be run or null if the task cannot be run.
It can be used for rapid development of missing features where the worker selection logic is to be changed or tuned often.
If the selection logic is quite complex and cannot be easily tested in javascript environment,
its better to write a druid extension module with extending current worker selection strategies written in java.

|Property|Description|Default|
|--------|-----------|-------|
|`type`|`javascript`.|required; must be `javascript`|
|`function`|String representing javascript function||

Example: a function that sends batch_index_task to workers 10.0.0.1 and 10.0.0.2 and all other tasks to other available workers.

```
{
"type":"javascript",
"function":"function (config, zkWorkers, task) {\nvar batch_workers = new java.util.ArrayList();\nbatch_workers.add(\"10.0.0.1\");\nbatch_workers.add(\"10.0.0.2\");\nworkers = zkWorkers.keySet().toArray();\nvar sortedWorkers = new Array()\n;for(var i = 0; i < workers.length; i++){\n sortedWorkers[i] = workers[i];\n}\nArray.prototype.sort.call(sortedWorkers,function(a, b){return zkWorkers.get(b).getCurrCapacityUsed() - zkWorkers.get(a).getCurrCapacityUsed();});\nvar minWorkerVer = config.getMinWorkerVersion();\nfor (var i = 0; i < sortedWorkers.length; i++) {\n var worker = sortedWorkers[i];\n  var zkWorker = zkWorkers.get(worker);\n  if(zkWorker.canRunTask(task) && zkWorker.isValidVersion(minWorkerVer)){\n    if(task.getType() == 'index_hadoop' && batch_workers.contains(worker)){\n      return worker;\n    } else {\n      if(task.getType() != 'index_hadoop' && !batch_workers.contains(worker)){\n        return worker;\n      }\n    }\n  }\n}\nreturn null;\n}"
}
```

<div class="note info">
JavaScript-based functionality is disabled by default. Please refer to the Druid <a href="../development/javascript.html">JavaScript programming guide</a> for guidelines about using Druid's JavaScript functionality, including instructions on how to enable it.
</div>

##### Affinity

Affinity configs can be provided to the _equalDistribution_ and _fillCapacity_ strategies using the "affinityConfig"
field. If not provided, the default is to not use affinity at all.

|Property|Description|Default|
|--------|-----------|-------|
|`affinity`|JSON object mapping a datasource String name to a list of indexing service middleManager host:port String values. Druid doesn't perform DNS resolution, so the 'host' value must match what is configured on the middleManager and what the middleManager announces itself as (examine the Overlord logs to see what your middleManager announces itself as).|{}|
|`strong`|With weak affinity (the default), tasks for a dataSource may be assigned to other middleManagers if their affinity-mapped middleManagers are not able to run all pending tasks in the queue for that dataSource. With strong affinity, tasks for a dataSource will only ever be assigned to their affinity-mapped middleManagers, and will wait in the pending queue if necessary.|false|

#### Autoscaler

Amazon's EC2 is currently the only supported autoscaler.

|Property|Description|Default|
|--------|-----------|-------|
|`minNumWorkers`|The minimum number of workers that can be in the cluster at any given time.|0|
|`maxNumWorkers`|The maximum number of workers that can be in the cluster at any given time.|0|
|`availabilityZone`|What availability zone to run in.|none|
|`nodeData`|A JSON object that describes how to launch new nodes.|none; required|
|`userData`|A JSON object that describes how to configure new nodes. If you have set druid.indexer.autoscale.workerVersion, this must have a versionReplacementString. Otherwise, a versionReplacementString is not necessary.|none; optional|

## MiddleManager and Peons

These MiddleManager and Peon configurations can be defined in the `middleManager/runtime.properties` file.

### MiddleManager Node Config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current node. This is used to advertise the current processes location as reachable from another node and should generally be specified such that `http://${druid.host}/` could actually talk to this process|InetAddress.getLocalHost().getCanonicalHostName()|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8091|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.enableTlsPort](../operations/tls-support.html) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative Integer.|8291|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|druid/middlemanager|

### MiddleManager Configuration

Middle managers pass their configurations down to their child peons. The middle manager requires the following configs:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.runner.allowedPrefixes`|Whitelist of prefixes for configs that can be passed down to child peons.|"com.metamx", "druid", "io.druid", "user.timezone", "file.encoding", "java.io.tmpdir", "hadoop"|
|`druid.indexer.runner.compressZnodes`|Indicates whether or not the middle managers should compress Znodes.|true|
|`druid.indexer.runner.classpath`|Java classpath for the peon.|System.getProperty("java.class.path")|
|`druid.indexer.runner.javaCommand`|Command required to execute java.|java|
|`druid.indexer.runner.javaOpts`|*DEPRECATED* A string of -X Java options to pass to the peon's JVM. Quotable parameters or parameters with spaces are encouraged to use javaOptsArray|""|
|`druid.indexer.runner.javaOptsArray`|A json array of strings to be passed in as options to the peon's jvm. This is additive to javaOpts and is recommended for properly handling arguments which contain quotes or spaces like `["-XX:OnOutOfMemoryError=kill -9 %p"]`|`[]`|
|`druid.indexer.runner.maxZnodeBytes`|The maximum size Znode in bytes that can be created in Zookeeper.|524288|
|`druid.indexer.runner.startPort`|Starting port used for peon processes, should be greater than 1023.|8100|
|`druid.indexer.runner.tlsStartPort`|Starting TLS port for peon processes, should be greater than 1023.|8300|
|`druid.indexer.runner.separateIngestionEndpoint`|*Deprecated.* Use separate server and consequently separate jetty thread pool for ingesting events. Not supported with TLS.|false|
|`druid.worker.ip`|The IP of the worker.|localhost|
|`druid.worker.version`|Version identifier for the middle manager.|0|
|`druid.worker.capacity`|Maximum number of tasks the middle manager can accept.|Number of available processors - 1|

### Peon Processing

Processing properties set on the Middlemanager will be passed through to Peons.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.processing.buffer.sizeBytes`|This specifies a buffer size for the storage of intermediate results. The computation engine in both the Historical and Realtime nodes will use a scratch buffer of this size to do all of their intermediate computations off-heap. Larger values allow for more aggregations in a single pass over the data while smaller values can require more passes depending on the query that is being executed.|1073741824 (1GB)|
|`druid.processing.buffer.poolCacheMaxCount`|processing buffer pool caches the buffers for later use, this is the maximum count cache will grow to. note that pool can create more buffers than it can cache if necessary.|Integer.MAX_VALUE|
|`druid.processing.formatString`|Realtime and historical nodes use this format string to name their processing threads.|processing-%s|
|`druid.processing.numMergeBuffers`|The number of direct memory buffers available for merging query results. The buffers are sized by `druid.processing.buffer.sizeBytes`. This property is effectively a concurrency limit for queries that require merging buffers. If you are using any queries that require merge buffers (currently, just groupBy v2) then you should have at least two of these.|`max(2, druid.processing.numThreads / 4)`|
|`druid.processing.numThreads`|The number of processing threads to have available for parallel processing of segments. Our rule of thumb is `num_cores - 1`, which means that even under heavy load there will still be one core available to do background tasks like talking with ZooKeeper and pulling down segments. If only one core is available, this property defaults to the value `1`.|Number of cores - 1 (or 1)|
|`druid.processing.columnCache.sizeBytes`|Maximum size in bytes for the dimension value lookup cache. Any value greater than `0` enables the cache. It is currently disabled by default. Enabling the lookup cache can significantly improve the performance of aggregators operating on dimension values, such as the JavaScript aggregator, or cardinality aggregator, but can slow things down if the cache hit rate is low (i.e. dimensions with few repeating values). Enabling it may also require additional garbage collection tuning to avoid long GC pauses.|`0` (disabled)|
|`druid.processing.fifo`|If the processing queue should treat tasks of equal priority in a FIFO manner|`false`|
|`druid.processing.tmpDir`|Path where temporary files created while processing a query should be stored. If specified, this configuration takes priority over the default `java.io.tmpdir` path.|path represented by `java.io.tmpdir`|

The amount of direct memory needed by Druid is at least
`druid.processing.buffer.sizeBytes * (druid.processing.numMergeBuffers + druid.processing.numThreads + 1)`. You can
ensure at least this amount of direct memory is available by providing `-XX:MaxDirectMemorySize=<VALUE>` in
`druid.indexer.runner.javaOptsArray` as documented above.

### Peon Query Configuration

See [general query configuration](#general-query-configuration).

### Peon Caching

You can optionally configure caching to be enabled on the peons by setting caching configs here.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.realtime.cache.useCache`|true, false|Enable the cache on the realtime.|false|
|`druid.realtime.cache.populateCache`|true, false|Populate the cache on the realtime.|false|
|`druid.realtime.cache.unCacheable`|All druid query types|All query types to not cache.|`["groupBy", "select"]`|

See [cache configuration](#cache-configuration) for how to configure cache settings.


### Additional Peon Configuration
Although peons inherit the configurations of their parent middle managers, explicit child peon configs in middle manager can be set by prefixing them with:

```
druid.indexer.fork.property
```
Additional peon configs include:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.peon.mode`|Choices are "local" and "remote". Setting this to local means you intend to run the peon as a standalone node (Not recommended).|remote|
|`druid.indexer.task.baseDir`|Base temporary working directory.|`System.getProperty("java.io.tmpdir")`|
|`druid.indexer.task.baseTaskDir`|Base temporary working directory for tasks.|`${druid.indexer.task.baseDir}/persistent/tasks`|
|`druid.indexer.task.defaultHadoopCoordinates`|Hadoop version to use with HadoopIndexTasks that do not request a particular version.|org.apache.hadoop:hadoop-client:2.3.0|
|`druid.indexer.task.defaultRowFlushBoundary`|Highest row count before persisting to disk. Used for indexing generating tasks.|75000|
|`druid.indexer.task.directoryLockTimeout`|Wait this long for zombie peons to exit before giving up on their replacements.|PT10M|
|`druid.indexer.task.gracefulShutdownTimeout`|Wait this long on middleManager restart for restorable tasks to gracefully exit.|PT5M|
|`druid.indexer.task.hadoopWorkingPath`|Temporary working directory for Hadoop tasks.|`/tmp/druid-indexing`|
|`druid.indexer.task.restoreTasksOnRestart`|If true, middleManagers will attempt to stop tasks gracefully on shutdown and restore them on restart.|false|
|`druid.indexer.server.maxChatRequests`|Maximum number of concurrent requests served by a task's chat handler. Set to 0 to disable limiting.|0|

If the deprecated `druid.indexer.runner.separateIngestionEndpoint` property is set to true then following configurations
are available for the ingestion server at peon:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.server.chathandler.http.numThreads`|*Deprecated.* Number of threads for HTTP requests.|Math.max(10, (Number of available processors * 17) / 16 + 2) + 30|
|`druid.indexer.server.chathandler.http.maxIdleTime`|*Deprecated.* The Jetty max idle time for a connection.|PT5m|

If the peon is running in remote mode, there must be an overlord up and running. Peons in remote mode can set the following configurations:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.peon.taskActionClient.retry.minWait`|The minimum retry time to communicate with overlord.|PT5S|
|`druid.peon.taskActionClient.retry.maxWait`|The maximum retry time to communicate with overlord.|PT1M|
|`druid.peon.taskActionClient.retry.maxRetryCount`|The maximum number of retries to communicate with overlord.|60|

##### SegmentWriteOutMediumFactory

When new segments are created, Druid temporarily stores some pre-processed data in some buffers. Currently two types of
*medium* exist for those buffers: *temporary files* and *off-heap memory*.

*Temporary files* (`tmpFile`) are stored under the task working directory (see `druid.indexer.task.baseTaskDir`
configuration above) and thus share it's mounting properies, e. g. they could be backed by HDD, SSD or memory (tmpfs).
This type of medium may do unnecessary disk I/O and requires some disk space to be available.

*Off-heap memory medium* (`offHeapMemory`) creates buffers in off-heap memory of a JVM process that is running a task.
This type of medium is preferred, but it may require to allow the JVM to have more off-heap memory, by changing
`-XX:MaxDirectMemorySize` configuration. It is not yet understood how does the required off-heap memory size relates
to the size of the segments being created. But definitely it doesn't make sense to add more extra off-heap memory,
than the configured maximum *heap* size (`-Xmx`) for the same JVM.

For most types of tasks SegmentWriteOutMediumFactory could be configured per-task (see [Tasks](../ingestion/tasks.html)
page, "TuningConfig" section), but if it's not specified for a task, or it's not supported for a particular task type,
then the value from the configuration below is used:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.peon.defaultSegmentWriteOutMediumFactory`|`tmpFile` or `offHeapMemory`, see explanation above|`tmpFile`|

## Broker

For general Broker Node information, see [here](../design/broker.html).

These Broker configurations can be defined in the `broker/runtime.properties` file.

### Broker Node Configs

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current node. This is used to advertise the current processes location as reachable from another node and should generally be specified such that `http://${druid.host}/` could actually talk to this process|InetAddress.getLocalHost().getCanonicalHostName()|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8082|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.enableTlsPort](../operations/tls-support.html) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative Integer.|8282|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|druid/broker|

### Query Configuration

#### Query Prioritization

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.broker.balancer.type`|`random`, `connectionCount`|Determines how the broker balances connections to historical nodes. `random` choose randomly, `connectionCount` picks the node with the fewest number of active connections to|`random`|
|`druid.broker.select.tier`|`highestPriority`, `lowestPriority`, `custom`|If segments are cross-replicated across tiers in a cluster, you can tell the broker to prefer to select segments in a tier with a certain priority.|`highestPriority`|
|`druid.broker.select.tier.custom.priorities`|`An array of integer priorities.`|Select servers in tiers with a custom priority list.|None|

#### Concurrent Requests

Druid uses Jetty to serve HTTP requests.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.http.numThreads`|Number of threads for HTTP requests.|max(10, (Number of cores * 17) / 16 + 2) + 30|
|`druid.server.http.queueSize`|Size of the worker queue used by Jetty server to temporarily store incoming client connections. If this value is set and a request is rejected by jetty because queue is full then client would observe request failure with TCP connection being closed immediately with a completely empty response from server.|Unbounded|
|`druid.server.http.maxIdleTime`|The Jetty max idle time for a connection.|PT5m|
|`druid.server.http.enableRequestLimit`|If enabled, no requests would be queued in jetty queue and "HTTP 429 Too Many Requests" error response would be sent. |false|
|`druid.server.http.defaultQueryTimeout`|Query timeout in millis, beyond which unfinished queries will be cancelled|300000|
|`druid.server.http.maxScatterGatherBytes`|Maximum number of bytes gathered from data nodes such as historicals and realtime processes to execute a query. This is an advance configuration that allows to protect in case broker is under heavy load and not utilizing the data gathered in memory fast enough and leading to OOMs. This limit can be further reduced at query time using `maxScatterGatherBytes` in the context. Note that having large limit is not necessarily bad if broker is never under heavy concurrent load in which case data gathered is processed quickly and freeing up the memory used.|Long.MAX_VALUE|
|`druid.broker.http.numConnections`|Size of connection pool for the Broker to connect to historical and real-time processes. If there are more queries than this number that all need to speak to the same node, then they will queue up.|20|
|`druid.broker.http.compressionCodec`|Compression codec the Broker uses to communicate with historical and real-time processes. May be "gzip" or "identity".|gzip|
|`druid.broker.http.readTimeout`|The timeout for data reads from historical and real-time processes.|PT15M|
|`druid.server.http.maxQueryTimeout`|Maximum allowed value (in milliseconds) for `timeout` parameter. See [query-context](query-context.html) to know more about `timeout`. Query is rejected if the query context `timeout` is greater than this value. |Long.MAX_VALUE|
|`druid.server.http.maxRequestHeaderSize`|Maximum size of a request header in bytes. Larger headers consume more memory and can make a server more vulnerable to denial of service attacks. |8 * 1024|


#### Retry Policy

Druid broker can optionally retry queries internally for transient errors.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.broker.retryPolicy.numTries`|Number of tries.|1|

#### Processing

The broker uses processing configs for nested groupBy queries. And, optionally, Long-interval queries (of any type) can be broken into shorter interval queries and processed in parallel inside this thread pool. For more details, see "chunkPeriod" in [Query Context](../querying/query-context.html) doc.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.processing.buffer.sizeBytes`|This specifies a buffer size for the storage of intermediate results. The computation engine in both the Historical and Realtime nodes will use a scratch buffer of this size to do all of their intermediate computations off-heap. Larger values allow for more aggregations in a single pass over the data while smaller values can require more passes depending on the query that is being executed.|1073741824 (1GB)|
|`druid.processing.buffer.poolCacheMaxCount`|processing buffer pool caches the buffers for later use, this is the maximum count cache will grow to. note that pool can create more buffers than it can cache if necessary.|Integer.MAX_VALUE|
|`druid.processing.formatString`|Realtime and historical nodes use this format string to name their processing threads.|processing-%s|
|`druid.processing.numMergeBuffers`|The number of direct memory buffers available for merging query results. The buffers are sized by `druid.processing.buffer.sizeBytes`. This property is effectively a concurrency limit for queries that require merging buffers. If you are using any queries that require merge buffers (currently, just groupBy v2) then you should have at least two of these.|`max(2, druid.processing.numThreads / 4)`|
|`druid.processing.numThreads`|The number of processing threads to have available for parallel processing of segments. Our rule of thumb is `num_cores - 1`, which means that even under heavy load there will still be one core available to do background tasks like talking with ZooKeeper and pulling down segments. If only one core is available, this property defaults to the value `1`.|Number of cores - 1 (or 1)|
|`druid.processing.columnCache.sizeBytes`|Maximum size in bytes for the dimension value lookup cache. Any value greater than `0` enables the cache. It is currently disabled by default. Enabling the lookup cache can significantly improve the performance of aggregators operating on dimension values, such as the JavaScript aggregator, or cardinality aggregator, but can slow things down if the cache hit rate is low (i.e. dimensions with few repeating values). Enabling it may also require additional garbage collection tuning to avoid long GC pauses.|`0` (disabled)|
|`druid.processing.fifo`|If the processing queue should treat tasks of equal priority in a FIFO manner|`false`|
|`druid.processing.tmpDir`|Path where temporary files created while processing a query should be stored. If specified, this configuration takes priority over the default `java.io.tmpdir` path.|path represented by `java.io.tmpdir`|

The amount of direct memory needed by Druid is at least
`druid.processing.buffer.sizeBytes * (druid.processing.numMergeBuffers + druid.processing.numThreads + 1)`. You can
ensure at least this amount of direct memory is available by providing `-XX:MaxDirectMemorySize=<VALUE>` at the command
line.


#### Broker Query Configuration

See [general query configuration](#general-query-configuration).

### SQL

The Druid SQL server is configured through the following properties on the broker.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.sql.enable`|Whether to enable SQL at all, including background metadata fetching. If false, this overrides all other SQL-related properties and disables SQL metadata, serving, and planning completely.|false|
|`druid.sql.avatica.enable`|Whether to enable JDBC querying at `/druid/v2/sql/avatica/`.|true|
|`druid.sql.avatica.maxConnections`|Maximum number of open connections for the Avatica server. These are not HTTP connections, but are logical client connections that may span multiple HTTP connections.|50|
|`druid.sql.avatica.maxRowsPerFrame`|Maximum number of rows to return in a single JDBC frame. Setting this property to -1 indicates that no row limit should be applied. Clients can optionally specify a row limit in their requests; if a client specifies a row limit, the lesser value of the client-provided limit and `maxRowsPerFrame` will be used.|100,000|
|`druid.sql.avatica.maxStatementsPerConnection`|Maximum number of simultaneous open statements per Avatica client connection.|1|
|`druid.sql.avatica.connectionIdleTimeout`|Avatica client connection idle timeout.|PT5M|
|`druid.sql.http.enable`|Whether to enable JSON over HTTP querying at `/druid/v2/sql/`.|true|
|`druid.sql.planner.maxQueryCount`|Maximum number of queries to issue, including nested queries. Set to 1 to disable sub-queries, or set to 0 for unlimited.|8|
|`druid.sql.planner.maxSemiJoinRowsInMemory`|Maximum number of rows to keep in memory for executing two-stage semi-join queries like `SELECT * FROM Employee WHERE DeptName IN (SELECT DeptName FROM Dept)`.|100000|
|`druid.sql.planner.maxTopNLimit`|Maximum threshold for a [TopN query](../querying/topnquery.html). Higher limits will be planned as [GroupBy queries](../querying/groupbyquery.html) instead.|100000|
|`druid.sql.planner.metadataRefreshPeriod`|Throttle for metadata refreshes.|PT1M|
|`druid.sql.planner.selectPageSize`|Page size threshold for [Select queries](../querying/select-query.html). Select queries for larger resultsets will be issued back-to-back using pagination.|1000|
|`druid.sql.planner.useApproximateCountDistinct`|Whether to use an approximate cardinalty algorithm for `COUNT(DISTINCT foo)`.|true|
|`druid.sql.planner.useApproximateTopN`|Whether to use approximate [TopN queries](../querying/topnquery.html) when a SQL query could be expressed as such. If false, exact [GroupBy queries](../querying/groupbyquery.html) will be used instead.|true|
|`druid.sql.planner.useFallback`|Whether to evaluate operations on the broker when they cannot be expressed as Druid queries. This option is not recommended for production since it can generate unscalable query plans. If false, SQL queries that cannot be translated to Druid queries will fail.|false|

### Broker Caching

You can optionally only configure caching to be enabled on the broker by setting caching configs here.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.broker.cache.useCache`|true, false|Enable the cache on the broker.|false|
|`druid.broker.cache.populateCache`|true, false|Populate the cache on the broker.|false|
|`druid.broker.cache.unCacheable`|All druid query types|All query types to not cache.|`["groupBy", "select"]`|
|`druid.broker.cache.cacheBulkMergeLimit`|positive integer or 0|Queries with more segments than this number will not attempt to fetch from cache at the broker level, leaving potential caching fetches (and cache result merging) to the historicals|`Integer.MAX_VALUE`|

See [cache configuration](#cache-configuration) for how to configure cache settings.

### Segment Discovery
|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.announcer.type`|batch or http|Segment discovery method to use. "http" enables discovering segments using HTTP instead of zookeeper.|batch|
|`druid.broker.segment.watchedTiers`|List of strings|Broker watches the segment announcements from nodes serving segments to build cache of which node is serving which segments, this configuration allows to only consider segments being served from a whitelist of tiers. By default, Broker would consider all tiers. This can be used to partition your dataSources in specific historical tiers and configure brokers in partitions so that they are only queryable for specific dataSources.|none|
|`druid.broker.segment.watchedDataSources`|List of strings|Broker watches the segment announcements from nodes serving segments to build cache of which node is serving which segments, this configuration allows to only consider segments being served from a whitelist of dataSources. By default, Broker would consider all datasources. This can be used to configure brokers in partitions so that they are only queryable for specific dataSources.|none|

## Historical

For general Historical Node information, see [here](../design/historical.html).

These Historical configurations can be defined in the `historical/runtime.properties` file.

### Historical Node Configuration

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current node. This is used to advertise the current processes location as reachable from another node and should generally be specified such that `http://${druid.host}/` could actually talk to this process|InetAddress.getLocalHost().getCanonicalHostName()|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8083|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.enableTlsPort](../operations/tls-support.html) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative Integer.|8283|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|druid/historical|

### Historical General Configuration

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.maxSize`|The maximum number of bytes-worth of segments that the node wants assigned to it. This is not a limit that Historical nodes actually enforces, just a value published to the Coordinator node so it can plan accordingly.|0|
|`druid.server.tier`| A string to name the distribution tier that the storage node belongs to. Many of the [rules Coordinator nodes use](../operations/rule-configuration.html) to manage segments can be keyed on tiers. |  `_default_tier` |
|`druid.server.priority`|In a tiered architecture, the priority of the tier, thus allowing control over which nodes are queried. Higher numbers mean higher priority. The default (no priority) works for architecture with no cross replication (tiers that have no data-storage overlap). Data centers typically have equal priority. | 0 |

### Storing Segments

|Property|Description|Default|
|--------|-----------|-------|
|`druid.segmentCache.locations`|Segments assigned to a Historical node are first stored on the local file system (in a disk cache) and then served by the Historical node. These locations define where that local cache resides. This value cannot be NULL or EMPTY. Here is an example `druid.segmentCache.locations=[{"path": "/mnt/druidSegments", "maxSize": 10000, "freeSpacePercent": 1.0}]`. "freeSpacePercent" is optional, if provided then enforces that much of free disk partition space while storing segments. But, it depends on File.getTotalSpace() and File.getFreeSpace() methods, so enable if only if they work for your File System.| none |
|`druid.segmentCache.deleteOnRemove`|Delete segment files from cache once a node is no longer serving a segment.|true|
|`druid.segmentCache.dropSegmentDelayMillis`|How long a node delays before completely dropping segment.|30000 (30 seconds)|
|`druid.segmentCache.infoDir`|Historical nodes keep track of the segments they are serving so that when the process is restarted they can reload the same segments without waiting for the Coordinator to reassign. This path defines where this metadata is kept. Directory will be created if needed.|${first_location}/info_dir|
|`druid.segmentCache.announceIntervalMillis`|How frequently to announce segments while segments are loading from cache. Set this value to zero to wait for all segments to be loaded before announcing.|5000 (5 seconds)|
|`druid.segmentCache.numLoadingThreads`|How many segments to drop or load concurrently from from deep storage.|10|
|`druid.segmentCache.numBootstrapThreads`|How many segments to load concurrently from local storage at startup.|Same as numLoadingThreads|

In `druid.segmentCache.locations`, *freeSpacePercent* was added because *maxSize* setting is only a theoretical limit and assumes that much space will always be available for storing segments. In case of any druid bug leading to unaccounted segment files left alone on disk or some other process writing stuff to disk, This check can start failing segment loading early before filling up the disk completely and leaving the host usable otherwise.

### Historical Query Configs

#### Concurrent Requests

Druid uses Jetty to serve HTTP requests.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.http.numThreads`|Number of threads for HTTP requests.|max(10, (Number of cores * 17) / 16 + 2) + 30|
|`druid.server.http.queueSize`|Size of the worker queue used by Jetty server to temporarily store incoming client connections. If this value is set and a request is rejected by jetty because queue is full then client would observe request failure with TCP connection being closed immediately with a completely empty response from server.|Unbounded|
|`druid.server.http.maxIdleTime`|The Jetty max idle time for a connection.|PT5m|
|`druid.server.http.enableRequestLimit`|If enabled, no requests would be queued in jetty queue and "HTTP 429 Too Many Requests" error response would be sent. |false|
|`druid.server.http.defaultQueryTimeout`|Query timeout in millis, beyond which unfinished queries will be cancelled|300000|
|`druid.server.http.maxQueryTimeout`|Maximum allowed value (in milliseconds) for `timeout` parameter. See [query-context](query-context.html) to know more about `timeout`. Query is rejected if the query context `timeout` is greater than this value. |Long.MAX_VALUE|
|`druid.server.http.maxRequestHeaderSize`|Maximum size of a request header in bytes. Larger headers consume more memory and can make a server more vulnerable to denial of service attacks.|8 * 1024|

#### Processing

|Property|Description|Default|
|--------|-----------|-------|
|`druid.processing.buffer.sizeBytes`|This specifies a buffer size for the storage of intermediate results. The computation engine in both the Historical and Realtime nodes will use a scratch buffer of this size to do all of their intermediate computations off-heap. Larger values allow for more aggregations in a single pass over the data while smaller values can require more passes depending on the query that is being executed.|1073741824 (1GB)|
|`druid.processing.buffer.poolCacheMaxCount`|processing buffer pool caches the buffers for later use, this is the maximum count cache will grow to. note that pool can create more buffers than it can cache if necessary.|Integer.MAX_VALUE|
|`druid.processing.formatString`|Realtime and historical nodes use this format string to name their processing threads.|processing-%s|
|`druid.processing.numMergeBuffers`|The number of direct memory buffers available for merging query results. The buffers are sized by `druid.processing.buffer.sizeBytes`. This property is effectively a concurrency limit for queries that require merging buffers. If you are using any queries that require merge buffers (currently, just groupBy v2) then you should have at least two of these.|`max(2, druid.processing.numThreads / 4)`|
|`druid.processing.numThreads`|The number of processing threads to have available for parallel processing of segments. Our rule of thumb is `num_cores - 1`, which means that even under heavy load there will still be one core available to do background tasks like talking with ZooKeeper and pulling down segments. If only one core is available, this property defaults to the value `1`.|Number of cores - 1 (or 1)|
|`druid.processing.columnCache.sizeBytes`|Maximum size in bytes for the dimension value lookup cache. Any value greater than `0` enables the cache. It is currently disabled by default. Enabling the lookup cache can significantly improve the performance of aggregators operating on dimension values, such as the JavaScript aggregator, or cardinality aggregator, but can slow things down if the cache hit rate is low (i.e. dimensions with few repeating values). Enabling it may also require additional garbage collection tuning to avoid long GC pauses.|`0` (disabled)|
|`druid.processing.fifo`|If the processing queue should treat tasks of equal priority in a FIFO manner|`false`|
|`druid.processing.tmpDir`|Path where temporary files created while processing a query should be stored. If specified, this configuration takes priority over the default `java.io.tmpdir` path.|path represented by `java.io.tmpdir`|

The amount of direct memory needed by Druid is at least
`druid.processing.buffer.sizeBytes * (druid.processing.numMergeBuffers + druid.processing.numThreads + 1)`. You can
ensure at least this amount of direct memory is available by providing `-XX:MaxDirectMemorySize=<VALUE>` at the command
line.

#### Historical Query Configuration

See [general query configuration](#general-query-configuration).

### Historical Caching

You can optionally only configure caching to be enabled on the historical by setting caching configs here.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.historical.cache.useCache`|true, false|Enable the cache on the historical.|false|
|`druid.historical.cache.populateCache`|true, false|Populate the cache on the historical.|false|
|`druid.historical.cache.unCacheable`|All druid query types|All query types to not cache.|["groupBy", "select"]|

See [cache configuration](#cache-configuration) for how to configure cache settings.

## Cache Configuration

This section describes caching configuration that is common to broker, historical, and middleManager/peon nodes.
 
Caching can optionally be enabled on the broker, historical, and realtime
processing. See [broker](broker.html#caching), 
[historical](historical.html#caching), and [realtime](realtime.html#caching)  
configuration options for how to enable it for different processes.

Druid uses a local in-memory cache by default, unless a diffrent type of cache is specified.
Use the `druid.cache.type` configuration to set a different kind of cache.

Cache settings are set globally, so the same configuration can be re-used
for both broker and historical nodes, when defined in the common properties file.

### Cache Type

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.cache.type`|`local`, `memcached`, `hybrid`|The type of cache to use for queries. See below of the configuration options for each cache type|`local`|

#### Local Cache

<div class="note caution">
DEPRECATED: Use caffeine instead
</div>

The local cache is deprecated in favor of the Caffeine cache, and may be removed in a future version of Druid. The Caffeine cache affords significantly better performance and control over eviction behavior compared to `local` cache, and is recommended in any situation where you are using JRE 8u60 or higher.

A simple in-memory LRU cache. Local cache resides in JVM heap memory, so if you enable it, make sure you increase heap size accordingly.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.cache.sizeInBytes`|Maximum cache size in bytes. Zero disables caching.|0|
|`druid.cache.initialSize`|Initial size of the hashtable backing the cache.|500000|
|`druid.cache.logEvictionCount`|If non-zero, log cache eviction every `logEvictionCount` items.|0|

#### Caffeine Cache

A highly performant local cache implementation for Druid based on [Caffeine](https://github.com/ben-manes/caffeine). Requires a JRE8u60 or higher if using `COMMON_FJP`.

##### Configuration

Below are the configuration options known to this module:

|`runtime.properties`|Description|Default|
|--------------------|-----------|-------|
|`druid.cache.type`| Set this to `caffeine`|`local`|
|`druid.cache.sizeInBytes`|The maximum size of the cache in bytes on heap.|None (unlimited)|
|`druid.cache.expireAfter`|The time (in ms) after an access for which a cache entry may be expired|None (no time limit)|
|`druid.cache.cacheExecutorFactory`|The executor factory to use for Caffeine maintenance. One of `COMMON_FJP`, `SINGLE_THREAD`, or `SAME_THREAD`|ForkJoinPool common pool (`COMMON_FJP`)|
|`druid.cache.evictOnClose`|If a close of a namespace (ex: removing a segment from a node) should cause an eager eviction of associated cache values|`false`|

##### `druid.cache.cacheExecutorFactory`

Here are the possible values for `druid.cache.cacheExecutorFactory`, which controls how maintenance tasks are run

* `COMMON_FJP` (default) use the common ForkJoinPool. Should use with [JRE 8u60 or higher](https://github.com/druid-io/druid/pull/4810#issuecomment-329922810). Older versions of the JRE may have worse performance than newer JRE versions.
* `SINGLE_THREAD` Use a single-threaded executor.
* `SAME_THREAD` Cache maintenance is done eagerly.

#### Metrics
In addition to the normal cache metrics, the caffeine cache implementation also reports the following in both `total` and `delta`

|Metric|Description|Normal value|
|------|-----------|------------|
|`query/cache/caffeine/*/requests`|Count of hits or misses|hit + miss|
|`query/cache/caffeine/*/loadTime`|Length of time caffeine spends loading new values (unused feature)|0|
|`query/cache/caffeine/*/evictionBytes`|Size in bytes that have been evicted from the cache|Varies, should tune cache `sizeInBytes` so that `sizeInBytes`/`evictionBytes` is approximately the rate of cache churn you desire|


#### Memcached

Uses memcached as cache backend. This allows all nodes to share the same cache.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.cache.expiration`|Memcached [expiration time](https://code.google.com/p/memcached/wiki/NewCommands#Standard_Protocol).|2592000 (30 days)|
|`druid.cache.timeout`|Maximum time in milliseconds to wait for a response from Memcached.|500|
|`druid.cache.hosts`|Comma separated list of Memcached hosts `<host:port>`.|none|
|`druid.cache.maxObjectSize`|Maximum object size in bytes for a Memcached object.|52428800 (50 MB)|
|`druid.cache.memcachedPrefix`|Key prefix for all keys in Memcached.|druid|
|`druid.cache.numConnections`|Number of memcached connections to use.|1|

#### Hybrid

Uses a combination of any two caches as a two-level L1 / L2 cache.
This may be used to combine a local in-memory cache with a remote memcached cache.

Cache requests will first check L1 cache before checking L2.
If there is an L1 miss and L2 hit, it will also populate L1.


|Property|Description|Default|
|--------|-----------|-------|
|`druid.cache.l1.type`|type of cache to use for L1 cache. See `druid.cache.type` configuration for valid types.|`local`|
|`druid.cache.l2.type`|type of cache to use for L2 cache. See `druid.cache.type` configuration for valid types.|`local`|
|`druid.cache.l1.*`|Any property valid for the given type of L1 cache can be set using this prefix. For instance, if you are using a `local` L1 cache, specify `druid.cache.l1.sizeInBytes` to set its size.|defaults are the same as for the given cache type.|
|`druid.cache.l2.*`|Prefix for L2 cache settings, see description for L1.|defaults are the same as for the given cache type.|
|`druid.cache.useL2`|A boolean indicating whether to query L2 cache, if it's a miss in L1. It makes sense to configure this to `false` on historical nodes, if L2 is a remote cache like `memcached`, and this cache also used on brokers, because in this case if a query reached historical it means that a broker didn't find corresponding results in the same remote cache, so a query to the remote cache from historical is guaranteed to be a miss.|`true`|
|`druid.cache.populateL2`|A boolean indicating whether to put results into L2 cache.|`true`|

## General Query Configuration

This section describes configurations that control behavior of Druid's query types, applicable to broker, historical, and middle manager nodes.

### TopN Query config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.topN.minTopNThreshold`|See [TopN Aliasing](../querying/topnquery.html#aliasing) for details.|1000|

### Search Query Config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.search.maxSearchLimit`|Maximum number of search results to return.|1000|
|`druid.query.search.searchStrategy`|Default search query strategy.|useIndexes|

### Segment Metadata Query Config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.segmentMetadata.defaultHistory`|When no interval is specified in the query, use a default interval of defaultHistory before the end time of the most recent segment, specified in ISO8601 format. This property also controls the duration of the default interval used by GET /druid/v2/datasources/{dataSourceName} interactions for retrieving datasource dimensions/metrics.|P1W|
|`druid.query.segmentMetadata.defaultAnalysisTypes`|This can be used to set the Default Analysis Types for all segment metadata queries, this can be overridden when making the query|["cardinality", "interval", "minmax"]|

### GroupBy Query Config

This section describes the configurations for groupBy queries. You can set the runtime properties in the `runtime.properties` file on broker, historical, and MiddleManager nodes. You can set the query context parameters through the [query context](../querying/query-context.html).
  
#### Configurations for groupBy v2

Supported runtime properties:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.groupBy.maxMergingDictionarySize`|Maximum amount of heap space (approximately) to use for the string dictionary during merging. When the dictionary exceeds this size, a spill to disk will be triggered.|100000000|
|`druid.query.groupBy.maxOnDiskStorage`|Maximum amount of disk space to use, per-query, for spilling result sets to disk when either the merging buffer or the dictionary fills up. Queries that exceed this limit will fail. Set to zero to disable disk spilling.|0 (disabled)|

Supported query contexts:

|Key|Description|
|---|-----------|
|`maxMergingDictionarySize`|Can be used to lower the value of `druid.query.groupBy.maxMergingDictionarySize` for this query.|
|`maxOnDiskStorage`|Can be used to lower the value of `druid.query.groupBy.maxOnDiskStorage` for this query.|

### Advanced configurations

#### Common configurations for all groupBy strategies

Supported runtime properties:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.groupBy.defaultStrategy`|Default groupBy query strategy.|v2|
|`druid.query.groupBy.singleThreaded`|Merge results using a single thread.|false|

Supported query contexts:

|Key|Description|
|---|-----------|
|`groupByStrategy`|Overrides the value of `druid.query.groupBy.defaultStrategy` for this query.|
|`groupByIsSingleThreaded`|Overrides the value of `druid.query.groupBy.singleThreaded` for this query.|


#### GroupBy v2 configurations

Supported runtime properties:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.groupBy.bufferGrouperInitialBuckets`|Initial number of buckets in the off-heap hash table used for grouping results. Set to 0 to use a reasonable default (1024).|0|
|`druid.query.groupBy.bufferGrouperMaxLoadFactor`|Maximum load factor of the off-heap hash table used for grouping results. When the load factor exceeds this size, the table will be grown or spilled to disk. Set to 0 to use a reasonable default (0.7).|0|
|`druid.query.groupBy.forceHashAggregation`|Force to use hash-based aggregation.|false|
|`druid.query.groupBy.intermediateCombineDegree`|Number of intermediate nodes combined together in the combining tree. Higher degrees will need less threads which might be helpful to improve the query performance by reducing the overhead of too many threads if the server has sufficiently powerful cpu cores.|8|
|`druid.query.groupBy.numParallelCombineThreads`|Hint for the number of parallel combining threads. This should be larger than 1 to turn on the parallel combining feature. The actual number of threads used for parallel combining is min(`druid.query.groupBy.numParallelCombineThreads`, `druid.processing.numThreads`).|1 (disabled)|

Supported query contexts:

|Key|Description|Default|
|---|-----------|-------|
|`bufferGrouperInitialBuckets`|Overrides the value of `druid.query.groupBy.bufferGrouperInitialBuckets` for this query.|None|
|`bufferGrouperMaxLoadFactor`|Overrides the value of `druid.query.groupBy.bufferGrouperMaxLoadFactor` for this query.|None|
|`forceHashAggregation`|Overrides the value of `druid.query.groupBy.forceHashAggregation`|None|
|`intermediateCombineDegree`|Overrides the value of `druid.query.groupBy.intermediateCombineDegree`|None|
|`numParallelCombineThreads`|Overrides the value of `druid.query.groupBy.numParallelCombineThreads`|None|
|`sortByDimsFirst`|Sort the results first by dimension values and then by timestamp.|false|
|`forceLimitPushDown`|When all fields in the orderby are part of the grouping key, the broker will push limit application down to the historical nodes. When the sorting order uses fields that are not in the grouping key, applying this optimization can result in approximate results with unknown accuracy, so this optimization is disabled by default in that case. Enabling this context flag turns on limit push down for limit/orderbys that contain non-grouping key columns.|false|

#### GroupBy v1 configurations

Supported runtime properties:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.groupBy.maxIntermediateRows`|Maximum number of intermediate rows for the per-segment grouping engine. This is a tuning parameter that does not impose a hard limit; rather, it potentially shifts merging work from the per-segment engine to the overall merging index. Queries that exceed this limit will not fail.|50000|
|`druid.query.groupBy.maxResults`|Maximum number of results. Queries that exceed this limit will fail.|500000|

Supported query contexts:

|Key|Description|Default|
|---|-----------|-------|
|`maxIntermediateRows`|Can be used to lower the value of `druid.query.groupBy.maxIntermediateRows` for this query.|None|
|`maxResults`|Can be used to lower the value of `druid.query.groupBy.maxResults` for this query.|None|
|`useOffheap`|Set to true to store aggregations off-heap when merging results.|false|


## Realtime nodes

Configuration for the deprecated realtime node can be found [here](../configuration/realtime.html).