---
id: index
title: "Configuration reference"
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->


This page documents all of the configuration properties for each Druid service type.

## Recommended Configuration File Organization

A recommended way of organizing Druid configuration files can be seen in the `conf` directory in the Druid package root, shown below:

```
$ ls -R conf
druid

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
```

Each directory has a `runtime.properties` file containing configuration properties for the specific Druid process corresponding to the directory (e.g., `historical`).

The `jvm.config` files contain JVM flags such as heap sizing properties for each service.

Common properties shared by all services are placed in `_common/common.runtime.properties`.

## Common Configurations

The properties under this section are common configurations that should be shared across all Druid services in a cluster.

### JVM Configuration Best Practices

There are four JVM parameters that we set on all of our processes:

1.  `-Duser.timezone=UTC` This sets the default timezone of the JVM to UTC. We always set this and do not test with other default timezones, so local timezones might work, but they also might uncover weird and interesting bugs. To issue queries in a non-UTC timezone, see [query granularities](../querying/granularities.md#period-granularities)
2.  `-Dfile.encoding=UTF-8` This is similar to timezone, we test assuming UTF-8. Local encodings might work, but they also might result in weird and interesting bugs.
3.  `-Djava.io.tmpdir=<a path>` Various parts of the system that interact with the file system do it via temporary files, and these files can get somewhat large. Many production systems are set up to have small (but fast) `/tmp` directories, which can be problematic with Druid so we recommend pointing the JVM’s tmp directory to something with a little more meat. This directory should not be volatile tmpfs. This directory should also have good read and write speed and hence NFS mount should strongly be avoided.
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
|`druid.modules.excludeList`|A JSON array of canonical class names (e.g., `"org.apache.druid.somepackage.SomeModule"`) of module classes which shouldn't be loaded, even if they are found in extensions specified by `druid.extensions.loadList`, or in the list of core modules specified to be loaded on a particular Druid process type. Useful when some useful extension contains some module, which shouldn't be loaded on some Druid process type because some dependencies of that module couldn't be satisfied.|[]|

### Zookeeper
We recommend just setting the base ZK path and the ZK service host, but all ZK paths that Druid uses can be overwritten to absolute paths.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.zk.paths.base`|Base Zookeeper path.|`/druid`|
|`druid.zk.service.host`|The ZooKeeper hosts to connect to. This is a REQUIRED property and therefore a host address must be supplied.|none|
|`druid.zk.service.user`|The username to authenticate with ZooKeeper. This is an optional property.|none|
|`druid.zk.service.pwd`|The [Password Provider](../operations/password-provider.md) or the string password to authenticate with ZooKeeper. This is an optional property.|none|
|`druid.zk.service.authScheme`|digest is the only authentication scheme supported. |digest|

#### Zookeeper Behavior

|Property|Description|Default|
|--------|-----------|-------|
|`druid.zk.service.sessionTimeoutMs`|ZooKeeper session timeout, in milliseconds.|`30000`|
|`druid.zk.service.connectionTimeoutMs`|ZooKeeper connection timeout, in milliseconds.|`15000`|
|`druid.zk.service.compress`|Boolean flag for whether or not created Znodes should be compressed.|`true`|
|`druid.zk.service.acl`|Boolean flag for whether or not to enable ACL security for ZooKeeper. If ACL is enabled, zNode creators will have all permissions.|`false`|

#### Path Configuration
Druid interacts with ZK through a set of standard path configurations. We recommend just setting the base ZK path, but all ZK paths that Druid uses can be overwritten to absolute paths.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.zk.paths.base`|Base Zookeeper path.|`/druid`|
|`druid.zk.paths.propertiesPath`|Zookeeper properties path.|`${druid.zk.paths.base}/properties`|
|`druid.zk.paths.announcementsPath`|Druid process announcement path.|`${druid.zk.paths.base}/announcements`|
|`druid.zk.paths.liveSegmentsPath`|Current path for where Druid processes announce their segments.|`${druid.zk.paths.base}/segments`|
|`druid.zk.paths.loadQueuePath`|Entries here cause Historical processes to load and drop segments.|`${druid.zk.paths.base}/loadQueue`|
|`druid.zk.paths.coordinatorPath`|Used by the Coordinator for leader election.|`${druid.zk.paths.base}/coordinator`|
|`druid.zk.paths.servedSegmentsPath`|@Deprecated. Legacy path for where Druid processes announce their segments.|`${druid.zk.paths.base}/servedSegments`|

The indexing service also uses its own set of paths. These configs can be included in the common configuration.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.zk.paths.indexer.base`|Base zookeeper path for |`${druid.zk.paths.base}/indexer`|
|`druid.zk.paths.indexer.announcementsPath`|Middle managers announce themselves here.|`${druid.zk.paths.indexer.base}/announcements`|
|`druid.zk.paths.indexer.tasksPath`|Used to assign tasks to MiddleManagers.|`${druid.zk.paths.indexer.base}/tasks`|
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
|`druid.exhibitor.service.pollingMs`|How often to poll the exhibitors for the list|`10000`|

Note that `druid.zk.service.host` is used as a backup in case an Exhibitor instance can't be contacted and therefore should still be set.

### TLS

#### General Configuration

|Property|Description|Default|
|--------|-----------|-------|
|`druid.enablePlaintextPort`|Enable/Disable HTTP connector.|`true`|
|`druid.enableTlsPort`|Enable/Disable HTTPS connector.|`false`|

Although not recommended but both HTTP and HTTPS connectors can be enabled at a time and respective ports are configurable using `druid.plaintextPort`
and `druid.tlsPort` properties on each process. Please see `Configuration` section of individual processes to check the valid and default values for these ports.

#### Jetty Server TLS Configuration

Druid uses Jetty as an embedded web server. To get familiar with TLS/SSL in general and related concepts like Certificates etc.
reading this [Jetty documentation](http://www.eclipse.org/jetty/documentation/9.4.32.v20200930/configuring-ssl.html) might be helpful.
To get more in depth knowledge of TLS/SSL support in Java in general, please refer to this [guide](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html).
The documentation [here](http://www.eclipse.org/jetty/documentation/9.4.32.v20200930/configuring-ssl.html#configuring-sslcontextfactory)
can help in understanding TLS/SSL configurations listed below. This [document](http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html) lists all the possible
values for the below mentioned configs among others provided by Java implementation.

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.server.https.keyStorePath`|The file path or URL of the TLS/SSL Key store.|none|yes|
|`druid.server.https.keyStoreType`|The type of the key store.|none|yes|
|`druid.server.https.certAlias`|Alias of TLS/SSL certificate for the connector.|none|yes|
|`druid.server.https.keyStorePassword`|The [Password Provider](../operations/password-provider.md) or String password for the Key Store.|none|yes|

Following table contains non-mandatory advanced configuration options, use caution.

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.server.https.keyManagerFactoryAlgorithm`|Algorithm to use for creating KeyManager, more details [here](https://docs.oracle.com/javase/7/docs/technotes/guides/security/jsse/JSSERefGuide.html#KeyManager).|`javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm()`|no|
|`druid.server.https.keyManagerPassword`|The [Password Provider](../operations/password-provider.md) or String password for the Key Manager.|none|no|
|`druid.server.https.includeCipherSuites`|List of cipher suite names to include. You can either use the exact cipher suite name or a regular expression.|Jetty's default include cipher list|no|
|`druid.server.https.excludeCipherSuites`|List of cipher suite names to exclude. You can either use the exact cipher suite name or a regular expression.|Jetty's default exclude cipher list|no|
|`druid.server.https.includeProtocols`|List of exact protocols names to include.|Jetty's default include protocol list|no|
|`druid.server.https.excludeProtocols`|List of exact protocols names to exclude.|Jetty's default exclude protocol list|no|


#### Internal Client TLS Configuration (requires `simple-client-sslcontext` extension)

These properties apply to the SSLContext that will be provided to the internal HTTP client that Druid services use to communicate with each other. These properties require the `simple-client-sslcontext` extension to be loaded. Without it, Druid services will be unable to communicate with each other when TLS is enabled.

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.client.https.protocol`|SSL protocol to use.|`TLSv1.2`|no|
|`druid.client.https.trustStoreType`|The type of the key store where trusted root certificates are stored.|`java.security.KeyStore.getDefaultType()`|no|
|`druid.client.https.trustStorePath`|The file path or URL of the TLS/SSL Key store where trusted root certificates are stored.|none|yes|
|`druid.client.https.trustStoreAlgorithm`|Algorithm to be used by TrustManager to validate certificate chains|`javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm()`|no|
|`druid.client.https.trustStorePassword`|The [Password Provider](../operations/password-provider.md) or String password for the Trust Store.|none|yes|

This [document](http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html) lists all the possible
values for the above mentioned configs among others provided by Java implementation.

### Authentication and Authorization

|Property|Type|Description|Default|Required|
|--------|-----------|--------|--------|--------|
|`druid.auth.authenticatorChain`|JSON List of Strings|List of Authenticator type names|["allowAll"]|no|
|`druid.escalator.type`|String|Type of the Escalator that should be used for internal Druid communications. This Escalator must use an authentication scheme that is supported by an Authenticator in `druid.auth.authenticatorChain`.|"noop"|no|
|`druid.auth.authorizers`|JSON List of Strings|List of Authorizer type names |["allowAll"]|no|
|`druid.auth.unsecuredPaths`| List of Strings|List of paths for which security checks will not be performed. All requests to these paths will be allowed.|[]|no|
|`druid.auth.allowUnauthenticatedHttpOptions`|Boolean|If true, skip authentication checks for HTTP OPTIONS requests. This is needed for certain use cases, such as supporting CORS pre-flight requests. Note that disabling authentication checks for OPTIONS requests will allow unauthenticated users to determine what Druid endpoints are valid (by checking if the OPTIONS request returns a 200 instead of 404), so enabling this option may reveal information about server configuration, including information about what extensions are loaded (if those extensions add endpoints).|false|no|

For more information, please see [Authentication and Authorization](../design/auth.md).

For configuration options for specific auth extensions, please refer to the extension documentation.

### Startup Logging

All processes can log debugging information on startup.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.startup.logging.logProperties`|Log all properties on startup (from common.runtime.properties, runtime.properties, and the JVM command line).|false|
|`druid.startup.logging.maskProperties`|Masks sensitive properties (passwords, for example) containing theses words.|["password"]|

Note that some sensitive information may be logged if these settings are enabled.

### Request Logging

All processes that can serve queries can also log the query requests they see. Broker processes can additionally log the SQL requests (both from HTTP and JDBC) they see.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.request.logging.type`|Choices: noop, file, emitter, slf4j, filtered, composing, switching. How to log every query request.|[required to configure request logging]|

Note that, you can enable sending all the HTTP requests to log by setting  "org.apache.druid.jetty.RequestLog" to DEBUG level. See [Logging](../configuration/logging.md)

#### File Request Logging

Daily request logs are stored on disk.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.request.logging.dir`|Historical, Realtime and Broker processes maintain request logs of all of the requests they get (interaction is via POST, so normal request logs don’t generally capture information about the actual query), this specifies the directory to store the request logs in|none|
|`druid.request.logging.filePattern`|[Joda datetime format](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html) for each file|"yyyy-MM-dd'.log'"|

The format of request logs is TSV, one line per requests, with five fields: timestamp, remote\_addr, native\_query, query\_context, sql\_query.

For native JSON request, the `sql_query` field is empty. Example
```
2019-01-14T10:00:00.000Z        127.0.0.1   {"queryType":"topN","dataSource":{"type":"table","name":"wikiticker"},"virtualColumns":[],"dimension":{"type":"LegacyDimensionSpec","dimension":"page","outputName":"page","outputType":"STRING"},"metric":{"type":"LegacyTopNMetricSpec","metric":"count"},"threshold":10,"intervals":{"type":"LegacySegmentSpec","intervals":["2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z"]},"filter":null,"granularity":{"type":"all"},"aggregations":[{"type":"count","name":"count"}],"postAggregations":[],"context":{"queryId":"74c2d540-d700-4ebd-b4a9-3d02397976aa"},"descending":false}    {"query/time":100,"query/bytes":800,"success":true,"identity":"user1"}
```

For SQL query request, the `native_query` field is empty. Example
```
2019-01-14T10:00:00.000Z        127.0.0.1       {"sqlQuery/time":100,"sqlQuery/bytes":600,"success":true,"identity":"user1"}  {"query":"SELECT page, COUNT(*) AS Edits FROM wikiticker WHERE __time BETWEEN TIMESTAMP '2015-09-12 00:00:00' AND TIMESTAMP '2015-09-13 00:00:00' GROUP BY page ORDER BY Edits DESC LIMIT 10","context":{"sqlQueryId":"c9d035a0-5ffd-4a79-a865-3ffdadbb5fdd","nativeQueryIds":"[490978e4-f5c7-4cf6-b174-346e63cf8863]"}}
```

#### Emitter Request Logging

Every request is emitted to some external location.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.request.logging.feed`|Feed name for requests.|none|

#### SLF4J Request Logging

Every request is logged via SLF4J. Native queries are serialized into JSON in the log message regardless of the SLF4J format specification. They will be logged under the class `org.apache.druid.server.log.LoggingRequestLogger`.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.request.logging.setMDC`|If MDC entries should be set in the log entry. Your logging setup still has to be configured to handle MDC to format this data|false|
|`druid.request.logging.setContextMDC`|If the druid query `context` should be added to the MDC entries. Has no effect unless `setMDC` is `true`|false|

For native query, the following MDC fields are populated with `setMDC`:

|MDC field|Description|
|---------|-----------|
|`queryId`   |The query ID|
|`sqlQueryId`|The SQL query ID if this query is part of a SQL request|
|`dataSource`|The datasource the query was against|
|`queryType` |The type of the query|
|`hasFilters`|If the query has any filters|
|`remoteAddr`|The remote address of the requesting client|
|`duration`  |The duration of the query interval|
|`resultOrdering`|The ordering of results|
|`descending`|If the query is a descending query|

#### Filtered Request Logging
Filtered Request Logger filters requests based on a configurable query/time threshold (for native query) and sqlQuery/time threshold (for SQL query).
For native query, only request logs where query/time is above the threshold are emitted. For SQL query, only request logs where sqlQuery/time is above the threshold are emitted.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.request.logging.queryTimeThresholdMs`|Threshold value for query/time in milliseconds.|0, i.e., no filtering|
|`druid.request.logging.sqlQueryTimeThresholdMs`|Threshold value for sqlQuery/time in milliseconds.|0, i.e., no filtering|
|`druid.request.logging.mutedQueryTypes` | Query requests of these types are not logged. Query types are defined as string objects corresponding to the "queryType" value for the specified query in the Druid's [native JSON query API](http://druid.apache.org/docs/latest/querying/querying). Misspelled query types will be ignored. Example to ignore scan and timeBoundary queries: ["scan", "timeBoundary"]| []|
|`druid.request.logging.delegate.type`|Type of delegate request logger to log requests.|none|

#### Composite Request Logging
Composite Request Logger emits request logs to multiple request loggers.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.request.logging.loggerProviders`|List of request loggers for emitting request logs.|none|

#### Switching Request Logging
Switching Request Logger routes native query's request logs to one request logger and SQL query's request logs to another request logger.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.request.logging.nativeQueryLogger`|request logger for emitting native query's request logs.|none|
|`druid.request.logging.sqlQueryLogger`|request logger for emitting SQL query's request logs.|none|

### Audit Logging

Coordinator and Overlord log changes to lookups, segment load/drop rules, dynamic configuration changes for auditing

|Property|Description|Default|
|--------|-----------|-------|
|`druid.audit.manager.auditHistoryMillis`|Default duration for querying audit history.|1 week|
|`druid.audit.manager.includePayloadAsDimensionInMetric`|Boolean flag on whether to add `payload` column in service metric.|false|

### Enabling Metrics

Druid processes periodically emit metrics and different metrics monitors can be included. Each process can overwrite the default list of monitors.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.monitoring.emissionPeriod`|How often metrics are emitted.|PT1M|
|`druid.monitoring.monitors`|Sets list of Druid monitors used by a process. See below for names and more information. For example, you can specify monitors for a Broker with `druid.monitoring.monitors=["org.apache.druid.java.util.metrics.SysMonitor","org.apache.druid.java.util.metrics.JvmMonitor"]`.|none (no monitors)|

The following monitors are available:

|Name|Description|
|----|-----------|
|`org.apache.druid.client.cache.CacheMonitor`|Emits metrics (to logs) about the segment results cache for Historical and Broker processes. Reports typical cache statistics include hits, misses, rates, and size (bytes and number of entries), as well as timeouts and and errors.|
|`org.apache.druid.java.util.metrics.SysMonitor`|This uses the [SIGAR library](https://github.com/hyperic/sigar) to report on various system activities and statuses.|
|`org.apache.druid.server.metrics.HistoricalMetricsMonitor`|Reports statistics on Historical processes.|
|`org.apache.druid.java.util.metrics.JvmMonitor`|Reports various JVM-related statistics.|
|`org.apache.druid.java.util.metrics.JvmCpuMonitor`|Reports statistics of CPU consumption by the JVM.|
|`org.apache.druid.java.util.metrics.CpuAcctDeltaMonitor`|Reports consumed CPU as per the cpuacct cgroup.|
|`org.apache.druid.java.util.metrics.JvmThreadsMonitor`|Reports Thread statistics in the JVM, like numbers of total, daemon, started, died threads.|
|`org.apache.druid.segment.realtime.RealtimeMetricsMonitor`|Reports statistics on Realtime processes.|
|`org.apache.druid.server.metrics.EventReceiverFirehoseMonitor`|Reports how many events have been queued in the EventReceiverFirehose.|
|`org.apache.druid.server.metrics.QueryCountStatsMonitor`|Reports how many queries have been successful/failed/interrupted.|
|`org.apache.druid.server.emitter.HttpEmittingMonitor`|Reports internal metrics of `http` or `parametrized` emitter (see below). Must not be used with another emitter type. See the description of the metrics here: https://github.com/apache/druid/pull/4973.|
|`org.apache.druid.server.metrics.TaskCountStatsMonitor`|Reports how many ingestion tasks are currently running/pending/waiting and also the number of successful/failed tasks per emission period.|


### Emitting Metrics

The Druid servers [emit various metrics](../operations/metrics.md) and alerts via something we call an Emitter. There are three emitter implementations included with the code, a "noop" emitter (the default if none is specified), one that just logs to log4j ("logging"), and one that does POSTs of JSON events to a server ("http"). The properties for using the logging emitter are described below.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.emitter`|Setting this value to "noop", "logging", "http" or "parametrized" will initialize one of the emitter modules. The value "composing" can be used to initialize multiple emitter modules. |noop|

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
|`druid.emitter.http.basicAuthentication`|[Password Provider](../operations/password-provider.md) for providing Login and password for authentication in "login:password" form, e.g., `druid.emitter.http.basicAuthentication=admin:adminpassword` uses Default Password Provider which allows plain text passwords.|not specified = no authentication|
|`druid.emitter.http.flushTimeOut`|The timeout after which an event should be sent to the endpoint, even if internal buffers are not filled, in milliseconds.|not specified = no timeout|
|`druid.emitter.http.batchingStrategy`|The strategy of how the batch is formatted. "ARRAY" means `[event1,event2]`, "NEWLINES" means `event1\nevent2`, ONLY_EVENTS means `event1event2`.|ARRAY|
|`druid.emitter.http.maxBatchSize`|The maximum batch size, in bytes.|the minimum of (10% of JVM heap size divided by 2) or (5191680 (i. e. 5 MB))|
|`druid.emitter.http.batchQueueSizeLimit`|The maximum number of batches in emitter queue, if there are problems with emitting.|the maximum of (2) or (10% of the JVM heap size divided by 5MB)|
|`druid.emitter.http.minHttpTimeoutMillis`|If the speed of filling batches imposes timeout smaller than that, not even trying to send batch to endpoint, because it will likely fail, not being able to send the data that fast. Configure this depending based on emitter/successfulSending/minTimeMs metric. Reasonable values are 10ms..100ms.|0|
|`druid.emitter.http.recipientBaseUrl`|The base URL to emit messages to. Druid will POST JSON to be consumed at the HTTP endpoint specified by this property.|none, required config|

#### Http Emitter Module TLS Overrides

When emitting events to a TLS-enabled receiver, the Http Emitter will by default use an SSLContext obtained via the
process described at [Druid's internal communication over TLS](../operations/tls-support.md), i.e., the same
SSLContext that would be used for internal communications between Druid processes.

In some use cases it may be desirable to have the Http Emitter use its own separate truststore configuration. For example, there may be organizational policies that prevent the TLS-enabled metrics receiver's certificate from being added to the same truststore used by Druid's internal HTTP client.

The following properties allow the Http Emitter to use its own truststore configuration when building its SSLContext.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.emitter.http.ssl.useDefaultJavaContext`|If set to true, the HttpEmitter will use `SSLContext.getDefault()`, the default Java SSLContext, and all other properties below are ignored.|false|
|`druid.emitter.http.ssl.trustStorePath`|The file path or URL of the TLS/SSL Key store where trusted root certificates are stored. If this is unspecified, the Http Emitter will use the same SSLContext as Druid's internal HTTP client, as described in the beginning of this section, and all other properties below are ignored.|null|
|`druid.emitter.http.ssl.trustStoreType`|The type of the key store where trusted root certificates are stored.|`java.security.KeyStore.getDefaultType()`|
|`druid.emitter.http.ssl.trustStoreAlgorithm`|Algorithm to be used by TrustManager to validate certificate chains|`javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm()`|
|`druid.emitter.http.ssl.trustStorePassword`|The [Password Provider](../operations/password-provider.md) or String password for the Trust Store.|none|
|`druid.emitter.http.ssl.protocol`|TLS protocol to use.|"TLSv1.2"|

#### Parametrized Http Emitter Module

`druid.emitter.parametrized.httpEmitting.*` configs correspond to the configs of Http Emitter Modules, see above.
Except `recipientBaseUrl`. E.g., `druid.emitter.parametrized.httpEmitting.flushMillis`,
`druid.emitter.parametrized.httpEmitting.flushCount`, `druid.emitter.parametrized.httpEmitting.ssl.trustStorePath`, etc.

The additional configs are:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.emitter.parametrized.recipientBaseUrlPattern`|The URL pattern to send an event to, based on the event's feed. E.g., `http://foo.bar/{feed}`, that will send event to `http://foo.bar/metrics` if the event's feed is "metrics".|none, required config|

#### Composing Emitter Module

|Property|Description|Default|
|--------|-----------|-------|
|`druid.emitter.composing.emitters`|List of emitter modules to load, e.g., ["logging","http"].|[]|

#### Graphite Emitter

To use graphite as emitter set `druid.emitter=graphite`. For configuration details please follow this [link](../development/extensions-contrib/graphite.md).


### Metadata storage

These properties specify the JDBC connection and other configuration around the metadata storage. The only processes that connect to the metadata storage with these properties are the [Coordinator](../design/coordinator.md) and [Overlord](../design/overlord.md).

|Property|Description|Default|
|--------|-----------|-------|
|`druid.metadata.storage.type`|The type of metadata storage to use. Choose from "mysql", "postgresql", or "derby".|derby|
|`druid.metadata.storage.connector.connectURI`|The JDBC URI for the database to connect to|none|
|`druid.metadata.storage.connector.user`|The username to connect with.|none|
|`druid.metadata.storage.connector.password`|The [Password Provider](../operations/password-provider.md) or String password used to connect with.|none|
|`druid.metadata.storage.connector.createTables`|If Druid requires a table and it doesn't exist, create it?|true|
|`druid.metadata.storage.tables.base`|The base name for tables.|druid|
|`druid.metadata.storage.tables.dataSource`|The table to use to look for dataSources which created by [Kafka Indexing Service](../development/extensions-core/kafka-ingestion.md).|druid_dataSource|
|`druid.metadata.storage.tables.pendingSegments`|The table to use to look for pending segments.|druid_pendingSegments|
|`druid.metadata.storage.tables.segments`|The table to use to look for segments.|druid_segments|
|`druid.metadata.storage.tables.rules`|The table to use to look for segment load/drop rules.|druid_rules|
|`druid.metadata.storage.tables.config`|The table to use to look for configs.|druid_config|
|`druid.metadata.storage.tables.tasks`|Used by the indexing service to store tasks.|druid_tasks|
|`druid.metadata.storage.tables.taskLog`|Used by the indexing service to store task logs.|druid_taskLog|
|`druid.metadata.storage.tables.taskLock`|Used by the indexing service to store task locks.|druid_taskLock|
|`druid.metadata.storage.tables.supervisors`|Used by the indexing service to store supervisor configurations.|druid_supervisors|
|`druid.metadata.storage.tables.audit`|The table to use for audit history of configuration changes, e.g., Coordinator rules.|druid_audit|

### Deep storage

The configurations concern how to push and pull [Segments](../design/segments.md) from deep storage.

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
The below table shows some important configurations for S3. See [S3 Deep Storage](../development/extensions-core/s3.md) for full configurations.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.storage.bucket`|S3 bucket name.|none|
|`druid.storage.baseKey`|S3 object key prefix for storage.|none|
|`druid.storage.disableAcl`|Boolean flag for ACL. If this is set to `false`, the full control would be granted to the bucket owner. This may require to set additional permissions. See [S3 permissions settings](../development/extensions-core/s3.md#s3-permissions-settings).|false|
|`druid.storage.archiveBucket`|S3 bucket name for archiving when running the *archive task*.|none|
|`druid.storage.archiveBaseKey`|S3 object key prefix for archiving.|none|
|`druid.storage.sse.type`|Server-side encryption type. Should be one of `s3`, `kms`, and `custom`. See the below [Server-side encryption section](../development/extensions-core/s3.md#server-side-encryption) for more details.|None|
|`druid.storage.sse.kms.keyId`|AWS KMS key ID. This is used only when `druid.storage.sse.type` is `kms` and can be empty to use the default key ID.|None|
|`druid.storage.sse.custom.base64EncodedKey`|Base64-encoded key. Should be specified if `druid.storage.sse.type` is `custom`.|None|
|`druid.storage.useS3aSchema`|If true, use the "s3a" filesystem when using Hadoop-based ingestion. If false, the "s3n" filesystem will be used. Only affects Hadoop-based ingestion.|false|

#### HDFS Deep Storage

This deep storage is used to interface with HDFS.  Note that the `druid-hdfs-storage` extension must be loaded.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.storage.storageDirectory`|HDFS directory to use as deep storage.|none|

#### Cassandra Deep Storage

This deep storage is used to interface with Cassandra.  Note that the `druid-cassandra-storage` extension must be loaded.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.storage.host`|Cassandra host.|none|
|`druid.storage.keyspace`|Cassandra key space.|none|


### Task Logging

If you are running the indexing service in remote mode, the task logs must be stored in S3, Azure Blob Store, Google Cloud Storage or HDFS.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.type`|Choices:noop, s3, azure, google, hdfs, file. Where to store task logs|file|

You can also configure the Overlord to automatically retain the task logs in log directory and entries in task-related metadata storage tables only for last x milliseconds by configuring following additional properties.
Caution: Automatic log file deletion typically works based on log file modification timestamp on the backing store, so large clock skews between druid processes and backing store nodes might result in unintended behavior.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.kill.enabled`|Boolean value for whether to enable deletion of old task logs. If set to true, Overlord will submit kill tasks periodically based on `druid.indexer.logs.kill.delay` specified, which will delete task logs from the log directory as well as tasks and tasklogs table entries in metadata storage except for tasks created in the last `druid.indexer.logs.kill.durationToRetain` period. |false|
|`druid.indexer.logs.kill.durationToRetain`| Required if kill is enabled. In milliseconds, task logs and entries in task-related metadata storage tables to be retained created in last x milliseconds. |None|
|`druid.indexer.logs.kill.initialDelay`| Optional. Number of milliseconds after Overlord start when first auto kill is run. |random value less than 300000 (5 mins)|
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
|`druid.indexer.logs.disableAcl`|Boolean flag for ACL. If this is set to `false`, the full control would be granted to the bucket owner. If the task logs bucket is the same as the deep storage (S3) bucket, then the value of this property will need to be set to true if druid.storage.disableAcl has been set to true.|false|

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

### Overlord Discovery

This config is used to find the [Overlord](../design/overlord.md) using Curator service discovery. Only required if you are actually running an Overlord.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.selectors.indexing.serviceName`|The druid.service name of the Overlord process. To start the Overlord with a different name, set it with this property. |druid/overlord|


### Coordinator Discovery

This config is used to find the [Coordinator](../design/coordinator.md) using Curator service discovery. This config is used by the realtime indexing processes to get information about the segments loaded in the cluster.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.selectors.coordinator.serviceName`|The druid.service name of the Coordinator process. To start the Coordinator with a different name, set it with this property. |druid/coordinator|


### Announcing Segments

You can configure how to announce and unannounce Znodes in ZooKeeper (using Curator). For normal operations you do not need to override any of these configs.

##### Batch Data Segment Announcer

In current Druid, multiple data segments may be announced under the same Znode.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.announcer.segmentsPerNode`|Each Znode contains info for up to this many segments.|50|
|`druid.announcer.maxBytesPerNode`|Max byte size for Znode.|524288|
|`druid.announcer.skipDimensionsAndMetrics`|Skip Dimensions and Metrics list from segment announcements. NOTE: Enabling this will also remove the dimensions and metrics list from Coordinator and Broker endpoints.|false|
|`druid.announcer.skipLoadSpec`|Skip segment LoadSpec from segment announcements. NOTE: Enabling this will also remove the loadspec from Coordinator and Broker endpoints.|false|

### JavaScript

Druid supports dynamic runtime extension through JavaScript functions. This functionality can be configured through
the following properties.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.javascript.enabled`|Set to "true" to enable JavaScript functionality. This affects the JavaScript parser, filter, extractionFn, aggregator, post-aggregator, router strategy, and worker selection strategy.|false|

> JavaScript-based functionality is disabled by default. Please refer to the Druid [JavaScript programming guide](../development/javascript.md) for guidelines about using Druid's JavaScript functionality, including instructions on how to enable it.

### Double Column storage

Prior to version 0.13.0, Druid's storage layer used a 32-bit float representation to store columns created by the
doubleSum, doubleMin, and doubleMax aggregators at indexing time.
Starting from version 0.13.0 the default will be 64-bit floats for Double columns.
Using 64-bit representation for double column will lead to avoid precision loss at the cost of doubling the storage size of such columns.
To keep the old format set the system-wide property `druid.indexing.doubleStorage=float`.
You can also use floatSum, floatMin and floatMax to use 32-bit float representation.
Support for 64-bit floating point columns was released in Druid 0.11.0, so if you use this feature then older versions of Druid will not be able to read your data segments.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexing.doubleStorage`|Set to "float" to use 32-bit double representation for double columns.|double|

### SQL compatible null handling
Prior to version 0.13.0, Druid string columns treated `''` and `null` values as interchangeable, and numeric columns were unable to represent `null` values, coercing `null` to `0`. Druid 0.13.0 introduced a mode which enabled SQL compatible null handling, allowing string columns to distinguish empty strings from nulls, and numeric columns to contain null rows.

|Property|Description|Default|
|---|---|---|
|`druid.generic.useDefaultValueForNull`|When set to `true`, `null` values will be stored as `''` for string columns and `0` for numeric columns. Set to `false` to store and query data in SQL compatible mode.|`true`|

This mode does have a storage size and query performance cost, see [segment documentation](../design/segments.md#sql-compatible-null-handling) for more details.

### HTTP Client

All Druid components can communicate with each other over HTTP.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.global.http.numConnections`|Size of connection pool per destination URL. If there are more HTTP requests than this number that all need to speak to the same URL, then they will queue up.|`20`|
|`druid.global.http.compressionCodec`|Compression codec to communicate with others. May be "gzip" or "identity".|`gzip`|
|`druid.global.http.readTimeout`|The timeout for data reads.|`PT15M`|
|`druid.global.http.unusedConnectionTimeout`|The timeout for idle connections in connection pool. The connection in the pool will be closed after this timeout and a new one will be established. This timeout should be less than `druid.global.http.readTimeout`. Set this timeout = ~90% of `druid.global.http.readTimeout`|`PT4M`|
|`druid.global.http.numMaxThreads`|Maximum number of I/O worker threads|`max(10, ((number of cores * 17) / 16 + 2) + 30)`|

## Master Server

This section contains the configuration options for the processes that reside on Master servers (Coordinators and Overlords) in the suggested [three-server configuration](../design/processes.md#server-types).

### Coordinator

For general Coordinator Process information, see [here](../design/coordinator.md).

#### Static Configuration

These Coordinator static configurations can be defined in the `coordinator/runtime.properties` file.

##### Coordinator Process Config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current process. This is used to advertise the current processes location as reachable from another process and should generally be specified such that `http://${druid.host}/` could actually talk to this process|InetAddress.getLocalHost().getCanonicalHostName()|
|`druid.bindOnHost`|Indicating whether the process's internal jetty server bind on `druid.host`. Default is false, which means binding to all interfaces.|false|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8081|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.enableTlsPort](../operations/tls-support.md) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative Integer.|8281|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|druid/coordinator|

##### Coordinator Operation

|Property|Description|Default|
|--------|-----------|-------|
|`druid.coordinator.period`|The run period for the Coordinator. The Coordinator operates by maintaining the current state of the world in memory and periodically looking at the set of "used" segments and segments being served to make decisions about whether any changes need to be made to the data topology. This property sets the delay between each of these runs.|PT60S|
|`druid.coordinator.period.indexingPeriod`|How often to send compact/merge/conversion tasks to the indexing service. It's recommended to be longer than `druid.manager.segments.pollDuration`|PT1800S (30 mins)|
|`druid.coordinator.startDelay`|The operation of the Coordinator works on the assumption that it has an up-to-date view of the state of the world when it runs, the current ZK interaction code, however, is written in a way that doesn’t allow the Coordinator to know for a fact that it’s done loading the current state of the world. This delay is a hack to give it enough time to believe that it has all the data.|PT300S|
|`druid.coordinator.load.timeout`|The timeout duration for when the Coordinator assigns a segment to a Historical process.|PT15M|
|`druid.coordinator.kill.pendingSegments.on`|Boolean flag for whether or not the Coordinator clean up old entries in the `pendingSegments` table of metadata store. If set to true, Coordinator will check the created time of most recently complete task. If it doesn't exist, it finds the created time of the earliest running/pending/waiting tasks. Once the created time is found, then for all dataSources not in the `killPendingSegmentsSkipList` (see [Dynamic configuration](#dynamic-configuration)), Coordinator will ask the Overlord to clean up the entries 1 day or more older than the found created time in the `pendingSegments` table. This will be done periodically based on `druid.coordinator.period.indexingPeriod` specified.|true|
|`druid.coordinator.kill.on`|Boolean flag for whether or not the Coordinator should submit kill task for unused segments, that is, hard delete them from metadata store and deep storage. If set to true, then for all whitelisted dataSources (or optionally all), Coordinator will submit tasks periodically based on `period` specified. These kill tasks will delete all unused segments except for the last `durationToRetain` period. Whitelist or All can be set via dynamic configuration `killAllDataSources` and `killDataSourceWhitelist` described later.|false|
|`druid.coordinator.kill.period`|How often to send kill tasks to the indexing service. Value must be greater than `druid.coordinator.period.indexingPeriod`. Only applies if kill is turned on.|P1D (1 Day)|
|`druid.coordinator.kill.durationToRetain`| Do not kill unused segments in last `durationToRetain`, must be greater or equal to 0. Only applies and MUST be specified if kill is turned on. Note that default value is invalid.|PT-1S (-1 seconds)|
|`druid.coordinator.kill.maxSegments`|Kill at most n unused segments per kill task submission, must be greater than 0. Only applies and MUST be specified if kill is turned on. Note that default value is invalid.|0|
|`druid.coordinator.balancer.strategy`|Specify the type of balancing strategy that the coordinator should use to distribute segments among the historicals. `cachingCost` is logically equivalent to `cost` but is more CPU-efficient on large clusters and will replace `cost` in the future versions, users are invited to try it. Use `diskNormalized` to distribute segments among processes so that the disks fill up uniformly and use `random` to randomly pick processes to distribute segments.|`cost`|
|`druid.coordinator.balancer.cachingCost.awaitInitialization`|Whether to wait for segment view initialization before creating the `cachingCost` balancing strategy. This property is enabled only when `druid.coordinator.balancer.strategy` is `cachingCost`. If set to 'true', the Coordinator will not start to assign segments, until the segment view is initialized. If set to 'false', the Coordinator will fallback to use the `cost` balancing strategy only if the segment view is not initialized yet. Notes, it may take much time to wait for the initialization since the `cachingCost` balancing strategy involves much computing to build itself.|false|
|`druid.coordinator.loadqueuepeon.repeatDelay`|The start and repeat delay for the loadqueuepeon, which manages the load and drop of segments.|PT0.050S (50 ms)|
|`druid.coordinator.asOverlord.enabled`|Boolean value for whether this Coordinator process should act like an Overlord as well. This configuration allows users to simplify a druid cluster by not having to deploy any standalone Overlord processes. If set to true, then Overlord console is available at `http://coordinator-host:port/console.html` and be sure to set `druid.coordinator.asOverlord.overlordService` also. See next.|false|
|`druid.coordinator.asOverlord.overlordService`| Required, if `druid.coordinator.asOverlord.enabled` is `true`. This must be same value as `druid.service` on standalone Overlord processes and `druid.selectors.indexing.serviceName` on Middle Managers.|NULL|

##### Segment Management
|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.serverview.type`|batch or http|Segment discovery method to use. "http" enables discovering segments using HTTP instead of zookeeper.|batch|
|`druid.coordinator.loadqueuepeon.type`|curator or http|Whether to use "http" or "curator" implementation to assign segment loads/drops to historical|curator|
|`druid.coordinator.segment.awaitInitializationOnStart`|true or false|Whether the Coordinator will wait for its view of segments to fully initialize before starting up. If set to 'true', the Coordinator's HTTP server will not start up, and the Coordinator will not announce itself as available, until the server view is initialized.|true|

###### Additional config when "http" loadqueuepeon is used
|Property|Description|Default|
|--------|-----------|-------|
|`druid.coordinator.loadqueuepeon.http.batchSize`|Number of segment load/drop requests to batch in one HTTP request. Note that it must be smaller than `druid.segmentCache.numLoadingThreads` config on Historical process.|1|

##### Metadata Retrieval

|Property|Description|Default|
|--------|-----------|-------|
|`druid.manager.config.pollDuration`|How often the manager polls the config table for updates.|PT1M|
|`druid.manager.segments.pollDuration`|The duration between polls the Coordinator does for updates to the set of active segments. Generally defines the amount of lag time it can take for the Coordinator to notice new segments.|PT1M|
|`druid.manager.rules.pollDuration`|The duration between polls the Coordinator does for updates to the set of active rules. Generally defines the amount of lag time it can take for the Coordinator to notice rules.|PT1M|
|`druid.manager.rules.defaultRule`|The default rule for the cluster|_default|
|`druid.manager.rules.alertThreshold`|The duration after a failed poll upon which an alert should be emitted.|PT10M|

#### Dynamic Configuration

The Coordinator has dynamic configuration to change certain behavior on the fly. The Coordinator uses a JSON spec object from the Druid [metadata storage](../dependencies/metadata-storage.md) config table. This object is detailed below:

It is recommended that you use the Coordinator Console to configure these parameters. However, if you need to do it via HTTP, the JSON object can be submitted to the Coordinator via a POST request at:

```
http://<COORDINATOR_IP>:<PORT>/druid/coordinator/v1/config
```

Optional Header Parameters for auditing the config change can also be specified.

|Header Param Name| Description | Default |
|----------|-------------|---------|
|`X-Druid-Author`| author making the config change|""|
|`X-Druid-Comment`| comment describing the change being done|""|

A sample Coordinator dynamic config JSON object is shown below:

```json
{
  "millisToWaitBeforeDeleting": 900000,
  "mergeBytesLimit": 100000000,
  "mergeSegmentsLimit" : 1000,
  "maxSegmentsToMove": 5,
  "percentOfSegmentsToConsiderPerMove": 100,
  "replicantLifetime": 15,
  "replicationThrottleLimit": 10,
  "emitBalancingStats": false,
  "killDataSourceWhitelist": ["wikipedia", "testDatasource"],
  "decommissioningNodes": ["localhost:8182", "localhost:8282"],
  "decommissioningMaxPercentOfMaxSegmentsToMove": 70,
  "pauseCoordination": false
}
```

Issuing a GET request at the same URL will return the spec that is currently in place. A description of the config setup spec is shown below.

|Property|Description|Default|
|--------|-----------|-------|
|`millisToWaitBeforeDeleting`|How long does the Coordinator need to be a leader before it can start marking overshadowed segments as unused in metadata storage.|900000 (15 mins)|
|`mergeBytesLimit`|The maximum total uncompressed size in bytes of segments to merge.|524288000L|
|`mergeSegmentsLimit`|The maximum number of segments that can be in a single [append task](../ingestion/tasks.md).|100|
|`maxSegmentsToMove`|The maximum number of segments that can be moved at any given time.|5|
|`percentOfSegmentsToConsiderPerMove`|The percentage of the total number of segments in the cluster that are considered every time a segment needs to be selected for a move. Druid orders servers by available capacity ascending (the least available capacity first) and then iterates over the servers. For each server, Druid iterates over the segments on the server, considering them for moving. The default config of 100% means that every segment on every server is a candidate to be moved. This should make sense for most small to medium-sized clusters. However, an admin may find it preferable to drop this value lower if they don't think that it is worthwhile to consider every single segment in the cluster each time it is looking for a segment to move.|100|
|`replicantLifetime`|The maximum number of Coordinator runs for a segment to be replicated before we start alerting.|15|
|`replicationThrottleLimit`|The maximum number of segments that can be replicated at one time.|10|
|`balancerComputeThreads`|Thread pool size for computing moving cost of segments in segment balancing. Consider increasing this if you have a lot of segments and moving segments starts to get stuck.|1|
|`emitBalancingStats`|Boolean flag for whether or not we should emit balancing stats. This is an expensive operation.|false|
|`killDataSourceWhitelist`|List of specific data sources for which kill tasks are sent if property `druid.coordinator.kill.on` is true. This can be a list of comma-separated data source names or a JSON array.|none|
|`killAllDataSources`|Send kill tasks for ALL dataSources if property `druid.coordinator.kill.on` is true. If this is set to true then `killDataSourceWhitelist` must not be specified or be empty list.|false|
|`killPendingSegmentsSkipList`|List of data sources for which pendingSegments are _NOT_ cleaned up if property `druid.coordinator.kill.pendingSegments.on` is true. This can be a list of comma-separated data sources or a JSON array.|none|
|`maxSegmentsInNodeLoadingQueue`|The maximum number of segments that could be queued for loading to any given server. This parameter could be used to speed up segments loading process, especially if there are "slow" nodes in the cluster (with low loading speed) or if too much segments scheduled to be replicated to some particular node (faster loading could be preferred to better segments distribution). Desired value depends on segments loading speed, acceptable replication time and number of nodes. Value 1000 could be a start point for a rather big cluster. Default value is 0 (loading queue is unbounded) |0|
|`decommissioningNodes`| List of historical servers to 'decommission'. Coordinator will not assign new segments to 'decommissioning' servers,  and segments will be moved away from them to be placed on non-decommissioning servers at the maximum rate specified by `decommissioningMaxPercentOfMaxSegmentsToMove`.|none|
|`decommissioningMaxPercentOfMaxSegmentsToMove`|  The maximum number of segments that may be moved away from 'decommissioning' servers to non-decommissioning (that is, active) servers during one Coordinator run. This value is relative to the total maximum segment movements allowed during one run which is determined by `maxSegmentsToMove`. If `decommissioningMaxPercentOfMaxSegmentsToMove` is 0, segments will neither be moved from _or to_ 'decommissioning' servers, effectively putting them in a sort of "maintenance" mode that will not participate in balancing or assignment by load rules. Decommissioning can also become stalled if there are no available active servers to place the segments. By leveraging the maximum percent of decommissioning segment movements, an operator can prevent active servers from overload by prioritizing balancing, or decrease decommissioning time instead. The value should be between 0 and 100.|70|
|`pauseCoordination`| Boolean flag for whether or not the coordinator should execute its various duties of coordinating the cluster. Setting this to true essentially pauses all coordination work while allowing the API to remain up. Duties that are paused include all classes that implement the `CoordinatorDuty` Interface. Such duties include: Segment balancing, Segment compaction, Emission of metrics controlled by the dynamic coordinator config `emitBalancingStats`, Submitting kill tasks for unused segments (if enabled), Logging of used segments in the cluster, Marking of newly unused or overshadowed segments, Matching and execution of load/drop rules for used segments, Unloading segments that are no longer marked as used from Historical servers. An example of when an admin may want to pause coordination would be if they are doing deep storage maintenance on HDFS Name Nodes with downtime and don't want the coordinator to be directing Historical Nodes to hit the Name Node with API requests until maintenance is done and the deep store is declared healthy for use again. |false|


To view the audit history of Coordinator dynamic config issue a GET request to the URL -

```
http://<COORDINATOR_IP>:<PORT>/druid/coordinator/v1/config/history?interval=<interval>
```

default value of interval can be specified by setting `druid.audit.manager.auditHistoryMillis` (1 week if not configured) in Coordinator runtime.properties

To view last <n> entries of the audit history of Coordinator dynamic config issue a GET request to the URL -

```
http://<COORDINATOR_IP>:<PORT>/druid/coordinator/v1/config/history?count=<n>
```

##### Lookups Dynamic Configuration
These configuration options control the behavior of the Lookup dynamic configuration described in the [lookups page](../querying/lookups.md)

|Property|Description|Default|
|--------|-----------|-------|
|`druid.manager.lookups.hostDeleteTimeout`|How long to wait for a `DELETE` request to a particular process before considering the `DELETE` a failure|PT1S|
|`druid.manager.lookups.hostUpdateTimeout`|How long to wait for a `POST` request to a particular process before considering the `POST` a failure|PT10S|
|`druid.manager.lookups.deleteAllTimeout`|How long to wait for all `DELETE` requests to finish before considering the delete attempt a failure|PT10S|
|`druid.manager.lookups.updateAllTimeout`|How long to wait for all `POST` requests to finish before considering the attempt a failure|PT60S|
|`druid.manager.lookups.threadPoolSize`|How many processes can be managed concurrently (concurrent POST and DELETE requests). Requests this limit will wait in a queue until a slot becomes available.|10|
|`druid.manager.lookups.period`|How many milliseconds between checks for configuration changes|30_000|

##### Compaction Dynamic Configuration

Compaction configurations can also be set or updated dynamically using
[Coordinator's API](../operations/api-reference.md#compaction-configuration) without restarting Coordinators.

For details about segment compaction, please check [Segment Size Optimization](../operations/segment-optimization.md).

A description of the compaction config is:

|Property|Description|Required|
|--------|-----------|--------|
|`dataSource`|dataSource name to be compacted.|yes|
|`taskPriority`|[Priority](../ingestion/tasks.md#priority) of compaction task.|no (default = 25)|
|`inputSegmentSizeBytes`|Maximum number of total segment bytes processed per compaction task. Since a time chunk must be processed in its entirety, if the segments for a particular time chunk have a total size in bytes greater than this parameter, compaction will not run for that time chunk. Because each compaction task runs with a single thread, setting this value too far above 1–2GB will result in compaction tasks taking an excessive amount of time.|no (default = 419430400)|
|`maxRowsPerSegment`|Max number of rows per segment after compaction.|no|
|`skipOffsetFromLatest`|The offset for searching segments to be compacted. Strongly recommended to set for realtime dataSources. |no (default = "P1D")|
|`tuningConfig`|Tuning config for compaction tasks. See below [Compaction Task TuningConfig](#compaction-tuningconfig).|no|
|`taskContext`|[Task context](../ingestion/tasks.md#context) for compaction tasks.|no|

An example of compaction config is:

```json
{
  "dataSource": "wikiticker"
}
```

Note that compaction tasks can fail if their locks are revoked by other tasks of higher priorities.
Since realtime tasks have a higher priority than compaction task by default,
it can be problematic if there are frequent conflicts between compaction tasks and realtime tasks.
If this is the case, the coordinator's automatic compaction might get stuck because of frequent compaction task failures.
This kind of problem may happen especially in Kafka/Kinesis indexing systems which allow late data arrival.
If you see this problem, it's recommended to set `skipOffsetFromLatest` to some large enough value to avoid such conflicts between compaction tasks and realtime tasks.

###### Compaction TuningConfig

Auto compaction supports a subset of the [tuningConfig for Parallel task](../ingestion/native-batch.md#tuningconfig).
The below is a list of the supported configurations for auto compaction.

|Property|Description|Required|
|--------|-----------|--------|
|type|The task type, this should always be `index_parallel`.|yes|
|`maxRowsInMemory`|Used in determining when intermediate persists to disk should occur. Normally user does not need to set this, but depending on the nature of data, if rows are short in terms of bytes, user may not want to store a million rows in memory and this value should be set.|no (default = 1000000)|
|`maxBytesInMemory`|Used in determining when intermediate persists to disk should occur. Normally this is computed internally and user does not need to set it. This value represents number of bytes to aggregate in heap memory before persisting. This is based on a rough estimate of memory usage and not actual usage. The maximum heap memory usage for indexing is `maxBytesInMemory` * (2 + `maxPendingPersists`)|no (default = 1/6 of max JVM memory)|
|`splitHintSpec`|Used to give a hint to control the amount of data that each first phase task reads. This hint could be ignored depending on the implementation of the input source. See [Split hint spec](../ingestion/native-batch.md#split-hint-spec) for more details.|no (default = size-based split hint spec)|
|`partitionsSpec`|Defines how to partition data in each time chunk, see [`PartitionsSpec`](../ingestion/native-batch.md#partitionsspec)|no (default = `dynamic`)|
|`indexSpec`|Defines segment storage format options to be used at indexing time, see [IndexSpec](../ingestion/index.md#indexspec)|no|
|`indexSpecForIntermediatePersists`|Defines segment storage format options to be used at indexing time for intermediate persisted temporary segments. this can be used to disable dimension/metric compression on intermediate segments to reduce memory required for final merging. however, disabling compression on intermediate segments might increase page cache use while they are used before getting merged into final segment published, see [IndexSpec](../ingestion/index.md#indexspec) for possible values.|no|
|`maxPendingPersists`|Maximum number of persists that can be pending but not started. If this limit would be exceeded by a new intermediate persist, ingestion will block until the currently-running persist finishes. Maximum heap memory usage for indexing scales with `maxRowsInMemory` * (2 + `maxPendingPersists`).|no (default = 0, meaning one persist can be running concurrently with ingestion, and none can be queued up)|
|`pushTimeout`|Milliseconds to wait for pushing segments. It must be >= 0, where 0 means to wait forever.|no (default = 0)|
|`segmentWriteOutMediumFactory`|Segment write-out medium to use when creating segments. See [SegmentWriteOutMediumFactory](../ingestion/native-batch.md#segmentwriteoutmediumfactory).|no (default is the value from `druid.peon.defaultSegmentWriteOutMediumFactory.type` is used)|
|`maxNumConcurrentSubTasks`|Maximum number of worker tasks which can be run in parallel at the same time. The supervisor task would spawn worker tasks up to `maxNumConcurrentSubTasks` regardless of the current available task slots. If this value is set to 1, the supervisor task processes data ingestion on its own instead of spawning worker tasks. If this value is set to too large, too many worker tasks can be created which might block other ingestion. Check [Capacity Planning](../ingestion/native-batch.md#capacity-planning) for more details.|no (default = 1)|
|`maxRetry`|Maximum number of retries on task failures.|no (default = 3)|
|`maxNumSegmentsToMerge`|Max limit for the number of segments that a single task can merge at the same time in the second phase. Used only with `hashed` or `single_dim` partitionsSpec.|no (default = 100)|
|`totalNumMergeTasks`|Total number of tasks to merge segments in the merge phase when `partitionsSpec` is set to `hashed` or `single_dim`.|no (default = 10)|
|`taskStatusCheckPeriodMs`|Polling period in milliseconds to check running task statuses.|no (default = 1000)|
|`chatHandlerTimeout`|Timeout for reporting the pushed segments in worker tasks.|no (default = PT10S)|
|`chatHandlerNumRetries`|Retries for reporting the pushed segments in worker tasks.|no (default = 5)|

### Overlord

For general Overlord Process information, see [here](../design/overlord.md).

#### Overlord Static Configuration

These Overlord static configurations can be defined in the `overlord/runtime.properties` file.

##### Overlord Process Configs

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current process. This is used to advertise the current processes location as reachable from another process and should generally be specified such that `http://${druid.host}/` could actually talk to this process|InetAddress.getLocalHost().getCanonicalHostName()|
|`druid.bindOnHost`|Indicating whether the process's internal jetty server bind on `druid.host`. Default is false, which means binding to all interfaces.|false|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8090|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.enableTlsPort](../operations/tls-support.md) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative Integer.|8290|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|druid/overlord|

##### Overlord Operations

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.runner.type`|Choices "local" or "remote". Indicates whether tasks should be run locally or in a distributed environment. Experimental task runner "httpRemote" is also available which is same as "remote" but uses HTTP to interact with Middle Managers instead of Zookeeper.|local|
|`druid.indexer.storage.type`|Choices are "local" or "metadata". Indicates whether incoming tasks should be stored locally (in heap) or in metadata storage. Storing incoming tasks in metadata storage allows for tasks to be resumed if the Overlord should fail.|local|
|`druid.indexer.storage.recentlyFinishedThreshold`|A duration of time to store task results.|PT24H|
|`druid.indexer.tasklock.forceTimeChunkLock`|_**Setting this to false is still experimental**_<br/> If set, all tasks are enforced to use time chunk lock. If not set, each task automatically chooses a lock type to use. This configuration can be overwritten by setting `forceTimeChunkLock` in the [task context](../ingestion/tasks.md#context). See [Task Locking & Priority](../ingestion/tasks.md#context) for more details about locking in tasks.|true|
|`druid.indexer.queue.maxSize`|Maximum number of active tasks at one time.|Integer.MAX_VALUE|
|`druid.indexer.queue.startDelay`|Sleep this long before starting Overlord queue management. This can be useful to give a cluster time to re-orient itself after e.g. a widespread network issue.|PT1M|
|`druid.indexer.queue.restartDelay`|Sleep this long when Overlord queue management throws an exception before trying again.|PT30S|
|`druid.indexer.queue.storageSyncRate`|Sync Overlord state this often with an underlying task persistence mechanism.|PT1M|

The following configs only apply if the Overlord is running in remote mode. For a description of local vs. remote mode, see [Overlord Process](../design/overlord.md).

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.runner.taskAssignmentTimeout`|How long to wait after a task as been assigned to a MiddleManager before throwing an error.|PT5M|
|`druid.indexer.runner.minWorkerVersion`|The minimum MiddleManager version to send tasks to. |"0"|
|`druid.indexer.runner.compressZnodes`|Indicates whether or not the Overlord should expect MiddleManagers to compress Znodes.|true|
|`druid.indexer.runner.maxZnodeBytes`|The maximum size Znode in bytes that can be created in Zookeeper, should be in the range of [10KiB, 2GiB). [Human-readable format](human-readable-byte.md) is supported.| 512 KiB |
|`druid.indexer.runner.taskCleanupTimeout`|How long to wait before failing a task after a MiddleManager is disconnected from Zookeeper.|PT15M|
|`druid.indexer.runner.taskShutdownLinkTimeout`|How long to wait on a shutdown request to a MiddleManager before timing out|PT1M|
|`druid.indexer.runner.pendingTasksRunnerNumThreads`|Number of threads to allocate pending-tasks to workers, must be at least 1.|1|
|`druid.indexer.runner.maxRetriesBeforeBlacklist`|Number of consecutive times the MiddleManager can fail tasks,  before the worker is blacklisted, must be at least 1|5|
|`druid.indexer.runner.workerBlackListBackoffTime`|How long to wait before a task is whitelisted again. This value should be greater that the value set for taskBlackListCleanupPeriod.|PT15M|
|`druid.indexer.runner.workerBlackListCleanupPeriod`|A duration after which the cleanup thread will startup to clean blacklisted workers.|PT5M|
|`druid.indexer.runner.maxPercentageBlacklistWorkers`|The maximum percentage of workers to blacklist, this must be between 0 and 100.|20|

There are additional configs for autoscaling (if it is enabled):

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.autoscale.strategy`|Choices are "noop", "ec2" or "gce". Sets the strategy to run when autoscaling is required.|noop|
|`druid.indexer.autoscale.doAutoscale`|If set to "true" autoscaling will be enabled.|false|
|`druid.indexer.autoscale.provisionPeriod`|How often to check whether or not new MiddleManagers should be added.|PT1M|
|`druid.indexer.autoscale.terminatePeriod`|How often to check when MiddleManagers should be removed.|PT5M|
|`druid.indexer.autoscale.originTime`|The starting reference timestamp that the terminate period increments upon.|2012-01-01T00:55:00.000Z|
|`druid.indexer.autoscale.workerIdleTimeout`|How long can a worker be idle (not a run task) before it can be considered for termination.|PT90M|
|`druid.indexer.autoscale.maxScalingDuration`|How long the Overlord will wait around for a MiddleManager to show up before giving up.|PT15M|
|`druid.indexer.autoscale.numEventsToTrack`|The number of autoscaling related events (node creation and termination) to track.|10|
|`druid.indexer.autoscale.pendingTaskTimeout`|How long a task can be in "pending" state before the Overlord tries to scale up.|PT30S|
|`druid.indexer.autoscale.workerVersion`|If set, will only create nodes of set version during autoscaling. Overrides dynamic configuration. |null|
|`druid.indexer.autoscale.workerPort`|The port that MiddleManagers will run on.|8080|

##### Supervisors

|Property|Description|Default|
|--------|-----------|-------|
|`druid.supervisor.healthinessThreshold`|The number of successful runs before an unhealthy supervisor is again considered healthy.|3|
|`druid.supervisor.unhealthinessThreshold`|The number of failed runs before the supervisor is considered unhealthy.|3|
|`druid.supervisor.taskHealthinessThreshold`|The number of consecutive task successes before an unhealthy supervisor is again considered healthy.|3|
|`druid.supervisor.taskUnhealthinessThreshold`|The number of consecutive task failures before the supervisor is considered unhealthy.|3|
|`druid.supervisor.storeStackTrace`|Whether full stack traces of supervisor exceptions should be stored and returned by the supervisor `/status` endpoint.|false|
|`druid.supervisor.maxStoredExceptionEvents`|The maximum number of exception events that can be returned through the supervisor `/status` endpoint.|`max(healthinessThreshold, unhealthinessThreshold)`|

#### Overlord Dynamic Configuration

The Overlord can dynamically change worker behavior.

The JSON object can be submitted to the Overlord via a POST request at:

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
|`selectStrategy`|How to assign tasks to MiddleManagers. Choices are `fillCapacity`, `equalDistribution`, and `javascript`.|equalDistribution|
|`autoScaler`|Only used if autoscaling is enabled. See below.|null|

To view the audit history of worker config issue a GET request to the URL -

```
http://<OVERLORD_IP>:<port>/druid/indexer/v1/worker/history?interval=<interval>
```

default value of interval can be specified by setting `druid.audit.manager.auditHistoryMillis` (1 week if not configured) in Overlord runtime.properties.

To view last <n> entries of the audit history of worker config issue a GET request to the URL -

```
http://<OVERLORD_IP>:<port>/druid/indexer/v1/worker/history?count=<n>
```

##### Worker Select Strategy

Worker select strategies control how Druid assigns tasks to MiddleManagers.

###### Equal Distribution

Tasks are assigned to the MiddleManager with the most free slots at the time the task begins running. This is useful if
you want work evenly distributed across your MiddleManagers.

|Property|Description|Default|
|--------|-----------|-------|
|`type`|`equalDistribution`.|required; must be `equalDistribution`|
|`affinityConfig`|[Affinity config](#affinity) object|null (no affinity)|

###### Equal Distribution With Category Spec

This strategy is a variant of `Equal Distribution`, which support `workerCategorySpec` field rather than `affinityConfig`. By specifying `workerCategorySpec`, you can assign tasks to run on different categories of MiddleManagers based on the tasks' **taskType** and **dataSource name**. This strategy can't work with `AutoScaler` since the behavior is undefined.

|Property|Description|Default|
|--------|-----------|-------|
|`type`|`equalDistributionWithCategorySpec`.|required; must be `equalDistributionWithCategorySpec`|
|`workerCategorySpec`|[Worker Category Spec](#workercategoryspec) object|null (no worker category spec)|

Example: specify tasks default to run on **c1** whose task
type is "index_kafka", while dataSource "ds1" run on **c2**.

```json
{
  "selectStrategy": {
    "type": "equalDistributionWithCategorySpec",
    "workerCategorySpec": {
      "strong": false,
      "categoryMap": {
        "index_kafka": {
           "defaultCategory": "c1",
           "categoryAffinity": {
              "ds1": "c2"
           }
        }
      }
    }
  }
}
```

###### Fill Capacity

Tasks are assigned to the worker with the most currently-running tasks at the time the task begins running. This is
useful in situations where you are elastically auto-scaling MiddleManagers, since it will tend to pack some full and
leave others empty. The empty ones can be safely terminated.

Note that if `druid.indexer.runner.pendingTasksRunnerNumThreads` is set to _N_ > 1, then this strategy will fill _N_
MiddleManagers up to capacity simultaneously, rather than a single MiddleManager.

|Property|Description|Default|
|--------|-----------|-------|
|`type`|`fillCapacity`.|required; must be `fillCapacity`|
|`affinityConfig`|[Affinity config](#affinity) object|null (no affinity)|

###### Fill Capacity With Category Spec

This strategy is a variant of `Fill Capacity`, which support `workerCategorySpec` field rather than `affinityConfig`. The usage is the same with _equalDistributionWithCategorySpec_ strategy. This strategy can't work with `AutoScaler` since the behavior is undefined.

|Property|Description|Default|
|--------|-----------|-------|
|`type`|`fillCapacityWithCategorySpec`.|required; must be `fillCapacityWithCategorySpec`|
|`workerCategorySpec`|[Worker Category Spec](#workercategoryspec) object|null (no worker category spec)|

> Before using the _equalDistributionWithCategorySpec_ and _fillCapacityWithCategorySpec_ strategies, you must upgrade overlord and all MiddleManagers to the version that support this feature.

<a name="javascript-worker-select-strategy"></a>

###### JavaScript

Allows defining arbitrary logic for selecting workers to run task using a JavaScript function.
The function is passed remoteTaskRunnerConfig, map of workerId to available workers and task to be executed and returns the workerId on which the task should be run or null if the task cannot be run.
It can be used for rapid development of missing features where the worker selection logic is to be changed or tuned often.
If the selection logic is quite complex and cannot be easily tested in JavaScript environment,
its better to write a druid extension module with extending current worker selection strategies written in java.

|Property|Description|Default|
|--------|-----------|-------|
|`type`|`javascript`.|required; must be `javascript`|
|`function`|String representing JavaScript function| |

Example: a function that sends batch_index_task to workers 10.0.0.1 and 10.0.0.2 and all other tasks to other available workers.

```
{
"type":"javascript",
"function":"function (config, zkWorkers, task) {\nvar batch_workers = new java.util.ArrayList();\nbatch_workers.add(\"middleManager1_hostname:8091\");\nbatch_workers.add(\"middleManager2_hostname:8091\");\nworkers = zkWorkers.keySet().toArray();\nvar sortedWorkers = new Array()\n;for(var i = 0; i < workers.length; i++){\n sortedWorkers[i] = workers[i];\n}\nArray.prototype.sort.call(sortedWorkers,function(a, b){return zkWorkers.get(b).getCurrCapacityUsed() - zkWorkers.get(a).getCurrCapacityUsed();});\nvar minWorkerVer = config.getMinWorkerVersion();\nfor (var i = 0; i < sortedWorkers.length; i++) {\n var worker = sortedWorkers[i];\n  var zkWorker = zkWorkers.get(worker);\n  if(zkWorker.canRunTask(task) && zkWorker.isValidVersion(minWorkerVer)){\n    if(task.getType() == 'index_hadoop' && batch_workers.contains(worker)){\n      return worker;\n    } else {\n      if(task.getType() != 'index_hadoop' && !batch_workers.contains(worker)){\n        return worker;\n      }\n    }\n  }\n}\nreturn null;\n}"
}
```

> JavaScript-based functionality is disabled by default. Please refer to the Druid [JavaScript programming guide](../development/javascript.md) for guidelines about using Druid's JavaScript functionality, including instructions on how to enable it.

###### Affinity

Affinity configs can be provided to the _equalDistribution_ and _fillCapacity_ strategies using the "affinityConfig"
field. If not provided, the default is to not use affinity at all.

|Property|Description|Default|
|--------|-----------|-------|
|`affinity`|JSON object mapping a datasource String name to a list of indexing service MiddleManager host:port String values. Druid doesn't perform DNS resolution, so the 'host' value must match what is configured on the MiddleManager and what the MiddleManager announces itself as (examine the Overlord logs to see what your MiddleManager announces itself as).|{}|
|`strong`|With weak affinity (the default), tasks for a dataSource may be assigned to other MiddleManagers if their affinity-mapped MiddleManagers are not able to run all pending tasks in the queue for that dataSource. With strong affinity, tasks for a dataSource will only ever be assigned to their affinity-mapped MiddleManagers, and will wait in the pending queue if necessary.|false|

###### WorkerCategorySpec

WorkerCategorySpec can be provided to the _equalDistributionWithCategorySpec_ and _fillCapacityWithCategorySpec_ strategies using the "workerCategorySpec"
field. If not provided, the default is to not use it at all.

|Property|Description|Default|
|--------|-----------|-------|
|`categoryMap`|A JSON map object mapping a task type String name to a [CategoryConfig](#categoryconfig) object, by which you can specify category config for different task type.|{}|
|`strong`|With weak workerCategorySpec (the default), tasks for a dataSource may be assigned to other MiddleManagers if the MiddleManagers specified in `categoryMap` are not able to run all pending tasks in the queue for that dataSource. With strong workerCategorySpec, tasks for a dataSource will only ever be assigned to their specified MiddleManagers, and will wait in the pending queue if necessary.|false|

###### CategoryConfig

|Property|Description|Default|
|--------|-----------|-------|
|`defaultCategory`|Specify default category for a task type.|null|
|`categoryAffinity`|A JSON map object mapping a datasource String name to a category String name of the MiddleManager. If category isn't specified for a datasource, then using the `defaultCategory`. If no specified category and the `defaultCategory` is also null, then tasks can run on any available MiddleManagers.|null|

##### Autoscaler

Amazon's EC2 together with Google's GCE are currently the only supported autoscalers.

EC2's autoscaler properties are:

|Property|Description|Default|
|--------|-----------|-------|
|`minNumWorkers`|The minimum number of workers that can be in the cluster at any given time.|0|
|`maxNumWorkers`|The maximum number of workers that can be in the cluster at any given time.|0|
|`availabilityZone`|What availability zone to run in.|none|
|`nodeData`|A JSON object that describes how to launch new nodes.|none; required|
|`userData`|A JSON object that describes how to configure new nodes. If you have set druid.indexer.autoscale.workerVersion, this must have a versionReplacementString. Otherwise, a versionReplacementString is not necessary.|none; optional|

For GCE's properties, please refer to the [gce-extensions](../development/extensions-contrib/gce-extensions.md).

## Data Server

This section contains the configuration options for the processes that reside on Data servers (MiddleManagers/Peons and Historicals) in the suggested [three-server configuration](../design/processes.md#server-types).

Configuration options for the experimental [Indexer process](../design/indexer.md) are also provided here.

### MiddleManager and Peons

These MiddleManager and Peon configurations can be defined in the `middleManager/runtime.properties` file.

#### MiddleManager Process Config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current process. This is used to advertise the current processes location as reachable from another process and should generally be specified such that `http://${druid.host}/` could actually talk to this process|InetAddress.getLocalHost().getCanonicalHostName()|
|`druid.bindOnHost`|Indicating whether the process's internal jetty server bind on `druid.host`. Default is false, which means binding to all interfaces.|false|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8091|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.enableTlsPort](../operations/tls-support.md) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative Integer.|8291|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|druid/middlemanager|

#### MiddleManager Configuration

Middle managers pass their configurations down to their child peons. The MiddleManager requires the following configs:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.runner.allowedPrefixes`|Whitelist of prefixes for configs that can be passed down to child peons.|"com.metamx", "druid", "org.apache.druid", "user.timezone", "file.encoding", "java.io.tmpdir", "hadoop"|
|`druid.indexer.runner.compressZnodes`|Indicates whether or not the MiddleManagers should compress Znodes.|true|
|`druid.indexer.runner.classpath`|Java classpath for the peon.|System.getProperty("java.class.path")|
|`druid.indexer.runner.javaCommand`|Command required to execute java.|java|
|`druid.indexer.runner.javaOpts`|*DEPRECATED* A string of -X Java options to pass to the peon's JVM. Quotable parameters or parameters with spaces are encouraged to use javaOptsArray|""|
|`druid.indexer.runner.javaOptsArray`|A JSON array of strings to be passed in as options to the peon's JVM. This is additive to javaOpts and is recommended for properly handling arguments which contain quotes or spaces like `["-XX:OnOutOfMemoryError=kill -9 %p"]`|`[]`|
|`druid.indexer.runner.maxZnodeBytes`|The maximum size Znode in bytes that can be created in Zookeeper, should be in the range of [10KiB, 2GiB). [Human-readable format](human-readable-byte.md) is supported.|512KiB|
|`druid.indexer.runner.startPort`|Starting port used for peon processes, should be greater than 1023 and less than 65536.|8100|
|`druid.indexer.runner.endPort`|Ending port used for peon processes, should be greater than or equal to `druid.indexer.runner.startPort` and less than 65536.|65535|
|`druid.indexer.runner.ports`|A JSON array of integers to specify ports that used for peon processes. If provided and non-empty, ports for peon processes will be chosen from these ports. And `druid.indexer.runner.startPort/druid.indexer.runner.endPort` will be completely ignored.|`[]`|
|`druid.worker.ip`|The IP of the worker.|localhost|
|`druid.worker.version`|Version identifier for the MiddleManager.|0|
|`druid.worker.capacity`|Maximum number of tasks the MiddleManager can accept.|Number of CPUs on the machine - 1|
|`druid.worker.category`|A string to name the category that the MiddleManager node belongs to.|`_default_worker_category`|

#### Peon Processing

Processing properties set on the Middlemanager will be passed through to Peons.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.processing.buffer.sizeBytes`|This specifies a buffer size (less than 2GiB) for the storage of intermediate results. The computation engine in both the Historical and Realtime processes will use a scratch buffer of this size to do all of their intermediate computations off-heap. Larger values allow for more aggregations in a single pass over the data while smaller values can require more passes depending on the query that is being executed. [Human-readable format](human-readable-byte.md) is supported.|auto (max 1 GiB)|
|`druid.processing.buffer.poolCacheMaxCount`|processing buffer pool caches the buffers for later use, this is the maximum count cache will grow to. note that pool can create more buffers than it can cache if necessary.|Integer.MAX_VALUE|
|`druid.processing.formatString`|Realtime and Historical processes use this format string to name their processing threads.|processing-%s|
|`druid.processing.numMergeBuffers`|The number of direct memory buffers available for merging query results. The buffers are sized by `druid.processing.buffer.sizeBytes`. This property is effectively a concurrency limit for queries that require merging buffers. If you are using any queries that require merge buffers (currently, just groupBy v2) then you should have at least two of these.|`max(2, druid.processing.numThreads / 4)`|
|`druid.processing.numThreads`|The number of processing threads to have available for parallel processing of segments. Our rule of thumb is `num_cores - 1`, which means that even under heavy load there will still be one core available to do background tasks like talking with ZooKeeper and pulling down segments. If only one core is available, this property defaults to the value `1`.|Number of cores - 1 (or 1)|
|`druid.processing.columnCache.sizeBytes`|Maximum size in bytes for the dimension value lookup cache. Any value greater than `0` enables the cache. It is currently disabled by default. Enabling the lookup cache can significantly improve the performance of aggregators operating on dimension values, such as the JavaScript aggregator, or cardinality aggregator, but can slow things down if the cache hit rate is low (i.e. dimensions with few repeating values). Enabling it may also require additional garbage collection tuning to avoid long GC pauses.|`0` (disabled)|
|`druid.processing.fifo`|If the processing queue should treat tasks of equal priority in a FIFO manner|`false`|
|`druid.processing.tmpDir`|Path where temporary files created while processing a query should be stored. If specified, this configuration takes priority over the default `java.io.tmpdir` path.|path represented by `java.io.tmpdir`|

The amount of direct memory needed by Druid is at least
`druid.processing.buffer.sizeBytes * (druid.processing.numMergeBuffers + druid.processing.numThreads + 1)`. You can
ensure at least this amount of direct memory is available by providing `-XX:MaxDirectMemorySize=<VALUE>` in
`druid.indexer.runner.javaOptsArray` as documented above.

#### Peon query configuration

See [general query configuration](#general-query-configuration).

#### Peon Caching

You can optionally configure caching to be enabled on the peons by setting caching configs here.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.realtime.cache.useCache`|true, false|Enable the cache on the realtime.|false|
|`druid.realtime.cache.populateCache`|true, false|Populate the cache on the realtime.|false|
|`druid.realtime.cache.unCacheable`|All druid query types|All query types to not cache.|`[]`|
|`druid.realtime.cache.maxEntrySize`|positive integer|Maximum cache entry size in bytes.|1_000_000|

See [cache configuration](#cache-configuration) for how to configure cache settings.


#### Additional Peon Configuration
Although peons inherit the configurations of their parent MiddleManagers, explicit child peon configs in MiddleManager can be set by prefixing them with:

```
druid.indexer.fork.property
```
Additional peon configs include:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.peon.mode`|Choices are "local" and "remote". Setting this to local means you intend to run the peon as a standalone process (Not recommended).|remote|
|`druid.indexer.task.baseDir`|Base temporary working directory.|`System.getProperty("java.io.tmpdir")`|
|`druid.indexer.task.baseTaskDir`|Base temporary working directory for tasks.|`${druid.indexer.task.baseDir}/persistent/task`|
|`druid.indexer.task.defaultHadoopCoordinates`|Hadoop version to use with HadoopIndexTasks that do not request a particular version.|org.apache.hadoop:hadoop-client:2.8.5|
|`druid.indexer.task.defaultRowFlushBoundary`|Highest row count before persisting to disk. Used for indexing generating tasks.|75000|
|`druid.indexer.task.directoryLockTimeout`|Wait this long for zombie peons to exit before giving up on their replacements.|PT10M|
|`druid.indexer.task.gracefulShutdownTimeout`|Wait this long on middleManager restart for restorable tasks to gracefully exit.|PT5M|
|`druid.indexer.task.hadoopWorkingPath`|Temporary working directory for Hadoop tasks.|`/tmp/druid-indexing`|
|`druid.indexer.task.restoreTasksOnRestart`|If true, MiddleManagers will attempt to stop tasks gracefully on shutdown and restore them on restart.|false|
|`druid.indexer.server.maxChatRequests`|Maximum number of concurrent requests served by a task's chat handler. Set to 0 to disable limiting.|0|

If the peon is running in remote mode, there must be an Overlord up and running. Peons in remote mode can set the following configurations:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.peon.taskActionClient.retry.minWait`|The minimum retry time to communicate with Overlord.|PT5S|
|`druid.peon.taskActionClient.retry.maxWait`|The maximum retry time to communicate with Overlord.|PT1M|
|`druid.peon.taskActionClient.retry.maxRetryCount`|The maximum number of retries to communicate with Overlord.|60|

##### SegmentWriteOutMediumFactory

When new segments are created, Druid temporarily stores some preprocessed data in some buffers. Currently three types of
*medium* exist for those buffers: *temporary files*, *off-heap memory*, and *on-heap memory*.

*Temporary files* (`tmpFile`) are stored under the task working directory (see `druid.indexer.task.baseTaskDir`
configuration above) and thus share it's mounting properties, e. g. they could be backed by HDD, SSD or memory (tmpfs).
This type of medium may do unnecessary disk I/O and requires some disk space to be available.

*Off-heap memory medium* (`offHeapMemory`) creates buffers in off-heap memory of a JVM process that is running a task.
This type of medium is preferred, but it may require to allow the JVM to have more off-heap memory, by changing
`-XX:MaxDirectMemorySize` configuration. It is not yet understood how does the required off-heap memory size relates
to the size of the segments being created. But definitely it doesn't make sense to add more extra off-heap memory,
than the configured maximum *heap* size (`-Xmx`) for the same JVM.

*On-heap memory medium* (`onHeapMemory`) creates buffers using the allocated heap memory of the JVM process running a task.
Using on-heap memory introduces garbage collection overhead and so is not recommended in most cases. This type of medium is
most helpful for tasks run on external clusters where it may be difficult to allocate and work with direct memory
effectively.

For most types of tasks SegmentWriteOutMediumFactory could be configured per-task (see [Tasks](../ingestion/tasks.md)
page, "TuningConfig" section), but if it's not specified for a task, or it's not supported for a particular task type,
then the value from the configuration below is used:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.peon.defaultSegmentWriteOutMediumFactory.type`|`tmpFile`, `offHeapMemory`, or `onHeapMemory`, see explanation above|`tmpFile`|

### Indexer

#### Indexer Process Configuration

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current process. This is used to advertise the current processes location as reachable from another process and should generally be specified such that `http://${druid.host}/` could actually talk to this process|InetAddress.getLocalHost().getCanonicalHostName()|
|`druid.bindOnHost`|Indicating whether the process's internal jetty server bind on `druid.host`. Default is false, which means binding to all interfaces.|false|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8091|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.enableTlsPort](../operations/tls-support.md) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative Integer.|8283|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|druid/indexer|

#### Indexer General Configuration

|Property|Description|Default|
|--------|-----------|-------|
|`druid.worker.version`|Version identifier for the Indexer.|0|
|`druid.worker.capacity`|Maximum number of tasks the Indexer can accept.|Number of available processors - 1|
|`druid.worker.globalIngestionHeapLimitBytes`|Total amount of heap available for ingestion processing. This is applied by automatically setting the `maxBytesInMemory` property on tasks.|60% of configured JVM heap|	
|`druid.worker.numConcurrentMerges`|Maximum number of segment persist or merge operations that can run concurrently across all tasks.|`druid.worker.capacity` / 2, rounded down|
|`druid.indexer.task.baseDir`|Base temporary working directory.|`System.getProperty("java.io.tmpdir")`|
|`druid.indexer.task.baseTaskDir`|Base temporary working directory for tasks.|`${druid.indexer.task.baseDir}/persistent/tasks`|
|`druid.indexer.task.defaultHadoopCoordinates`|Hadoop version to use with HadoopIndexTasks that do not request a particular version.|org.apache.hadoop:hadoop-client:2.8.5|
|`druid.indexer.task.gracefulShutdownTimeout`|Wait this long on Indexer restart for restorable tasks to gracefully exit.|PT5M|
|`druid.indexer.task.hadoopWorkingPath`|Temporary working directory for Hadoop tasks.|`/tmp/druid-indexing`|
|`druid.indexer.task.restoreTasksOnRestart`|If true, the Indexer will attempt to stop tasks gracefully on shutdown and restore them on restart.|false|
|`druid.peon.taskActionClient.retry.minWait`|The minimum retry time to communicate with Overlord.|PT5S|
|`druid.peon.taskActionClient.retry.maxWait`|The maximum retry time to communicate with Overlord.|PT1M|
|`druid.peon.taskActionClient.retry.maxRetryCount`|The maximum number of retries to communicate with Overlord.|60|

#### Indexer Concurrent Requests

Druid uses Jetty to serve HTTP requests.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.http.numThreads`|Number of threads for HTTP requests. Please see the [Indexer Server HTTP threads](../design/indexer.md#server-http-threads) documentation for more details on how the Indexer uses this configuration.|max(10, (Number of cores * 17) / 16 + 2) + 30|
|`druid.server.http.queueSize`|Size of the worker queue used by Jetty server to temporarily store incoming client connections. If this value is set and a request is rejected by jetty because queue is full then client would observe request failure with TCP connection being closed immediately with a completely empty response from server.|Unbounded|
|`druid.server.http.maxIdleTime`|The Jetty max idle time for a connection.|PT5M|
|`druid.server.http.enableRequestLimit`|If enabled, no requests would be queued in jetty queue and "HTTP 429 Too Many Requests" error response would be sent. |false|
|`druid.server.http.defaultQueryTimeout`|Query timeout in millis, beyond which unfinished queries will be cancelled|300000|
|`druid.server.http.gracefulShutdownTimeout`|The maximum amount of time Jetty waits after receiving shutdown signal. After this timeout the threads will be forcefully shutdown. This allows any queries that are executing to complete(Only values greater than zero are valid).|`PT30S`|
|`druid.server.http.unannouncePropagationDelay`|How long to wait for zookeeper unannouncements to propagate before shutting down Jetty. This is a minimum and `druid.server.http.gracefulShutdownTimeout` does not start counting down until after this period elapses.|`PT0S` (do not wait)|
|`druid.server.http.maxQueryTimeout`|Maximum allowed value (in milliseconds) for `timeout` parameter. See [query-context](../querying/query-context.md) to know more about `timeout`. Query is rejected if the query context `timeout` is greater than this value. |Long.MAX_VALUE|
|`druid.server.http.maxRequestHeaderSize`|Maximum size of a request header in bytes. Larger headers consume more memory and can make a server more vulnerable to denial of service attacks.|8 * 1024|
|`druid.server.http.enableForwardedRequestCustomizer`|If enabled, adds Jetty ForwardedRequestCustomizer which reads X-Forwarded-* request headers to manipulate servlet request object when Druid is used behind a proxy.|false|
|`druid.server.http.allowedHttpMethods`|List of HTTP methods that should be allowed in addition to the ones required by Druid APIs. Druid APIs require GET, PUT, POST, and DELETE, which are always allowed. This option is not useful unless you have installed an extension that needs these additional HTTP methods or that adds functionality related to CORS. None of Druid's bundled extensions require these methods.|[]|

#### Indexer Processing Resources

|Property|Description|Default|
|--------|-----------|-------|
|`druid.processing.buffer.sizeBytes`|This specifies a buffer size (less than 2GiB) for the storage of intermediate results. The computation engine in the Indexer processes will use a scratch buffer of this size to do all of their intermediate computations off-heap. Larger values allow for more aggregations in a single pass over the data while smaller values can require more passes depending on the query that is being executed. [Human-readable format](human-readable-byte.md) is supported.|auto (max 1GB)|
|`druid.processing.buffer.poolCacheMaxCount`|processing buffer pool caches the buffers for later use, this is the maximum count cache will grow to. note that pool can create more buffers than it can cache if necessary.|Integer.MAX_VALUE|
|`druid.processing.formatString`|Indexer processes use this format string to name their processing threads.|processing-%s|
|`druid.processing.numMergeBuffers`|The number of direct memory buffers available for merging query results. The buffers are sized by `druid.processing.buffer.sizeBytes`. This property is effectively a concurrency limit for queries that require merging buffers. If you are using any queries that require merge buffers (currently, just groupBy v2) then you should have at least two of these.|`max(2, druid.processing.numThreads / 4)`|
|`druid.processing.numThreads`|The number of processing threads to have available for parallel processing of segments. Our rule of thumb is `num_cores - 1`, which means that even under heavy load there will still be one core available to do background tasks like talking with ZooKeeper and pulling down segments. If only one core is available, this property defaults to the value `1`.|Number of cores - 1 (or 1)|
|`druid.processing.columnCache.sizeBytes`|Maximum size in bytes for the dimension value lookup cache. Any value greater than `0` enables the cache. It is currently disabled by default. Enabling the lookup cache can significantly improve the performance of aggregators operating on dimension values, such as the JavaScript aggregator, or cardinality aggregator, but can slow things down if the cache hit rate is low (i.e. dimensions with few repeating values). Enabling it may also require additional garbage collection tuning to avoid long GC pauses.|`0` (disabled)|
|`druid.processing.fifo`|If the processing queue should treat tasks of equal priority in a FIFO manner|`false`|
|`druid.processing.tmpDir`|Path where temporary files created while processing a query should be stored. If specified, this configuration takes priority over the default `java.io.tmpdir` path.|path represented by `java.io.tmpdir`|

The amount of direct memory needed by Druid is at least
`druid.processing.buffer.sizeBytes * (druid.processing.numMergeBuffers + druid.processing.numThreads + 1)`. You can
ensure at least this amount of direct memory is available by providing `-XX:MaxDirectMemorySize=<VALUE>` at the command
line.

#### Indexer Security Configuration
You can optionally configure following additional configs to restrict druid ingestion
 
|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.ingestion.http.allowListDomains`|List of domains|Allowed domains from which ingestion will be allowed. Only one of allowList or denyList can be set.|empty list|
|`druid.ingestion.http.denyListDomains`|List of domains|Blacklisted domains from which ingestion will NOT be allowed. Only one of allowList or denyList can be set. |empty list|


#### Query Configurations

See [general query configuration](#general-query-configuration).

#### Indexer Caching

You can optionally configure caching to be enabled on the Indexer by setting caching configs here.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.realtime.cache.useCache`|true, false|Enable the cache on the realtime.|false|
|`druid.realtime.cache.populateCache`|true, false|Populate the cache on the realtime.|false|
|`druid.realtime.cache.unCacheable`|All druid query types|All query types to not cache.|`[]`|
|`druid.realtime.cache.maxEntrySize`|positive integer|Maximum cache entry size in bytes.|1_000_000|

See [cache configuration](#cache-configuration) for how to configure cache settings.

Note that only local caches such as the `local`-type cache and `caffeine` cache are supported. If a remote cache such as `memcached` is used, it will be ignored.

### Historical

For general Historical Process information, see [here](../design/historical.md).

These Historical configurations can be defined in the `historical/runtime.properties` file.

#### Historical Process Configuration
|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current process. This is used to advertise the current processes location as reachable from another process and should generally be specified such that `http://${druid.host}/` could actually talk to this process|InetAddress.getLocalHost().getCanonicalHostName()|
|`druid.bindOnHost`|Indicating whether the process's internal jetty server bind on `druid.host`. Default is false, which means binding to all interfaces.|false|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8083|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.enableTlsPort](../operations/tls-support.md) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative Integer.|8283|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|druid/historical|

#### Historical General Configuration

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.maxSize`|The maximum number of bytes-worth of segments that the process wants assigned to it. The Coordinator process will attempt to assign segments to a Historical process only if this property is greater than the total size of segments served by it. Since this property defines the upper limit on the total segment size that can be assigned to a Historical, it is defaulted to the sum of all `maxSize` values specified within `druid.segmentCache.locations` property. Human-readable format is supported, see [here](human-readable-byte.md). |Sum of `maxSize` values defined within `druid.segmentCache.locations`|
|`druid.server.tier`| A string to name the distribution tier that the storage process belongs to. Many of the [rules Coordinator processes use](../operations/rule-configuration.md) to manage segments can be keyed on tiers. |  `_default_tier` |
|`druid.server.priority`|In a tiered architecture, the priority of the tier, thus allowing control over which processes are queried. Higher numbers mean higher priority. The default (no priority) works for architecture with no cross replication (tiers that have no data-storage overlap). Data centers typically have equal priority. | 0 |

#### Storing Segments

|Property|Description|Default|
|--------|-----------|-------|
|`druid.segmentCache.locations`|Segments assigned to a Historical process are first stored on the local file system (in a disk cache) and then served by the Historical process. These locations define where that local cache resides. This value cannot be NULL or EMPTY. Here is an example `druid.segmentCache.locations=[{"path": "/mnt/druidSegments", "maxSize": "10k", "freeSpacePercent": 1.0}]`. "freeSpacePercent" is optional, if provided then enforces that much of free disk partition space while storing segments. But, it depends on File.getTotalSpace() and File.getFreeSpace() methods, so enable if only if they work for your File System.| none |
|`druid.segmentCache.locationSelector.strategy`|The strategy used to select a location from the configured `druid.segmentCache.locations` for segment distribution. Possible values are `leastBytesUsed`, `roundRobin`, `random`, or `mostAvailableSize`. |leastBytesUsed|
|`druid.segmentCache.deleteOnRemove`|Delete segment files from cache once a process is no longer serving a segment.|true|
|`druid.segmentCache.dropSegmentDelayMillis`|How long a process delays before completely dropping segment.|30000 (30 seconds)|
|`druid.segmentCache.infoDir`|Historical processes keep track of the segments they are serving so that when the process is restarted they can reload the same segments without waiting for the Coordinator to reassign. This path defines where this metadata is kept. Directory will be created if needed.|${first_location}/info_dir|
|`druid.segmentCache.announceIntervalMillis`|How frequently to announce segments while segments are loading from cache. Set this value to zero to wait for all segments to be loaded before announcing.|5000 (5 seconds)|
|`druid.segmentCache.numLoadingThreads`|How many segments to drop or load concurrently from deep storage. Note that the work of loading segments involves downloading segments from deep storage, decompressing them and loading them to a memory mapped location. So the work is not all I/O Bound. Depending on CPU and network load, one could possibly increase this config to a higher value.|max(1,Number of cores / 6)|
|`druid.segmentCache.numBootstrapThreads`|How many segments to load concurrently during historical startup.|`druid.segmentCache.numLoadingThreads`|
|`druid.segmentCache.lazyLoadOnStart`|Whether or not to load segment columns metadata lazily during historical startup. When set to true, Historical startup time will be dramatically improved by deferring segment loading until the first time that segment takes part in a query, which will incur this cost instead. One catch is that if there were corrupted segments which historical served before on historical disk, this requires manual intervention to delete corrupted files. When the flag is set true, historical startup would complete successfully and queries using this segment would fail at runtime.|false|
|`druid.coordinator.loadqueuepeon.curator.numCallbackThreads`|Number of threads for executing callback actions associated with loading or dropping of segments. One might want to increase this number when noticing clusters are lagging behind w.r.t. balancing segments across historical nodes.|2|

In `druid.segmentCache.locations`, *freeSpacePercent* was added because *maxSize* setting is only a theoretical limit and assumes that much space will always be available for storing segments. In case of any druid bug leading to unaccounted segment files left alone on disk or some other process writing stuff to disk, This check can start failing segment loading early before filling up the disk completely and leaving the host usable otherwise.

In `druid.segmentCache.locationSelector.strategy`, one of `leastBytesUsed`, `roundRobin`, `random`, or `mostAvailableSize` could be specified to represent the strategy to distribute segments across multiple segment cache locations.

|Strategy|Description|
|--------|-----------|
|`leastBytesUsed`|selects a location which has least bytes used in absolute terms.|
|`roundRobin`|selects a location in a round robin fashion oblivious to the bytes used or the capacity.|
|`random`|selects a segment cache location randomly each time among the available storage locations.|
|`mostAvailableSize`|selects a segment cache location that has most free space among the available storage locations.|

Note that if `druid.segmentCache.numLoadingThreads` > 1, multiple threads can download different segments at the same time. In this case, with the leastBytesUsed strategy or mostAvailableSize strategy, historicals may select a sub-optimal storage location because each decision is based on a snapshot of the storage location status of when a segment is requested to download.

#### Historical query configs

##### Concurrent Requests

Druid uses Jetty to serve HTTP requests.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.http.numThreads`|Number of threads for HTTP requests.|max(10, (Number of cores * 17) / 16 + 2) + 30|
|`druid.server.http.queueSize`|Size of the worker queue used by Jetty server to temporarily store incoming client connections. If this value is set and a request is rejected by jetty because queue is full then client would observe request failure with TCP connection being closed immediately with a completely empty response from server.|Unbounded|
|`druid.server.http.maxIdleTime`|The Jetty max idle time for a connection.|PT5M|
|`druid.server.http.enableRequestLimit`|If enabled, no requests would be queued in jetty queue and "HTTP 429 Too Many Requests" error response would be sent. |false|
|`druid.server.http.defaultQueryTimeout`|Query timeout in millis, beyond which unfinished queries will be cancelled|300000|
|`druid.server.http.gracefulShutdownTimeout`|The maximum amount of time Jetty waits after receiving shutdown signal. After this timeout the threads will be forcefully shutdown. This allows any queries that are executing to complete(Only values greater than zero are valid).|`PT30S`|
|`druid.server.http.unannouncePropagationDelay`|How long to wait for zookeeper unannouncements to propagate before shutting down Jetty. This is a minimum and `druid.server.http.gracefulShutdownTimeout` does not start counting down until after this period elapses.|`PT0S` (do not wait)|
|`druid.server.http.maxQueryTimeout`|Maximum allowed value (in milliseconds) for `timeout` parameter. See [query-context](../querying/query-context.md) to know more about `timeout`. Query is rejected if the query context `timeout` is greater than this value. |Long.MAX_VALUE|
|`druid.server.http.maxRequestHeaderSize`|Maximum size of a request header in bytes. Larger headers consume more memory and can make a server more vulnerable to denial of service attacks.|8 * 1024|

##### Processing

|Property|Description|Default|
|--------|-----------|-------|
|`druid.processing.buffer.sizeBytes`|This specifies a buffer size (less than 2GiB), for the storage of intermediate results. The computation engine in both the Historical and Realtime processes will use a scratch buffer of this size to do all of their intermediate computations off-heap. Larger values allow for more aggregations in a single pass over the data while smaller values can require more passes depending on the query that is being executed.  [Human-readable format](human-readable-byte.md) is supported.|auto (max 1GB)|
|`druid.processing.buffer.poolCacheMaxCount`|processing buffer pool caches the buffers for later use, this is the maximum count cache will grow to. note that pool can create more buffers than it can cache if necessary.|Integer.MAX_VALUE|
|`druid.processing.formatString`|Realtime and Historical processes use this format string to name their processing threads.|processing-%s|
|`druid.processing.numMergeBuffers`|The number of direct memory buffers available for merging query results. The buffers are sized by `druid.processing.buffer.sizeBytes`. This property is effectively a concurrency limit for queries that require merging buffers. If you are using any queries that require merge buffers (currently, just groupBy v2) then you should have at least two of these.|`max(2, druid.processing.numThreads / 4)`|
|`druid.processing.numThreads`|The number of processing threads to have available for parallel processing of segments. Our rule of thumb is `num_cores - 1`, which means that even under heavy load there will still be one core available to do background tasks like talking with ZooKeeper and pulling down segments. If only one core is available, this property defaults to the value `1`.|Number of cores - 1 (or 1)|
|`druid.processing.columnCache.sizeBytes`|Maximum size in bytes for the dimension value lookup cache. Any value greater than `0` enables the cache. It is currently disabled by default. Enabling the lookup cache can significantly improve the performance of aggregators operating on dimension values, such as the JavaScript aggregator, or cardinality aggregator, but can slow things down if the cache hit rate is low (i.e. dimensions with few repeating values). Enabling it may also require additional garbage collection tuning to avoid long GC pauses.|`0` (disabled)|
|`druid.processing.fifo`|If the processing queue should treat tasks of equal priority in a FIFO manner|`false`|
|`druid.processing.tmpDir`|Path where temporary files created while processing a query should be stored. If specified, this configuration takes priority over the default `java.io.tmpdir` path.|path represented by `java.io.tmpdir`|

The amount of direct memory needed by Druid is at least
`druid.processing.buffer.sizeBytes * (druid.processing.numMergeBuffers + druid.processing.numThreads + 1)`. You can
ensure at least this amount of direct memory is available by providing `-XX:MaxDirectMemorySize=<VALUE>` at the command
line.

##### Historical query configuration

See [general query configuration](#general-query-configuration).

#### Historical Caching

You can optionally only configure caching to be enabled on the Historical by setting caching configs here.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.historical.cache.useCache`|true, false|Enable the cache on the Historical.|false|
|`druid.historical.cache.populateCache`|true, false|Populate the cache on the Historical.|false|
|`druid.historical.cache.unCacheable`|All druid query types|All query types to not cache.|`[]`|
|`druid.historical.cache.maxEntrySize`|positive integer|Maximum cache entry size in bytes.|1_000_000|

See [cache configuration](#cache-configuration) for how to configure cache settings.

## Query Server

This section contains the configuration options for the processes that reside on Query servers (Brokers) in the suggested [three-server configuration](../design/processes.md#server-types).

Configuration options for the experimental [Router process](../design/router.md) are also provided here.

### Broker

For general Broker process information, see [here](../design/broker.md).

These Broker configurations can be defined in the `broker/runtime.properties` file.

#### Broker Process Configs

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current process. This is used to advertise the current processes location as reachable from another process and should generally be specified such that `http://${druid.host}/` could actually talk to this process|InetAddress.getLocalHost().getCanonicalHostName()|
|`druid.bindOnHost`|Indicating whether the process's internal jetty server bind on `druid.host`. Default is false, which means binding to all interfaces.|false|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8082|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.enableTlsPort](../operations/tls-support.md) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative Integer.|8282|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|druid/broker|

#### Query configuration

##### Query routing

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.broker.balancer.type`|`random`, `connectionCount`|Determines how the broker balances connections to Historical processes. `random` choose randomly, `connectionCount` picks the process with the fewest number of active connections to|`random`|
|`druid.broker.select.tier`|`highestPriority`, `lowestPriority`, `custom`|If segments are cross-replicated across tiers in a cluster, you can tell the broker to prefer to select segments in a tier with a certain priority.|`highestPriority`|
|`druid.broker.select.tier.custom.priorities`|`An array of integer priorities.` E.g., `[-1, 0, 1, 2]`|Select servers in tiers with a custom priority list.|The config only has effect if `druid.broker.select.tier` is set to `custom`. If `druid.broker.select.tier` is set to `custom` but this config is not specified, the effect is the same as `druid.broker.select.tier` set to `highestPriority`. Any of the integers in this config can be ignored if there's no corresponding tiers with such priorities. Tiers with priorities explicitly specified in this config always have higher priority than those not and those not specified fall back to use `highestPriority` strategy among themselves.|

##### Query prioritization and laning

*Laning strategies* allow you to control capacity utilization for heterogeneous query workloads. With laning, the broker examines and classifies a query for the purpose of assigning it to a 'lane'. Lanes have capacity limits, enforced by the broker, that can be used to ensure sufficient resources are available for other lanes or for interactive queries (with no lane), or to limit overall throughput for queries within the lane. Requests in excess of the capacity are discarded with an HTTP 429 status code.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.scheduler.numThreads`|Maximum number of HTTP threads to dedicate to query processing. To save HTTP thread capacity, this should be lower than `druid.server.http.numThreads`, but it is worth noting that like `druid.server.http.enableRequestLimit` is set that query requests over this limit will be denied instead of waiting in the Jetty HTTP request queue.|Unbounded|
|`druid.query.scheduler.laning.strategy`|Query laning strategy to use to assign queries to a lane in order to control capacities for certain classes of queries.|`none`|
|`druid.query.scheduler.prioritization.strategy`|Query prioritization strategy to automatically assign priorities.|`manual`|

##### Prioritization strategies

###### Manual prioritization strategy
With this configuration, queries are never assigned a priority automatically, but will preserve a priority manually set on the [query context](../querying/query-context.md) with the `priority` key. This mode can be explicitly set by setting `druid.query.scheduler.prioritization.strategy` to `none`.

###### Threshold prioritization strategy

This prioritization strategy lowers the priority of queries that cross any of a configurable set of thresholds, such as how far in the past the data is, how large of an interval a query covers, or the number of segments taking part in a query.

This strategy can be enabled by setting `druid.query.scheduler.prioritization.strategy` to `threshold`.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.scheduler.prioritization.periodThreshold`|ISO duration threshold for how old data can be queried before automatically adjusting query priority.|None|
|`druid.query.scheduler.prioritization.durationThreshold`|ISO duration threshold for maximum duration a queries interval can span before the priority is automatically adjusted.|None|
|`druid.query.scheduler.prioritization.segmentCountThreshold`|Number threshold for maximum number of segments that can take part in a query before its priority is automatically adjusted.|None|
|`druid.query.scheduler.prioritization.adjustment`|Amount to reduce the priority of queries which cross any threshold.|None|

##### Laning strategies

###### No laning strategy

In this mode, queries are never assigned a lane, and the concurrent query count will only be limited by `druid.server.http.numThreads` or `druid.query.scheduler.numThreads`, if set. This is the default Druid query scheduler operating mode. Enable this strategy explicitly by setting `druid.query.scheduler.laning.strategy` to `none`.

###### 'High/Low' laning strategy
This laning strategy splits queries with a `priority` below zero into a `low` query lane, automatically. Queries with priority of zero (the default) or above are considered 'interactive'. The limit on `low` queries can be set to some desired percentage of the total capacity (or HTTP thread pool size), reserving capacity for interactive queries. Queries in the `low` lane are _not_ guaranteed their capacity, which may be consumed by interactive queries, but may use up to this limit if total capacity is available.

If the `low` lane is specified in the [query context](../querying/query-context.md) `lane` parameter, this will override the computed lane. 

This strategy can be enabled by setting `druid.query.scheduler.laning.strategy=hilo`.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.scheduler.laning.maxLowPercent`|Maximum percent of the smaller number of `druid.server.http.numThreads` or `druid.query.scheduler.numThreads`, defining the number of HTTP threads that can be used by queries with a priority lower than 0. Value must be an integer in the range 1 to 100, and will be rounded up|No default, must be set if using this mode|


###### 'Manual' laning strategy
This laning strategy is best suited for cases where one or more external applications which query Druid are capable of manually deciding what lane a given query should belong to. Configured with a map of lane names to percent or exact max capacities, queries with a matching `lane` parameter in the [query context](../querying/query-context.md) will be subjected to those limits.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.scheduler.laning.lanes.{name}`|Maximum percent or exact limit of queries that can concurrently run in the defined lanes. Any number of lanes may be defined like this. The lane names 'total' and 'default' are reserved for internal use.|No default, must define at least one lane with a limit above 0. If `druid.query.scheduler.laning.isLimitPercent` is set to `true`, values must be integers in the range of 1 to 100.|
|`druid.query.scheduler.laning.isLimitPercent`|If set to `true`, the values set for `druid.query.scheduler.laning.lanes` will be treated as a percent of the smaller number of `druid.server.http.numThreads` or `druid.query.scheduler.numThreads`. Note that in this mode, these lane values across lanes are _not_ required to add up to, and can exceed, 100%.|`false`|

##### Server Configuration

Druid uses Jetty to serve HTTP requests. Each query being processed consumes a single thread from `druid.server.http.numThreads`, so consider defining `druid.query.scheduler.numThreads` to a lower value in order to reserve HTTP threads for responding to health checks, lookup loading, and other non-query, and in most cases comparatively very short lived, HTTP requests.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.http.numThreads`|Number of threads for HTTP requests.|max(10, (Number of cores * 17) / 16 + 2) + 30|
|`druid.server.http.queueSize`|Size of the worker queue used by Jetty server to temporarily store incoming client connections. If this value is set and a request is rejected by jetty because queue is full then client would observe request failure with TCP connection being closed immediately with a completely empty response from server.|Unbounded|
|`druid.server.http.maxIdleTime`|The Jetty max idle time for a connection.|PT5M|
|`druid.server.http.enableRequestLimit`|If enabled, no requests would be queued in jetty queue and "HTTP 429 Too Many Requests" error response would be sent. |false|
|`druid.server.http.defaultQueryTimeout`|Query timeout in millis, beyond which unfinished queries will be cancelled|300000|
|`druid.server.http.maxScatterGatherBytes`|Maximum number of bytes gathered from data processes such as Historicals and realtime processes to execute a query. Queries that exceed this limit will fail. This is an advance configuration that allows to protect in case Broker is under heavy load and not utilizing the data gathered in memory fast enough and leading to OOMs. This limit can be further reduced at query time using `maxScatterGatherBytes` in the context. Note that having large limit is not necessarily bad if broker is never under heavy concurrent load in which case data gathered is processed quickly and freeing up the memory used. Human-readable format is supported, see [here](human-readable-byte.md). |Long.MAX_VALUE|
|`druid.server.http.maxSubqueryRows`|Maximum number of rows from subqueries per query. These rows are stored in memory.|100000|
|`druid.server.http.gracefulShutdownTimeout`|The maximum amount of time Jetty waits after receiving shutdown signal. After this timeout the threads will be forcefully shutdown. This allows any queries that are executing to complete(Only values greater than zero are valid).|`PT30S`|
|`druid.server.http.unannouncePropagationDelay`|How long to wait for zookeeper unannouncements to propagate before shutting down Jetty. This is a minimum and `druid.server.http.gracefulShutdownTimeout` does not start counting down until after this period elapses.|`PT0S` (do not wait)|
|`druid.server.http.maxQueryTimeout`|Maximum allowed value (in milliseconds) for `timeout` parameter. See [query-context](../querying/query-context.md) to know more about `timeout`. Query is rejected if the query context `timeout` is greater than this value. |Long.MAX_VALUE|
|`druid.server.http.maxRequestHeaderSize`|Maximum size of a request header in bytes. Larger headers consume more memory and can make a server more vulnerable to denial of service attacks. |8 * 1024|

##### Client Configuration

Druid Brokers use an HTTP client to communicate with with data servers (Historical servers and real-time tasks). This
client has the following configuration options.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.broker.http.numConnections`|Size of connection pool for the Broker to connect to Historical and real-time processes. If there are more queries than this number that all need to speak to the same process, then they will queue up.|`20`|
|`druid.broker.http.compressionCodec`|Compression codec the Broker uses to communicate with Historical and real-time processes. May be "gzip" or "identity".|`gzip`|
|`druid.broker.http.readTimeout`|The timeout for data reads from Historical servers and real-time tasks.|`PT15M`|
|`druid.broker.http.unusedConnectionTimeout`|The timeout for idle connections in connection pool. The connection in the pool will be closed after this timeout and a new one will be established. This timeout should be less than `druid.broker.http.readTimeout`. Set this timeout = ~90% of `druid.broker.http.readTimeout`|`PT4M`|
|`druid.broker.http.maxQueuedBytes`|Maximum number of bytes queued per query before exerting backpressure on the channel to the data server. Similar to `druid.server.http.maxScatterGatherBytes`, except unlike that configuration, this one will trigger backpressure rather than query failure. Zero means disabled. Can be overridden by the ["maxQueuedBytes" query context parameter](../querying/query-context.md). Human-readable format is supported, see [here](human-readable-byte.md). |`0` (disabled)|
|`druid.broker.http.numMaxThreads`|`Maximum number of I/O worker threads|max(10, ((number of cores * 17) / 16 + 2) + 30)`|

##### Retry Policy

Druid broker can optionally retry queries internally for transient errors.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.broker.retryPolicy.numTries`|Number of tries.|1|

##### Processing

The broker uses processing configs for nested groupBy queries.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.processing.buffer.sizeBytes`|This specifies a buffer size (less than 2GiB) for the storage of intermediate results. The computation engine in both the Historical and Realtime processes will use a scratch buffer of this size to do all of their intermediate computations off-heap. Larger values allow for more aggregations in a single pass over the data while smaller values can require more passes depending on the query that is being executed. [Human-readable format](human-readable-byte.md) is supported.|auto (max 1GiB)|
|`druid.processing.buffer.poolCacheMaxCount`|processing buffer pool caches the buffers for later use, this is the maximum count cache will grow to. note that pool can create more buffers than it can cache if necessary.|Integer.MAX_VALUE|
|`druid.processing.formatString`|Realtime and Historical processes use this format string to name their processing threads.|processing-%s|
|`druid.processing.numMergeBuffers`|The number of direct memory buffers available for merging query results. The buffers are sized by `druid.processing.buffer.sizeBytes`. This property is effectively a concurrency limit for queries that require merging buffers. If you are using any queries that require merge buffers (currently, just groupBy v2) then you should have at least two of these.|`max(2, druid.processing.numThreads / 4)`|
|`druid.processing.numThreads`|The number of processing threads to have available for parallel processing of segments. Our rule of thumb is `num_cores - 1`, which means that even under heavy load there will still be one core available to do background tasks like talking with ZooKeeper and pulling down segments. If only one core is available, this property defaults to the value `1`.|Number of cores - 1 (or 1)|
|`druid.processing.columnCache.sizeBytes`|Maximum size in bytes for the dimension value lookup cache. Any value greater than `0` enables the cache. It is currently disabled by default. Enabling the lookup cache can significantly improve the performance of aggregators operating on dimension values, such as the JavaScript aggregator, or cardinality aggregator, but can slow things down if the cache hit rate is low (i.e. dimensions with few repeating values). Enabling it may also require additional garbage collection tuning to avoid long GC pauses.|`0` (disabled)|
|`druid.processing.fifo`|If the processing queue should treat tasks of equal priority in a FIFO manner|`false`|
|`druid.processing.tmpDir`|Path where temporary files created while processing a query should be stored. If specified, this configuration takes priority over the default `java.io.tmpdir` path.|path represented by `java.io.tmpdir`|
|`druid.processing.merge.useParallelMergePool`|Enable automatic parallel merging for Brokers on a dedicated async ForkJoinPool. If `false`, instead merges will be done serially on the `HTTP` thread pool.|`true`|
|`druid.processing.merge.pool.parallelism`|Size of ForkJoinPool. Note that the default configuration assumes that the value returned by `Runtime.getRuntime().availableProcessors()` represents 2 hyper-threads per physical core, and multiplies this value by `0.75` in attempt to size `1.5` times the number of _physical_ cores.|`Runtime.getRuntime().availableProcessors() * 0.75` (rounded up)|
|`druid.processing.merge.pool.defaultMaxQueryParallelism`|Default maximum number of parallel merge tasks per query. Note that the default configuration assumes that the value returned by `Runtime.getRuntime().availableProcessors()` represents 2 hyper-threads per physical core, and multiplies this value by `0.5` in attempt to size to the number of _physical_ cores.|`Runtime.getRuntime().availableProcessors() * 0.5` (rounded up)|
|`druid.processing.merge.pool.awaitShutdownMillis`|Time to wait for merge ForkJoinPool tasks to complete before ungracefully stopping on process shutdown in milliseconds.|`60_000`|
|`druid.processing.merge.task.targetRunTimeMillis`|Ideal run-time of each ForkJoinPool merge task, before forking off a new task to continue merging sequences.|`100`|
|`druid.processing.merge.task.initialYieldNumRows`|Number of rows to yield per ForkJoinPool merge task, before forking off a new task to continue merging sequences.|`16384`|
|`druid.processing.merge.task.smallBatchNumRows`|Size of result batches to operate on in ForkJoinPool merge tasks.|`4096`|

The amount of direct memory needed by Druid is at least
`druid.processing.buffer.sizeBytes * (druid.processing.numMergeBuffers + druid.processing.numThreads + 1)`. You can
ensure at least this amount of direct memory is available by providing `-XX:MaxDirectMemorySize=<VALUE>` at the command
line.

##### Broker query configuration

See [general query configuration](#general-query-configuration).

#### SQL

The Druid SQL server is configured through the following properties on the Broker.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.sql.enable`|Whether to enable SQL at all, including background metadata fetching. If false, this overrides all other SQL-related properties and disables SQL metadata, serving, and planning completely.|true|
|`druid.sql.avatica.enable`|Whether to enable JDBC querying at `/druid/v2/sql/avatica/`.|true|
|`druid.sql.avatica.maxConnections`|Maximum number of open connections for the Avatica server. These are not HTTP connections, but are logical client connections that may span multiple HTTP connections.|25|
|`druid.sql.avatica.maxRowsPerFrame`|Maximum number of rows to return in a single JDBC frame. Setting this property to -1 indicates that no row limit should be applied. Clients can optionally specify a row limit in their requests; if a client specifies a row limit, the lesser value of the client-provided limit and `maxRowsPerFrame` will be used.|5,000|
|`druid.sql.avatica.maxStatementsPerConnection`|Maximum number of simultaneous open statements per Avatica client connection.|4|
|`druid.sql.avatica.connectionIdleTimeout`|Avatica client connection idle timeout.|PT5M|
|`druid.sql.http.enable`|Whether to enable JSON over HTTP querying at `/druid/v2/sql/`.|true|
|`druid.sql.planner.maxTopNLimit`|Maximum threshold for a [TopN query](../querying/topnquery.md). Higher limits will be planned as [GroupBy queries](../querying/groupbyquery.md) instead.|100000|
|`druid.sql.planner.metadataRefreshPeriod`|Throttle for metadata refreshes.|PT1M|
|`druid.sql.planner.useApproximateCountDistinct`|Whether to use an approximate cardinality algorithm for `COUNT(DISTINCT foo)`.|true|
|`druid.sql.planner.useApproximateTopN`|Whether to use approximate [TopN queries](../querying/topnquery.md) when a SQL query could be expressed as such. If false, exact [GroupBy queries](../querying/groupbyquery.md) will be used instead.|true|
|`druid.sql.planner.requireTimeCondition`|Whether to require SQL to have filter conditions on __time column so that all generated native queries will have user specified intervals. If true, all queries without filter condition on __time column will fail|false|
|`druid.sql.planner.sqlTimeZone`|Sets the default time zone for the server, which will affect how time functions and timestamp literals behave. Should be a time zone name like "America/Los_Angeles" or offset like "-08:00".|UTC|
|`druid.sql.planner.metadataSegmentCacheEnable`|Whether to keep a cache of published segments in broker. If true, broker polls coordinator in background to get segments from metadata store and maintains a local cache. If false, coordinator's REST API will be invoked when broker needs published segments info.|false|
|`druid.sql.planner.metadataSegmentPollPeriod`|How often to poll coordinator for published segments list if `druid.sql.planner.metadataSegmentCacheEnable` is set to true. Poll period is in milliseconds. |60000|

> Previous versions of Druid had properties named `druid.sql.planner.maxQueryCount` and `druid.sql.planner.maxSemiJoinRowsInMemory`.
> These properties are no longer available. Since Druid 0.18.0, you can use `druid.server.http.maxSubqueryRows` to control the maximum
> number of rows permitted across all subqueries.

#### Broker Caching

You can optionally only configure caching to be enabled on the Broker by setting caching configs here.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.broker.cache.useCache`|true, false|Enable the cache on the Broker.|false|
|`druid.broker.cache.populateCache`|true, false|Populate the cache on the Broker.|false|
|`druid.broker.cache.useResultLevelCache`|true, false|Enable result level caching on the Broker.|false|
|`druid.broker.cache.populateResultLevelCache`|true, false|Populate the result level cache on the Broker.|false|
|`druid.broker.cache.resultLevelCacheLimit`|positive integer|Maximum size of query response that can be cached.|`Integer.MAX_VALUE`|
|`druid.broker.cache.unCacheable`|All druid query types|All query types to not cache.|`[]`|
|`druid.broker.cache.cacheBulkMergeLimit`|positive integer or 0|Queries with more segments than this number will not attempt to fetch from cache at the broker level, leaving potential caching fetches (and cache result merging) to the Historicals|`Integer.MAX_VALUE`|
|`druid.broker.cache.maxEntrySize`|positive integer|Maximum cache entry size in bytes.|1_000_000|

See [cache configuration](#cache-configuration) for how to configure cache settings.

#### Segment Discovery
|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.serverview.type`|batch or http|Segment discovery method to use. "http" enables discovering segments using HTTP instead of zookeeper.|batch|
|`druid.broker.segment.watchedTiers`|List of strings|Broker watches the segment announcements from processes serving segments to build cache of which process is serving which segments, this configuration allows to only consider segments being served from a whitelist of tiers. By default, Broker would consider all tiers. This can be used to partition your dataSources in specific Historical tiers and configure brokers in partitions so that they are only queryable for specific dataSources.|none|
|`druid.broker.segment.watchedDataSources`|List of strings|Broker watches the segment announcements from processes serving segments to build cache of which process is serving which segments, this configuration allows to only consider segments being served from a whitelist of dataSources. By default, Broker would consider all datasources. This can be used to configure brokers in partitions so that they are only queryable for specific dataSources.|none|
|`druid.broker.segment.awaitInitializationOnStart`|Boolean|Whether the Broker will wait for its view of segments to fully initialize before starting up. If set to 'true', the Broker's HTTP server will not start up, and the Broker will not announce itself as available, until the server view is initialized. See also `druid.sql.planner.awaitInitializationOnStart`, a related setting.|true|

## Cache Configuration

This section describes caching configuration that is common to Broker, Historical, and MiddleManager/Peon processes.

Caching could optionally be enabled on the Broker, Historical, and MiddleManager/Peon processes. See
[Broker](#broker-caching), [Historical](#historical-caching), and [Peon](#peon-caching) configuration options for how to
enable it for different processes.

Druid uses a local in-memory cache by default, unless a different type of cache is specified.
Use the `druid.cache.type` configuration to set a different kind of cache.

Cache settings are set globally, so the same configuration can be re-used
for both Broker and Historical processes, when defined in the common properties file.


### Cache Type

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.cache.type`|`local`, `memcached`, `hybrid`, `caffeine`|The type of cache to use for queries. See below of the configuration options for each cache type|`caffeine`|

#### Local Cache

> DEPRECATED: Use caffeine (default as of v0.12.0) instead

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
|`druid.cache.type`| Set this to `caffeine` or leave out parameter|`caffeine`|
|`druid.cache.sizeInBytes`|The maximum size of the cache in bytes on heap. It can be configured as described in [here](human-readable-byte.md). |min(1GiB, Runtime.maxMemory / 10)|
|`druid.cache.expireAfter`|The time (in ms) after an access for which a cache entry may be expired|None (no time limit)|
|`druid.cache.cacheExecutorFactory`|The executor factory to use for Caffeine maintenance. One of `COMMON_FJP`, `SINGLE_THREAD`, or `SAME_THREAD`|ForkJoinPool common pool (`COMMON_FJP`)|
|`druid.cache.evictOnClose`|If a close of a namespace (ex: removing a segment from a process) should cause an eager eviction of associated cache values|`false`|

##### `druid.cache.cacheExecutorFactory`

Here are the possible values for `druid.cache.cacheExecutorFactory`, which controls how maintenance tasks are run

* `COMMON_FJP` (default) use the common ForkJoinPool. Should use with [JRE 8u60 or higher](https://github.com/apache/druid/pull/4810#issuecomment-329922810). Older versions of the JRE may have worse performance than newer JRE versions.
* `SINGLE_THREAD` Use a single-threaded executor.
* `SAME_THREAD` Cache maintenance is done eagerly.

##### Metrics
In addition to the normal cache metrics, the caffeine cache implementation also reports the following in both `total` and `delta`

|Metric|Description|Normal value|
|------|-----------|------------|
|`query/cache/caffeine/*/requests`|Count of hits or misses|hit + miss|
|`query/cache/caffeine/*/loadTime`|Length of time caffeine spends loading new values (unused feature)|0|
|`query/cache/caffeine/*/evictionBytes`|Size in bytes that have been evicted from the cache|Varies, should tune cache `sizeInBytes` so that `sizeInBytes`/`evictionBytes` is approximately the rate of cache churn you desire|


##### Memcached

Uses memcached as cache backend. This allows all processes to share the same cache.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.cache.expiration`|Memcached [expiration time](https://code.google.com/p/memcached/wiki/NewCommands#Standard_Protocol).|2592000 (30 days)|
|`druid.cache.timeout`|Maximum time in milliseconds to wait for a response from Memcached.|500|
|`druid.cache.hosts`|Comma separated list of Memcached hosts `<host:port>`.|none|
|`druid.cache.maxObjectSize`|Maximum object size in bytes for a Memcached object.|52428800 (50 MB)|
|`druid.cache.memcachedPrefix`|Key prefix for all keys in Memcached.|druid|
|`druid.cache.numConnections`|Number of memcached connections to use.|1|
|`druid.cache.protocol`|Memcached communication protocol. Can be binary or text.|binary|
|`druid.cache.locator`|Memcached locator. Can be consistent or array_mod.|consistent|

#### Hybrid

Uses a combination of any two caches as a two-level L1 / L2 cache.
This may be used to combine a local in-memory cache with a remote memcached cache.

Cache requests will first check L1 cache before checking L2.
If there is an L1 miss and L2 hit, it will also populate L1.


|Property|Description|Default|
|--------|-----------|-------|
|`druid.cache.l1.type`|type of cache to use for L1 cache. See `druid.cache.type` configuration for valid types.|`caffeine`|
|`druid.cache.l2.type`|type of cache to use for L2 cache. See `druid.cache.type` configuration for valid types.|`caffeine`|
|`druid.cache.l1.*`|Any property valid for the given type of L1 cache can be set using this prefix. For instance, if you are using a `caffeine` L1 cache, specify `druid.cache.l1.sizeInBytes` to set its size.|defaults are the same as for the given cache type.|
|`druid.cache.l2.*`|Prefix for L2 cache settings, see description for L1.|defaults are the same as for the given cache type.|
|`druid.cache.useL2`|A boolean indicating whether to query L2 cache, if it's a miss in L1. It makes sense to configure this to `false` on Historical processes, if L2 is a remote cache like `memcached`, and this cache also used on brokers, because in this case if a query reached Historical it means that a broker didn't find corresponding results in the same remote cache, so a query to the remote cache from Historical is guaranteed to be a miss.|`true`|
|`druid.cache.populateL2`|A boolean indicating whether to put results into L2 cache.|`true`|

## General query configuration

This section describes configurations that control behavior of Druid's query types, applicable to Broker, Historical, and MiddleManager processes.

### Overriding default query context values

Any [Query Context General Parameter](../querying/query-context.md#general-parameters) default value can be 
overridden by setting runtime property in the format of `druid.query.default.context.{query_context_key}`. 
`druid.query.default.context.{query_context_key}` runtime property prefix applies to all current and future 
query context keys, the same as how query context parameter passed with the query works. Note that the runtime property 
value can be overridden if value for the same key is explicitly specify in the query contexts.

The precedence chain for query context values is as follows: 

hard-coded default value in Druid code <- runtime property not prefixed with `druid.query.default.context` 
<- runtime property prefixed with `druid.query.default.context` <- context parameter in the query

Note that not all query context key has a runtime property not prefixed with `druid.query.default.context` that can 
override the hard-coded default value. For example, `maxQueuedBytes` has `druid.broker.http.maxQueuedBytes` 
but `joinFilterRewriteMaxSize` does not. Hence, the only way of overriding `joinFilterRewriteMaxSize` hard-coded default 
value is with runtime property `druid.query.default.context.joinFilterRewriteMaxSize`. 

To further elaborate on the previous example:

If neither `druid.broker.http.maxQueuedBytes` or `druid.query.default.context.maxQueuedBytes` is set and
the query does not have `maxQueuedBytes` in the context, then the hard-coded value in Druid code is use.
If runtime property only contains `druid.broker.http.maxQueuedBytes=x` and query does not have `maxQueuedBytes` in the 
context, then the value of the property, `x`, is use. However, if query does have `maxQueuedBytes` in the context, 
then that value is use instead.
If runtime property only contains `druid.query.default.context.maxQueuedBytes=y` OR runtime property contains both
`druid.broker.http.maxQueuedBytes=x` and `druid.query.default.context.maxQueuedBytes=y`, then the value of 
`druid.query.default.context.maxQueuedBytes`, `y`, is use (given that query does not have `maxQueuedBytes` in the 
context). If query does have `maxQueuedBytes` in the context, then that value is use instead.

### TopN query config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.topN.minTopNThreshold`|See [TopN Aliasing](../querying/topnquery.md#aliasing) for details.|1000|

### Search query config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.search.maxSearchLimit`|Maximum number of search results to return.|1000|
|`druid.query.search.searchStrategy`|Default search query strategy.|useIndexes|

### SegmentMetadata query config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.segmentMetadata.defaultHistory`|When no interval is specified in the query, use a default interval of defaultHistory before the end time of the most recent segment, specified in ISO8601 format. This property also controls the duration of the default interval used by GET /druid/v2/datasources/{dataSourceName} interactions for retrieving datasource dimensions/metrics.|P1W|
|`druid.query.segmentMetadata.defaultAnalysisTypes`|This can be used to set the Default Analysis Types for all segment metadata queries, this can be overridden when making the query|["cardinality", "interval", "minmax"]|

### GroupBy query config

This section describes the configurations for groupBy queries. You can set the runtime properties in the `runtime.properties` file on Broker, Historical, and MiddleManager processes. You can set the query context parameters through the [query context](../querying/query-context.md).

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
|`druid.query.groupBy.intermediateCombineDegree`|Number of intermediate processes combined together in the combining tree. Higher degrees will need less threads which might be helpful to improve the query performance by reducing the overhead of too many threads if the server has sufficiently powerful CPU cores.|8|
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
|`forceLimitPushDown`|When all fields in the orderby are part of the grouping key, the broker will push limit application down to the Historical processes. When the sorting order uses fields that are not in the grouping key, applying this optimization can result in approximate results with unknown accuracy, so this optimization is disabled by default in that case. Enabling this context flag turns on limit push down for limit/orderbys that contain non-grouping key columns.|false|


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

### Router

#### Router Process Configs

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current process. This is used to advertise the current processes location as reachable from another process and should generally be specified such that `http://${druid.host}/` could actually talk to this process|InetAddress.getLocalHost().getCanonicalHostName()|
|`druid.bindOnHost`|Indicating whether the process's internal jetty server bind on `druid.host`. Default is false, which means binding to all interfaces.|false|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8888|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.enableTlsPort](../operations/tls-support.md) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative Integer.|9088|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|druid/router|

#### Runtime Configuration

|Property|Description|Default|
|--------|-----------|-------|
|`druid.router.defaultBrokerServiceName`|The default Broker to connect to in case service discovery fails.|druid/broker|
|`druid.router.tierToBrokerMap`|Queries for a certain tier of data are routed to their appropriate Broker. This value should be an ordered JSON map of tiers to Broker names. The priority of Brokers is based on the ordering.|{"_default_tier": "<defaultBrokerServiceName>"}|
|`druid.router.defaultRule`|The default rule for all datasources.|"_default"|
|`druid.router.pollPeriod`|How often to poll for new rules.|PT1M|
|`druid.router.strategies`|Please see [Router Strategies](../design/router.md#router-strategies) for details.|[{"type":"timeBoundary"},{"type":"priority"}]|
|`druid.router.avatica.balancer.type`|Class to use for balancing Avatica queries across Brokers. Please see [Avatica Query Balancing](../design/router.md#avatica-query-balancing).|rendezvousHash|
|`druid.router.managementProxy.enabled`|Enables the Router's [management proxy](../design/router.md#router-as-management-proxy) functionality.|false|
|`druid.router.http.numConnections`|Size of connection pool for the Router to connect to Broker processes. If there are more queries than this number that all need to speak to the same process, then they will queue up.|`20`|
|`druid.router.http.readTimeout`|The timeout for data reads from Broker processes.|`PT15M`|
|`druid.router.http.numMaxThreads`|Maximum number of worker threads to handle HTTP requests and responses|`max(10, ((number of cores * 17) / 16 + 2) + 30)`|
|`druid.router.http.numRequestsQueued`|Maximum number of requests that may be queued to a destination|`1024`|
|`druid.router.http.requestBuffersize`|Size of the content buffer for receiving requests. These buffers are only used for active connections that have requests with bodies that will not fit within the header buffer|`8 * 1024`|
