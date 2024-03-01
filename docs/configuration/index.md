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

## Recommended configuration file organization

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

Each directory has a `runtime.properties` file containing configuration properties for the specific Druid service corresponding to the directory, such as `historical`.

The `jvm.config` files contain JVM flags such as heap sizing properties for each service.

Common properties shared by all services are placed in `_common/common.runtime.properties`.

## Configuration interpolation

Configuration values can be interpolated from System Properties, Environment Variables, or local files. Below is an example of how this can be used:

```
druid.metadata.storage.type=${env:METADATA_STORAGE_TYPE}
druid.processing.tmpDir=${sys:java.io.tmpdir}
druid.segmentCache.locations=${file:UTF-8:/config/segment-cache-def.json}
```

Interpolation is also recursive so you can do:

```
druid.segmentCache.locations=${file:UTF-8:${env:SEGMENT_DEF_LOCATION}}
```

If the property is not set, an exception will be thrown on startup, but a default can be provided if desired. Setting a default value will not work with file interpolation as an exception will be thrown if the file does not exist.

```
druid.metadata.storage.type=${env:METADATA_STORAGE_TYPE:-mysql}
druid.processing.tmpDir=${sys:java.io.tmpdir:-/tmp}
```

If you need to set a variable that is wrapped by `${...}` but do not want it to be interpolated, you can escape it by adding another `$`. For example:

```
config.name=$${value}
```

## Common configurations

The properties under this section are common configurations that should be shared across all Druid services in a cluster.

### JVM configuration best practices

There are four JVM parameters that we set on all of our services:

-  `-Duser.timezone=UTC`: This sets the default timezone of the JVM to UTC. We always set this and do not test with other default timezones, so local timezones might work, but they also might uncover weird and interesting bugs. To issue queries in a non-UTC timezone, see [query granularities](../querying/granularities.md#period-granularities)
-  `-Dfile.encoding=UTF-8` This is similar to timezone, we test assuming UTF-8. Local encodings might work, but they also might result in weird and interesting bugs.
-  `-Djava.io.tmpdir=<a path>` Various parts of Druid use temporary files to interact with the file system. These files can become quite large. This means that systems that have small `/tmp` directories can cause problems for Druid. Therefore, set the JVM tmp directory to a location with ample space.

     Also consider the following when configuring the JVM tmp directory:
     - The temp directory should not be volatile tmpfs.
     - This directory should also have good read and write speed.
     - Avoid NFS mount.
     - The `org.apache.druid.java.util.metrics.SysMonitor` requires execute privileges on files in `java.io.tmpdir`. If you are using the system monitor, do not set `java.io.tmpdir` to `noexec`.
-  `-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager` This allows log4j2 to handle logs for non-log4j2 components (like jetty) which use standard java logging.

### Extensions

Many of Druid's external dependencies can be plugged in as modules. Extensions can be provided using the following configs:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.extensions.directory`|The root extension directory where user can put extensions related files. Druid will load extensions stored under this directory.|`extensions` (This is a relative path to Druid's working directory)|
|`druid.extensions.hadoopDependenciesDir`|The root Hadoop dependencies directory where user can put Hadoop related dependencies files. Druid will load the dependencies based on the Hadoop coordinate specified in the Hadoop index task.|`hadoop-dependencies` (This is a relative path to Druid's working directory|
|`druid.extensions.loadList`|A JSON array of extensions to load from extension directories by Druid. If it is not specified, its value will be `null` and Druid will load all the extensions under `druid.extensions.directory`. If its value is empty list `[]`, then no extensions will be loaded at all. It is also allowed to specify absolute path of other custom extensions not stored in the common extensions directory.|null|
|`druid.extensions.searchCurrentClassloader`|This is a boolean flag that determines if Druid will search the main classloader for extensions. It defaults to true but can be turned off if you have reason to not automatically add all modules on the classpath.|true|
|`druid.extensions.useExtensionClassloaderFirst`|This is a boolean flag that determines if Druid extensions should prefer loading classes from their own jars rather than jars bundled with Druid. If false, extensions must be compatible with classes provided by any jars bundled with Druid. If true, extensions may depend on conflicting versions.|false|
|`druid.extensions.hadoopContainerDruidClasspath`|Hadoop Indexing launches Hadoop jobs and this configuration provides way to explicitly set the user classpath for the Hadoop job. By default, this is computed automatically by Druid based on the Druid service classpath and set of extensions. However, sometimes you might want to be explicit to resolve dependency conflicts between Druid and Hadoop.|null|
|`druid.extensions.addExtensionsToHadoopContainer`|Only applicable if `druid.extensions.hadoopContainerDruidClasspath` is provided. If set to true, then extensions specified in the loadList are added to Hadoop container classpath. Note that when `druid.extensions.hadoopContainerDruidClasspath` is not provided then extensions are always added to Hadoop container classpath.|false|

### Modules

|Property|Description|Default|
|--------|-----------|-------|
|`druid.modules.excludeList`|A JSON array of canonical class names (e.g., `"org.apache.druid.somepackage.SomeModule"`) of module classes which shouldn't be loaded, even if they are found in extensions specified by `druid.extensions.loadList`, or in the list of core modules specified to be loaded on a particular Druid service type. Useful when some useful extension contains some module, which shouldn't be loaded on some Druid service type because some dependencies of that module couldn't be satisfied.|[]|

### ZooKeeper

We recommend just setting the base ZK path and the ZK service host, but all ZK paths that Druid uses can be overwritten to absolute paths.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.zk.paths.base`|Base ZooKeeper path.|`/druid`|
|`druid.zk.service.host`|The ZooKeeper hosts to connect to. This is a REQUIRED property and therefore a host address must be supplied.|none|
|`druid.zk.service.user`|The username to authenticate with ZooKeeper. This is an optional property.|none|
|`druid.zk.service.pwd`|The [Password Provider](../operations/password-provider.md) or the string password to authenticate with ZooKeeper. This is an optional property.|none|
|`druid.zk.service.authScheme`|digest is the only authentication scheme supported. |digest|

#### ZooKeeper behavior

|Property|Description|Default|
|--------|-----------|-------|
|`druid.zk.service.sessionTimeoutMs`|ZooKeeper session timeout, in milliseconds.|`30000`|
|`druid.zk.service.connectionTimeoutMs`|ZooKeeper connection timeout, in milliseconds.|`15000`|
|`druid.zk.service.compress`|Boolean flag for whether or not created Znodes should be compressed.|`true`|
|`druid.zk.service.acl`|Boolean flag for whether or not to enable ACL security for ZooKeeper. If ACL is enabled, zNode creators will have all permissions.|`false`|

#### Path configuration

Druid interacts with ZooKeeper through a set of standard path configurations. We recommend just setting the base ZooKeeper path, but all ZooKeeper paths that Druid uses can be overwritten to absolute paths.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.zk.paths.base`|Base ZooKeeper path.|`/druid`|
|`druid.zk.paths.propertiesPath`|ZooKeeper properties path.|`${druid.zk.paths.base}/properties`|
|`druid.zk.paths.announcementsPath`|Druid service announcement path.|`${druid.zk.paths.base}/announcements`|
|`druid.zk.paths.liveSegmentsPath`|Current path for where Druid services announce their segments.|`${druid.zk.paths.base}/segments`|
|`druid.zk.paths.loadQueuePath`|Entries here cause Historical services to load and drop segments.|`${druid.zk.paths.base}/loadQueue`|
|`druid.zk.paths.coordinatorPath`|Used by the Coordinator for leader election.|`${druid.zk.paths.base}/coordinator`|
|`druid.zk.paths.servedSegmentsPath`|Deprecated. Legacy path for where Druid services announce their segments.|`${druid.zk.paths.base}/servedSegments`|

The indexing service also uses its own set of paths. These configs can be included in the common configuration.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.zk.paths.indexer.base`|Base ZooKeeper path for |`${druid.zk.paths.base}/indexer`|
|`druid.zk.paths.indexer.announcementsPath`|MiddleManagers announce themselves here.|`${druid.zk.paths.indexer.base}/announcements`|
|`druid.zk.paths.indexer.tasksPath`|Used to assign tasks to MiddleManagers.|`${druid.zk.paths.indexer.base}/tasks`|
|`druid.zk.paths.indexer.statusPath`|Parent path for announcement of task statuses.|`${druid.zk.paths.indexer.base}/status`|

If `druid.zk.paths.base` and `druid.zk.paths.indexer.base` are both set, and none of the other `druid.zk.paths.*` or `druid.zk.paths.indexer.*` values are set, then the other properties will be evaluated relative to their respective `base`.
For example, if `druid.zk.paths.base` is set to `/druid1` and `druid.zk.paths.indexer.base` is set to `/druid2` then `druid.zk.paths.announcementsPath` will default to `/druid1/announcements` while `druid.zk.paths.indexer.announcementsPath` will default to `/druid2/announcements`.

The following path is used for service discovery. It is **not** affected by `druid.zk.paths.base` and **must** be specified separately.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.discovery.curator.path`|Services announce themselves under this ZooKeeper path.|`/druid/discovery`|

### TLS

#### General configuration

|Property|Description|Default|
|--------|-----------|-------|
|`druid.enablePlaintextPort`|Enable/Disable HTTP connector.|`true`|
|`druid.enableTlsPort`|Enable/Disable HTTPS connector.|`false`|

Although not recommended but both HTTP and HTTPS connectors can be enabled at a time and respective ports are configurable using `druid.plaintextPort`
and `druid.tlsPort` properties on each service. Please see `Configuration` section of individual services to check the valid and default values for these ports.

#### Jetty server TLS configuration

Druid uses Jetty as an embedded web server. To learn more about TLS/SSL, certificates, and related concepts in Jetty, including explanations of the configuration settings below, see "Configuring SSL/TLS KeyStores" in the [Jetty Operations Guide](https://www.eclipse.org/jetty/documentation.php).

For information about TLS/SSL support in Java in general, see the [Java Secure Socket Extension (JSSE) Reference Guide](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html).
The [Java Cryptography Architecture
Standard Algorithm Name Documentation for JDK 8](http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html) lists all possible
values for the following properties, among others provided by the Java implementation.

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.server.https.keyStorePath`|The file path or URL of the TLS/SSL KeyStore.|none|yes|
|`druid.server.https.keyStoreType`|The type of the KeyStore.|none|yes|
|`druid.server.https.certAlias`|Alias of TLS/SSL certificate for the connector.|none|yes|
|`druid.server.https.keyStorePassword`|The [Password Provider](../operations/password-provider.md) or String password for the KeyStore.|none|yes|

Following table contains non-mandatory advanced configuration options, use caution.

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.server.https.keyManagerFactoryAlgorithm`|Algorithm to use for creating KeyManager, more details [here](https://docs.oracle.com/javase/7/docs/technotes/guides/security/jsse/JSSERefGuide.html#KeyManager).|`javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm()`|no|
|`druid.server.https.keyManagerPassword`|The [Password Provider](../operations/password-provider.md) or String password for the Key Manager.|none|no|
|`druid.server.https.includeCipherSuites`|List of cipher suite names to include. You can either use the exact cipher suite name or a regular expression.|Jetty's default include cipher list|no|
|`druid.server.https.excludeCipherSuites`|List of cipher suite names to exclude. You can either use the exact cipher suite name or a regular expression.|Jetty's default exclude cipher list|no|
|`druid.server.https.includeProtocols`|List of exact protocols names to include.|Jetty's default include protocol list|no|
|`druid.server.https.excludeProtocols`|List of exact protocols names to exclude.|Jetty's default exclude protocol list|no|

#### Internal client TLS configuration (requires `simple-client-sslcontext` extension)

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

### Authentication and authorization

|Property|Type|Description|Default|Required|
|--------|-----------|--------|--------|--------|
|`druid.auth.authenticatorChain`|JSON List of Strings|List of Authenticator type names|["allowAll"]|no|
|`druid.escalator.type`|String|Type of the Escalator that should be used for internal Druid communications. This Escalator must use an authentication scheme that is supported by an Authenticator in `druid.auth.authenticatorChain`.|`noop`|no|
|`druid.auth.authorizers`|JSON List of Strings|List of Authorizer type names |["allowAll"]|no|
|`druid.auth.unsecuredPaths`| List of Strings|List of paths for which security checks will not be performed. All requests to these paths will be allowed.|[]|no|
|`druid.auth.allowUnauthenticatedHttpOptions`|Boolean|If true, skip authentication checks for HTTP OPTIONS requests. This is needed for certain use cases, such as supporting CORS pre-flight requests. Note that disabling authentication checks for OPTIONS requests will allow unauthenticated users to determine what Druid endpoints are valid (by checking if the OPTIONS request returns a 200 instead of 404), so enabling this option may reveal information about server configuration, including information about what extensions are loaded (if those extensions add endpoints).|false|no|

For more information, please see [Authentication and Authorization](../operations/auth.md).

For configuration options for specific auth extensions, please refer to the extension documentation.

### Startup logging

All services can log debugging information on startup.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.startup.logging.logProperties`|Log all properties on startup (from common.runtime.properties, runtime.properties, and the JVM command line).|false|
|`druid.startup.logging.maskProperties`|Masks sensitive properties (passwords, for example) containing theses words.|["password"]|

Note that some sensitive information may be logged if these settings are enabled.

### Request logging

All services that can serve queries can also log the query requests they see. Broker services can additionally log the SQL requests (both from HTTP and JDBC) they see.
For an example of setting up request logging, see [Request logging](../operations/request-logging.md).

|Property|Description|Default|
|--------|-----------|-------|
|`druid.request.logging.type`|How to log every query request. Choices: `noop`, [`file`](#file-request-logging), [`emitter`](#emitter-request-logging), [`slf4j`](#slf4j-request-logging), [`filtered`](#filtered-request-logging), [`composing`](#composing-request-logging), [`switching`](#switching-request-logging)|`noop` (request logging disabled by default)|

To enable sending all the HTTP requests to a log, set `org.apache.druid.jetty.RequestLog` to the `DEBUG` level. See [Logging](../configuration/logging.md) for more information.

#### File request logging

The `file` request logger stores daily request logs on disk.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.request.logging.dir`|Historical, Realtime, and Broker services maintain request logs of all of the requests they get (interaction is via POST, so normal request logs don’t generally capture information about the actual query), this specifies the directory to store the request logs in|none|
|`druid.request.logging.filePattern`|[Joda datetime format](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html) for each file|"yyyy-MM-dd'.log'"|
| `druid.request.logging.durationToRetain`| Period to retain the request logs on disk. The period should be at least longer than `P1D`.| none

The format of request logs is TSV, one line per requests, with five fields: timestamp, remote\_addr, native\_query, query\_context, sql\_query.

For native JSON request, the `sql_query` field is empty. For example:

```
2019-01-14T10:00:00.000Z        127.0.0.1   {"queryType":"topN","dataSource":{"type":"table","name":"wikiticker"},"virtualColumns":[],"dimension":{"type":"LegacyDimensionSpec","dimension":"page","outputName":"page","outputType":"STRING"},"metric":{"type":"LegacyTopNMetricSpec","metric":"count"},"threshold":10,"intervals":{"type":"LegacySegmentSpec","intervals":["2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z"]},"filter":null,"granularity":{"type":"all"},"aggregations":[{"type":"count","name":"count"}],"postAggregations":[],"context":{"queryId":"74c2d540-d700-4ebd-b4a9-3d02397976aa"},"descending":false}    {"query/time":100,"query/bytes":800,"success":true,"identity":"user1"}
```

For SQL query request, the `native_query` field is empty. For example:

```
2019-01-14T10:00:00.000Z        127.0.0.1       {"sqlQuery/time":100, "sqlQuery/planningTimeMs":10, "sqlQuery/bytes":600, "success":true, "identity":"user1"}  {"query":"SELECT page, COUNT(*) AS Edits FROM wikiticker WHERE TIME_IN_INTERVAL(\"__time\", '2015-09-12/2015-09-13') GROUP BY page ORDER BY Edits DESC LIMIT 10","context":{"sqlQueryId":"c9d035a0-5ffd-4a79-a865-3ffdadbb5fdd","nativeQueryIds":"[490978e4-f5c7-4cf6-b174-346e63cf8863]"}}
```

#### Emitter request logging

The `emitter` request logger emits every request to the external location specified in the [emitter](#enabling-metrics) configuration.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.request.logging.feed`|Feed name for requests.|none|

#### SLF4J request logging

The `slf4j` request logger logs every request using SLF4J. It serializes native queries into JSON in the log message regardless of the SLF4J format specification. Requests are logged under the class `org.apache.druid.server.log.LoggingRequestLogger`.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.request.logging.setMDC`|If you want to set MDC entries within the log entry, set this value to `true`. Your logging system must be configured to support MDC in order to format this data.|false|
|`druid.request.logging.setContextMDC`|Set to "true" to add  the Druid query `context` to the MDC entries. Only applies when `setMDC` is `true`.|false|

For a native query, the following MDC fields are populated when `setMDC` is `true`:

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

#### Filtered request logging

The `filtered` request logger filters requests based on the query type or how long a query takes to complete.
For native queries, the logger only logs requests when the `query/time` metric exceeds the threshold provided in `queryTimeThresholdMs`.
For SQL queries, it only logs requests when the `sqlQuery/time` metric exceeds threshold provided in `sqlQueryTimeThresholdMs`.
See [Metrics](../operations/metrics.md) for more details on query metrics.

Requests that meet the threshold are logged using the request logger type set in `druid.request.logging.delegate.type`.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.request.logging.queryTimeThresholdMs`|Threshold value for the `query/time` metric in milliseconds.|0, i.e., no filtering|
|`druid.request.logging.sqlQueryTimeThresholdMs`|Threshold value for the `sqlQuery/time` metric in milliseconds.|0, i.e., no filtering|
|`druid.request.logging.mutedQueryTypes` | Query requests of these types are not logged. Query types are defined as string objects corresponding to the "queryType" value for the specified query in the Druid's [native JSON query API](../querying/querying.md). Misspelled query types will be ignored. Example to ignore scan and timeBoundary queries: `["scan", "timeBoundary"]`| []|
|`druid.request.logging.delegate.type`|Type of delegate request logger to log requests.|none|

#### Composing request logging

The `composing` request logger emits request logs to multiple request loggers.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.request.logging.loggerProviders`|List of request loggers for emitting request logs.|none|

#### Switching request logging

The `switching` request logger routes native query request logs to one request logger and SQL query request logs to another request logger.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.request.logging.nativeQueryLogger`|Request logger for emitting native query request logs.|none|
|`druid.request.logging.sqlQueryLogger`|Request logger for emitting SQL query request logs.|none|

### Audit logging

Coordinator and Overlord log changes to lookups, segment load/drop rules, and dynamic configuration changes for auditing.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.audit.manager.auditHistoryMillis`|Default duration for querying audit history.|1 week|
|`druid.audit.manager.includePayloadAsDimensionInMetric`|Boolean flag on whether to add `payload` column in service metric.|false|
|`druid.audit.manager.maxPayloadSizeBytes`|The maximum size of audit payload to store in Druid's metadata store audit table. If the size of audit payload exceeds this value, the audit log would be stored with a message indicating that the payload was omitted instead. Setting `maxPayloadSizeBytes` to -1 (default value) disables this check, meaning Druid will always store audit payload regardless of it's size. Setting to any negative number other than `-1` is invalid. Human-readable format is supported, see [here](human-readable-byte.md).  |-1|
|`druid.audit.manager.skipNullField`|If true, the audit payload stored in metadata store will exclude any field with null value. |false|

### Enabling metrics

You can configure Druid services to emit [metrics](../operations/metrics.md) regularly from a number of [monitors](#metrics-monitors) via [emitters](#metrics-emitters).

|Property|Description|Default|
|--------|-----------|-------|
|`druid.monitoring.emissionPeriod`| Frequency that Druid emits metrics.|`PT1M`|
|[`druid.monitoring.monitors`](#metrics-monitors)|Sets list of Druid monitors used by a service.<br /><br />Because individual monitors sometimes only work on specific process types, it is best to set this property for each process type individually in e.g. `historical/runtime.properties` rather than `_common/common.runtime.properties`.|none (no monitors)|
|[`druid.emitter`](#metrics-emitters)|Setting this value initializes one of the emitter modules.|`noop` (metric emission disabled by default)|

#### Metrics monitors

Metric monitoring is an essential part of Druid operations. The following monitors are available:

|Name|Description|
|----|-----------|
|`org.apache.druid.client.cache.CacheMonitor`|Emits metrics (to logs) about the segment results cache for Historical and Broker services. Reports typical cache statistics include hits, misses, rates, and size (bytes and number of entries), as well as timeouts and and errors.|
|`org.apache.druid.java.util.metrics.SysMonitor`|Reports on various system activities and statuses using the [SIGAR library](https://github.com/hyperic/sigar). Requires execute privileges on files in `java.io.tmpdir`. Do not set `java.io.tmpdir` to `noexec` when using `SysMonitor`.|
|`org.apache.druid.java.util.metrics.JvmMonitor`|Reports various JVM-related statistics.|
|`org.apache.druid.java.util.metrics.JvmCpuMonitor`|Reports statistics of CPU consumption by the JVM.|
|`org.apache.druid.java.util.metrics.CpuAcctDeltaMonitor`|Reports consumed CPU as per the cpuacct cgroup.|
|`org.apache.druid.java.util.metrics.JvmThreadsMonitor`|Reports Thread statistics in the JVM, like numbers of total, daemon, started, died threads.|
|`org.apache.druid.java.util.metrics.CgroupCpuMonitor`|Reports CPU shares and quotas as per the `cpu` cgroup.|
|`org.apache.druid.java.util.metrics.CgroupCpuSetMonitor`|Reports CPU core/HT and memory node allocations as per the `cpuset` cgroup.|
|`org.apache.druid.java.util.metrics.CgroupMemoryMonitor`|Reports memory statistic as per the memory cgroup.|
|`org.apache.druid.server.metrics.EventReceiverFirehoseMonitor`|Reports how many events have been queued in the EventReceiverFirehose.|
|`org.apache.druid.server.metrics.HistoricalMetricsMonitor`|Reports statistics on Historical services. Available only on Historical services.|
|`org.apache.druid.server.metrics.SegmentStatsMonitor` | **EXPERIMENTAL** Reports statistics about segments on Historical services. Available only on Historical services. Not to be used when lazy loading is configured.|
|`org.apache.druid.server.metrics.QueryCountStatsMonitor`|Reports how many queries have been successful/failed/interrupted.|
|`org.apache.druid.server.metrics.SubqueryCountStatsMonitor`|Reports how many subqueries have been materialized as rows or bytes and various other statistics related to the subquery execution|
|`org.apache.druid.server.emitter.HttpEmittingMonitor`|Reports internal metrics of `http` or `parametrized` emitter (see below). Must not be used with another emitter type. See the description of the metrics here: https://github.com/apache/druid/pull/4973.|
|`org.apache.druid.server.metrics.TaskCountStatsMonitor`|Reports how many ingestion tasks are currently running/pending/waiting and also the number of successful/failed tasks per emission period.|
|`org.apache.druid.server.metrics.TaskSlotCountStatsMonitor`|Reports metrics about task slot usage per emission period.|
|`org.apache.druid.server.metrics.WorkerTaskCountStatsMonitor`|Reports how many ingestion tasks are currently running/pending/waiting, the number of successful/failed tasks, and metrics about task slot usage for the reporting worker, per emission period. Only supported by MiddleManager node types.|
|`org.apache.druid.server.metrics.ServiceStatusMonitor`|Reports a heartbeat for the service.|

For example, you might configure monitors on all services for system and JVM information within `common.runtime.properties` as follows:

```
druid.monitoring.monitors=["org.apache.druid.java.util.metrics.SysMonitor","org.apache.druid.java.util.metrics.JvmMonitor"]
```

You can override cluster-wide configuration by amending the `runtime.properties` of individual services.

#### Metrics emitters

There are several emitters available:

- `noop` (default) disables metric emission.
- [`logging`](#logging-emitter-module) emits logs using Log4j2.
- [`http`](#http-emitter-module) sends `POST` requests of JSON events.
- [`parametrized`](#parametrized-http-emitter-module) operates like the `http` emitter but fine-tunes the recipient URL based on the event feed.
- [`composing`](#composing-emitter-module) initializes multiple emitter modules.
- [`graphite`](#graphite-emitter) emits metrics to a [Graphite](https://graphiteapp.org/) Carbon service.
- [`switching`](#switching-emitter) initializes and emits to multiple emitter modules based on the event feed.

##### Logging emitter module

The use this emitter module, set `druid.emitter=logging`. The `logging` emitter uses a Log4j2 logger named
`druid.emitter.logging.loggerClass` to emit events. Each event is logged as a single `json` object with a
[Marker](https://logging.apache.org/log4j/2.x/manual/markers.html) as the feed of the event. Users may wish to edit the
log4j config to route these logs to different sources based on the feed of the event.

|Property|Description| Default|
|--------|-----------|--------|
|`druid.emitter.logging.loggerClass`|The class used for logging.|`org.apache.druid.java.util.emitter.core.LoggingEmitter`|
|`druid.emitter.logging.logLevel`|Choices: debug, info, warn, error. The log level at which message are logged.|info|

##### HTTP emitter module

|Property|Description|Default|
|--------|-----------|-------|
|`druid.emitter.http.flushMillis`|How often the internal message buffer is flushed (data is sent).|60000|
|`druid.emitter.http.flushCount`|How many messages the internal message buffer can hold before flushing (sending).|500|
|`druid.emitter.http.basicAuthentication`|[Password Provider](../operations/password-provider.md) for providing login and password for authentication in `"login:password"` form. For example, `druid.emitter.http.basicAuthentication=admin:adminpassword` uses Default Password Provider which allows plain text passwords.|not specified = no authentication|
|`druid.emitter.http.flushTimeOut`|The timeout after which an event should be sent to the endpoint, even if internal buffers are not filled, in milliseconds.|not specified = no timeout|
|`druid.emitter.http.batchingStrategy`|The strategy of how the batch is formatted. "ARRAY" means `[event1,event2]`, "NEWLINES" means `event1\nevent2`, ONLY_EVENTS means `event1event2`.|ARRAY|
|`druid.emitter.http.maxBatchSize`|The maximum batch size, in bytes.|the minimum of (10% of JVM heap size divided by 2) or (5242880 (i. e. 5 MiB))|
|`druid.emitter.http.batchQueueSizeLimit`|The maximum number of batches in emitter queue, if there are problems with emitting.|the maximum of (2) or (10% of the JVM heap size divided by 5MiB)|
|`druid.emitter.http.minHttpTimeoutMillis`|If the speed of filling batches imposes timeout smaller than that, not even trying to send batch to endpoint, because it will likely fail, not being able to send the data that fast. Configure this depending based on emitter/successfulSending/minTimeMs metric. Reasonable values are 10ms..100ms.|0|
|`druid.emitter.http.recipientBaseUrl`|The base URL to emit messages to. Druid will POST JSON to be consumed at the HTTP endpoint specified by this property.|none, required config|

##### HTTP emitter module TLS overrides

By default, when sending events to a TLS-enabled receiver, the HTTP Emitter uses an SSLContext obtained from the service described at [Druid's internal communication over TLS](../operations/tls-support.md), that is the same SSLContext that would be used for internal communications between Druid services.

In some use cases it may be desirable to have the HTTP Emitter use its own separate truststore configuration. For example, there may be organizational policies that prevent the TLS-enabled metrics receiver's certificate from being added to the same truststore used by Druid's internal HTTP client.

The following properties allow the HTTP Emitter to use its own truststore configuration when building its SSLContext.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.emitter.http.ssl.useDefaultJavaContext`|If set to true, the HttpEmitter will use `SSLContext.getDefault()`, the default Java SSLContext, and all other properties below are ignored.|false|
|`druid.emitter.http.ssl.trustStorePath`|The file path or URL of the TLS/SSL Key store where trusted root certificates are stored. If this is unspecified, the HTTP Emitter will use the same SSLContext as Druid's internal HTTP client, as described in the beginning of this section, and all other properties below are ignored.|null|
|`druid.emitter.http.ssl.trustStoreType`|The type of the key store where trusted root certificates are stored.|`java.security.KeyStore.getDefaultType()`|
|`druid.emitter.http.ssl.trustStoreAlgorithm`|Algorithm to be used by TrustManager to validate certificate chains|`javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm()`|
|`druid.emitter.http.ssl.trustStorePassword`|The [Password Provider](../operations/password-provider.md) or String password for the Trust Store.|none|
|`druid.emitter.http.ssl.protocol`|TLS protocol to use.|"TLSv1.2"|

##### Parametrized HTTP emitter module

The parametrized emitter takes the same configs as the [`http` emitter](#http-emitter-module) using the prefix `druid.emitter.parametrized.httpEmitting.`.
For example:
* `druid.emitter.parametrized.httpEmitting.flushMillis`
* `druid.emitter.parametrized.httpEmitting.flushCount`
* `druid.emitter.parametrized.httpEmitting.ssl.trustStorePath`

Do not specify `recipientBaseUrl` with the parametrized emitter.
Instead use `recipientBaseUrlPattern` described in the table below.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.emitter.parametrized.recipientBaseUrlPattern`|The URL pattern to send an event to, based on the event's feed. For example, `http://foo.bar/{feed}`, that will send event to `http://foo.bar/metrics` if the event's feed is "metrics".|none, required config|

##### Composing emitter module

|Property|Description|Default|
|--------|-----------|-------|
|`druid.emitter.composing.emitters`|List of emitter modules to load, such as ["logging","http"].|[]|

##### Graphite emitter

To use graphite as emitter set `druid.emitter=graphite`. For configuration details, see [Graphite emitter](../development/extensions-contrib/graphite.md) for the Graphite emitter Druid extension.

##### Switching emitter

To use switching as emitter set `druid.emitter=switching`.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.emitter.switching.emitters`|JSON map of feed to list of emitter modules that will be used for the mapped feed, such as `{"metrics":["http"], "alerts":["logging"]}`|{}|
|`druid.emitter.switching.defaultEmitters`|JSON list of emitter modules to load that will be used if there is no emitter specifically designated for that event's feed, such as `["logging","http"]`.|[]|

### Metadata storage

These properties specify the JDBC connection and other configuration around the metadata storage. The only services that connect to the metadata storage with these properties are the [Coordinator](../design/coordinator.md) and [Overlord](../design/overlord.md).

|Property|Description|Default|
|--------|-----------|-------|
|`druid.metadata.storage.type`|The type of metadata storage to use. One of `mysql`, `postgresql`, or `derby`.|`derby`|
|`druid.metadata.storage.connector.connectURI`|The JDBC URI for the database to connect to|none|
|`druid.metadata.storage.connector.user`|The username to connect with.|none|
|`druid.metadata.storage.connector.password`|The [Password Provider](../operations/password-provider.md) or String password used to connect with.|none|
|`druid.metadata.storage.connector.createTables`|If Druid requires a table and it doesn't exist, create it?|true|
|`druid.metadata.storage.tables.base`|The base name for tables.|`druid`|
|`druid.metadata.storage.tables.dataSource`|The table to use to look for datasources created by [Kafka Indexing Service](../ingestion/kafka-ingestion.md).|`druid_dataSource`|
|`druid.metadata.storage.tables.pendingSegments`|The table to use to look for pending segments.|`druid_pendingSegments`|
|`druid.metadata.storage.tables.segments`|The table to use to look for segments.|`druid_segments`|
|`druid.metadata.storage.tables.rules`|The table to use to look for segment load/drop rules.|`druid_rules`|
|`druid.metadata.storage.tables.config`|The table to use to look for configs.|`druid_config`|
|`druid.metadata.storage.tables.tasks`|Used by the indexing service to store tasks.|`druid_tasks`|
|`druid.metadata.storage.tables.taskLog`|Used by the indexing service to store task logs.|`druid_tasklogs`|
|`druid.metadata.storage.tables.taskLock`|Used by the indexing service to store task locks.|`druid_tasklocks`|
|`druid.metadata.storage.tables.supervisors`|Used by the indexing service to store supervisor configurations.|`druid_supervisors`|
|`druid.metadata.storage.tables.audit`|The table to use for audit history of configuration changes, such as Coordinator rules.|`druid_audit`|

### Deep storage

The configurations concern how to push and pull [Segments](../design/segments.md) from deep storage.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.storage.type`|The type of deep storage to use. One of `local`, `noop`, `s3`, `hdfs`, `c*`.|local|

#### Local deep storage

Local deep storage uses the local filesystem.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.storage.storageDirectory`|Directory on disk to use as deep storage.|`/tmp/druid/localStorage`|

#### Noop deep storage

This deep storage doesn't do anything. There are no configs.

#### S3 deep storage

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

#### HDFS deep storage

This deep storage is used to interface with HDFS. You must load the `druid-hdfs-storage` extension.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.storage.storageDirectory`|HDFS directory to use as deep storage.|none|

#### Cassandra deep storage

This deep storage is used to interface with Cassandra. You must load the `druid-cassandra-storage` extension.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.storage.host`|Cassandra host.|none|
|`druid.storage.keyspace`|Cassandra key space.|none|


### Ingestion security configuration

#### HDFS input source

You can set the following property to specify permissible protocols for
the [HDFS input source](../ingestion/input-sources.md#hdfs-input-source).

|Property|Possible values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.ingestion.hdfs.allowedProtocols`|List of protocols|Allowed protocols for the HDFS input source and HDFS firehose.|`["hdfs"]`|


#### HTTP input source

You can set the following property to specify permissible protocols for
the [HTTP input source](../ingestion/input-sources.md#http-input-source).

|Property|Possible values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.ingestion.http.allowedProtocols`|List of protocols|Allowed protocols for the HTTP input source and HTTP firehose.|`["http", "https"]`|


### External data access security configuration

#### JDBC connections to external databases

You can use the following properties to specify permissible JDBC options for:
- [SQL input source](../ingestion/input-sources.md#sql-input-source)
- [globally cached JDBC lookups](../development/extensions-core/lookups-cached-global.md#jdbc-lookup)
- [JDBC Data Fetcher for per-lookup caching](../development/extensions-core/druid-lookups.md#data-fetcher-layer).

These properties do not apply to metadata storage connections.

|Property|Possible values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.access.jdbc.enforceAllowedProperties`|Boolean|When true, Druid applies `druid.access.jdbc.allowedProperties` to JDBC connections starting with `jdbc:postgresql:`, `jdbc:mysql:`, or `jdbc:mariadb:`. When false, Druid allows any kind of JDBC connections without JDBC property validation. This config is for backward compatibility especially during upgrades since enforcing allow list can break existing ingestion jobs or lookups based on JDBC. This config is deprecated and will be removed in a future release.|true|
|`druid.access.jdbc.allowedProperties`|List of JDBC properties|Defines a list of allowed JDBC properties. Druid always enforces the list for all JDBC connections starting with `jdbc:postgresql:`, `jdbc:mysql:`, and `jdbc:mariadb:` if `druid.access.jdbc.enforceAllowedProperties` is set to true.<br/><br/>This option is tested against MySQL connector 5.1.49, MariaDB connector 2.7.4, and PostgreSQL connector 42.2.14. Other connector versions might not work.|`["useSSL", "requireSSL", "ssl", "sslmode"]`|
|`druid.access.jdbc.allowUnknownJdbcUrlFormat`|Boolean|When false, Druid only accepts JDBC connections starting with `jdbc:postgresql:` or `jdbc:mysql:`. When true, Druid allows JDBC connections to any kind of database, but only enforces `druid.access.jdbc.allowedProperties` for PostgreSQL and MySQL/MariaDB.|true|

### Task logging

You can use the `druid.indexer` configuration to set a [long-term storage](#log-long-term-storage) location for task log files, and to set a [retention policy](#log-retention-policy).

For more information about ingestion tasks and the services of generating logs, see the [task reference](../ingestion/tasks.md).

#### Log long-term storage

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.type`|Where to store task logs.  `noop`, [`s3`](#s3-task-logs), [`azure`](#azure-blob-store-task-logs), [`google`](#google-cloud-storage-task-logs), [`hdfs`](#hdfs-task-logs), [`file`](#file-task-logs) |`file`|

##### File task logs

Store task logs in the local filesystem.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.directory`|Local filesystem path.|log|

##### S3 task logs

Store task logs in S3. Note that the `druid-s3-extensions` extension must be loaded.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.s3Bucket`|S3 bucket name.|none|
|`druid.indexer.logs.s3Prefix`|S3 key prefix.|none|
|`druid.indexer.logs.disableAcl`|Boolean flag for ACL. If this is set to `false`, the full control would be granted to the bucket owner. If the task logs bucket is the same as the deep storage (S3) bucket, then the value of this property will need to be set to true if druid.storage.disableAcl has been set to true.|false|

##### Azure Blob Store task logs

Store task logs in Azure Blob Store.

Note: The `druid-azure-extensions` extension must be loaded, and this uses the same storage account as the deep storage module for azure.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.container`|The Azure Blob Store container to write logs to|none|
|`druid.indexer.logs.prefix`|The path to prepend to logs|none|

##### Google Cloud Storage task logs

Store task logs in Google Cloud Storage.

Note: The `druid-google-extensions` extension must be loaded, and this uses the same storage settings as the deep storage module for google.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.bucket`|The Google Cloud Storage bucket to write logs to|none|
|`druid.indexer.logs.prefix`|The path to prepend to logs|none|

##### HDFS task logs

Store task logs in HDFS. Note that the `druid-hdfs-storage` extension must be loaded.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.directory`|The directory to store logs.|none|

#### Log retention policy

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.kill.enabled`|Boolean value for whether to enable deletion of old task logs. If set to true, Overlord will submit kill tasks periodically based on `druid.indexer.logs.kill.delay` specified, which will delete task logs from the log directory as well as tasks and tasklogs table entries in metadata storage except for tasks created in the last `druid.indexer.logs.kill.durationToRetain` period. |false|
|`druid.indexer.logs.kill.durationToRetain`| Required if kill is enabled. In milliseconds, task logs and entries in task-related metadata storage tables to be retained created in last x milliseconds. |None|
|`druid.indexer.logs.kill.initialDelay`| Optional. Number of milliseconds after Overlord start when first auto kill is run. |random value less than 300000 (5 mins)|
|`druid.indexer.logs.kill.delay`|Optional. Number of milliseconds of delay between successive executions of auto kill run. |21600000 (6 hours)|

### API error response

You can configure Druid API error responses to hide internal information like the Druid class name, stack trace, thread name, servlet name, code, line/column number, host, or IP address.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.http.showDetailedJettyErrors`|When set to true, any error from the Jetty layer / Jetty filter includes the following fields  in the JSON response: `servlet`, `message`, `url`, `status`, and `cause`, if it exists. When set to false, the JSON response only includes `message`, `url`, and `status`. The field values remain unchanged.|true|
|`druid.server.http.errorResponseTransform.strategy`|Error response transform strategy. The strategy controls how Druid transforms error responses from Druid services. When unset or set to `none`, Druid leaves error responses unchanged.|`none`|

##### Error response transform strategy

You can use an error response transform strategy to transform error responses from within Druid services to hide internal information.
When you specify an error response transform strategy other than `none`, Druid transforms the error responses from Druid services as follows:
 - For any query API that fails in the Router service, Druid sets the fields `errorClass` and `host` to null. Druid applies the transformation strategy to the `errorMessage` field.
 - For any SQL query API that fails, for example `POST /druid/v2/sql/...`, Druid sets the fields `errorClass` and `host` to null. Druid applies the transformation strategy to the `errorMessage` field.
 - For any JDBC related exceptions, Druid will turn all checked exceptions into `QueryInterruptedException` otherwise druid will attempt to keep the exception as the same type. For example if the original exception isn't owned by Druid it will become `QueryInterruptedException`. Druid applies the transformation strategy to the `errorMessage` field.

###### No error response transform strategy

In this mode, Druid leaves error responses from underlying services unchanged and returns the unchanged errors to the API client.
This is the default Druid error response mode. To explicitly enable this strategy, set `druid.server.http.errorResponseTransform.strategy` to `none`.

###### Allowed regular expression error response transform strategy

In this mode, Druid validates the error responses from underlying services against a list of regular expressions. Only error messages that match a configured regular expression are returned. To enable this strategy, set `druid.server.http.errorResponseTransform.strategy` to `allowedRegex`.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.http.errorResponseTransform.allowedRegex`|The list of regular expressions Druid uses to validate error messages. If the error message matches any of the regular expressions, then Druid includes it in the response unchanged. If the error message does not match any of the regular expressions, Druid replaces the error message with null or with a default message depending on the type of underlying Exception. |`[]`|

For example, consider the following error response:

```
{"error":"Plan validation failed","errorMessage":"org.apache.calcite.runtime.CalciteContextException: From line 1, column 15 to line 1, column 38: Object 'nonexistent-datasource' not found","errorClass":"org.apache.calcite.tools.ValidationException","host":null}
```

If `druid.server.http.errorResponseTransform.allowedRegex` is set to `[]`, Druid transforms the query error response to the following:

```
{"error":"Plan validation failed","errorMessage":null,"errorClass":null,"host":null}
```

On the other hand, if `druid.server.http.errorResponseTransform.allowedRegex` is set to `[".*CalciteContextException.*"]` then Druid transforms the query error response to the following:

```
{"error":"Plan validation failed","errorMessage":"org.apache.calcite.runtime.CalciteContextException: From line 1, column 15 to line 1, column 38: Object 'nonexistent-datasource' not found","errorClass":null,"host":null}
```

### Overlord discovery

This config is used to find the [Overlord](../design/overlord.md) using Curator service discovery. Only required if you are actually running an Overlord.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.selectors.indexing.serviceName`|The druid.service name of the Overlord service. To start the Overlord with a different name, set it with this property. |druid/overlord|

### Coordinator discovery

This config is used to find the [Coordinator](../design/coordinator.md) using Curator service discovery. This config is used by the realtime indexing services to get information about the segments loaded in the cluster.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.selectors.coordinator.serviceName`|The druid.service name of the Coordinator service. To start the Coordinator with a different name, set it with this property. |druid/coordinator|

### Announcing segments

You can configure how to announce and unannounce Znodes in ZooKeeper (using Curator). For normal operations you do not need to override any of these configs.

##### Batch data segment announcer

In current Druid, multiple data segments may be announced under the same Znode.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.announcer.segmentsPerNode`|Each Znode contains info for up to this many segments.|50|
|`druid.announcer.maxBytesPerNode`|Max byte size for Znode. Allowed range is [1024, 1048576].|524288|
|`druid.announcer.skipDimensionsAndMetrics`|Skip Dimensions and Metrics list from segment announcements. NOTE: Enabling this will also remove the dimensions and metrics list from Coordinator and Broker endpoints.|false|
|`druid.announcer.skipLoadSpec`|Skip segment LoadSpec from segment announcements. NOTE: Enabling this will also remove the loadspec from Coordinator and Broker endpoints.|false|

If you want to turn off the batch data segment announcer, you can add a property to skip announcing segments. **You do not want to enable this config if you have any services using `batch` for `druid.serverview.type`**

|Property|Description|Default|
|--------|-----------|-------|
|`druid.announcer.skipSegmentAnnouncementOnZk`|Skip announcing segments to ZooKeeper. Note that the batch server view will not work if this is set to true.|false|

### JavaScript

Druid supports dynamic runtime extension through JavaScript functions. This functionality can be configured through
the following properties.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.javascript.enabled`|Set to "true" to enable JavaScript functionality. This affects the JavaScript parser, filter, extractionFn, aggregator, post-aggregator, router strategy, and worker selection strategy.|false|

:::info
 JavaScript-based functionality is disabled by default. Please refer to the Druid [JavaScript programming guide](../development/javascript.md) for guidelines about using Druid's JavaScript functionality, including instructions on how to enable it.
:::

### Double column storage

Prior to version 0.13.0, Druid's storage layer used a 32-bit float representation to store columns created by the
doubleSum, doubleMin, and doubleMax aggregators at indexing time.
Starting from version 0.13.0 the default will be 64-bit floats for Double columns.
Using 64-bit representation for double column will lead to avoid precision loss at the cost of doubling the storage size of such columns.
To keep the old format set the system-wide property `druid.indexing.doubleStorage=float`.
You can also use `floatSum`, `floatMin`, and `floatMax` to use 32-bit float representation.
Support for 64-bit floating point columns was released in Druid 0.11.0, so if you use this feature then older versions of Druid will not be able to read your data segments.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexing.doubleStorage`|Set to "float" to use 32-bit double representation for double columns.|double|

### SQL compatible null handling
These configurations are deprecated and will be removed in a future release at which point Druid will always have SQl compatible null handling.

Prior to version 0.13.0, Druid string columns treated `''` and `null` values as interchangeable, and numeric columns were unable to represent `null` values, coercing `null` to `0`. Druid 0.13.0 introduced a mode which enabled SQL compatible null handling, allowing string columns to distinguish empty strings from nulls, and numeric columns to contain null rows.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.generic.useDefaultValueForNull`|Set to `false` to store and query data in SQL compatible mode. This configuration has been deprecated and will be removed in a future release, taking on the `false` behavior. When set to `true` (deprecated legacy mode), `null` values will be stored as `''` for string columns and `0` for numeric columns.|`false`|
|`druid.generic.useThreeValueLogicForNativeFilters`|Set to `true` to use SQL compatible three-value logic when processing native Druid filters when `druid.generic.useDefaultValueForNull=false` and `druid.expressions.useStrictBooleans=true`. This configuration has been deprecated and will be removed in a future release, taking on the `true` behavior. When set to `false` Druid uses 2 value logic for filter processing, even when `druid.generic.useDefaultValueForNull=false` and `druid.expressions.useStrictBooleans=true`. See [boolean handling](../querying/sql-data-types.md#boolean-logic) for more details|`true`|
|`druid.generic.ignoreNullsForStringCardinality`|When set to `true`, `null` values will be ignored for the built-in cardinality aggregator over string columns. Set to `false` to include `null` values while estimating cardinality of only string columns using the built-in cardinality aggregator. This setting takes effect only when `druid.generic.useDefaultValueForNull` is set to `true` and is ignored in SQL compatibility mode. Additionally, empty strings (equivalent to null) are not counted when this is set to `true`. This configuration has been deprecated and will be removed in a future release since it has no effect when `druid.generic.useDefaultValueForNull=false`. |`false`|

This mode does have a storage size and query performance cost, see [segment documentation](../design/segments.md#handling-null-values) for more details.

### HTTP client

All Druid components can communicate with each other over HTTP.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.global.http.numConnections`|Size of connection pool per destination URL. If there are more HTTP requests than this number that all need to speak to the same URL, then they will queue up.|`20`|
|`druid.global.http.eagerInitialization`|Indicates that http connections should be eagerly initialized. If set to true, `numConnections` connections are created upon initialization|`false`|
|`druid.global.http.compressionCodec`|Compression codec to communicate with others. May be "gzip" or "identity".|`gzip`|
|`druid.global.http.readTimeout`|The timeout for data reads.|`PT15M`|
|`druid.global.http.unusedConnectionTimeout`|The timeout for idle connections in connection pool. The connection in the pool will be closed after this timeout and a new one will be established. This timeout should be less than `druid.global.http.readTimeout`. Set this timeout = ~90% of `druid.global.http.readTimeout`|`PT4M`|
|`druid.global.http.numMaxThreads`|Maximum number of I/O worker threads|`max(10, ((number of cores * 17) / 16 + 2) + 30)`|

### Common endpoints configuration

This section contains the configuration options for endpoints that are supported by all services.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.hiddenProperties`| If property names or substring of property names (case insensitive) is in this list, responses of the `/status/properties` endpoint do not show these properties | `["druid.s3.accessKey","druid.s3.secretKey","druid.metadata.storage.connector.password", "password", "key", "token", "pwd"]` |

## Master server

This section contains the configuration options for the services that reside on Master servers (Coordinators and Overlords) in the suggested [three-server configuration](../design/architecture.md#druid-servers).

### Coordinator

For general Coordinator services information, see [Coordinator service](../design/coordinator.md).

#### Static Configuration

These Coordinator static configurations can be defined in the `coordinator/runtime.properties` file.

##### Coordinator service config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current service. This is used to advertise the current service location as reachable from another service and should generally be specified such that `http://${druid.host}/` could actually talk to this service.|`InetAddress.getLocalHost().getCanonicalHostName()`|
|`druid.bindOnHost`|Indicating whether the service's internal jetty server bind on `druid.host`. Default is false, which means binding to all interfaces.|false|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8081|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.enableTlsPort](../operations/tls-support.md) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative integer.|8281|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services.|`druid/coordinator`|

##### Coordinator operation

|Property|Description|Default|
|--------|-----------|-------|
|`druid.coordinator.period`|The run period for the Coordinator. The Coordinator operates by maintaining the current state of the world in memory and periodically looking at the set of "used" segments and segments being served to make decisions about whether any changes need to be made to the data topology. This property sets the delay between each of these runs.|`PT60S`|
|`druid.coordinator.period.indexingPeriod`|How often to send compact/merge/conversion tasks to the indexing service. It's recommended to be longer than `druid.manager.segments.pollDuration`|`PT1800S` (30 mins)|
|`druid.coordinator.startDelay`|The operation of the Coordinator works on the assumption that it has an up-to-date view of the state of the world when it runs, the current ZooKeeper interaction code, however, is written in a way that doesn’t allow the Coordinator to know for a fact that it’s done loading the current state of the world. This delay is a hack to give it enough time to believe that it has all the data.|`PT300S`|
|`druid.coordinator.load.timeout`|The timeout duration for when the Coordinator assigns a segment to a Historical service.|`PT15M`|
|`druid.coordinator.kill.pendingSegments.on`|Boolean flag for whether or not the Coordinator clean up old entries in the `pendingSegments` table of metadata store. If set to true, Coordinator will check the created time of most recently complete task. If it doesn't exist, it finds the created time of the earliest running/pending/waiting tasks. Once the created time is found, then for all datasources not in the `killPendingSegmentsSkipList` (see [Dynamic configuration](#dynamic-configuration)), Coordinator will ask the Overlord to clean up the entries 1 day or more older than the found created time in the `pendingSegments` table. This will be done periodically based on `druid.coordinator.period.indexingPeriod` specified.|true|
|`druid.coordinator.kill.on`|Boolean flag for whether or not the Coordinator should submit kill task for unused segments, that is, permanently delete them from metadata store and deep storage. If set to true, then for all whitelisted datasources (or optionally all), Coordinator will submit tasks periodically based on `period` specified. A whitelist can be set via dynamic configuration `killDataSourceWhitelist` described later.<br /><br />When `druid.coordinator.kill.on` is true, segments are eligible for permanent deletion once their data intervals are older than `druid.coordinator.kill.durationToRetain` relative to the current time. If a segment's data interval is older than this threshold at the time it is marked unused, it is eligible for permanent deletion immediately after being marked unused.|false|
|`druid.coordinator.kill.period`| The frequency of sending kill tasks to the indexing service. The value must be greater than or equal to `druid.coordinator.period.indexingPeriod`. Only applies if kill is turned on.|P1D (1 day)|
|`druid.coordinator.kill.durationToRetain`|Only applies if you set `druid.coordinator.kill.on` to `true`. This value is ignored if `druid.coordinator.kill.ignoreDurationToRetain` is `true`. Valid configurations must be a ISO8601 period. Druid will not kill unused segments whose interval end date is beyond `now - durationToRetain`. `durationToRetain` can be a negative ISO8601 period, which would result in `now - durationToRetain` to be in the future.<br /><br />Note that the `durationToRetain` parameter applies to the segment interval, not the time that the segment was last marked unused. For example, if `durationToRetain` is set to `P90D`, then a segment for a time chunk 90 days in the past is eligible for permanent deletion immediately after being marked unused.|`P90D`|
|`druid.coordinator.kill.ignoreDurationToRetain`|A way to override `druid.coordinator.kill.durationToRetain` and tell the coordinator that you do not care about the end date of unused segment intervals when it comes to killing them. If true, the coordinator considers all unused segments as eligible to be killed.|false|
|`druid.coordinator.kill.bufferPeriod`|The amount of time that a segment must be unused before it is able to be permanently removed from metadata and deep storage. This can serve as a buffer period to prevent data loss if data ends up being needed after being marked unused.|`P30D`|
|`druid.coordinator.kill.maxSegments`|The number of unused segments to kill per kill task. This number must be greater than 0. This only applies when `druid.coordinator.kill.on=true`.|100|
|`druid.coordinator.balancer.strategy`|Specify the type of balancing strategy for the Coordinator to use to distribute segments among the Historical services. `cachingCost` is logically equivalent to `cost` but is more CPU-efficient on large clusters. `diskNormalized` weights the costs according to the servers' disk usage ratios - there are known issues with this strategy distributing segments unevenly across the cluster. `random` distributes segments among services randomly.|`cost`|
|`druid.coordinator.balancer.cachingCost.awaitInitialization`|Whether to wait for segment view initialization before creating the `cachingCost` balancing strategy. This property is enabled only when `druid.coordinator.balancer.strategy` is `cachingCost`. If set to true, the Coordinator will not start to assign segments, until the segment view is initialized. If set to false, the Coordinator will fallback to use the `cost` balancing strategy only if the segment view is not initialized yet. It may take much time to wait for the initialization since the `cachingCost` balancing strategy involves much computing to build itself.|false|
|`druid.coordinator.loadqueuepeon.repeatDelay`|The start and repeat delay for the `loadqueuepeon`, which manages the load and drop of segments.|`PT0.050S` (50 ms)|
|`druid.coordinator.asOverlord.enabled`|Boolean value for whether this Coordinator service should act like an Overlord as well. This configuration allows users to simplify a Druid cluster by not having to deploy any standalone Overlord services. If set to true, then Overlord console is available at `http://coordinator-host:port/console.html` and be sure to set `druid.coordinator.asOverlord.overlordService` also.|false|
|`druid.coordinator.asOverlord.overlordService`| Required, if `druid.coordinator.asOverlord.enabled` is `true`. This must be same value as `druid.service` on standalone Overlord services and `druid.selectors.indexing.serviceName` on Middle Managers.|NULL|
|`druid.centralizedDatasourceSchema.enabled`|Boolean flag for enabling datasource schema building on the Coordinator.|false|

##### Metadata management

|Property|Description|Required|Default|
|--------|-----------|---------|-------|
|`druid.coordinator.period.metadataStoreManagementPeriod`|How often to run metadata management tasks in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) duration format. |No | `PT1H`|
|`druid.coordinator.kill.supervisor.on`| Boolean value for whether to enable automatic deletion of terminated supervisors. If set to true, Coordinator will periodically remove terminated supervisors from the supervisor table in metadata storage.| No |true|
|`druid.coordinator.kill.supervisor.period`| How often to do automatic deletion of terminated supervisor in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) duration format. Value must be equal to or greater than  `druid.coordinator.period.metadataStoreManagementPeriod`. Only applies if `druid.coordinator.kill.supervisor.on` is set to true.| No| `P1D`|
|`druid.coordinator.kill.supervisor.durationToRetain`| Duration of terminated supervisor to be retained from created time in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) duration format. Only applies if `druid.coordinator.kill.supervisor.on` is set to true.| Yes if `druid.coordinator.kill.supervisor.on` is set to true.| `P90D`|
|`druid.coordinator.kill.audit.on`| Boolean value for whether to enable automatic deletion of audit logs. If set to true, Coordinator will periodically remove audit logs from the audit table entries in metadata storage.| No | True|
|`druid.coordinator.kill.audit.period`| How often to do automatic deletion of audit logs in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) duration format. Value must be equal to or greater than  `druid.coordinator.period.metadataStoreManagementPeriod`. Only applies if `druid.coordinator.kill.audit.on` is set to true.| No| `P1D`|
|`druid.coordinator.kill.audit.durationToRetain`| Duration of audit logs to be retained from created time in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) duration format. Only applies if `druid.coordinator.kill.audit.on` is set to true.| Yes if `druid.coordinator.kill.audit.on` is set to true.| `P90D`|
|`druid.coordinator.kill.compaction.on`| Boolean value for whether to enable automatic deletion of compaction configurations. If set to true, Coordinator will periodically remove compaction configuration of inactive datasource (datasource with no used and unused segments) from the config table in metadata storage.  | No | False|
|`druid.coordinator.kill.compaction.period`| How often to do automatic deletion of compaction configurations in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) duration format. Value must be equal to or greater than  `druid.coordinator.period.metadataStoreManagementPeriod`. Only applies if `druid.coordinator.kill.compaction.on` is set to true.| No| `P1D`|
|`druid.coordinator.kill.rule.on`| Boolean value for whether to enable automatic deletion of rules. If set to true, Coordinator will periodically remove rules of inactive datasource (datasource with no used and unused segments) from the rule table in metadata storage.| No | True|
|`druid.coordinator.kill.rule.period`| How often to do automatic deletion of rules in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) duration format. Value must be equal to or greater than  `druid.coordinator.period.metadataStoreManagementPeriod`. Only applies if `druid.coordinator.kill.rule.on` is set to true.| No| `P1D`|
|`druid.coordinator.kill.rule.durationToRetain`| Duration of rules to be retained from created time in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) duration format. Only applies if `druid.coordinator.kill.rule.on` is set to true.| Yes if `druid.coordinator.kill.rule.on` is set to true.| `P90D`|
|`druid.coordinator.kill.datasource.on`| Boolean value for whether to enable automatic deletion of datasource metadata (Note: datasource metadata only exists for datasource created from supervisor). If set to true, Coordinator will periodically remove datasource metadata of terminated supervisor from the datasource table in metadata storage.  | No | True|
|`druid.coordinator.kill.datasource.period`| How often to do automatic deletion of datasource metadata in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) duration format. Value must be equal to or greater than  `druid.coordinator.period.metadataStoreManagementPeriod`. Only applies if `druid.coordinator.kill.datasource.on` is set to true.| No| `P1D`|
|`druid.coordinator.kill.datasource.durationToRetain`| Duration of datasource metadata to be retained from created time in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) duration format. Only applies if `druid.coordinator.kill.datasource.on` is set to true.| Yes if `druid.coordinator.kill.datasource.on` is set to true.| `P90D`|

##### Segment management

|Property|Possible values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.serverview.type`|batch or http|Segment discovery method to use. "http" enables discovering segments using HTTP instead of ZooKeeper.|http|
|`druid.coordinator.loadqueuepeon.type`|curator or http|Implementation to use to assign segment loads and drops to historicals. Curator-based implementation is now deprecated, so you should transition to using HTTP-based segment assignments.|http|
|`druid.coordinator.segment.awaitInitializationOnStart`|true or false|Whether the Coordinator will wait for its view of segments to fully initialize before starting up. If set to 'true', the Coordinator's HTTP server will not start up, and the Coordinator will not announce itself as available, until the server view is initialized.|true|

###### Additional config when "http" loadqueuepeon is used

|Property|Description|Default|
|--------|-----------|-------|
|`druid.coordinator.loadqueuepeon.http.batchSize`|Number of segment load/drop requests to batch in one HTTP request. Note that it must be smaller than `druid.segmentCache.numLoadingThreads` config on Historical service.|1|

##### Metadata retrieval

|Property|Description|Default|
|--------|-----------|-------|
|`druid.manager.config.pollDuration`|How often the manager polls the config table for updates.|`PT1M`|
|`druid.manager.segments.pollDuration`|The duration between polls the Coordinator does for updates to the set of active segments. Generally defines the amount of lag time it can take for the Coordinator to notice new segments.|`PT1M`|
|`druid.manager.rules.pollDuration`|The duration between polls the Coordinator does for updates to the set of active rules. Generally defines the amount of lag time it can take for the Coordinator to notice rules.|`PT1M`|
|`druid.manager.rules.defaultRule`|The default rule for the cluster|`_default`|
|`druid.manager.rules.alertThreshold`|The duration after a failed poll upon which an alert should be emitted.|`PT10M`|

#### Dynamic configuration

The Coordinator has dynamic configurations to tune certain behavior on the fly, without requiring a service restart.
You can configure these parameters using the [web console](../operations/web-console.md)(recommended) or through the [Coordinator dynamic configuration API](../api-reference/dynamic-configuration-api.md#coordinator-dynamic-configuration).

The following table shows the dynamic configuration properties for the Coordinator.

|Property|Description|Default|
|--------|-----------|-------|
|`millisToWaitBeforeDeleting`|How long does the Coordinator need to be a leader before it can start marking overshadowed segments as unused in metadata storage.| 900000 (15 mins)|
|`mergeBytesLimit`|The maximum total uncompressed size in bytes of segments to merge.|524288000L|
|`mergeSegmentsLimit`|The maximum number of segments that can be in a single [append task](../ingestion/tasks.md).|100|
|`smartSegmentLoading`|Enables ["smart" segment loading mode](#smart-segment-loading) which dynamically computes the optimal values of several properties that maximize Coordinator performance.|true|
|`maxSegmentsToMove`|The maximum number of segments that can be moved in a Historical tier at any given time.|100|
|`replicantLifetime`|The maximum number of Coordinator runs for which a segment can wait in the load queue of a Historical before Druid raises an alert.|15|
|`replicationThrottleLimit`|The maximum number of segment replicas that can be assigned to a historical tier in a single Coordinator run. This property prevents Historical services from becoming overwhelmed when loading extra replicas of segments that are already available in the cluster.|500|
|`balancerComputeThreads`|Thread pool size for computing moving cost of segments during segment balancing. Consider increasing this if you have a lot of segments and moving segments begins to stall.|`num_cores` / 2|
|`killDataSourceWhitelist`|List of specific data sources for which kill tasks are sent if property `druid.coordinator.kill.on` is true. This can be a list of comma-separated data source names or a JSON array.|none|
|`killTaskSlotRatio`|Ratio of total available task slots, including autoscaling if applicable that will be allowed for kill tasks. This limit only applies for kill tasks that are spawned automatically by the coordinator's auto kill duty, which is enabled when `druid.coordinator.kill.on` is true.| 1 - all task slots can be used|
|`maxKillTaskSlots`|Maximum number of tasks that will be allowed for kill tasks. This limit only applies for kill tasks that are spawned automatically by the coordinator's auto kill duty, which is enabled when `druid.coordinator.kill.on` is true.|`Integer.MAX_VALUE` - no limit|
|`killPendingSegmentsSkipList`|List of data sources for which pendingSegments are _NOT_ cleaned up if property `druid.coordinator.kill.pendingSegments.on` is true. This can be a list of comma-separated data sources or a JSON array.|none|
|`maxSegmentsInNodeLoadingQueue`|The maximum number of segments allowed in the load queue of any given server. Use this parameter to load segments faster if, for example, the cluster contains slow-loading nodes or if there are too many segments to be replicated to a particular node (when faster loading is preferred to better segments distribution). The optimal value depends on the loading speed of segments, acceptable replication time and number of nodes.|500|
|`useRoundRobinSegmentAssignment`|Boolean flag for whether segments should be assigned to Historical services in a round robin fashion. When disabled, segment assignment is done using the chosen balancer strategy. When enabled, this can speed up segment assignments leaving balancing to move the segments to their optimal locations (based on the balancer strategy) lazily.|true|
|`decommissioningNodes`|List of Historical servers to decommission. Coordinator will not assign new segments to decommissioning servers, and segments will be moved away from them to be placed on non-decommissioning servers at the maximum rate specified by `maxSegmentsToMove`.|none|
|`pauseCoordination`|Boolean flag for whether or not the Coordinator should execute its various duties of coordinating the cluster. Setting this to true essentially pauses all coordination work while allowing the API to remain up. Duties that are paused include all classes that implement the `CoordinatorDuty` interface. Such duties include: segment balancing, segment compaction, submitting kill tasks for unused segments (if enabled), logging of used segments in the cluster, marking of newly unused or overshadowed segments, matching and execution of load/drop rules for used segments, unloading segments that are no longer marked as used from Historical servers. An example of when an admin may want to pause coordination would be if they are doing deep storage maintenance on HDFS name nodes with downtime and don't want the Coordinator to be directing Historical nodes to hit the name node with API requests until maintenance is done and the deep store is declared healthy for use again.|false|
|`replicateAfterLoadTimeout`|Boolean flag for whether or not additional replication is needed for segments that have failed to load due to the expiry of `druid.coordinator.load.timeout`. If this is set to true, the Coordinator will attempt to replicate the failed segment on a different historical server. This helps improve the segment availability if there are a few slow Historicals in the cluster. However, the slow Historical may still load the segment later and the Coordinator may issue drop requests if the segment is over-replicated.|false|

##### Smart segment loading

The `smartSegmentLoading` mode simplifies Coordinator configuration for segment loading and balancing.
If you enable this mode, do not provide values for the properties in the table below as the Coordinator computes them automatically.
Druid computes the values to optimize Coordinator performance, based on the current state of the cluster.

If you enable `smartSegmentLoading` mode, Druid ignores any value you provide for the following properties.

|Property|Computed value|Description|
|--------|--------------|-----------|
|`useRoundRobinSegmentAssignment`|true|Speeds up segment assignment.|
|`maxSegmentsInNodeLoadingQueue`|0|Removes the limit on load queue size.|
|`replicationThrottleLimit`|5% of used segments, minimum value 100|Prevents aggressive replication when a Historical disappears only intermittently.|
|`replicantLifetime`|60|Allows segments to wait about an hour (assuming a Coordinator period of 1 minute) in the load queue before an alert is raised. In `smartSegmentLoading` mode, load queues are not limited by size. Segments might therefore assigned to a load queue even if the corresponding server is slow to load them.|
|`maxSegmentsToMove`|2% of used segments, minimum value 100, maximum value 1000|Ensures that some segments are always moving in the cluster to keep it well balanced. The maximum value keeps the Coordinator run times bounded.|
|`balancerComputeThreads`|`num_cores` / 2|Ensures that there are enough threads to perform balancing computations without hogging all Coordinator resources.|

When `smartSegmentLoading` is disabled, Druid uses the configured values of these properties.
Disable `smartSegmentLoading` only if you want to explicitly set the values of any of the above properties.

##### Lookups dynamic configuration

These configuration options control Coordinator lookup management. For configurations that affect lookup propagation, see [Dynamic configuration for lookups](../querying/lookups.md#dynamic-configuration).

|Property|Description|Default|
|--------|-----------|-------|
|`druid.manager.lookups.hostDeleteTimeout`|How long to wait for a `DELETE` request to a particular service before considering the `DELETE` a failure.|`PT1S`|
|`druid.manager.lookups.hostUpdateTimeout`|How long to wait for a `POST` request to a particular service before considering the `POST` a failure.|`PT10S`|
|`druid.manager.lookups.deleteAllTimeout`|How long to wait for all `DELETE` requests to finish before considering the delete attempt a failure.|`PT10S`|
|`druid.manager.lookups.updateAllTimeout`|How long to wait for all `POST` requests to finish before considering the attempt a failure.|`PT60S`|
|`druid.manager.lookups.threadPoolSize`|How many services can be managed concurrently (concurrent `POST` and `DELETE` requests). Requests this limit will wait in a queue until a slot becomes available.|10|
|`druid.manager.lookups.period`|Number of milliseconds between checks for configuration changes.|120000 (2 minutes)|

##### Automatic compaction dynamic configuration

You can set or update [automatic compaction](../data-management/automatic-compaction.md) properties dynamically using the
[Automatic compaction API](../api-reference/automatic-compaction-api.md) without restarting Coordinators.

For details about segment compaction, see [Segment size optimization](../operations/segment-optimization.md).

You can configure automatic compaction through the following properties:

|Property|Description|Required|
|--------|-----------|--------|
|`dataSource`|The datasource name to be compacted.|yes|
|`taskPriority`|[Priority](../ingestion/tasks.md#priority) of compaction task.|no (default = 25)|
|`inputSegmentSizeBytes`|Maximum number of total segment bytes processed per compaction task. Since a time chunk must be processed in its entirety, if the segments for a particular time chunk have a total size in bytes greater than this parameter, compaction will not run for that time chunk.|no (default = 100,000,000,000,000 i.e. 100TB)|
|`skipOffsetFromLatest`|The offset for searching segments to be compacted in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) duration format. Strongly recommended to set for realtime datasources. See [Data handling with compaction](../data-management/compaction.md#data-handling-with-compaction).|no (default = "P1D")|
|`tuningConfig`|Tuning config for compaction tasks. See below [Automatic compaction tuningConfig](#automatic-compaction-tuningconfig).|no|
|`taskContext`|[Task context](../ingestion/tasks.md#context) for compaction tasks.|no|
|`granularitySpec`|Custom `granularitySpec`. See [Automatic compaction granularitySpec](#automatic-compaction-granularityspec).|no|
|`dimensionsSpec`|Custom `dimensionsSpec`. See [Automatic compaction dimensionsSpec](#automatic-compaction-dimensionsspec).|no|
|`transformSpec`|Custom `transformSpec`. See [Automatic compaction transformSpec](#automatic-compaction-transformspec).|no|
|`metricsSpec`|Custom [`metricsSpec`](../ingestion/ingestion-spec.md#metricsspec). The compaction task preserves any existing metrics regardless of whether `metricsSpec` is specified. If `metricsSpec` is specified, Druid does not reapply any aggregators matching the metric names specified in `metricsSpec` to rows that already have the associated metrics. For rows that do not already have the metric specified in `metricsSpec`, Druid applies the metric aggregator on the source column, then proceeds to combine the metrics across segments as usual. If `metricsSpec` is not specified, Druid automatically discovers the metrics in the existing segments and combines existing metrics with the same metric name across segments. Aggregators for metrics with the same name are assumed to be compatible for combining across segments, otherwise the compaction task may fail.|no|
|`ioConfig`|IO config for compaction tasks. See [Automatic compaction ioConfig](#automatic-compaction-ioconfig).|no|

Automatic compaction config example:

```json
{
  "dataSource": "wikiticker",
  "granularitySpec" : {
    "segmentGranularity" : "none"
  }
}
```

Compaction tasks fail when higher priority tasks cause Druid to revoke their locks. By default, realtime tasks like ingestion have a higher priority than compaction tasks. Frequent conflicts between compaction tasks and realtime tasks can cause the Coordinator's automatic compaction to hang.
You may see this issue with streaming ingestion from Kafka and Kinesis, which ingest late-arriving data.

To mitigate this problem, set `skipOffsetFromLatest` to a value large enough so that arriving data tends to fall outside the offset value from the current time. This way you can avoid conflicts between compaction tasks and realtime ingestion tasks.
For example, if you want to skip over segments from thirty days prior to the end time of the most recent segment, assign `"skipOffsetFromLatest": "P30D"`.
For more information, see [Avoid conflicts with ingestion](../data-management/automatic-compaction.md#avoid-conflicts-with-ingestion).

###### Automatic compaction tuningConfig

Auto-compaction supports a subset of the [tuningConfig for Parallel task](../ingestion/native-batch.md#tuningconfig).

The following table shows the supported configurations for auto-compaction.

|Property|Description|Required|
|--------|-----------|--------|
|type|The task type, this should always be `index_parallel`.|yes|
|`maxRowsInMemory`|Used in determining when intermediate persists to disk should occur. Normally user does not need to set this, but depending on the nature of data, if rows are short in terms of bytes, user may not want to store a million rows in memory and this value should be set.|no (default = 1000000)|
|`maxBytesInMemory`|Used in determining when intermediate persists to disk should occur. Normally this is computed internally and user does not need to set it. This value represents number of bytes to aggregate in heap memory before persisting. This is based on a rough estimate of memory usage and not actual usage. The maximum heap memory usage for indexing is `maxBytesInMemory` * (2 + `maxPendingPersists`)|no (default = 1/6 of max JVM memory)|
|`splitHintSpec`|Used to give a hint to control the amount of data that each first phase task reads. This hint could be ignored depending on the implementation of the input source. See [Split hint spec](../ingestion/native-batch.md#split-hint-spec) for more details.|no (default = size-based split hint spec)|
|`partitionsSpec`|Defines how to partition data in each time chunk, see [`PartitionsSpec`](../ingestion/native-batch.md#partitionsspec)|no (default = `dynamic`)|
|`indexSpec`|Defines segment storage format options to be used at indexing time, see [IndexSpec](../ingestion/ingestion-spec.md#indexspec)|no|
|`indexSpecForIntermediatePersists`|Defines segment storage format options to be used at indexing time for intermediate persisted temporary segments. this can be used to disable dimension/metric compression on intermediate segments to reduce memory required for final merging. however, disabling compression on intermediate segments might increase page cache use while they are used before getting merged into final segment published, see [IndexSpec](../ingestion/ingestion-spec.md#indexspec) for possible values.|no|
|`maxPendingPersists`|Maximum number of persists that can be pending but not started. If this limit would be exceeded by a new intermediate persist, ingestion will block until the currently-running persist finishes. Maximum heap memory usage for indexing scales with `maxRowsInMemory` * (2 + `maxPendingPersists`).|no (default = 0, meaning one persist can be running concurrently with ingestion, and none can be queued up)|
|`pushTimeout`|Milliseconds to wait for pushing segments. It must be >= 0, where 0 means to wait forever.|no (default = 0)|
|`segmentWriteOutMediumFactory`|Segment write-out medium to use when creating segments. See [SegmentWriteOutMediumFactory](../ingestion/native-batch.md#segmentwriteoutmediumfactory).|no (default is the value from `druid.peon.defaultSegmentWriteOutMediumFactory.type` is used)|
|`maxNumConcurrentSubTasks`|Maximum number of worker tasks which can be run in parallel at the same time. The supervisor task would spawn worker tasks up to `maxNumConcurrentSubTasks` regardless of the current available task slots. If this value is set to 1, the Supervisor task processes data ingestion on its own instead of spawning worker tasks. If this value is set to too large, too many worker tasks can be created which might block other ingestion. Check [Capacity Planning](../ingestion/native-batch.md#capacity-planning) for more details.|no (default = 1)|
|`maxRetry`|Maximum number of retries on task failures.|no (default = 3)|
|`maxNumSegmentsToMerge`|Max limit for the number of segments that a single task can merge at the same time in the second phase. Used only with `hashed` or `single_dim` partitionsSpec.|no (default = 100)|
|`totalNumMergeTasks`|Total number of tasks to merge segments in the merge phase when `partitionsSpec` is set to `hashed` or `single_dim`.|no (default = 10)|
|`taskStatusCheckPeriodMs`|Polling period in milliseconds to check running task statuses.|no (default = 1000)|
|`chatHandlerTimeout`|Timeout for reporting the pushed segments in worker tasks.|no (default = PT10S)|
|`chatHandlerNumRetries`|Retries for reporting the pushed segments in worker tasks.|no (default = 5)|

###### Automatic compaction granularitySpec

|Field|Description|Required|
|-----|-----------|--------|
|`segmentGranularity`|Time chunking period for the segment granularity. Defaults to 'null', which preserves the original segment granularity. Accepts all [Query granularity](../querying/granularities.md) values.|No|
|`queryGranularity`|The resolution of timestamp storage within each segment. Defaults to 'null', which preserves the original query granularity. Accepts all [Query granularity](../querying/granularities.md) values.|No|
|`rollup`|Whether to enable ingestion-time rollup or not. Defaults to null, which preserves the original setting. Note that once data is rollup, individual records can no longer be recovered. |No|

###### Automatic compaction dimensionsSpec

|Field|Description|Required|
|-----|-----------|--------|
|`dimensions`| A list of dimension names or objects. Defaults to null, which preserves the original dimensions. Note that setting this will cause segments manually compacted with `dimensionExclusions` to be compacted again.|No|

###### Automatic compaction transformSpec

|Field|Description|Required|
|-----|-----------|--------|
|`filter`| Conditionally filters input rows during compaction. Only rows that pass the filter will be included in the compacted segments. Any of Druid's standard [query filters](../querying/filters.md) can be used. Defaults to null, which will not filter any row. |No|

###### Automatic compaction ioConfig

Auto-compaction supports a subset of the [ioConfig for Parallel task](../ingestion/native-batch.md).
The below is a list of the supported configurations for auto-compaction.

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`dropExisting`|If `true` the compaction task replaces all existing segments fully contained by the umbrella interval of the compacted segments when the task publishes new segments and tombstones. If compaction fails, Druid does not publish any segments or tombstones. WARNING: this functionality is still in beta. Note that changing this config does not cause intervals to be compacted again.|false|no|

### Overlord

For general Overlord service information, see [Overlord](../design/overlord.md).

#### Overlord static configuration

These Overlord static configurations can be defined in the `overlord/runtime.properties` file.

##### Overlord service configs

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current service. This is used to advertise the current service location as reachable from another service and should generally be specified such that `http://${druid.host}/` could actually talk to this service.|`InetAddress.getLocalHost().getCanonicalHostName()`|
|`druid.bindOnHost`|Indicating whether the service's internal jetty server bind on `druid.host`. Default is false, which means binding to all interfaces.|false|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`.|8090|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.enableTlsPort](../operations/tls-support.md) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative Integer.|8290|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services.|`druid/overlord`|

##### Overlord operations

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.runner.type`|Indicates whether tasks should be run locally using `local` or in a distributed environment using `remote`. The recommended option is `httpRemote`, which is similar to `remote` but uses HTTP to interact with Middle Managers instead of ZooKeeper.|`httpRemote`|
|`druid.indexer.storage.type`|Indicates whether incoming tasks should be stored locally (in heap) or in metadata storage. One of `local` or `metadata`. `local` is mainly for internal testing while `metadata` is recommended in production because storing incoming tasks in metadata storage allows for tasks to be resumed if the Overlord should fail.|`local`|
|`druid.indexer.storage.recentlyFinishedThreshold`|Duration of time to store task results. Default is 24 hours. If you have hundreds of tasks running in a day, consider increasing this threshold.|`PT24H`|
|`druid.indexer.tasklock.forceTimeChunkLock`|_**Setting this to false is still experimental**_<br/> If set, all tasks are enforced to use time chunk lock. If not set, each task automatically chooses a lock type to use. This configuration can be overwritten by setting `forceTimeChunkLock` in the [task context](../ingestion/tasks.md#context). See [Task Locking & Priority](../ingestion/tasks.md#context) for more details about locking in tasks.|true|
|`druid.indexer.tasklock.batchSegmentAllocation`| If set to true, Druid performs segment allocate actions in batches to improve throughput and reduce the average `task/action/run/time`. See [batching `segmentAllocate` actions](../ingestion/tasks.md#batching-segmentallocate-actions) for details.|true|
|`druid.indexer.tasklock.batchAllocationWaitTime`|Number of milliseconds after Druid adds the first segment allocate action to a batch, until it executes the batch. Allows the batch to add more requests and improve the average segment allocation run time. This configuration takes effect only if `batchSegmentAllocation` is enabled.|500|
|`druid.indexer.task.default.context`|Default task context that is applied to all tasks submitted to the Overlord. Any default in this config does not override neither the context values the user provides nor `druid.indexer.tasklock.forceTimeChunkLock`.|empty context|
|`druid.indexer.queue.maxSize`|Maximum number of active tasks at one time.|`Integer.MAX_VALUE`|
|`druid.indexer.queue.startDelay`|Sleep this long before starting Overlord queue management. This can be useful to give a cluster time to re-orient itself (for example, after a widespread network issue).|`PT1M`|
|`druid.indexer.queue.restartDelay`|Sleep this long when Overlord queue management throws an exception before trying again.|`PT30S`|
|`druid.indexer.queue.storageSyncRate`|Sync Overlord state this often with an underlying task persistence mechanism.|`PT1M`|

The following configs only apply if the Overlord is running in remote mode. For a description of local vs. remote mode, see [Overlord service](../design/overlord.md).

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.runner.taskAssignmentTimeout`|How long to wait after a task has been assigned to a MiddleManager before throwing an error.|`PT5M`|
|`druid.indexer.runner.minWorkerVersion`|The minimum MiddleManager version to send tasks to. The version number is a string. This affects the expected behavior during certain operations like comparison against `druid.worker.version`. Specifically, the version comparison follows dictionary order. Use ISO8601 date format for the version to accommodate date comparisons. |"0"|
| `druid.indexer.runner.parallelIndexTaskSlotRatio`| The ratio of task slots available for parallel indexing supervisor tasks per worker. The specified value must be in the range `[0, 1]`. |1|
|`druid.indexer.runner.compressZnodes`|Indicates whether or not the Overlord should expect MiddleManagers to compress Znodes.|true|
|`druid.indexer.runner.maxZnodeBytes`|The maximum size Znode in bytes that can be created in ZooKeeper, should be in the range of `[10KiB, 2GiB)`. [Human-readable format](human-readable-byte.md) is supported.| 512 KiB |
|`druid.indexer.runner.taskCleanupTimeout`|How long to wait before failing a task after a MiddleManager is disconnected from ZooKeeper.|`PT15M`|
|`druid.indexer.runner.taskShutdownLinkTimeout`|How long to wait on a shutdown request to a MiddleManager before timing out|`PT1M`|
|`druid.indexer.runner.pendingTasksRunnerNumThreads`|Number of threads to allocate pending-tasks to workers, must be at least 1.|1|
|`druid.indexer.runner.maxRetriesBeforeBlacklist`|Number of consecutive times the MiddleManager can fail tasks,  before the worker is blacklisted, must be at least 1|5|
|`druid.indexer.runner.workerBlackListBackoffTime`|How long to wait before a task is whitelisted again. This value should be greater that the value set for taskBlackListCleanupPeriod.|`PT15M`|
|`druid.indexer.runner.workerBlackListCleanupPeriod`|A duration after which the cleanup thread will startup to clean blacklisted workers.|`PT5M`|
|`druid.indexer.runner.maxPercentageBlacklistWorkers`|The maximum percentage of workers to blacklist, this must be between 0 and 100.|20|

If autoscaling is enabled, you can set these additional configs:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.autoscale.strategy`|Sets the strategy to run when autoscaling is required. One of `noop`, `ec2` or `gce`.|`noop`|
|`druid.indexer.autoscale.doAutoscale`|If set to true, autoscaling will be enabled.|false|
|`druid.indexer.autoscale.provisionPeriod`|How often to check whether or not new MiddleManagers should be added.|`PT1M`|
|`druid.indexer.autoscale.terminatePeriod`|How often to check when MiddleManagers should be removed.|`PT5M`|
|`druid.indexer.autoscale.originTime`|The starting reference timestamp that the terminate period increments upon.|`2012-01-01T00:55:00.000Z`|
|`druid.indexer.autoscale.workerIdleTimeout`|How long can a worker be idle (not a run task) before it can be considered for termination.|`PT90M`|
|`druid.indexer.autoscale.maxScalingDuration`|How long the Overlord will wait around for a MiddleManager to show up before giving up.|`PT15M`|
|`druid.indexer.autoscale.numEventsToTrack`|The number of autoscaling related events (node creation and termination) to track.|10|
|`druid.indexer.autoscale.pendingTaskTimeout`|How long a task can be in "pending" state before the Overlord tries to scale up.|`PT30S`|
|`druid.indexer.autoscale.workerVersion`|If set, will only create nodes of set version during autoscaling. Overrides dynamic configuration. |null|
|`druid.indexer.autoscale.workerPort`|The port that MiddleManagers will run on.|8080|
|`druid.indexer.autoscale.workerCapacityHint`| An estimation of the number of task slots available for each worker launched by the auto scaler when there are no workers running. The auto scaler uses the worker capacity hint to launch workers with an adequate capacity to handle pending tasks. When unset or set to a value less than or equal to 0, the auto scaler scales workers equal to the value for `minNumWorkers` in autoScaler config instead. The auto scaler assumes that each worker, either a MiddleManager or indexer, has the same amount of task slots. Therefore, when all your workers have the same capacity (homogeneous capacity), set the value for `autoscale.workerCapacityHint` equal to `druid.worker.capacity`. If your workers have different capacities (heterogeneous capacity), set the value to the average of `druid.worker.capacity` across the workers. For example, if two workers have `druid.worker.capacity=10`, and one has `druid.worker.capacity=4`, set `autoscale.workerCapacityHint=8`. Only applies to `pendingTaskBased` provisioning strategy.|-1|

##### Supervisors

|Property|Description|Default|
|--------|-----------|-------|
|`druid.supervisor.healthinessThreshold`|The number of successful runs before an unhealthy supervisor is again considered healthy.|3|
|`druid.supervisor.unhealthinessThreshold`|The number of failed runs before the supervisor is considered unhealthy.|3|
|`druid.supervisor.taskHealthinessThreshold`|The number of consecutive task successes before an unhealthy supervisor is again considered healthy.|3|
|`druid.supervisor.taskUnhealthinessThreshold`|The number of consecutive task failures before the supervisor is considered unhealthy.|3|
|`druid.supervisor.storeStackTrace`|Whether full stack traces of supervisor exceptions should be stored and returned by the supervisor `/status` endpoint.|false|
|`druid.supervisor.maxStoredExceptionEvents`|The maximum number of exception events that can be returned through the supervisor `/status` endpoint.|`max(healthinessThreshold, unhealthinessThreshold)`|
|`druid.supervisor.idleConfig.enabled`|If `true`, supervisor can become idle if there is no data on input stream/topic for some time.|false|
|`druid.supervisor.idleConfig.inactiveAfterMillis`|Supervisor is marked as idle if all existing data has been read from input topic and no new data has been published for `inactiveAfterMillis` milliseconds.|`600_000`|

The `druid.supervisor.idleConfig.*` specification in the Overlord runtime properties defines the default behavior for the entire cluster. See [Idle Configuration in Kafka Supervisor IOConfig](../ingestion/kinesis-ingestion.md#io-configuration) to override it for an individual supervisor.

#### Overlord dynamic configuration

The Overlord has dynamic configurations to tune how Druid assigns tasks to workers.
You can configure these parameters using the [web console](../operations/web-console.md) or through the [Overlord dynamic configuration API](../api-reference/dynamic-configuration-api.md#overlord-dynamic-configuration).

The following table shows the dynamic configuration properties for the Overlord.

|Property|Description|Default|
|--------|-----------|-------|
|`selectStrategy`| Describes how to assign tasks to MiddleManagers. The type can be `equalDistribution`, `equalDistributionWithCategorySpec`, `fillCapacity`, `fillCapacityWithCategorySpec`, and `javascript`. | `{"type":"equalDistribution"}` |
|`autoScaler`| Only used if [autoscaling](#autoscaler) is enabled.| null |

The following is an example of an Overlord dynamic config:

<details><summary>Click to view the example</summary>

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

</details>

##### Worker select strategy

The select strategy controls how Druid assigns tasks to workers (MiddleManagers).
At a high level, the select strategy determines the list of eligible workers for a given task using
either an `affinityConfig` or a `categorySpec`. Then, Druid assigns the task by either trying to distribute load equally
(`equalDistribution`) or to fill as many workers as possible to capacity (`fillCapacity`).
There are 4 options for select strategies:

- [`equalDistribution`](#equaldistribution)
- [`equalDistributionWithCategorySpec`](#equaldistributionwithcategoryspec)
- [`fillCapacity`](#fillcapacity)
- [`fillCapacityWithCategorySpec`](#fillcapacitywithcategoryspec)

A `javascript` option is also available but should only be used for prototyping new strategies.

If an `affinityConfig` is provided (as part of `fillCapacity` and `equalDistribution` strategies) for a given task, the list of workers eligible to be assigned is determined as follows:

- a non-affinity worker if no affinity is specified for that datasource. Any worker not listed in the `affinityConfig` is considered a non-affinity worker.
- a non-affinity worker if preferred workers are not available and the affinity is _weak_ i.e. `strong: false`.
- a preferred worker listed in the `affinityConfig` for this datasource if it has available capacity
- no worker if preferred workers are not available and affinity is _strong_ i.e. `strong: true`. In this case, the task remains in "pending" state. The chosen provisioning strategy (e.g. `pendingTaskBased`) may then use the total number of pending tasks to determine if a new node should be provisioned.

Note that every worker listed in the `affinityConfig` will only be used for the assigned datasources and no other.

If a `categorySpec` is provided (as part of `fillCapacityWithCategorySpec` and `equalDistributionWithCategorySpec` strategies), then a task of a given datasource may be assigned to:

- any worker if no category config is given for task type
- any worker if category config is given for task type but no category is given for datasource and there's no default category
- a preferred worker (based on category config and category for datasource) if available
- any worker if category config and category are given but no preferred worker is available and category config is `weak`
- not assigned at all if preferred workers are not available and category config is `strong`

In both the cases, Druid determines the list of eligible workers and selects one depending on their load with the goal of either distributing the load equally or filling as few workers as possible.

If you are using auto-scaling, use the `fillCapacity` select strategy since auto-scaled nodes can
not be assigned a category, and you want the work to be concentrated on the fewest number of workers to allow the empty ones to scale down.

###### `equalDistribution`

Tasks are assigned to the MiddleManager with the most free slots at the time the task begins running.
This evenly distributes work across your MiddleManagers.

|Property|Description|Default|
|--------|-----------|-------|
|`type`|`equalDistribution`|required; must be `equalDistribution`|
|`affinityConfig`|[`AffinityConfig`](#affinityconfig) object|null (no affinity)|

###### `equalDistributionWithCategorySpec`

This strategy is a variant of `equalDistribution`, which supports `workerCategorySpec` field rather than `affinityConfig`.
By specifying `workerCategorySpec`, you can assign tasks to run on different categories of MiddleManagers based on the **type** and **dataSource** of the task.
This strategy doesn't work with `AutoScaler` since the behavior is undefined.

|Property|Description|Default|
|--------|-----------|-------|
|`type`|`equalDistributionWithCategorySpec`|required; must be `equalDistributionWithCategorySpec`|
|`workerCategorySpec`|[`WorkerCategorySpec`](#workercategoryspec) object|null (no worker category spec)|

The following example shows tasks of type `index_kafka` that default to running on MiddleManagers of category `c1`, except for tasks that write to datasource `ds1`, which run on MiddleManagers of category `c2`.

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

###### `fillCapacity`

Tasks are assigned to the worker with the most currently-running tasks. This is
useful when you are auto-scaling MiddleManagers since it tends to pack some full and
leave others empty. The empty ones can be safely terminated.

Note that if `druid.indexer.runner.pendingTasksRunnerNumThreads` is set to _N_ > 1, then this strategy will fill _N_
MiddleManagers up to capacity simultaneously, rather than a single MiddleManager.

|Property|Description|Default|
|--------|-----------|-------|
|`type`| `fillCapacity`|required; must be `fillCapacity`|
|`affinityConfig`| [`AffinityConfig`](#affinityconfig) object |null (no affinity)|

###### `fillCapacityWithCategorySpec`

This strategy is a variant of `fillCapacity`, which supports `workerCategorySpec` instead of an `affinityConfig`.
The usage is the same as `equalDistributionWithCategorySpec` strategy.
This strategy doesn't work with `AutoScaler` since the behavior is undefined.

|Property|Description|Default|
|--------|-----------|-------|
|`type`|`fillCapacityWithCategorySpec`.|required; must be `fillCapacityWithCategorySpec`|
|`workerCategorySpec`|[`WorkerCategorySpec`](#workercategoryspec) object|null (no worker category spec)|

<a name="javascript-worker-select-strategy"></a>

###### `javascript`

Allows defining arbitrary logic for selecting workers to run task using a JavaScript function.
The function is passed remoteTaskRunnerConfig, map of workerId to available workers and task to be executed and returns the workerId on which the task should be run or null if the task cannot be run.
It can be used for rapid development of missing features where the worker selection logic is to be changed or tuned often.
If the selection logic is quite complex and cannot be easily tested in JavaScript environment,
its better to write a druid extension module with extending current worker selection strategies written in java.

|Property|Description|Default|
|--------|-----------|-------|
|`type`|`javascript`|required; must be `javascript`|
|`function`|String representing JavaScript function| |

The following example shows a function that sends `batch_index_task` to workers `10.0.0.1` and `10.0.0.2` and all other tasks to other available workers.

```json
{
  "type":"javascript",
  "function":"function (config, zkWorkers, task) {\nvar batch_workers = new java.util.ArrayList();\nbatch_workers.add(\"middleManager1_hostname:8091\");\nbatch_workers.add(\"middleManager2_hostname:8091\");\nworkers = zkWorkers.keySet().toArray();\nvar sortedWorkers = new Array()\n;for(var i = 0; i < workers.length; i++){\n sortedWorkers[i] = workers[i];\n}\nArray.prototype.sort.call(sortedWorkers,function(a, b){return zkWorkers.get(b).getCurrCapacityUsed() - zkWorkers.get(a).getCurrCapacityUsed();});\nvar minWorkerVer = config.getMinWorkerVersion();\nfor (var i = 0; i < sortedWorkers.length; i++) {\n var worker = sortedWorkers[i];\n  var zkWorker = zkWorkers.get(worker);\n  if(zkWorker.canRunTask(task) && zkWorker.isValidVersion(minWorkerVer)){\n    if(task.getType() == 'index_hadoop' && batch_workers.contains(worker)){\n      return worker;\n    } else {\n      if(task.getType() != 'index_hadoop' && !batch_workers.contains(worker)){\n        return worker;\n      }\n    }\n  }\n}\nreturn null;\n}"
}
```

:::info
 JavaScript-based functionality is disabled by default. Refer to the Druid [JavaScript programming guide](../development/javascript.md) for guidelines about using Druid's JavaScript functionality, including instructions on how to enable it.
:::

###### affinityConfig

Use the `affinityConfig` field to pass affinity configuration to the `equalDistribution` and `fillCapacity` strategies.
If not provided, the default is to have no affinity.

|Property|Description|Default|
|--------|-----------|-------|
|`affinity`|JSON object mapping a datasource String name to a list of indexing service MiddleManager `host:port` values. Druid doesn't perform DNS resolution, so the 'host' value must match what is configured on the MiddleManager and what the MiddleManager announces itself as (examine the Overlord logs to see what your MiddleManager announces itself as).|`{}`|
|`strong`|When `true` tasks for a datasource must be assigned to affinity-mapped MiddleManagers. Tasks remain queued until a slot becomes available. When `false`, Druid may assign tasks for a datasource to other MiddleManagers when affinity-mapped MiddleManagers are unavailable to run queued tasks.|false|

###### workerCategorySpec

You can provide `workerCategorySpec` to the `equalDistributionWithCategorySpec` and `fillCapacityWithCategorySpec` strategies using the `workerCategorySpec`
field. If not provided, the default is to not use it at all.

|Property|Description|Default|
|--------|-----------|-------|
|`categoryMap`|A JSON map object mapping a task type String name to a [CategoryConfig](#categoryconfig) object, by which you can specify category config for different task type.|`{}`|
|`strong`|With weak workerCategorySpec (the default), tasks for a dataSource may be assigned to other MiddleManagers if the MiddleManagers specified in `categoryMap` are not able to run all pending tasks in the queue for that dataSource. With strong workerCategorySpec, tasks for a dataSource will only ever be assigned to their specified MiddleManagers, and will wait in the pending queue if necessary.|false|

###### CategoryConfig

|Property|Description|Default|
|--------|-----------|-------|
|`defaultCategory`|Specify default category for a task type.|null|
|`categoryAffinity`|A JSON map object mapping a datasource String name to a category String name of the MiddleManager. If category isn't specified for a datasource, then using the `defaultCategory`. If no specified category and the `defaultCategory` is also null, then tasks can run on any available MiddleManagers.|null|

##### Autoscaler

Amazon's EC2 together with Google's GCE are currently the only supported autoscalers.

EC2's autoscaler properties are:

|Property| Description|Default|
|--------|------------|-------|
|`type`|`ec2`|0|
|`minNumWorkers`| The minimum number of workers that can be in the cluster at any given time.|0|
|`maxNumWorkers`| The maximum number of workers that can be in the cluster at any given time.|0|
|`envConfig.availabilityZone` | What Amazon availability zone to run in.|none|
|`envConfig.nodeData`| A JSON object that describes how to launch new nodes.|none; required|
| `envConfig.userData`| A JSON object that describes how to configure new nodes. If you have set `druid.indexer.autoscale.workerVersion`, this must have a `versionReplacementString`. Otherwise, a `versionReplacementString` is not necessary.|none; optional|

For GCE's properties, please refer to the [gce-extensions](../development/extensions-contrib/gce-extensions.md).

## Data server

This section contains the configuration options for the services that reside on Data servers (MiddleManagers/Peons and Historicals) in the suggested [three-server configuration](../design/architecture.md#druid-servers).

Configuration options for the [Indexer process](../design/indexer.md) are also provided here.

### MiddleManager and Peons

These MiddleManager and Peon configurations can be defined in the `middleManager/runtime.properties` file.

#### MiddleManager service config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current service. This is used to advertise the current service location as reachable from another service and should generally be specified such that `http://${druid.host}/` could actually talk to this service|`InetAddress.getLocalHost().getCanonicalHostName()`|
|`druid.bindOnHost`|Indicating whether the service's internal jetty server bind on `druid.host`. Default is false, which means binding to all interfaces.|false|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8091|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.enableTlsPort](../operations/tls-support.md) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative Integer.|8291|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|`druid/middlemanager`|

#### MiddleManager configuration

MiddleManagers pass their configurations down to their child peons. The MiddleManager requires the following configs:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.runner.allowedPrefixes`|Whitelist of prefixes for configs that can be passed down to child peons.|`com.metamx`, `druid`, `org.apache.druid`, `user.timezone`, `file.encoding`, `java.io.tmpdir`, `hadoop`|
|`druid.indexer.runner.compressZnodes`|Indicates whether or not the MiddleManagers should compress Znodes.|true|
|`druid.indexer.runner.classpath`|Java classpath for the peon.|`System.getProperty("java.class.path")`|
|`druid.indexer.runner.javaCommand`|Command required to execute java.|java|
|`druid.indexer.runner.javaOpts`|*DEPRECATED* A string of -X Java options to pass to the peon's JVM. Quotable parameters or parameters with spaces are encouraged to use javaOptsArray|`''`|
|`druid.indexer.runner.javaOptsArray`|A JSON array of strings to be passed in as options to the peon's JVM. This is additive to `druid.indexer.runner.javaOpts` and is recommended for properly handling arguments which contain quotes or spaces like `["-XX:OnOutOfMemoryError=kill -9 %p"]`|`[]`|
|`druid.indexer.runner.maxZnodeBytes`|The maximum size Znode in bytes that can be created in ZooKeeper, should be in the range of [10KiB, 2GiB). [Human-readable format](human-readable-byte.md) is supported.|512KiB|
|`druid.indexer.runner.startPort`|Starting port used for Peon services, should be greater than 1023 and less than 65536.|8100|
|`druid.indexer.runner.endPort`|Ending port used for Peon services, should be greater than or equal to `druid.indexer.runner.startPort` and less than 65536.|65535|
|`druid.indexer.runner.ports`|A JSON array of integers to specify ports that used for Peon services. If provided and non-empty, ports for Peon services will be chosen from these ports. And `druid.indexer.runner.startPort/druid.indexer.runner.endPort` will be completely ignored.|`[]`|
|`druid.worker.ip`|The IP of the worker.|`localhost`|
|`druid.worker.version`|Version identifier for the MiddleManager. The version number is a string. This affects the expected behavior during certain operations like comparison against `druid.indexer.runner.minWorkerVersion`. Specifically, the version comparison follows dictionary order. Use ISO8601 date format for the version to accommodate date comparisons.|0|
|`druid.worker.capacity`|Maximum number of tasks the MiddleManager can accept.|Number of CPUs on the machine - 1|
|`druid.worker.baseTaskDirs`|List of base temporary working directories, one of which is assigned per task in a round-robin fashion. This property can be used to allow usage of multiple disks for indexing. This property is recommended in place of and takes precedence over `${druid.indexer.task.baseTaskDir}`.  If this configuration is not set, `${druid.indexer.task.baseTaskDir}` is used. For example, `druid.worker.baseTaskDirs=[\"PATH1\",\"PATH2\",...]`.|null|
|`druid.worker.baseTaskDirSize`|The total amount of bytes that can be used by tasks on any single task dir. This value is treated symmetrically across all directories, that is, if this is 500 GB and there are 3 `baseTaskDirs`, then each of those task directories is assumed to allow for 500 GB to be used and a total of 1.5 TB will potentially be available across all tasks. The actual amount of memory assigned to each task is discussed in [Configuring task storage sizes](../ingestion/tasks.md#configuring-task-storage-sizes)|`Long.MAX_VALUE`|
|`druid.worker.category`|A string to name the category that the MiddleManager node belongs to.|`_default_worker_category`|

#### Peon processing

Processing properties set on the MiddleManager are passed through to Peons.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.processing.buffer.sizeBytes`|This specifies a buffer size (less than 2GiB) for the storage of intermediate results. The computation engine in both the Historical and Realtime processes will use a scratch buffer of this size to do all of their intermediate computations off-heap. Larger values allow for more aggregations in a single pass over the data while smaller values can require more passes depending on the query that is being executed. [Human-readable format](human-readable-byte.md) is supported.|auto (max 1 GiB)|
|`druid.processing.buffer.poolCacheMaxCount`|Processing buffer pool caches the buffers for later use. This is the maximum count that the cache will grow to. Note that pool can create more buffers than it can cache if necessary.|`Integer.MAX_VALUE`|
|`druid.processing.formatString`|Realtime and Historical processes use this format string to name their processing threads.|processing-%s|
|`druid.processing.numMergeBuffers`|The number of direct memory buffers available for merging query results. The buffers are sized by `druid.processing.buffer.sizeBytes`. This property is effectively a concurrency limit for queries that require merging buffers. If you are using any queries that require merge buffers (currently, just groupBy) then you should have at least two of these.|`max(2, druid.processing.numThreads / 4)`|
|`druid.processing.numThreads`|The number of processing threads to have available for parallel processing of segments. Our rule of thumb is `num_cores - 1`, which means that even under heavy load there will still be one core available to do background tasks like talking with ZooKeeper and pulling down segments. If only one core is available, this property defaults to the value `1`.|Number of cores - 1 (or 1)|
|`druid.processing.fifo`|Enables the processing queue to treat tasks of equal priority in a FIFO manner.|`true`|
|`druid.processing.tmpDir`|Path where temporary files created while processing a query should be stored. If specified, this configuration takes priority over the default `java.io.tmpdir` path.|path represented by `java.io.tmpdir`|
|`druid.processing.intermediaryData.storage.type`|Storage type for intermediary segments of data shuffle between native parallel index tasks. <br />Set to `local` to store segment files in the local storage of the MiddleManager or Indexer. <br />Set to `deepstore` to use configured deep storage for better fault tolerance during rolling updates. When the storage type is `deepstore`, Druid stores the data in the `shuffle-data` directory under the configured deep storage path. Druid does not support automated cleanup for the `shuffle-data` directory. You can set up cloud storage lifecycle rules for automated cleanup of data at the `shuffle-data` prefix location.|`local`|

The amount of direct memory needed by Druid is at least
`druid.processing.buffer.sizeBytes * (druid.processing.numMergeBuffers + druid.processing.numThreads + 1)`. You can
ensure at least this amount of direct memory is available by providing `-XX:MaxDirectMemorySize=<VALUE>` in
`druid.indexer.runner.javaOptsArray` as documented above.

#### Peon query configuration

See [general query configuration](#general-query-configuration).

#### Peon caching

You can optionally configure caching to be enabled on the peons by setting caching configs here.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.realtime.cache.useCache`|true, false|Enable the cache on the realtime.|false|
|`druid.realtime.cache.populateCache`|true, false|Populate the cache on the realtime.|false|
|`druid.realtime.cache.unCacheable`|All druid query types|All query types to not cache.|`[]`|
|`druid.realtime.cache.maxEntrySize`|positive integer|Maximum cache entry size in bytes.|1_000_000|

See [cache configuration](#cache-configuration) for how to configure cache settings.


#### Additional Peon configuration
Although Peons inherit the configurations of their parent MiddleManagers, explicit child Peon configs in MiddleManager can be set by prefixing them with:

```
druid.indexer.fork.property
```

Additional Peon configs include:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.peon.mode`|One of `local` or `remote`. Setting this property to `local` means you intend to run the Peon as a standalone process which is not recommended.|`remote`|
|`druid.indexer.task.baseDir`|Base temporary working directory.|`System.getProperty("java.io.tmpdir")`|
|`druid.indexer.task.baseTaskDir`|Base temporary working directory for tasks.|`${druid.indexer.task.baseDir}/persistent/task`|
|`druid.indexer.task.batchProcessingMode`| Batch ingestion tasks have three operating modes to control construction and tracking for intermediary segments: `OPEN_SEGMENTS`, `CLOSED_SEGMENTS`, and `CLOSED_SEGMENT_SINKS`. `OPEN_SEGMENTS` uses the streaming ingestion code path and performs a `mmap` on intermediary segments to build a timeline to make these segments available to realtime queries. Batch ingestion doesn't require intermediary segments, so the default mode, `CLOSED_SEGMENTS`, eliminates `mmap` of intermediary segments. `CLOSED_SEGMENTS` mode still tracks the entire set of segments in heap. The `CLOSED_SEGMENTS_SINKS` mode is the most aggressive configuration and should have the smallest memory footprint. It eliminates in-memory tracking and `mmap` of intermediary segments produced during segment creation. `CLOSED_SEGMENTS_SINKS` mode isn't as well tested as other modes so is currently considered experimental. You can use `OPEN_SEGMENTS` mode if problems occur with the 2 newer modes. |`CLOSED_SEGMENTS`|
|`druid.indexer.task.defaultHadoopCoordinates`|Hadoop version to use with HadoopIndexTasks that do not request a particular version.|`org.apache.hadoop:hadoop-client-api:3.3.6`, `org.apache.hadoop:hadoop-client-runtime:3.3.6`|
|`druid.indexer.task.defaultRowFlushBoundary`|Highest row count before persisting to disk. Used for indexing generating tasks.|75000|
|`druid.indexer.task.directoryLockTimeout`|Wait this long for zombie Peons to exit before giving up on their replacements.|PT10M|
|`druid.indexer.task.gracefulShutdownTimeout`|Wait this long on MiddleManager restart for restorable tasks to gracefully exit.|PT5M|
|`druid.indexer.task.hadoopWorkingPath`|Temporary working directory for Hadoop tasks.|`/tmp/druid-indexing`|
|`druid.indexer.task.restoreTasksOnRestart`|If true, MiddleManagers will attempt to stop tasks gracefully on shutdown and restore them on restart.|false|
|`druid.indexer.task.ignoreTimestampSpecForDruidInputSource`|If true, tasks using the [Druid input source](../ingestion/input-sources.md) will ignore the provided timestampSpec, and will use the `__time` column of the input datasource. This option is provided for compatibility with ingestion specs written before Druid 0.22.0.|false|
|`druid.indexer.task.storeEmptyColumns`|Boolean value for whether or not to store empty columns during ingestion. When set to true, Druid stores every column specified in the [`dimensionsSpec`](../ingestion/ingestion-spec.md#dimensionsspec). If you use the string-based schemaless ingestion and don't specify any dimensions to ingest, you must also set [`includeAllDimensions`](../ingestion/ingestion-spec.md#dimensionsspec) for Druid to store empty columns.<br/><br/>If you set `storeEmptyColumns` to false, Druid SQL queries referencing empty columns will fail. If you intend to leave `storeEmptyColumns` disabled, you should either ingest placeholder data for empty columns or else not query on empty columns.<br/><br/>You can overwrite this configuration  by setting `storeEmptyColumns` in the [task context](../ingestion/tasks.md#context-parameters).|true|
|`druid.indexer.task.tmpStorageBytesPerTask`|Maximum number of bytes per task to be used to store temporary files on disk. This config is generally intended for internal usage. Attempts to set it are very likely to be overwritten by the TaskRunner that executes the task, so be sure of what you expect to happen before directly adjusting this configuration parameter. The config is documented here primarily to provide an understanding of what it means if/when someone sees that it has been set. A value of -1 disables this limit.  |-1|
|`druid.indexer.server.maxChatRequests`|Maximum number of concurrent requests served by a task's chat handler. Set to 0 to disable limiting.|0|

If the Peon is running in remote mode, there must be an Overlord up and running. Peons in remote mode can set the following configurations:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.peon.taskActionClient.retry.minWait`|The minimum retry time to communicate with Overlord.|`PT5S`|
|`druid.peon.taskActionClient.retry.maxWait`|The maximum retry time to communicate with Overlord.|`PT1M`|
|`druid.peon.taskActionClient.retry.maxRetryCount`|The maximum number of retries to communicate with Overlord.|60|

##### SegmentWriteOutMediumFactory

When new segments are created, Druid temporarily stores some preprocessed data in some buffers. 
The following types of medium exist for the buffers:

* **Temporary files** (`tmpFile`) are stored under the task working directory (see `druid.worker.baseTaskDirs` configuration above) and thus share it's mounting properties. For example, they could be backed by HDD, SSD or memory (tmpfs).
This type of medium may do unnecessary disk I/O and requires some disk space to be available.

* **Off-heap memory** (`offHeapMemory`) creates buffers in off-heap memory of a JVM process that is running a task.
This type of medium is preferred, but it may require to allow the JVM to have more off-heap memory, by changing `-XX:MaxDirectMemorySize` configuration. It is not yet understood how does the required off-heap memory size relates to the size of the segments being created. But definitely it doesn't make sense to add more extra off-heap memory, than the configured maximum *heap* size (`-Xmx`) for the same JVM.

* **On-heap memory** (`onHeapMemory`) creates buffers using the allocated heap memory of the JVM process running a task. Using on-heap memory introduces garbage collection overhead and so is not recommended in most cases. This type of medium is most helpful for tasks run on external clusters where it may be difficult to allocate and work with direct memory effectively.

For most types of tasks, `SegmentWriteOutMediumFactory` can be configured per-task (see [Tasks](../ingestion/tasks.md) for more information), but if it's not specified for a task, or it's not supported for a particular task type, then Druid uses the value from the following configuration:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.peon.defaultSegmentWriteOutMediumFactory.type`|`tmpFile`, `offHeapMemory`, or `onHeapMemory`|`tmpFile`|

### Indexer

#### Indexer process configuration

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current process. This is used to advertise the current processes location as reachable from another process and should generally be specified such that `http://${druid.host}/` could actually talk to this process|`InetAddress.getLocalHost().getCanonicalHostName()`|
|`druid.bindOnHost`|Indicating whether the process's internal jetty server bind on `druid.host`. Default is false, which means binding to all interfaces.|false|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8091|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.enableTlsPort](../operations/tls-support.md) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative Integer.|8283|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|`druid/indexer`|

#### Indexer general configuration

|Property|Description|Default|
|--------|-----------|-------|
|`druid.worker.version`|Version identifier for the Indexer.|0|
|`druid.worker.capacity`|Maximum number of tasks the Indexer can accept.|Number of available processors - 1|
|`druid.worker.baseTaskDirs`|List of base temporary working directories, one of which is assigned per task in a round-robin fashion. This property can be used to allow usage of multiple disks for indexing. This property is recommended in place of and takes precedence over `${druid.indexer.task.baseTaskDir}`.  If this configuration is not set, `${druid.indexer.task.baseTaskDir}` is used. Example: `druid.worker.baseTaskDirs=[\"PATH1\",\"PATH2\",...]`.|null|
|`druid.worker.baseTaskDirSize`|The total amount of bytes that can be used by tasks on any single task dir. This value is treated symmetrically across all directories, that is, if this is 500 GB and there are 3 `baseTaskDirs`, then each of those task directories is assumed to allow for 500 GB to be used and a total of 1.5 TB will potentially be available across all tasks. The actual amount of memory assigned to each task is discussed in [Configuring task storage sizes](../ingestion/tasks.md#configuring-task-storage-sizes)|`Long.MAX_VALUE`|
|`druid.worker.globalIngestionHeapLimitBytes`|Total amount of heap available for ingestion processing. This is applied by automatically setting the `maxBytesInMemory` property on tasks.|60% of configured JVM heap|
|`druid.worker.numConcurrentMerges`|Maximum number of segment persist or merge operations that can run concurrently across all tasks.|`druid.worker.capacity` / 2, rounded down|
|`druid.indexer.task.baseDir`|Base temporary working directory.|`System.getProperty("java.io.tmpdir")`|
|`druid.indexer.task.baseTaskDir`|Base temporary working directory for tasks.|`${druid.indexer.task.baseDir}/persistent/tasks`|
|`druid.indexer.task.defaultHadoopCoordinates`|Hadoop version to use with HadoopIndexTasks that do not request a particular version.|`org.apache.hadoop:hadoop-client-api:3.3.6`, `org.apache.hadoop:hadoop-client-runtime:3.3.6`|
|`druid.indexer.task.gracefulShutdownTimeout`|Wait this long on Indexer restart for restorable tasks to gracefully exit.|`PT5M`|
|`druid.indexer.task.hadoopWorkingPath`|Temporary working directory for Hadoop tasks.|`/tmp/druid-indexing`|
|`druid.indexer.task.restoreTasksOnRestart`|If true, the Indexer will attempt to stop tasks gracefully on shutdown and restore them on restart.|false|
|`druid.indexer.task.ignoreTimestampSpecForDruidInputSource`|If true, tasks using the [Druid input source](../ingestion/input-sources.md) will ignore the provided timestampSpec, and will use the `__time` column of the input datasource. This option is provided for compatibility with ingestion specs written before Druid 0.22.0.|false|
|`druid.indexer.task.storeEmptyColumns`|Boolean value for whether or not to store empty columns during ingestion. When set to true, Druid stores every column specified in the [`dimensionsSpec`](../ingestion/ingestion-spec.md#dimensionsspec). <br/><br/>If you set `storeEmptyColumns` to false, Druid SQL queries referencing empty columns will fail. If you intend to leave `storeEmptyColumns` disabled, you should either ingest placeholder data for empty columns or else not query on empty columns.<br/><br/>You can overwrite this configuration by setting `storeEmptyColumns` in the [task context](../ingestion/tasks.md#context-parameters).|true||`druid.peon.taskActionClient.retry.minWait`|The minimum retry time to communicate with Overlord.|`PT5S`|
|`druid.peon.taskActionClient.retry.maxWait`|The maximum retry time to communicate with Overlord.|`PT1M`|
|`druid.peon.taskActionClient.retry.maxRetryCount`|The maximum number of retries to communicate with Overlord.|60|

#### Indexer concurrent requests

Druid uses Jetty to serve HTTP requests.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.http.numThreads`|Number of threads for HTTP requests. Please see the [Indexer Server HTTP threads](../design/indexer.md#server-http-threads) documentation for more details on how the Indexer uses this configuration.|max(10, (Number of cores * 17) / 16 + 2) + 30|
|`druid.server.http.queueSize`|Size of the worker queue used by Jetty server to temporarily store incoming client connections. If this value is set and a request is rejected by jetty because queue is full then client would observe request failure with TCP connection being closed immediately with a completely empty response from server.|Unbounded|
|`druid.server.http.maxIdleTime`|The Jetty max idle time for a connection.|`PT5M`|
|`druid.server.http.enableRequestLimit`|If enabled, no requests would be queued in jetty queue and "HTTP 429 Too Many Requests" error response would be sent. |false|
|`druid.server.http.defaultQueryTimeout`|Query timeout in millis, beyond which unfinished queries will be cancelled|300000|
|`druid.server.http.gracefulShutdownTimeout`|The maximum amount of time Jetty waits after receiving shutdown signal. After this timeout the threads will be forcefully shutdown. This allows any queries that are executing to complete(Only values greater than zero are valid).|`PT30S`|
|`druid.server.http.unannouncePropagationDelay`|How long to wait for ZooKeeper unannouncements to propagate before shutting down Jetty. This is a minimum and `druid.server.http.gracefulShutdownTimeout` does not start counting down until after this period elapses.|`PT0S` (do not wait)|
|`druid.server.http.maxQueryTimeout`|Maximum allowed value (in milliseconds) for `timeout` parameter. See [query-context](../querying/query-context.md) to know more about `timeout`. Query is rejected if the query context `timeout` is greater than this value. |`Long.MAX_VALUE`|
|`druid.server.http.maxRequestHeaderSize`|Maximum size of a request header in bytes. Larger headers consume more memory and can make a server more vulnerable to denial of service attacks.|8 * 1024|
|`druid.server.http.enableForwardedRequestCustomizer`|If enabled, adds Jetty ForwardedRequestCustomizer which reads X-Forwarded-* request headers to manipulate servlet request object when Druid is used behind a proxy.|false|
|`druid.server.http.allowedHttpMethods`|List of HTTP methods that should be allowed in addition to the ones required by Druid APIs. Druid APIs require GET, PUT, POST, and DELETE, which are always allowed. This option is not useful unless you have installed an extension that needs these additional HTTP methods or that adds functionality related to CORS. None of Druid's bundled extensions require these methods.|`[]`|
|`druid.server.http.contentSecurityPolicy`|Content-Security-Policy header value to set on each non-POST response. Setting this property to an empty string, or omitting it, both result in the default `frame-ancestors: none` being set.|`frame-ancestors 'none'`|

#### Indexer processing resources

|Property|Description|Default|
|--------|-----------|-------|
|`druid.processing.buffer.sizeBytes`|This specifies a buffer size (less than 2GiB) for the storage of intermediate results. The computation engine in the Indexer processes will use a scratch buffer of this size to do all of their intermediate computations off-heap. Larger values allow for more aggregations in a single pass over the data while smaller values can require more passes depending on the query that is being executed. [Human-readable format](human-readable-byte.md) is supported.|auto (max 1GiB)|
|`druid.processing.buffer.poolCacheMaxCount`|processing buffer pool caches the buffers for later use, this is the maximum count cache will grow to. note that pool can create more buffers than it can cache if necessary.|`Integer.MAX_VALUE`|
|`druid.processing.formatString`|Indexer processes use this format string to name their processing threads.|processing-%s|
|`druid.processing.numMergeBuffers`|The number of direct memory buffers available for merging query results. The buffers are sized by `druid.processing.buffer.sizeBytes`. This property is effectively a concurrency limit for queries that require merging buffers. If you are using any queries that require merge buffers (currently, just groupBy) then you should have at least two of these.|`max(2, druid.processing.numThreads / 4)`|
|`druid.processing.numThreads`|The number of processing threads to have available for parallel processing of segments. Our rule of thumb is `num_cores - 1`, which means that even under heavy load there will still be one core available to do background tasks like talking with ZooKeeper and pulling down segments. If only one core is available, this property defaults to the value `1`.|Number of cores - 1 (or 1)|
|`druid.processing.fifo`|If the processing queue should treat tasks of equal priority in a FIFO manner|`true`|
|`druid.processing.tmpDir`|Path where temporary files created while processing a query should be stored. If specified, this configuration takes priority over the default `java.io.tmpdir` path.|path represented by `java.io.tmpdir`|

The amount of direct memory needed by Druid is at least
`druid.processing.buffer.sizeBytes * (druid.processing.numMergeBuffers + druid.processing.numThreads + 1)`. You can
ensure at least this amount of direct memory is available by providing `-XX:MaxDirectMemorySize=<VALUE>` at the command
line.

#### Query configurations

See [general query configuration](#general-query-configuration).

#### Indexer caching

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

For general Historical service information, see [Historical](../design/historical.md).

These Historical configurations can be defined in the `historical/runtime.properties` file.

#### Historical service configuration

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current service. This is used to advertise the current service location as reachable from another service and should generally be specified such that `http://${druid.host}/` could actually talk to this service|`InetAddress.getLocalHost().getCanonicalHostName()`|
|`druid.bindOnHost`|Indicating whether the service's internal jetty server bind on `druid.host`. Default is false, which means binding to all interfaces.|false|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8083|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.enableTlsPort](../operations/tls-support.md) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative Integer.|8283|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|`druid/historical`|

#### Historical general configuration

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.maxSize`|The maximum number of bytes-worth of segments that the service wants assigned to it. The Coordinator service will attempt to assign segments to a Historical service only if this property is greater than the total size of segments served by it. Since this property defines the upper limit on the total segment size that can be assigned to a Historical, it is defaulted to the sum of all `maxSize` values specified within `druid.segmentCache.locations` property. Human-readable format is supported, see [here](human-readable-byte.md). |Sum of `maxSize` values defined within `druid.segmentCache.locations`|
|`druid.server.tier`| A string to name the distribution tier that the storage service belongs to. Many of the [rules Coordinator services use](../operations/rule-configuration.md) to manage segments can be keyed on tiers. |  `_default_tier` |
|`druid.server.priority`|In a tiered architecture, the priority of the tier, thus allowing control over which services are queried. Higher numbers mean higher priority. The default (no priority) works for architecture with no cross replication (tiers that have no data-storage overlap). Data centers typically have equal priority. | 0 |

#### Storing segments

|Property|Description|Default|
|--------|-----------|-------|
|`druid.segmentCache.locations`|Segments assigned to a Historical services are first stored on the local file system (in a disk cache) and then served by the Historical services. These locations define where that local cache resides. This value cannot be NULL or EMPTY. Here is an example `druid.segmentCache.locations=[{"path": "/mnt/druidSegments", "maxSize": "10k", "freeSpacePercent": 1.0}]`. "freeSpacePercent" is optional, if provided then enforces that much of free disk partition space while storing segments. But, it depends on `File.getTotalSpace()` and `File.getFreeSpace()` methods, so enable if only if they work for your File System.| none |
|`druid.segmentCache.locationSelector.strategy`|The strategy used to select a location from the configured `druid.segmentCache.locations` for segment distribution. Possible values are `leastBytesUsed`, `roundRobin`, `random`, or `mostAvailableSize`. |leastBytesUsed|
|`druid.segmentCache.deleteOnRemove`|Delete segment files from cache once a service is no longer serving a segment.|true|
|`druid.segmentCache.dropSegmentDelayMillis`|How long a service delays before completely dropping segment.|30000 (30 seconds)|
|`druid.segmentCache.infoDir`|Historical services keep track of the segments they are serving so that when the service is restarted they can reload the same segments without waiting for the Coordinator to reassign. This path defines where this metadata is kept. Directory will be created if needed.|`${first_location}/info_dir`|
|`druid.segmentCache.announceIntervalMillis`|How frequently to announce segments while segments are loading from cache. Set this value to zero to wait for all segments to be loaded before announcing.|5000 (5 seconds)|
|`druid.segmentCache.numLoadingThreads`|How many segments to drop or load concurrently from deep storage. Note that the work of loading segments involves downloading segments from deep storage, decompressing them and loading them to a memory mapped location. So the work is not all I/O Bound. Depending on CPU and network load, one could possibly increase this config to a higher value.|max(1,Number of cores / 6)|
|`druid.segmentCache.numBootstrapThreads`|How many segments to load concurrently during historical startup.|`druid.segmentCache.numLoadingThreads`|
|`druid.segmentCache.lazyLoadOnStart`|Whether or not to load segment columns metadata lazily during historical startup. When set to true, Historical startup time will be dramatically improved by deferring segment loading until the first time that segment takes part in a query, which will incur this cost instead.|false|
|`druid.coordinator.loadqueuepeon.curator.numCallbackThreads`|Number of threads for executing callback actions associated with loading or dropping of segments. One might want to increase this number when noticing clusters are lagging behind w.r.t. balancing segments across historical nodes.|2|
|`druid.segmentCache.numThreadsToLoadSegmentsIntoPageCacheOnDownload`|Number of threads to asynchronously read segment index files into null output stream on each new segment download after the Historical service finishes bootstrapping. Recommended to set to 1 or 2 or leave unspecified to disable. See also `druid.segmentCache.numThreadsToLoadSegmentsIntoPageCacheOnBootstrap`|0|
|`druid.segmentCache.numThreadsToLoadSegmentsIntoPageCacheOnBootstrap`|Number of threads to asynchronously read segment index files into null output stream during Historical service bootstrap. This thread pool is terminated after Historical service finishes bootstrapping. Recommended to set to half of available cores. If left unspecified, `druid.segmentCache.numThreadsToLoadSegmentsIntoPageCacheOnDownload` will be used. If both configs are unspecified, this feature is disabled. Preemptively loading segments into page cache helps in the sense that later when a segment is queried, it's already in page cache and only a minor page fault needs to be triggered instead of a more costly major page fault to make the query latency more consistent. Note that loading segment into page cache just does a blind loading of segment index files and will evict any existing segments from page cache at the discretion of operating system when the total segment size on local disk is larger than the page cache usable in the RAM, which roughly equals to total available RAM in the host - druid process memory including both heap and direct memory allocated - memory used by other non druid processes on the host, so it is the user's responsibility to ensure the host has enough RAM to host all the segments to avoid random evictions to fully leverage this feature.|`druid.segmentCache.numThreadsToLoadSegmentsIntoPageCacheOnDownload`|

In `druid.segmentCache.locations`, `freeSpacePercent` was added because the `maxSize` setting is only a theoretical limit and assumes that much space will always be available for storing segments. In case of any druid bug leading to unaccounted segment files left alone on disk or some other service writing stuff to disk, This check can start failing segment loading early before filling up the disk completely and leaving the host usable otherwise.

In `druid.segmentCache.locationSelector.strategy`, one of `leastBytesUsed`, `roundRobin`, `random`, or `mostAvailableSize` could be specified to represent the strategy to distribute segments across multiple segment cache locations.

|Strategy|Description|
|--------|-----------|
|`leastBytesUsed`|Selects a location which has least bytes used in absolute terms.|
|`roundRobin`|Selects a location in a round robin fashion oblivious to the bytes used or the capacity.|
|`random`|Selects a segment cache location randomly each time among the available storage locations.|
|`mostAvailableSize`|Selects a segment cache location that has most free space among the available storage locations.|

Note that if `druid.segmentCache.numLoadingThreads` > 1, multiple threads can download different segments at the same time. In this case, with the `leastBytesUsed` strategy or `mostAvailableSize` strategy, Historicals may select a sub-optimal storage location because each decision is based on a snapshot of the storage location status of when a segment is requested to download.

#### Historical query configs

##### Concurrent requests

Druid uses Jetty to serve HTTP requests.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.http.numThreads`|Number of threads for HTTP requests.|max(10, (Number of cores * 17) / 16 + 2) + 30|
|`druid.server.http.queueSize`|Size of the worker queue used by Jetty server to temporarily store incoming client connections. If this value is set and a request is rejected by jetty because queue is full then client would observe request failure with TCP connection being closed immediately with a completely empty response from server.|Unbounded|
|`druid.server.http.maxIdleTime`|The Jetty max idle time for a connection.|`PT5M`|
|`druid.server.http.enableRequestLimit`|If enabled, no requests would be queued in jetty queue and "HTTP 429 Too Many Requests" error response would be sent. |false|
|`druid.server.http.defaultQueryTimeout`|Query timeout in millis, beyond which unfinished queries will be cancelled|300000|
|`druid.server.http.gracefulShutdownTimeout`|The maximum amount of time Jetty waits after receiving shutdown signal. After this timeout the threads will be forcefully shutdown. This allows any queries that are executing to complete(Only values greater than zero are valid).|`PT30S`|
|`druid.server.http.unannouncePropagationDelay`|How long to wait for ZooKeeper unannouncements to propagate before shutting down Jetty. This is a minimum and `druid.server.http.gracefulShutdownTimeout` does not start counting down until after this period elapses.|`PT0S` (do not wait)|
|`druid.server.http.maxQueryTimeout`|Maximum allowed value (in milliseconds) for `timeout` parameter. See [query-context](../querying/query-context.md) to know more about `timeout`. Query is rejected if the query context `timeout` is greater than this value. |`Long.MAX_VALUE`|
|`druid.server.http.maxRequestHeaderSize`|Maximum size of a request header in bytes. Larger headers consume more memory and can make a server more vulnerable to denial of service attacks.|8 * 1024|
|`druid.server.http.contentSecurityPolicy`|Content-Security-Policy header value to set on each non-POST response. Setting this property to an empty string, or omitting it, both result in the default `frame-ancestors: none` being set.|`frame-ancestors 'none'`|

##### Processing

|Property|Description|Default|
|--------|-----------|-------|
|`druid.processing.buffer.sizeBytes`|This specifies a buffer size (less than 2GiB), for the storage of intermediate results. The computation engine in both the Historical and Realtime processes will use a scratch buffer of this size to do all of their intermediate computations off-heap. Larger values allow for more aggregations in a single pass over the data while smaller values can require more passes depending on the query that is being executed.  [Human-readable format](human-readable-byte.md) is supported.|auto (max 1GiB)|
|`druid.processing.buffer.poolCacheMaxCount`|processing buffer pool caches the buffers for later use, this is the maximum count cache will grow to. note that pool can create more buffers than it can cache if necessary.|`Integer.MAX_VALUE`|
|`druid.processing.formatString`|Realtime and Historical processes use this format string to name their processing threads.|processing-%s|
|`druid.processing.numMergeBuffers`|The number of direct memory buffers available for merging query results. The buffers are sized by `druid.processing.buffer.sizeBytes`. This property is effectively a concurrency limit for queries that require merging buffers. If you are using any queries that require merge buffers (currently, just groupBy) then you should have at least two of these.|`max(2, druid.processing.numThreads / 4)`|
|`druid.processing.numThreads`|The number of processing threads to have available for parallel processing of segments. Our rule of thumb is `num_cores - 1`, which means that even under heavy load there will still be one core available to do background tasks like talking with ZooKeeper and pulling down segments. If only one core is available, this property defaults to the value `1`.|Number of cores - 1 (or 1)|
|`druid.processing.fifo`|If the processing queue should treat tasks of equal priority in a FIFO manner|`true`|
|`druid.processing.tmpDir`|Path where temporary files created while processing a query should be stored. If specified, this configuration takes priority over the default `java.io.tmpdir` path.|path represented by `java.io.tmpdir`|

The amount of direct memory needed by Druid is at least
`druid.processing.buffer.sizeBytes * (druid.processing.numMergeBuffers + druid.processing.numThreads + 1)`. You can
ensure at least this amount of direct memory is available by providing `-XX:MaxDirectMemorySize=<VALUE>` at the command
line.

##### Historical query configuration

See [general query configuration](#general-query-configuration).

#### Historical caching

You can optionally only configure caching to be enabled on the Historical by setting caching configs here.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.historical.cache.useCache`|true, false|Enable the cache on the Historical.|false|
|`druid.historical.cache.populateCache`|true, false|Populate the cache on the Historical.|false|
|`druid.historical.cache.unCacheable`|All druid query types|All query types to not cache.|`[]`|
|`druid.historical.cache.maxEntrySize`|positive integer|Maximum cache entry size in bytes.|1_000_000|

See [cache configuration](#cache-configuration) for how to configure cache settings.

## Query server

This section contains the configuration options for the services that reside on Query servers (Brokers) in the suggested [three-server configuration](../design/architecture.md#druid-servers).

Configuration options for the experimental [Router process](../design/router.md) are also provided here.

### Broker

For general Broker process information, see [here](../design/broker.md).

These Broker configurations can be defined in the `broker/runtime.properties` file.

#### Broker process configs

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current process. This is used to advertise the current processes location as reachable from another process and should generally be specified such that `http://${druid.host}/` could actually talk to this process|`InetAddress.getLocalHost().getCanonicalHostName()`|
|`druid.bindOnHost`|Indicating whether the process's internal jetty server bind on `druid.host`. Default is false, which means binding to all interfaces.|false|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8082|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.enableTlsPort](../operations/tls-support.md) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative Integer.|8282|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|`druid/broker`|

#### Query configuration

##### Query routing

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.broker.balancer.type`|`random`, `connectionCount`|Determines how the broker balances connections to Historical processes. `random` choose randomly, `connectionCount` picks the process with the fewest number of active connections to|`random`|
|`druid.broker.select.tier`|`highestPriority`, `lowestPriority`, `custom`|If segments are cross-replicated across tiers in a cluster, you can tell the broker to prefer to select segments in a tier with a certain priority.|`highestPriority`|
|`druid.broker.select.tier.custom.priorities`|An array of integer priorities, such as `[-1, 0, 1, 2]`|Select servers in tiers with a custom priority list.|The config only has effect if `druid.broker.select.tier` is set to `custom`. If `druid.broker.select.tier` is set to `custom` but this config is not specified, the effect is the same as `druid.broker.select.tier` set to `highestPriority`. Any of the integers in this config can be ignored if there's no corresponding tiers with such priorities. Tiers with priorities explicitly specified in this config always have higher priority than those not and those not specified fall back to use `highestPriority` strategy among themselves.|

##### Query prioritization and laning

Laning strategies allow you to control capacity utilization for heterogeneous query workloads. With laning, the broker examines and classifies a query for the purpose of assigning it to a lane. Lanes have capacity limits, enforced by the broker, that can be used to ensure sufficient resources are available for other lanes or for interactive queries (with no lane), or to limit overall throughput for queries within the lane. Requests in excess of the capacity are discarded with an HTTP 429 status code.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.scheduler.numThreads`|Maximum number of concurrently-running queries. When this parameter is set lower than `druid.server.http.numThreads`, query requests beyond the limit are put into the Jetty request queue. This has the effect of reserving the leftover Jetty threads for non-query requests.<br /><br />When this parameter is set equal to or higher than `druid.server.http.numThreads`, it has no effect.|Unbounded|
|`druid.query.scheduler.laning.strategy`|Query laning strategy to use to assign queries to a lane in order to control capacities for certain classes of queries.|`none`|
|`druid.query.scheduler.prioritization.strategy`|Query prioritization strategy to automatically assign priorities.|`manual`|

##### Prioritization strategies

###### Manual prioritization strategy

With this configuration, queries are never assigned a priority automatically, but will preserve a priority manually set on the [query context](../querying/query-context.md) with the `priority` key. This mode can be explicitly set by setting `druid.query.scheduler.prioritization.strategy` to `manual`.

###### Threshold prioritization strategy

This prioritization strategy lowers the priority of queries that cross any of a configurable set of thresholds, such as how far in the past the data is, how large of an interval a query covers, or the number of segments taking part in a query.

This strategy can be enabled by setting `druid.query.scheduler.prioritization.strategy` to `threshold`.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.scheduler.prioritization.periodThreshold`|ISO duration threshold for how old data can be queried before automatically adjusting query priority.|none|
|`druid.query.scheduler.prioritization.durationThreshold`|ISO duration threshold for maximum duration a queries interval can span before the priority is automatically adjusted.|none|
|`druid.query.scheduler.prioritization.segmentCountThreshold`|Number threshold for maximum number of segments that can take part in a query before its priority is automatically adjusted.|none|
|`druid.query.scheduler.prioritization.adjustment`|Amount to reduce the priority of queries which cross any threshold.|none|

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

##### Guardrails for materialization of subqueries

Druid stores the subquery rows in temporary tables that live in the Java heap. It is a good practice to avoid large subqueries in Druid.
Therefore, there are guardrails that are built in Druid to prevent the queries from generating subquery results which can exhaust the heap
space. They can be set on a cluster level or modified per query level as desired.
Note the following guardrails that can be set by the cluster admin to limit the subquery results:

1. `druid.server.http.maxSubqueryRows` in broker's config to set a default for the entire cluster or `maxSubqueryRows` in the query context to set an upper limit on the number of rows a subquery can generate
2. `druid.server.http.maxSubqueryBytes` in broker's config to set a default for the entire cluster or `maxSubqueryBytes` in the query context to set an upper limit on the number of bytes a subquery can generate

Limiting the subquery by bytes is an experimental feature as it materializes the results differently.

You can configure `maxSubqueryBytes` to the following values:

* `disabled`: It is the default setting out of the box. It disables the subquery's from the byte based limit, and effectively disables this feature.
* `auto`: Druid automatically decides the optimal byte based limit based upon the heap space available and the max number of concurrent queries.
* A positive long value: User can manually specify the number of bytes that the results of the subqueries of a single query can occupy on the heap.

Due to the conversion between the Java objects and the Frame's format, setting `maxSubqueryBytes` can become slow if the subquery starts generating
rows in the order of magnitude of around 10 million and above. In those scenarios, disable the `maxSubqueryBytes` settings for such queries, assess the number of rows that the subqueries generate and override the `maxSubqueryRows` to appropriate value.

If you choose to modify or set any of the above limits, you must also think about the heap size of all Brokers, Historicals, and task Peons that process data for the subqueries to accommodate the subquery results.
There is no formula to calculate the correct value. Trial and error is the best approach.

###### Manual laning strategy

This laning strategy is best suited for cases where one or more external applications which query Druid are capable of manually deciding what lane a given query should belong to. Configured with a map of lane names to percent or exact max capacities, queries with a matching `lane` parameter in the [query context](../querying/query-context.md) will be subjected to those limits.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.scheduler.laning.lanes.{name}`|Maximum percent or exact limit of queries that can concurrently run in the defined lanes. Any number of lanes may be defined like this. The lane names 'total' and 'default' are reserved for internal use.|No default, must define at least one lane with a limit above 0. If `druid.query.scheduler.laning.isLimitPercent` is set to `true`, values must be integers in the range of 1 to 100.|
|`druid.query.scheduler.laning.isLimitPercent`|If set to `true`, the values set for `druid.query.scheduler.laning.lanes` will be treated as a percent of the smaller number of `druid.server.http.numThreads` or `druid.query.scheduler.numThreads`. Note that in this mode, these lane values across lanes are _not_ required to add up to, and can exceed, 100%.|`false`|

##### Server configuration

Druid uses Jetty to serve HTTP requests. Each query being processed consumes a single thread from `druid.server.http.numThreads`, so consider defining `druid.query.scheduler.numThreads` to a lower value in order to reserve HTTP threads for responding to health checks, lookup loading, and other non-query, (in most cases) comparatively very short-lived, HTTP requests.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.http.numThreads`|Number of threads for HTTP requests.|max(10, (Number of cores * 17) / 16 + 2) + 30|
|`druid.server.http.queueSize`|Size of the worker queue used by Jetty server to temporarily store incoming client connections. If this value is set and a request is rejected by jetty because queue is full then client would observe request failure with TCP connection being closed immediately with a completely empty response from server.|Unbounded|
|`druid.server.http.maxIdleTime`|The Jetty max idle time for a connection.|`PT5M`|
|`druid.server.http.enableRequestLimit`|If enabled, no requests would be queued in jetty queue and "HTTP 429 Too Many Requests" error response would be sent. |false|
|`druid.server.http.defaultQueryTimeout`|Query timeout in millis, beyond which unfinished queries will be cancelled|300000|
|`druid.server.http.maxScatterGatherBytes`|Maximum number of bytes gathered from data processes such as Historicals and realtime processes to execute a query. Queries that exceed this limit will fail. This is an advance configuration that allows to protect in case Broker is under heavy load and not utilizing the data gathered in memory fast enough and leading to OOMs. This limit can be further reduced at query time using `maxScatterGatherBytes` in the context. Note that having large limit is not necessarily bad if broker is never under heavy concurrent load in which case data gathered is processed quickly and freeing up the memory used. Human-readable format is supported, see [here](human-readable-byte.md). |`Long.MAX_VALUE`|
|`druid.server.http.maxSubqueryRows`|Maximum number of rows from all subqueries per query. Druid stores the subquery rows in temporary tables that live in the Java heap. `druid.server.http.maxSubqueryRows` is a guardrail to prevent the system from exhausting available heap. When a subquery exceeds the row limit, Druid throws a resource limit exceeded exception: "Subquery generated results beyond maximum."<br /><br />It is a good practice to avoid large subqueries in Druid. However, if you choose to raise the subquery row limit, you must also increase the heap size of all Brokers, Historicals, and task Peons that process data for the subqueries to accommodate the subquery results.<br /><br />There is no formula to calculate the correct value. Trial and error is the best approach.|100000|
|`druid.server.http.maxSubqueryBytes`|Maximum number of bytes from all subqueries per query. Since the results are stored on the Java heap, `druid.server.http.maxSubqueryBytes` is a guardrail like `druid.server.http.maxSubqueryRows` to prevent the heap space from exhausting. When a subquery exceeds the byte limit, Druid throws a resource limit exceeded exception. A negative value for the guardrail indicates that Druid won't guardrail by memory. This can be set to 'disabled' which disables the results from being limited via the byte limit, 'auto' which sets this value automatically taking free heap space into account, or a positive long value depicting the number of bytes per query's subqueries' results can occupy. This is an experimental feature for now as this materializes the results in a different format.|'disabled'|
|`druid.server.http.gracefulShutdownTimeout`|The maximum amount of time Jetty waits after receiving shutdown signal. After this timeout the threads will be forcefully shutdown. This allows any queries that are executing to complete(Only values greater than zero are valid).|`PT30S`|
|`druid.server.http.unannouncePropagationDelay`|How long to wait for ZooKeeper unannouncements to propagate before shutting down Jetty. This is a minimum and `druid.server.http.gracefulShutdownTimeout` does not start counting down until after this period elapses.|`PT0S` (do not wait)|
|`druid.server.http.maxQueryTimeout`|Maximum allowed value (in milliseconds) for `timeout` parameter. See [query-context](../querying/query-context.md) to know more about `timeout`. Query is rejected if the query context `timeout` is greater than this value. |`Long.MAX_VALUE`|
|`druid.server.http.maxRequestHeaderSize`|Maximum size of a request header in bytes. Larger headers consume more memory and can make a server more vulnerable to denial of service attacks. |8 * 1024|
|`druid.server.http.contentSecurityPolicy`|Content-Security-Policy header value to set on each non-POST response. Setting this property to an empty string, or omitting it, both result in the default `frame-ancestors: none` being set.|`frame-ancestors 'none'`|
|`druid.server.http.enableHSTS`|If set to true, druid services will add strict transport security header `Strict-Transport-Security: max-age=63072000; includeSubDomains` to all HTTP responses|`false`|

##### Client configuration

Druid Brokers use an HTTP client to communicate with data servers (Historical servers and real-time tasks). This
client has the following configuration options.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.broker.http.numConnections`|Size of connection pool for the Broker to connect to Historical and real-time processes. If there are more queries than this number that all need to speak to the same process, then they will queue up.|20|
|`druid.broker.http.eagerInitialization`|Indicates that http connections from Broker to Historical and Real-time processes should be eagerly initialized. If set to true, `numConnections` connections are created upon initialization|`true`|
|`druid.broker.http.compressionCodec`|Compression codec the Broker uses to communicate with Historical and real-time processes. May be "gzip" or "identity".|`gzip`|
|`druid.broker.http.readTimeout`|The timeout for data reads from Historical servers and real-time tasks.|`PT15M`|
|`druid.broker.http.unusedConnectionTimeout`|The timeout for idle connections in connection pool. The connection in the pool will be closed after this timeout and a new one will be established. This timeout should be less than `druid.broker.http.readTimeout`. Set this timeout = ~90% of `druid.broker.http.readTimeout`|`PT4M`|
|`druid.broker.http.maxQueuedBytes`|Maximum number of bytes queued per query before exerting [backpressure](../operations/basic-cluster-tuning.md#broker-backpressure) on channels to the data servers.<br /><br />Similar to `druid.server.http.maxScatterGatherBytes`, except that `maxQueuedBytes` triggers [backpressure](../operations/basic-cluster-tuning.md#broker-backpressure) instead of query failure. Set to zero to disable. You can override this setting by using the [`maxQueuedBytes` query context parameter](../querying/query-context.md). Druid supports [human-readable](human-readable-byte.md) format. |25 MB or 2% of maximum Broker heap size, whichever is greater.|
|`druid.broker.http.numMaxThreads`|`Maximum number of I/O worker threads|max(10, ((number of cores * 17) / 16 + 2) + 30)`|

##### Retry policy

Druid broker can optionally retry queries internally for transient errors.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.broker.retryPolicy.numTries`|Number of tries.|1|

##### Processing

The broker uses processing configs for nested groupBy queries.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.processing.buffer.sizeBytes`|This specifies a buffer size (less than 2GiB) for the storage of intermediate results. The computation engine in both the Historical and Realtime processes will use a scratch buffer of this size to do all of their intermediate computations off-heap. Larger values allow for more aggregations in a single pass over the data while smaller values can require more passes depending on the query that is being executed. [Human-readable format](human-readable-byte.md) is supported.|auto (max 1GiB)|
|`druid.processing.buffer.poolCacheInitialCount`|initializes the number of buffers allocated on the intermediate results pool. Note that pool can create more buffers if necessary.|`0`|
|`druid.processing.buffer.poolCacheMaxCount`|processing buffer pool caches the buffers for later use, this is the maximum count cache will grow to. note that pool can create more buffers than it can cache if necessary.|`Integer.MAX_VALUE`|
|`druid.processing.numMergeBuffers`|The number of direct memory buffers available for merging query results. The buffers are sized by `druid.processing.buffer.sizeBytes`. This property is effectively a concurrency limit for queries that require merging buffers. If you are using any queries that require merge buffers (currently, just groupBy) then you should have at least two of these.|`max(2, druid.processing.numThreads / 4)`|
|`druid.processing.fifo`|If the processing queue should treat tasks of equal priority in a FIFO manner|`true`|
|`druid.processing.tmpDir`|Path where temporary files created while processing a query should be stored. If specified, this configuration takes priority over the default `java.io.tmpdir` path.|path represented by `java.io.tmpdir`|
|`druid.processing.merge.useParallelMergePool`|Enable automatic parallel merging for Brokers on a dedicated async ForkJoinPool. If `false`, instead merges will be done serially on the `HTTP` thread pool.|`true`|
|`druid.processing.merge.pool.parallelism`|Size of ForkJoinPool. Note that the default configuration assumes that the value returned by `Runtime.getRuntime().availableProcessors()` represents 2 hyper-threads per physical core, and multiplies this value by `0.75` in attempt to size `1.5` times the number of _physical_ cores.|`Runtime.getRuntime().availableProcessors() * 0.75` (rounded up)|
|`druid.processing.merge.pool.defaultMaxQueryParallelism`|Default maximum number of parallel merge tasks per query. Note that the default configuration assumes that the value returned by `Runtime.getRuntime().availableProcessors()` represents 2 hyper-threads per physical core, and multiplies this value by `0.5` in attempt to size to the number of _physical_ cores.|`Runtime.getRuntime().availableProcessors() * 0.5` (rounded up)|
|`druid.processing.merge.pool.awaitShutdownMillis`|Time to wait for merge ForkJoinPool tasks to complete before ungracefully stopping on process shutdown in milliseconds.|`60_000`|
|`druid.processing.merge.task.targetRunTimeMillis`|Ideal run-time of each ForkJoinPool merge task, before forking off a new task to continue merging sequences.|100|
|`druid.processing.merge.task.initialYieldNumRows`|Number of rows to yield per ForkJoinPool merge task, before forking off a new task to continue merging sequences.|16384|
|`druid.processing.merge.task.smallBatchNumRows`|Size of result batches to operate on in ForkJoinPool merge tasks.|4096|

The amount of direct memory needed by Druid is at least
`druid.processing.buffer.sizeBytes * (druid.processing.numMergeBuffers + 1)`. You can
ensure at least this amount of direct memory is available by providing `-XX:MaxDirectMemorySize=<VALUE>` at the command
line.

##### Broker query configuration

See [general query configuration](#general-query-configuration).

###### Broker generated query configuration supplementation

The Broker generates queries internally. This configuration section describes how an operator can augment the configuration
of these queries.

As of now the only supported augmentation is overriding the default query context. This allows an operator the flexibility
to adjust it as they see fit. A common use of this configuration is to override the query priority of the cluster generated
queries in order to avoid running as a default priority of 0.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.broker.internal.query.config.context`|A string formatted `key:value` map of a query context to add to internally generated broker queries.|null|

#### SQL

The Druid SQL server is configured through the following properties on the Broker.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.sql.enable`|Whether to enable SQL at all, including background metadata fetching. If false, this overrides all other SQL-related properties and disables SQL metadata, serving, and planning completely.|true|
|`druid.sql.avatica.enable`|Whether to enable JDBC querying at `/druid/v2/sql/avatica/`.|true|
|`druid.sql.avatica.maxConnections`|Maximum number of open connections for the Avatica server. These are not HTTP connections, but are logical client connections that may span multiple HTTP connections.|25|
|`druid.sql.avatica.maxRowsPerFrame`|Maximum acceptable value for the JDBC client `Statement.setFetchSize` method. This setting determines the maximum number of rows that Druid will populate in a single 'fetch' for a JDBC `ResultSet`. Set this property to -1 to enforce no row limit on the server-side and potentially return the entire set of rows on the initial statement execution. If the JDBC client calls `Statement.setFetchSize` with a value other than -1, Druid uses the lesser value of the client-provided limit and `maxRowsPerFrame`. If `maxRowsPerFrame` is smaller than `minRowsPerFrame`, then the `ResultSet` size will be fixed. To handle queries that produce results with a large number of rows, you can increase value of `druid.sql.avatica.maxRowsPerFrame` to reduce the number of fetches required to completely transfer the result set.|5,000|
|`druid.sql.avatica.minRowsPerFrame`|Minimum acceptable value for the JDBC client `Statement.setFetchSize` method. The value for this property must greater than 0. If the JDBC client calls `Statement.setFetchSize` with a lesser value, Druid uses `minRowsPerFrame` instead. If `maxRowsPerFrame` is less than `minRowsPerFrame`, Druid uses the minimum value of the two. For handling queries which produce results with a large number of rows, you can increase this value to reduce the number of fetches required to completely transfer the result set.|100|
|`druid.sql.avatica.maxStatementsPerConnection`|Maximum number of simultaneous open statements per Avatica client connection.|4|
|`druid.sql.avatica.connectionIdleTimeout`|Avatica client connection idle timeout.|`PT5M`|
|`druid.sql.avatica.fetchTimeoutMs`|Avatica fetch timeout, in milliseconds. When a request for the next batch of data takes longer than this time, Druid returns an empty result set, causing the client to poll again. This avoids HTTP timeouts for long-running queries. The default of 5 sec. is good for most cases. |5000|
|`druid.sql.http.enable`|Whether to enable JSON over HTTP querying at `/druid/v2/sql/`.|true|
|`druid.sql.planner.maxTopNLimit`|Maximum threshold for a [TopN query](../querying/topnquery.md). Higher limits will be planned as [GroupBy queries](../querying/groupbyquery.md) instead.|100000|
|`druid.sql.planner.metadataRefreshPeriod`|Throttle for metadata refreshes.|`PT1M`|
|`druid.sql.planner.metadataColumnTypeMergePolicy`|Defines how column types will be chosen when faced with differences between segments when computing the SQL schema. Options are specified as a JSON object, with valid choices of `leastRestrictive` or `latestInterval`. For `leastRestrictive`, Druid will automatically widen the type computed for the schema to a type which data across all segments can be converted into, however planned schema migrations can only take effect once all segments have been re-ingested to the new schema. With `latestInterval`, the column type in most recent time chunks defines the type for the schema. |`leastRestrictive`|
|`druid.sql.planner.useApproximateCountDistinct`|Whether to use an approximate cardinality algorithm for `COUNT(DISTINCT foo)`.|true|
|`druid.sql.planner.useGroupingSetForExactDistinct`|Only relevant when `useApproximateCountDistinct` is disabled. If set to true, exact distinct queries are re-written using grouping sets. Otherwise, exact distinct queries are re-written using joins. This should be set to true for group by query with multiple exact distinct aggregations. This flag can be overridden per query.|false|
|`druid.sql.planner.useApproximateTopN`|Whether to use approximate [TopN queries](../querying/topnquery.md) when a SQL query could be expressed as such. If false, exact [GroupBy queries](../querying/groupbyquery.md) will be used instead.|true|
|`druid.sql.planner.requireTimeCondition`|Whether to require SQL to have filter conditions on __time column so that all generated native queries will have user specified intervals. If true, all queries without filter condition on __time column will fail|false|
|`druid.sql.planner.sqlTimeZone`|Sets the default time zone for the server, which will affect how time functions and timestamp literals behave. Should be a time zone name like "America/Los_Angeles" or offset like "-08:00".|UTC|
|`druid.sql.planner.metadataSegmentCacheEnable`|Whether to keep a cache of published segments in broker. If true, broker polls coordinator in background to get segments from metadata store and maintains a local cache. If false, coordinator's REST API will be invoked when broker needs published segments info.|false|
|`druid.sql.planner.metadataSegmentPollPeriod`|How often to poll coordinator for published segments list if `druid.sql.planner.metadataSegmentCacheEnable` is set to true. Poll period is in milliseconds. |60000|
|`druid.sql.planner.authorizeSystemTablesDirectly`|If true, Druid authorizes queries against any of the system schema tables (`sys` in SQL) as `SYSTEM_TABLE` resources which require `READ` access, in addition to permissions based content filtering.|false|
|`druid.sql.planner.useNativeQueryExplain`|If true, `EXPLAIN PLAN FOR` will return the explain plan as a JSON representation of equivalent native query(s), else it will return the original version of explain plan generated by Calcite. It can be overridden per query with `useNativeQueryExplain` context key.|true|
|`druid.sql.planner.maxNumericInFilters`|Max limit for the amount of numeric values that can be compared for a string type dimension when the entire SQL WHERE clause of a query translates to an [OR](../querying/filters.md#or) of [Bound filter](../querying/filters.md#bound-filter). By default, Druid does not restrict the amount of numeric Bound Filters on String columns, although this situation may block other queries from running. Set this property to a smaller value to prevent Druid from running queries that have prohibitively long segment processing times. The optimal limit requires some trial and error; we recommend starting with 100.  Users who submit a query that exceeds the limit of `maxNumericInFilters` should instead rewrite their queries to use strings in the `WHERE` clause instead of numbers. For example, `WHERE someString IN (‘123’, ‘456’)`. If this value is disabled, `maxNumericInFilters` set through query context is ignored.|`-1` (disabled)|
|`druid.sql.approxCountDistinct.function`|Implementation to use for the [`APPROX_COUNT_DISTINCT` function](../querying/sql-aggregations.md). Without extensions loaded, the only valid value is `APPROX_COUNT_DISTINCT_BUILTIN` (a HyperLogLog, or HLL, based implementation). If the [DataSketches extension](../development/extensions-core/datasketches-extension.md) is loaded, this can also be `APPROX_COUNT_DISTINCT_DS_HLL` (alternative HLL implementation) or `APPROX_COUNT_DISTINCT_DS_THETA`.<br /><br />Theta sketches use significantly more memory than HLL sketches, so you should prefer one of the two HLL implementations.|`APPROX_COUNT_DISTINCT_BUILTIN`|

:::info
 Previous versions of Druid had properties named `druid.sql.planner.maxQueryCount` and `druid.sql.planner.maxSemiJoinRowsInMemory`.
 These properties are no longer available. Since Druid 0.18.0, you can use `druid.server.http.maxSubqueryRows` to control the maximum
 number of rows permitted across all subqueries.
:::

#### Broker caching

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

:::info
 Note: Even if cache is enabled, for [groupBy](../querying/groupbyquery.md) queries, segment level cache does not work on Brokers.
 See [Query caching](../querying/caching.md) for more information.
:::

#### Segment discovery

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.serverview.type`|batch or http|Segment discovery method to use. "http" enables discovering segments using HTTP instead of ZooKeeper.|http|
|`druid.broker.segment.watchedTiers`|List of strings|The Broker watches segment announcements from processes that serve segments to build a cache to relate each process to the segments it serves. This configuration allows the Broker to only consider segments being served from a list of tiers. By default, Broker considers all tiers. This can be used to partition your dataSources in specific Historical tiers and configure brokers in partitions so that they are only queryable for specific dataSources. This config is mutually exclusive from `druid.broker.segment.ignoredTiers` and at most one of these can be configured on a Broker.|none|
|`druid.broker.segment.ignoredTiers`|List of strings|The Broker watches segment announcements from processes that serve segments to build a cache to relate each process to the segments it serves. This configuration allows the Broker to ignore the segments being served from a list of tiers. By default, Broker considers all tiers. This config is mutually exclusive from `druid.broker.segment.watchedTiers` and at most one of these can be configured on a Broker.|none|
|`druid.broker.segment.watchedDataSources`|List of strings|Broker watches the segment announcements from processes serving segments to build cache of which process is serving which segments, this configuration allows to only consider segments being served from a whitelist of dataSources. By default, Broker would consider all datasources. This can be used to configure brokers in partitions so that they are only queryable for specific dataSources.|none|
|`druid.broker.segment.watchRealtimeTasks`|Boolean|The Broker watches segment announcements from processes that serve segments to build a cache to relate each process to the segments it serves. When `watchRealtimeTasks` is true, the Broker watches for segment announcements from both Historicals and realtime processes. To configure a broker to exclude segments served by realtime processes, set `watchRealtimeTasks` to false. |true|
|`druid.broker.segment.awaitInitializationOnStart`|Boolean|Whether the Broker will wait for its view of segments to fully initialize before starting up. If set to 'true', the Broker's HTTP server will not start up, and the Broker will not announce itself as available, until the server view is initialized. See also `druid.sql.planner.awaitInitializationOnStart`, a related setting.|true|

## Cache configuration

This section describes caching configuration that is common to Broker, Historical, and MiddleManager/Peon processes.

Caching could optionally be enabled on the Broker, Historical, and MiddleManager/Peon processes. See
[Broker](#broker-caching), [Historical](#historical-caching), and [Peon](#peon-caching) configuration options for how to
enable it for different processes.

Druid uses a local in-memory cache by default, unless a different type of cache is specified.
Use the `druid.cache.type` configuration to set a different kind of cache.

Cache settings are set globally, so the same configuration can be re-used
for both Broker and Historical processes, when defined in the common properties file.


### Cache type

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.cache.type`|`local`, `memcached`, `hybrid`, `caffeine`|The type of cache to use for queries. See below of the configuration options for each cache type|`caffeine`|

#### Local cache

:::info
 DEPRECATED: Use caffeine (default as of v0.12.0) instead
:::

The local cache is deprecated in favor of the Caffeine cache, and may be removed in a future version of Druid. The Caffeine cache affords significantly better performance and control over eviction behavior compared to `local` cache, and is recommended in any situation where you are using JRE 8u60 or higher.

A simple in-memory LRU cache. Local cache resides in JVM heap memory, so if you enable it, make sure you increase heap size accordingly.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.cache.sizeInBytes`|Maximum cache size in bytes. Zero disables caching.|0|
|`druid.cache.initialSize`|Initial size of the hashtable backing the cache.|500000|
|`druid.cache.logEvictionCount`|If non-zero, log cache eviction every `logEvictionCount` items.|0|

#### Caffeine cache

A highly performant local cache implementation for Druid based on [Caffeine](https://github.com/ben-manes/caffeine). Requires a JRE8u60 or higher if using `COMMON_FJP`.

##### Configuration

The following table shows the configuration options known to this module:

|`runtime.properties`|Description|Default|
|--------------------|-----------|-------|
|`druid.cache.type`| Set this to `caffeine` or leave out parameter|`caffeine`|
|`druid.cache.sizeInBytes`|The maximum size of the cache in bytes on heap. It can be configured as described in [here](human-readable-byte.md). |min(1GiB, Runtime.maxMemory / 10)|
|`druid.cache.expireAfter`|The time (in ms) after an access for which a cache entry may be expired|None (no time limit)|
|`druid.cache.cacheExecutorFactory`|The executor factory to use for Caffeine maintenance. One of `COMMON_FJP`, `SINGLE_THREAD`, or `SAME_THREAD`|ForkJoinPool common pool (`COMMON_FJP`)|
|`druid.cache.evictOnClose`|If a close of a namespace (ex: removing a segment from a process) should cause an eager eviction of associated cache values|`false`|

##### `druid.cache.cacheExecutorFactory`

The following are the possible values for `druid.cache.cacheExecutorFactory`, which controls how maintenance tasks are run:

* `COMMON_FJP` (default) use the common ForkJoinPool. Should use with [JRE 8u60 or higher](https://github.com/apache/druid/pull/4810#issuecomment-329922810). Older versions of the JRE may have worse performance than newer JRE versions.
* `SINGLE_THREAD` Use a single-threaded executor.
* `SAME_THREAD` Cache maintenance is done eagerly.

##### Metrics

In addition to the normal cache metrics, the caffeine cache implementation also reports the following in both `total` and `delta`:

|Metric|Description|Normal value|
|------|-----------|------------|
|`query/cache/caffeine/*/requests`|Count of hits or misses.|hit + miss|
|`query/cache/caffeine/*/loadTime`|Length of time caffeine spends loading new values (unused feature).|0|
|`query/cache/caffeine/*/evictionBytes`|Size in bytes that have been evicted from the cache|Varies, should tune cache `sizeInBytes` so that `sizeInBytes`/`evictionBytes` is approximately the rate of cache churn you desire.|

##### Memcached

Uses memcached as cache backend. This allows all processes to share the same cache.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.cache.expiration`|Memcached [expiration time](https://code.google.com/p/memcached/wiki/NewCommands#Standard_Protocol).|2592000 (30 days)|
|`druid.cache.timeout`|Maximum time in milliseconds to wait for a response from Memcached.|500|
|`druid.cache.hosts`|Comma separated list of Memcached hosts `<host:port>`. Need to specify all nodes when `druid.cache.clientMode` is set to static. Dynamic mode [automatically identifies nodes in your cluster](https://docs.aws.amazon.com/AmazonElastiCache/latest/mem-ug/AutoDiscovery.html) so just specifying the configuration endpoint and port is fine.|none|
|`druid.cache.maxObjectSize`|Maximum object size in bytes for a Memcached object.|52428800 (50 MiB)|
|`druid.cache.memcachedPrefix`|Key prefix for all keys in Memcached.|druid|
|`druid.cache.numConnections`| Number of memcached connections to use.|1|
|`druid.cache.protocol`| Memcached communication protocol. Can be binary or text.|binary|
|`druid.cache.locator`| Memcached locator. Can be consistent or `array_mod`.|consistent|
|`druid.cache.enableTls`|Enable TLS based connection for Memcached client. Boolean.|false|
|`druid.cache.clientMode`|Client Mode. Static mode requires the user to specify individual cluster nodes. Dynamic mode uses [AutoDiscovery](https://docs.aws.amazon.com/AmazonElastiCache/latest/mem-ug/AutoDiscovery.HowAutoDiscoveryWorks.html) feature of AWS Memcached. String. ["static"](https://docs.aws.amazon.com/AmazonElastiCache/latest/mem-ug/AutoDiscovery.Manual.html) or ["dynamic"](https://docs.aws.amazon.com/AmazonElastiCache/latest/mem-ug/AutoDiscovery.Using.ModifyApp.Java.html)|static|
|`druid.cache.skipTlsHostnameVerification`|Skip TLS Hostname Verification. Boolean.|true| 

#### Hybrid

Uses a combination of any two caches as a two-level L1 / L2 cache.
This may be used to combine a local in-memory cache with a remote memcached cache.

Cache requests will first check L1 cache before checking L2.
If there is an L1 miss and L2 hit, it will also populate L1.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.cache.l1.type`|The type of cache to use for L1 cache. See `druid.cache.type` configuration for valid types.|`caffeine`|
|`druid.cache.l2.type`|The type of cache to use for L2 cache. See `druid.cache.type` configuration for valid types.|`caffeine`|
|`druid.cache.l1.*`|Any property valid for the given type of L1 cache can be set using this prefix. For instance, if you are using a `caffeine` L1 cache, specify `druid.cache.l1.sizeInBytes` to set its size.|defaults are the same as for the given cache type|
|`druid.cache.l2.*`|Prefix for L2 cache settings, see description for L1.|defaults are the same as for the given cache type|
|`druid.cache.useL2`|A boolean indicating whether to query L2 cache, if it's a miss in L1. It makes sense to configure this to `false` on Historical processes, if L2 is a remote cache like `memcached`, and this cache also used on brokers, because in this case if a query reached Historical it means that a broker didn't find corresponding results in the same remote cache, so a query to the remote cache from Historical is guaranteed to be a miss.|`true`|
|`druid.cache.populateL2`|A boolean indicating whether to put results into L2 cache.|`true`|

## General query configuration

This section describes configurations that control behavior of Druid's query types, applicable to Broker, Historical, and MiddleManager processes.

### Overriding default query context values

You can override any [query context general parameter](../querying/query-context.md#general-parameters) default value by setting the runtime property in the format of `druid.query.default.context.{query_context_key}`.
The `druid.query.default.context.{query_context_key}` runtime property prefix applies to all current and future query context keys, the same as how query context parameter passed with the query works. You can override the runtime property value if the value for the same key is specified in the query contexts.

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
|`druid.query.search.searchStrategy`|Default search query strategy.|`useIndexes`|

### SegmentMetadata query config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.segmentMetadata.defaultHistory`|When no interval is specified in the query, use a default interval of defaultHistory before the end time of the most recent segment, specified in ISO8601 format. This property also controls the duration of the default interval used by `GET` `/druid/v2/datasources/{dataSourceName}` interactions for retrieving datasource dimensions and metrics.|`P1W`|
|`druid.query.segmentMetadata.defaultAnalysisTypes`|This can be used to set the Default Analysis Types for all segment metadata queries, this can be overridden when making the query|`["cardinality", "interval", "minmax"]`|

### GroupBy query config

This section describes the configurations for groupBy queries. You can set the runtime properties in the `runtime.properties` file on Broker, Historical, and MiddleManager processes. You can set the query context parameters through the [query context](../querying/query-context.md).

Supported runtime properties:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.groupBy.maxSelectorDictionarySize`|Maximum amount of heap space (approximately) to use for per-segment string dictionaries. See [groupBy memory tuning and resource limits](../querying/groupbyquery.md#memory-tuning-and-resource-limits) for details.|100000000|
|`druid.query.groupBy.maxMergingDictionarySize`|Maximum amount of heap space (approximately) to use for per-query string dictionaries. When the dictionary exceeds this size, a spill to disk will be triggered. See [groupBy memory tuning and resource limits](../querying/groupbyquery.md#memory-tuning-and-resource-limits) for details.|100000000|
|`druid.query.groupBy.maxOnDiskStorage`|Maximum amount of disk space to use, per-query, for spilling result sets to disk when either the merging buffer or the dictionary fills up. Queries that exceed this limit will fail. Set to zero to disable disk spilling.|0 (disabled)|
|`druid.query.groupBy.defaultOnDiskStorage`|Default amount of disk space to use, per-query, for spilling the result sets to disk when either the merging buffer or the dictionary fills up. Set to zero to disable disk spilling for queries which don't override `maxOnDiskStorage` in their context.|`druid.query.groupBy.maxOnDiskStorage`|

Supported query contexts:

|Key|Description|
|---|-----------|
|`maxSelectorDictionarySize`|Can be used to lower the value of `druid.query.groupBy.maxMergingDictionarySize` for this query.|
|`maxMergingDictionarySize`|Can be used to lower the value of `druid.query.groupBy.maxMergingDictionarySize` for this query.|
|`maxOnDiskStorage`|Can be used to set `maxOnDiskStorage` to a value between 0 and `druid.query.groupBy.maxOnDiskStorage` for this query. If this query context override exceeds `druid.query.groupBy.maxOnDiskStorage`, the query will use `druid.query.groupBy.maxOnDiskStorage`. Omitting this from the query context will cause the query to use `druid.query.groupBy.defaultOnDiskStorage` for `maxOnDiskStorage`|


### Advanced configurations

Supported runtime properties:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.groupBy.singleThreaded`|Merge results using a single thread.|false|
|`druid.query.groupBy.bufferGrouperInitialBuckets`|Initial number of buckets in the off-heap hash table used for grouping results. Set to 0 to use a reasonable default (1024).|0|
|`druid.query.groupBy.bufferGrouperMaxLoadFactor`|Maximum load factor of the off-heap hash table used for grouping results. When the load factor exceeds this size, the table will be grown or spilled to disk. Set to 0 to use a reasonable default (0.7).|0|
|`druid.query.groupBy.forceHashAggregation`|Force to use hash-based aggregation.|false|
|`druid.query.groupBy.intermediateCombineDegree`|Number of intermediate processes combined together in the combining tree. Higher degrees will need less threads which might be helpful to improve the query performance by reducing the overhead of too many threads if the server has sufficiently powerful CPU cores.|8|
|`druid.query.groupBy.numParallelCombineThreads`|Hint for the number of parallel combining threads. This should be larger than 1 to turn on the parallel combining feature. The actual number of threads used for parallel combining is min(`druid.query.groupBy.numParallelCombineThreads`, `druid.processing.numThreads`).|1 (disabled)|

Supported query contexts:

|Key|Description|Default|
|---|-----------|-------|
|`groupByIsSingleThreaded`|Overrides the value of `druid.query.groupBy.singleThreaded` for this query.| |
|`bufferGrouperInitialBuckets`|Overrides the value of `druid.query.groupBy.bufferGrouperInitialBuckets` for this query.|none|
|`bufferGrouperMaxLoadFactor`|Overrides the value of `druid.query.groupBy.bufferGrouperMaxLoadFactor` for this query.|none|
|`forceHashAggregation`|Overrides the value of `druid.query.groupBy.forceHashAggregation`|none|
|`intermediateCombineDegree`|Overrides the value of `druid.query.groupBy.intermediateCombineDegree`|none|
|`numParallelCombineThreads`|Overrides the value of `druid.query.groupBy.numParallelCombineThreads`|none|
|`sortByDimsFirst`|Sort the results first by dimension values and then by timestamp.|false|
|`forceLimitPushDown`|When all fields in the orderby are part of the grouping key, the broker will push limit application down to the Historical processes. When the sorting order uses fields that are not in the grouping key, applying this optimization can result in approximate results with unknown accuracy, so this optimization is disabled by default in that case. Enabling this context flag turns on limit push down for limit/orderbys that contain non-grouping key columns.|false|

#### Expression processing configurations

|Key|Description|Default|
|---|-----------|-------|
|`druid.expressions.useStrictBooleans`|Controls the behavior of Druid boolean operators and functions, if set to `true` all boolean values are either `1` or `0`. This configuration has been deprecated and will be removed in a future release, taking on the `true` behavior. See [expression documentation](../querying/math-expr.md#logical-operator-modes) for more information.|true|
|`druid.expressions.allowNestedArrays`|If enabled, Druid array expressions can create nested arrays. This configuration has been deprecated and will be removed in a future release, taking on the `true` behavior.|true|

### Router

#### Router process configs

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current process. This is used to advertise the current processes location as reachable from another process and should generally be specified such that `http://${druid.host}/` could actually talk to this process|`InetAddress.getLocalHost().getCanonicalHostName()`|
|`druid.bindOnHost`|Indicating whether the process's internal jetty server bind on `druid.host`. Default is false, which means binding to all interfaces.|false|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8888|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.enableTlsPort](../operations/tls-support.md) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative Integer.|9088|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|`druid/router`|

#### Runtime configuration

|Property|Description|Default|
|--------|-----------|-------|
|`druid.router.defaultBrokerServiceName`|The default Broker to connect to in case service discovery fails.|`druid/broker`|
|`druid.router.tierToBrokerMap`|Queries for a certain tier of data are routed to their appropriate Broker. This value should be an ordered JSON map of tiers to Broker names. The priority of Brokers is based on the ordering.|`{"_default_tier": "<defaultBrokerServiceName>"}`|
|`druid.router.defaultRule`|The default rule for all datasources.|`_default`|
|`druid.router.pollPeriod`|How often to poll for new rules.|`PT1M`|
|`druid.router.sql.enable`|Enable routing of SQL queries using strategies. When`true`, the Router uses the  strategies defined in `druid.router.strategies` to determine the broker service for a given SQL query. When `false`, the Router uses the `defaultBrokerServiceName`.|`false`|
|`druid.router.strategies`|Please see [Router Strategies](../design/router.md#router-strategies) for details.|`[{"type":"timeBoundary"},{"type":"priority"}]`|
|`druid.router.avatica.balancer.type`|Class to use for balancing Avatica queries across Brokers. Please see [Avatica Query Balancing](../design/router.md#avatica-query-balancing).|`rendezvousHash`|
|`druid.router.managementProxy.enabled`|Enables the Router's [management proxy](../design/router.md#router-as-management-proxy) functionality.|false|
|`druid.router.http.numConnections`|Size of connection pool for the Router to connect to Broker processes. If there are more queries than this number that all need to speak to the same process, then they will queue up.|`20`|
|`druid.router.http.eagerInitialization`|Indicates that http connections from Router to Broker should be eagerly initialized. If set to true, `numConnections` connections are created upon initialization|`true`|
|`druid.router.http.readTimeout`|The timeout for data reads from Broker processes.|`PT15M`|
|`druid.router.http.numMaxThreads`|Maximum number of worker threads to handle HTTP requests and responses|`max(10, ((number of cores * 17) / 16 + 2) + 30)`|
|`druid.router.http.numRequestsQueued`|Maximum number of requests that may be queued to a destination|`1024`|
|`druid.router.http.requestBuffersize`|Size of the content buffer for receiving requests. These buffers are only used for active connections that have requests with bodies that will not fit within the header buffer|`8 * 1024`|
