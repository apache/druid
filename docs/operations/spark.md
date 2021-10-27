---
id: spark
title: "Apache Spark Reader and Writer"
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

# Apache Spark Reader and Writer for Druid

## Reader
The reader reads Druid segments from deep storage into Spark. It locates the segments to read and determines their
schema if not provided by querying the brokers for the relevant metadata but otherwise does not interact with a running
Druid cluster.

Sample Code:
```scala
import org.apache.druid.spark.DruidDataFrameReader

val deepStorageConfig = new LocalDeepStorageConfig().storageDirectory("/mnt/druid/druid-segments/")

sparkSession
  .read
  .brokerHost("localhost")
  .brokerPort(8082)
  .metadataDbType("mysql")
  .metadataUri("jdbc:mysql://druid.metadata.server:3306/druid")
  .metadataUser("druid")
  .metadataPassword("diurd")
  .dataSource("dataSource")
  .deepStorage(deepStorageConfig)
  .druid()
```

Alternatively, the reader can be configured via a properties map with no additional import needed:
```scala
val properties = Map[String, String](
  "metadata.dbType" -> "mysql",
  "metadata.connectUri" -> "jdbc:mysql://druid.metadata.server:3306/druid",
  "metadata.user" -> "druid",
  "metadata.password" -> "diurd",
  "broker.host" -> "localhost",
  "broker.port" -> 8082,
  "table" -> "dataSource",
  "reader.deepStorageType" -> "local",
  "local.storageDirectory" -> "/mnt/druid/druid-segments/"
)

sparkSession
  .read
  .format("druid")
  .options(properties)
  .load()
```

If you know the schema of the Druid data source you're reading from, you can save needing to determine the schema via
calls to the broker with
```scala
sparkSession
  .read
  .format("druid")
  .schema(schema)
  .options(properties)
  .load()
```

Filters should be applied to the read-in data frame before any [Spark actions](http://spark.apache.org/docs/2.4.7/api/scala/index.html#org.apache.spark.sql.Dataset)
are triggered, to allow predicates to be pushed down to the reader and avoid full scans of the underlying Druid data.

## Plugin Registries and Druid Extension Support
One of Druid's strengths is its extensibility. Since these Spark readers and writers will not execute on a Druid cluster
and won't have the ability to dynamically load classes or integrate with Druid's Guice injectors, Druid extensions can't
be used directly. Instead, these connectors use a plugin registry architecture, including default plugins that support
most functionality in `extensions-core`. Custom plugins consisting of a string name and one or more serializable
generator functions must be registered before the first Spark action which would depend on them is called.

### ComplexMetricRegistry
The `ComplexMetricRegistry` provides support for serializing and deserializing complex metric types between Spark and
Druid. Support for complex metric types in Druid core extensions is provided out of the box.

Users wishing to override the default behavior or who need to add support for additional complex metric types can
use the `ComplexMetricRegistry.register` functions to associate serde functions with a given complex metric type. The
name used to register custom behavior must match the complex metric type name reported by Druid.
**Note that custom plugins must be registered with both the executors and the Spark driver.**

### DynamicConfigProviderRegistry
The `DynamicConfigProviderRegistry` provides support for deserializing dynamic configuration values via Druid
[DynamicConfigProviders](dynamic-config-provider.md).

Custom providers should be registered on the driver if used to supply passwords for the backing metadata database and on
the executors if used to supply deep storage credentials or keys.

### SegmentReaderRegistry
The `SegmentReaderRegistry` provides support for reading segments from deep storage. Local, HDFS, GCS, S3, and Azure
Storage deep storage implementations are supported by default.

Users wishing to override the default behavior or who need to add support for additional deep storage implementations
can use either `SegmentReaderRegistry.registerInitializer` (to provide any necessary Jackson configuration for
deserializing a `LoadSpec` object from a segment load spec) or `SegmentReaderRegistry.registerLoadFunction` (to register
a function for creating a URI from a segment load spec). These two functions correspond to the first and second approach
[outlined below](#deep-storage). **Note that custom plugins must be registered on the executors, not the Spark driver.**

### SQLConnectorRegistry
The `SQLConnectorRegistry` provides support for configuring connectors to Druid metadata databases. Support for MySQL,
PostgreSQL, and Derby databases are provided out of the box.

Users wishing to override the default behavior or who need to add support for additional metadata database
implementations can use the `SQLConnectorRegistry.register` function. Custom connectors should be registered on the
driver.

## Deploying to a Spark cluster
This extension can be run on a Spark cluster in one of two ways: bundled as part of an application jar or uploaded as
a library jar to a Spark cluster and included in the classpath provided to Spark applications by the application
manager. If the second approach is used, this extension should be built with
`mvn clean package -pl spark` and the resulting jar `druid-spark-<VERSION>.jar`
uploaded to the Spark cluster. Application jars should then be built with a compile-time dependency on
`org.apache.druid:druid-spark` (e.g. marked as `provided` in Maven or with `compileOnly` in Gradle).

## Configuration Reference

### Metadata Client Configs
The properties used to configure the client that interacts with the Druid metadata server directly. Used by both reader
and the writer. The `metadataPassword` property can either be provided as a string that will be used as-is or can be
provided as a serialized DynamicConfigProvider that will be resolved when the metadata client is first instantiated. If
a  custom DynamicConfigProvider is used, be sure to register the provider with the DynamicConfigProviderRegistry before use.

|Key|Description|Required|Default|
|---|-----------|--------|-------|
|`metadata.dbType`|The metadata server's database type (e.g. `mysql`)|Yes||
|`metadata.host`|The metadata server's host name|If using derby|`localhost`|
|`metadata.port`|The metadata server's port|If using derby|1527|
|`metadata.connectUri`|The URI to use to connect to the metadata server|If not using derby||
|`metadata.user`|The user to use when connecting to the metadata server|If required by the metadata database||
|`metadata.password`|The password to use when connecting to the metadata server. This can optionally be a serialized instance of a Druid DynamicConfigProvider or a plain string|If required by the metadata database||
|`metadata.dbcpProperties`|The connection pooling properties to use when connecting to the metadata server|No||
|`metadata.baseName`|The base name used when creating Druid metadata tables|No|`druid`|

### Druid Client Configs
The configuration properties used to query the Druid cluster for segment metadata. Only used in the reader.

|Key|Description|Required|Default|
|---|-----------|--------|-------|
|`broker.host`|The hostname of a broker in the Druid cluster to read from|No|`localhost`|
|`broker.port`|The port of the broker in the Druid cluster to read from|No|8082|
|`broker.numRetries`|The number of times to retry a timed-out segment metadata request|No|5|
|`broker.retryWaitSeconds`|How long (in seconds) to wait before retrying a timed-out segment metadata request|No|5|
|`broker.timeoutMilliseconds`|How long (in milliseconds) to wait before timing out a segment metadata request|No|300000|

### Reader Configs
The properties used to configure the DataSourceReader when reading data from Druid in Spark.

|Key|Description|Required|Default|
|---|-----------|--------|-------|
|`table`|The Druid data source to read from|Yes||
|`reader.deepStorageType`|The type of deep storage used to back the target Druid cluster|No|`local`|
|`reader.segments`|A hard-coded list of Druid segments to read. If set, the table and druid client configurations are ignored and the specified segments are read directly. Must be deserializable into Druid DataSegment instances|No|
|`reader.useCompactSketches`|Controls whether or not compact representations of complex metrics are used (only for metrics that support compact forms)|No|False|
|`reader.useDefaultValueForNull`|If true, use Druid's default values for null values. If false, explicitly use null for null values. See the [Druid configuration reference](../configuration/index.html#sql-compatible-null-handling) for more details|No|True|
|`reader.useSparkConfForDeepStorage`|If true, use the Spark job's configuration to set up access to deep storage|No|False|
|`reader.vectorize`|**Experimental!** If true, reads data from segments in batches if possible|No|False|
|`reader.batchSize`|**Experimental!** The number of rows to read in one batch if `reader.vectorize` is set to true|No|512|

#### Deep Storage
There are two ways to configure the DataSourceReader's access to deep storage.

1. Users can directly configure the necessary keys and properties following the [deep storage configuration options](#deep-storage-configs) below.
2. Users can delegate to the Spark application config by setting `reader.useSparkConfForDeepStorage` to true.

In the second case, the reader will construct a URI from the load spec of each segment to read and pull the segment from
that URI using a FileSystem created from the calling Spark application's Hadoop Configuration. This case is useful for
users running on clusters that rely on GCS ADCs or AWS IAM roles for machine authorization to GCS/S3, or for clusters
that manage access keys for their users. Currently local, HDFS, S3, and GCS deep storage implementation are supported
out of the box for this approach (Azure users will need to use the first approach or register a custom load function
via the `SegmentReaderRegistry`).

If other deep storage implementations are used or custom behavior is required, users can register plugins providing the
additional functionality with the `SegmentReaderRegistry`.


#### Vectorized Reads
**Experimental!** The DataSourceReader can optionally attempt to read data from segments in batches.
Spark 2.4 does not take full advantage of the capability, but vectorized reads may speed up data load
times considerably. The default value for `reader.batchSize` isn't much more than a SWAG, so please
test your workload with a few different batch sizes to determine the value that best balances speed
and memory usage for your use case (and then let us know what worked best for you so we can improve
the documentation!).

### Deep Storage Configs
The configuration properties used when interacting with deep storage systems directly.

#### Local Deep Storage Config
`deepStorageType` = `local`

|Key|Description|Required|Default|
|---|-----------|--------|-------|
|`local.storageDirectory`|The location to write segments out to|Yes|`/tmp/druid/localStorage`|

#### HDFS Deep Storage Config
`deepStorageType` = `hdfs`

|Key|Description|Required|Default|
|---|-----------|--------|-------|
|`hdfs.storageDirectory`|The location to write segments out to|Yes||
|`hdfs.hadoopConf`|A Base64 encoded representation of dumping a Hadoop Configuration to a byte array via `.write`|Yes||

#### S3 Deep Storage Config
`deepStorageType` = `s3`

These configs generally shadow the [Connecting to S3 configuration](../development/extensions-core/s3.md#connecting-to-s3-configuration)
section of the Druid s3 extension doc, including in the inconsistent use of `disable` vs `enable` as boolean property
name prefixes

|Key|Description|Required|Default|
|---|-----------|--------|-------|
|`s3.bucket`|The S3 bucket to write segments to|Yes||
|`s3.baseKey`|The base key to prefix segments with when writing to S3|Yes||
|`s3.maxListingLength`|The maximum number of input files matching a prefix to retrieve or delete in one call|No|1000/1024|
|`s3.disableAcl`|Whether or not to disable ACLs on the output segments. If this is false, additional S3 permissions are required|No|False|
|`s3.useS3aSchema`|Whether or not to use the `s3a` filesystem when writing segments to S3.|No|True ***(note this is the opposite of the druid-s3 extension!)***|
|`s3.accessKey`|The S3 access key. See [S3 authentication methods](../development/extensions-core/s3.md#s3-authentication-methods) for more details||
|`s3.secretKey`|The S3 secret key. See [S3 authentication methods](../development/extensions-core/s3.md#s3-authentication-methods) for more details||
|`s3.fileSessionCredentials`|The path to a properties file containing S3 session credentials. See [S3 authentication methods](../development/extensions-core/s3.md#s3-authentication-methods) for more details||
|`s3.proxy.host`|The proxy host to connect to S3 through|No||
|`s3.proxy.port`|The proxy port to connect to S3 through|No||
|`s3.proxy.username`|The user name to use when connecting through a proxy|No||
|`s3.proxy.password`|The password to use when connecting through a proxy. Plain string|No||
|`s3.endpoint.url`|The S3 service endpoint to connect to||
|`s3.endpoint.signingRegion`|The region to use for singing requests (e.g. `us-west-1`)|||
|`s3.protocol`|The communication protocol to use when communicating with AWS. This configuration is ignored if `s3EndpointConfigUrl` includes the protocol| |`https`|
|`s3.disableChunkedEncoding`|Whether or not to disable chunked encoding| |False| <!-- Keeping the irritating inconsistency in property naming here to match the S3 extension names -->
|`s3.enablePathStyleAccess`|Whether or not to enable path-style access| |False|
|`s3.forceGlobalBucketAccessEnabled`|Whether or not to force global bucket access| |False|
|`s3.sse.type`|The type of Server Side Encryption to use (`s3`, `kms`, or `custom`). If not set, server side encryption will not be used||
|`s3.sse.kms.keyId`|The keyId to use for `kms` server side encryption|Only if `s3ServerSideEncryptionTypeKey` is `kms`|
|`s3.sse.custom.base64EncodedKey`|The base-64 encoded key to use for `custom` server side encryption|Only if `s3ServerSideEncryptionTypeKey` is `custom`|

#### GCS Deep Storage Config
`deepStorageType` = `google`

These configs shadow the [Google Cloud Storage Extension](../development/extensions-core/google.md) docs. The environment variable
`GOOGLE_APPLICATION_CREDENTIALS` must be set to write segments to GCS.

|Key|Description|Required|Default|
|---|-----------|--------|-------|
|`google.bucket`|The GCS bucket to write segments to|Yes||
|`google.prefix`|The base key to prefix segments with when writing to GCS|Yes||
|`google.maxListingLength`|The maximum number of input files matching a prefix to retrieve or delete in one call|No|1024|

#### Azure Deep Storage Config
`deepStorageType` = `azure`

Writing data to Azure deep storage is currently experimental. It should work but is untested. If you use this connector
to write segments to Azure, please update this documentation.

|Key|Description|Required|Default|
|---|-----------|--------|-------|
|`azure.account`|The Azure Storage account name to use|Yes||
|`azure.key`|The key for the Azure Storage account used|Yes||
|`azure.container`|The Azure Storage container name|Yes||
|`azure.maxTries`|The number of tries before failing an Azure operation|No|3|
|`azure.protocol`|The communication protocol to use when interacting with Azure Storage|No|`https`|
|`azure.prefix`|The string to prepend to all segment blob names written to Azure Storage|No|`""`|
|`azure.maxListingLength`|The maximum number of input files matching a prefix to retrieve or delete in one call|No|1024|

#### Custom Deep Storage Implementations

The Spark-Druid extension includes support for writing segments to the above deep storage options. If you want to write
to a different deep storage option or use a different implementation for one of the options above, you can implement and
register a plugin with the SegmentWriterRegistry. Any properties specified in the options map specified in the `write()`
call will be passed along to the plugin functions.