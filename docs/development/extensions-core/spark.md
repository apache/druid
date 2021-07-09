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

sparkSession
  .read
  .brokerHost("localhost")
  .brokerPort(8082)
  .metadataDbType("mysql")
  .metadataUri("jdbc:mysql://druid.metadata.server:3306/druid")
  .metadataUser("druid")
  .metadataPassword("diurd")
  .dataSource("dataSource")
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
  "table" -> "dataSource"
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

## Writer
The writer writes Druid segments directly to deep storage and then updates the Druid cluster's metadata, bypassing the
running cluster entirely.

**Note**: SaveModes do not currently map cleanly to the writer's behavior because this connector does not provide Druid
operational functionality. That work is left to the Druid coordinator.

|SaveMode|Behavior|
|--------|--------|
|`SaveMode.Append`|Unsupported|
|`SaveMode.ErrorIfExists`|Throws an error if the target datasource already exists|
|`SaveMode.Ignore`|Does nothing if the target datasource already exists|
|`SaveMode.Overwrite`|Writes all data to the target datasource. If segments already exist for the target intervals, they will be overshadowed if the new version is higher than the existing version. All other segments are unaffected (e.g. the target datasource will _not_ be truncated)|

Sample Code:
```scala
import org.apache.druid.spark.DruidDataFrameWriter

val deepStorageConfig = new LocalDeepStorageConfig().storageDirectory("/mnt/druid/druid-segments/")

df
  .write
  .metadataDbType("mysql")
  .metadataUri("jdbc:mysql://druid.metadata.server:3306/druid")
  .metadataUser("druid")
  .metadataPassword("diurd")
  .version(1)
  .deepStorage(deepStorageConfig)
  .mode(SaveMode.Overwrite)
  .dataSource("dataSource")
  .druid()
```

Like the reader, you can also configure the writer via a properties map instead of using the semi-typed wrapper
```scala
val metadataProperties = Map[String, String](
  "metadata.dbType" -> "mysql",
  "metadata.connectUri" -> "jdbc:mysql://druid.metadata.server:3306/druid",
  "metadata.user" -> "druid",
  "metadata.password" -> "diurd"
)

val writerConfigs = Map[String, String] (
  "table" -> "dataSource",
  "writer.version" -> 1,
  "writer.deepStorageType" -> "local",
  "writer.storageDirectory" -> "/mnt/druid/druid-segments/"
)

df
  .write
  .format("druid")
  .mode(SaveMode.Overwrite)
  .options(Map[String, String](writerConfigs ++ metadataProperties))
  .save()
```

### Partitioning & `PartitionMap`s
The segments written by this writer are controlled by the calling dataframe's internal partitioning.
If there are many small partitions, or the dataframe's partitions span output intervals, then many
small Druid segments will be written with poor overall roll-up. If the dataframe's partitions are
skewed, then the Druid segments produced will also be skewed. To avoid this, users should take
care to properly partition their dataframes prior to calling `.write()`. Additionally, for some shard
specs such as `HashBasedNumberedShardSpec` or `SingleDimensionShardSpec` which require a higher-level
view of the data than can be obtained from a partition, callers should pass along a `PartitionMap`
containing metadata for each Spark partition. This map should have the signature `Map[Int, Map[String, String]]`,
where the keys are a Spark partition id and the values are maps from property names to string values
(see the provided partitioners for examples). This partition map can be serialized using the
`PartitionMapProvider.serializePartitionMap` method and passed along with the writer options using the
`partitionMap` key.

If a "simpler" shard spec such as `NumberedShardSpec` or `LinearShardSpec` is used, a `partitionMap`
can be provided but is unnecessary unless the name of a segment's directory on deep storage should
match the segment's id exactly. The writer will rationalize shard specs within time chunks to
ensure data is atomically loaded in Druid. Either way, callers should still take care when partitioning
dataframes to  write to avoid ingesting skewed data or too many small segments.

#### Provided Partitioners

For initial development work, sample `HashBasedNumberedPartitioner`, `SingleDimensionPartitioner`, and
`NumberedPartitioner` partitioners are provided. These partitioners all implement the `PartitionMapProvider`
interface, so once they've partitioned a dataframe the corresponding partition map can be retrieved via the
`getPartitionMap` method. If calling code imports `org.apache.druid.spark.DruidDataFrame`, dataframes will
have the additional methods `hashPartitionAndWrite`, `rangePartitionAndWrite`, and `partitionAndWrite`
which will partition the dataframe using the respective partitioner, set the appropriate writer
options including passing the partition map, and return a DataFrameWriter object for final configuration
before writing. **Please note that these writers are significantly less efficient than partitioning a
dataframe using your knowledge of the underlying data.** These partitioners use the column names
`__timeBucket`, `__rank`, `__partitionNum`, and `__partitionKey` internally, so they will not work
if a source dataframe uses those column names as well. If the source dataframe uses those column names,
`HashedNumberedSegmentPartitioner` or a custom partitioner will need to be used.

|Target Shard Spec|Provided Partitioner|`DruidDataFrame` Convenience Method|
|-----------------|-----------|-----------|
|`HashBasedNumberedShardSpec`|`HashBasedNumberedPartitioner`|`df.hashPartitionAndWrite(...)`|
|`SingleDimensionShardSpec`|`SingleDimensionPartitioner`|`df.rangePartitionAndWrite(...)`|
|`NumberedShardSpec`|`NumberedPartitioner`|`df.partitionAndWrite(...)`|
|`HashBasedNumberedShardSpec`|`HashedNumberedSegmentPartitioner`|None|

## Plugin Registries and Druid Extension Support
One of Druid's strengths is its extensibility. Since these Spark readers and writers will not execute on a Druid cluster
and won't have the ability to dynamically load classes or integrate with Druid's Guice injectors, Druid extensions can't
be used directly. Instead, these connectors use a plugin registry architecture, including default plugins that support
most functionality in `extensions-core`. Custom plugins consisting of a string name and one or more serializable
generator functions must be registered before the first Spark action which would depend on them is called.

## Deploying to a Spark cluster
This extension can be run on a Spark cluster in one of two ways: bundled as part of an application jar or uploaded as
a library jar to a Spark cluster and included in the classpath provided to Spark applications by the application
manager. If the second approach is used, this extension should be built with
`mvn clean package -pl extensions-core/spark-extensions` and the resulting jar `druid-spark-extensions-<VERSION>.jar`
uploaded to the Spark cluster. Application jars should then be built with a compile-time dependency on
`org.apache.druid.extensions:druid-spark-extensions` (e.g. marked as `provided` in Maven or with `compile` in Gradle).

## Configuration Reference

### Metadata Client Configs
The properties used to configure the client that interacts with the Druid metadata server directly. Used by both reader
and the writer. The `metadataPassword` property can either be provided as a string that will be used as-is or can be
provided as a serialized PasswordProvider that will be resolved when the metadata client is first instantiated. If a
custom PasswordProvider is used, be sure to register the provider with the PasswordProviderRegistry before use.

|Key|Description|Required|Default|
|---|-----------|--------|-------|
|`metadata.dbType`|The metadata server's database type (e.g. `mysql`)|Yes||
|`metadata.host`|The metadata server's host name|If using derby|`localhost`|
|`metadata.port`|The metadata server's port|If using derby|1527|
|`metadata.connectUri`|The URI to use to connect to the metadata server|If not using derby||
|`metadata.user`|The user to use when connecting to the metadata server|If required by the metadata database||
|`metadata.password`|The password to use when connecting to the metadata server. This can optionally be a serialized instance of a Druid PasswordProvider or a plain string|If required by the metadata database||
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
|`reader.segments`|A hard-coded list of Druid segments to read. If set, all other configurations are ignored and the specified segments are read directly. Must be deserializable into Druid DataSegment instances|No|
|`reader.useCompactSketches`|Controls whether or not compact representations of complex metrics are used (only for metrics that support compact forms)|No|False|
|`reader.useDefaultValueForNull`|If true, use Druid's default values for null values. If false, explicitly use null for null values. See the [Druid configuration reference](../../configuration/index.html#sql-compatible-null-handling) for more details|No|True|
|`reader.vectorize`|**Experimental!** If true, reads data from segments in batches if possible|No|False|
|`reader.batchSize`|**Experimental!** The number of rows to read in one batch if `reader.vectorize` is set to true|No|512|

#### Vectorized Reads
**Experimental!** The DataSourceReader can optionally attempt to read data from segments in batches.
Spark 2.4 does not take full advantage of the capability, but vectorized reads may speed up data load
times considerably. The default value for `reader.batchSize` isn't much more than a SWAG, so please
test your workload with a few different batch sizes to determine the value that best balances speed
and memory usage for your use case (and then let us know what worked best for you so we can improve
the documentation!).


### Writer Configs
The properties used to configure the DataSourceWriter when writing data to Druid from Spark. See the
[ingestion specs documentation](../../ingestion/index.md#ingestion-specs) for more details.

|Key|Description|Required|Default|
|---|-----------|--------|-------|
|`table`|The Druid data source to write to|Yes||
|`writer.deepStorageType`|The type of deep storage used to back the target Druid cluster|No|`local`|
|`writer.version`|The version of the segments to be written|No|The current Unix epoch time|
|`writer.dimensions`|A list of the dimensions to write to Druid. If not set, all dimensions in the dataframe that aren't either explicitly set as metric or timestamp columns or excluded via `excludedDimensions` will be used|No||
|`writer.metrics`|The [metrics spec](../../ingestion/index.md#metricsspec) used to define the metrics for the segments written to Druid. `fieldName` must match a column in the source dataframe|No|`[]`|
|`writer.excludedDimensions`|A comma-delimited list of the columns in the data frame to exclude when writing to Druid. Ignored if `dimensions` is set|No||
|`writer.segmentGranularity`|The chunking [granularity](../../querying/granularities.md) of the Druid segments written (e.g. what granularity to partition the output segments by on disk)|No|`all`|
|`writer.queryGranularity`|The resolution [granularity](../../querying/granularities.md) of rows _within_ the Druid segments written|No|`none`|
|`writer.partitionMap`|A mapping between partitions of the source Spark dataframe and the necessary information for generating Druid segment partitions from the Spark partitions. Has the type signature `Map[Int, Map[String, String]]`|No||
|`writer.timestampColumn`|The Spark dataframe column to use as a timestamp for each record|No|`ts`|
|`writer.timestampFormat`|The format of the timestamps in `timestampColumn`|No|`auto`|
|`writer.shardSpecType`|The type of shard spec used to partition the segments produced|No|`numbered`|
|`writer.rollUpSegments`|Whether or not to roll up segments produced|No|True|
|`writer.rowsPerPersist`|How many rows to hold in memory before flushing intermediate indices to disk|No|2000000|
|`writer.rationalizeSegments`|Whether or not to rationalize segments to ensure contiguity and completeness|No|True if `partitionMap` is not set, False otherwise|
|`writer.useCompactSketches`|Controls whether or not compact representations of complex metrics are used (only for metrics that support compact forms)|No|False|
|`writer.useDefaultValueForNull`|If true, use Druid's default values for null values. If false, explicitly use null for null values. See the [Druid configuration reference](../../configuration/index.html#sql-compatible-null-handling) for more details|No|True|

`writer.dimensions` may be either a comma-delimited list of column names _or_ a JSON list of Druid DimensionSchema
objects (i.e. the `dimensions` section of a `DimensionSpec`). If DimensionSchemas are provided, dimension types must
match the type of the corresponding Spark columns exactly. Otherwise, an IllegalArgumentException will be thrown.

Users can also specify properties to be passed along to the [IndexSpec](../../ingestion/index.md#indexspec). The
defaults will likely be more performant for most users.

|Key|Description|Required|Default|
|---|-----------|--------|-------|
|`indexSpec.bitmap`|The compression format for bitmap indices|No|`roaring`|
|`indexSpec.compressRunOnSerialization`|(Only if `indexSpec.bitmap` is `roaring`) Whether or not to use run-length encoding when they will be more space-efficient|No|True|
|`indexSpec.dimensionCompression`|Compression format for dimension columns. Options are `lz4`, `lzf`, or `uncompressed`|No|`lz4`|
|`indexSpec.metricCompression`|	Compression format for metric columns. Options are `lz4`, `lzf`, `uncompressed`, or `none` (which is more efficient than `uncompressed`, but not supported by older versions of Druid)|No|`lz4`|
|`indexSpec.longEncoding`|Encoding format for long-typed columns. Applies regardless of whether they are dimensions or metrics. Options are `auto` or `longs`. `auto` encodes the values using offset or lookup table depending on column cardinality, and store them with variable size. `longs` stores the value as-is with 8 bytes each|No|`longs`|

### Deep Storage Configs
The configuration properties used when interacting with deep storage systems directly. Only used in the writer. (The
reader interacts with deep storage as well, but for now operates purely off the metadata returned in segment LoadSpecs).

**Caution**: The Azure storage config doesn't work. Users will have to implement their own segment writers
and register them with the SegmentWriterRegistry (and hopefully contribute them back to this warning can be removed! :))

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

These configs generally shadow the [Connecting to S3 configuration](../extensions-core/s3.md#connecting-to-s3-configuration)
section of the Druid s3 extension doc, including in the inconsistent use of `disable` vs `enable` as boolean property
name prefixes

|Key|Description|Required|Default|
|---|-----------|--------|-------|
|`s3.bucket`|The S3 bucket to write segments to|Yes||
|`s3.baseKey`|The base key to prefix segments with when writing to S3|Yes||
|`s3.maxListingLength`|The maximum number of input files matching a prefix to retrieve or delete in one call|No|1000/1024|
|`s3.disableAcl`|Whether or not to disable ACLs on the output segments. If this is false, additional S3 permissions are required|No|False|
|`s3.useS3aSchema`|Whether or not to use the `s3a` filesystem when writing segments to S3.|No|True ***(note this is the opposite of the druid-s3 extension!)***|
|`s3.accessKey`|The S3 access key. See [S3 authentication methods](../extensions-core/s3.md#s3-authentication-methods) for more details||
|`s3.secretKey`|The S3 secret key. See [S3 authentication methods](../extensions-core/s3.md#s3-authentication-methods) for more details||
|`s3.fileSessionCredentials`|The path to a properties file containing S3 session credentials. See [S3 authentication methods](../extensions-core/s3.md#s3-authentication-methods) for more details||
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

These configs shadow the [Google Cloud Storage Extension](../extensions-core/google.md) docs. The environment variable
`GOOGLE_APPLICATION_CREDENTIALS` must be set to write segments to GCS.

|Key|Description|Required|Default|
|---|-----------|--------|-------|
|`google.bucket`|The GCS bucket to write segments to|Yes||
|`google.prefix`|The base key to prefix segments with when writing to GCS|Yes||
|`google.maxListingLength`|The maximum number of input files matching a prefix to retrieve or delete in one call|No|1024|

#### Azure Deep Storage Config
`deepStorageType` = `azure`

|Key|Description|Required|Default|
|---|-----------|--------|-------|

#### Custom Deep Storage Implementations

The Spark-Druid extension includes support for writing segments to the above deep storage options. If you want to write
to a different deep storage option or use a different implementation for one of the options above, you can implement and
register a plugin with the SegmentWriterRegistry. Any properties specified in the options map specified in the `write()`
call will be passed along to the plugin functions.