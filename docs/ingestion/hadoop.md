---
id: hadoop
title: "Hadoop-based ingestion"
sidebar_label: "Hadoop-based"
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

Apache Hadoop-based batch ingestion in Apache Druid is supported via a Hadoop-ingestion task. These tasks can be posted to a running
instance of a Druid [Overlord](../design/overlord.md). Please refer to our [Hadoop-based vs. native batch comparison table](index.md#batch) for
comparisons between Hadoop-based, native batch (simple), and native batch (parallel) ingestion.

To run a Hadoop-based ingestion task, write an ingestion spec as specified below. Then POST it to the
[`/druid/indexer/v1/task`](../operations/api-reference.md#tasks) endpoint on the Overlord, or use the
`bin/post-index-task` script included with Druid.

## Tutorial

This page contains reference documentation for Hadoop-based ingestion.
For a walk-through instead, check out the [Loading from Apache Hadoop](../tutorials/tutorial-batch-hadoop.md) tutorial.

## Task syntax

A sample task is shown below:

```json
{
  "type" : "index_hadoop",
  "spec" : {
    "dataSchema" : {
      "dataSource" : "wikipedia",
      "parser" : {
        "type" : "hadoopyString",
        "parseSpec" : {
          "format" : "json",
          "timestampSpec" : {
            "column" : "timestamp",
            "format" : "auto"
          },
          "dimensionsSpec" : {
            "dimensions": ["page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city"],
            "dimensionExclusions" : [],
            "spatialDimensions" : []
          }
        }
      },
      "metricsSpec" : [
        {
          "type" : "count",
          "name" : "count"
        },
        {
          "type" : "doubleSum",
          "name" : "added",
          "fieldName" : "added"
        },
        {
          "type" : "doubleSum",
          "name" : "deleted",
          "fieldName" : "deleted"
        },
        {
          "type" : "doubleSum",
          "name" : "delta",
          "fieldName" : "delta"
        }
      ],
      "granularitySpec" : {
        "type" : "uniform",
        "segmentGranularity" : "DAY",
        "queryGranularity" : "NONE",
        "intervals" : [ "2013-08-31/2013-09-01" ]
      }
    },
    "ioConfig" : {
      "type" : "hadoop",
      "inputSpec" : {
        "type" : "static",
        "paths" : "/MyDirectory/example/wikipedia_data.json"
      }
    },
    "tuningConfig" : {
      "type": "hadoop"
    }
  },
  "hadoopDependencyCoordinates": <my_hadoop_version>
}
```

|property|description|required?|
|--------|-----------|---------|
|type|The task type, this should always be "index_hadoop".|yes|
|spec|A Hadoop Index Spec. See [Ingestion](../ingestion/index.md)|yes|
|hadoopDependencyCoordinates|A JSON array of Hadoop dependency coordinates that Druid will use, this property will override the default Hadoop coordinates. Once specified, Druid will look for those Hadoop dependencies from the location specified by `druid.extensions.hadoopDependenciesDir`|no|
|classpathPrefix|Classpath that will be prepended for the Peon process.|no|

Also note that Druid automatically computes the classpath for Hadoop job containers that run in the Hadoop cluster. But in case of conflicts between Hadoop and Druid's dependencies, you can manually specify the classpath by setting `druid.extensions.hadoopContainerDruidClasspath` property. See the extensions config in [base druid configuration](../configuration/index.md#extensions).

## `dataSchema`

This field is required. See the [`dataSchema`](index.md#legacy-dataschema-spec) section of the main ingestion page for details on
what it should contain.

## `ioConfig`

This field is required.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|This should always be 'hadoop'.|yes|
|inputSpec|Object|A specification of where to pull the data in from. See below.|yes|
|segmentOutputPath|String|The path to dump segments into.|Only used by the [Command-line Hadoop indexer](#cli). This field must be null otherwise.|
|metadataUpdateSpec|Object|A specification of how to update the metadata for the druid cluster these segments belong to.|Only used by the [Command-line Hadoop indexer](#cli). This field must be null otherwise.|

### `inputSpec`

There are multiple types of inputSpecs:

#### `static`

A type of inputSpec where a static path to the data files is provided.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|inputFormat|String|Specifies the Hadoop InputFormat class to use. e.g. `org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat` |no|
|paths|Array of String|A String of input paths indicating where the raw data is located.|yes|

For example, using the static input paths:

```
"paths" : "hdfs://path/to/data/is/here/data.gz,hdfs://path/to/data/is/here/moredata.gz,hdfs://path/to/data/is/here/evenmoredata.gz"
```

You can also read from cloud storage such as AWS S3 or Google Cloud Storage.
To do so, you need to install the necessary library under Druid's classpath in _all MiddleManager or Indexer processes_.
For S3, you can run the below command to install the [Hadoop AWS module](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/).

```bash
java -classpath "${DRUID_HOME}lib/*" org.apache.druid.cli.Main tools pull-deps -h "org.apache.hadoop:hadoop-aws:${HADOOP_VERSION}";
cp ${DRUID_HOME}/hadoop-dependencies/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar ${DRUID_HOME}/extensions/druid-hdfs-storage/
```

Once you install the Hadoop AWS module in all MiddleManager and Indexer processes, you can put
your S3 paths in the inputSpec with the below job properties.
For more configurations, see the [Hadoop AWS module](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/).

```
"paths" : "s3a://billy-bucket/the/data/is/here/data.gz,s3a://billy-bucket/the/data/is/here/moredata.gz,s3a://billy-bucket/the/data/is/here/evenmoredata.gz"
```

```json
"jobProperties" : {
  "fs.s3a.impl" : "org.apache.hadoop.fs.s3a.S3AFileSystem",
  "fs.AbstractFileSystem.s3a.impl" : "org.apache.hadoop.fs.s3a.S3A",
  "fs.s3a.access.key" : "YOUR_ACCESS_KEY",
  "fs.s3a.secret.key" : "YOUR_SECRET_KEY"
}
```

For Google Cloud Storage, you need to install [GCS connector jar](https://github.com/GoogleCloudPlatform/bigdata-interop/blob/master/gcs/INSTALL.md)
under `${DRUID_HOME}/hadoop-dependencies` in _all MiddleManager or Indexer processes_.
Once you install the GCS Connector jar in all MiddleManager and Indexer processes, you can put
your Google Cloud Storage paths in the inputSpec with the below job properties.
For more configurations, see the [instructions to configure Hadoop](https://github.com/GoogleCloudPlatform/bigdata-interop/blob/master/gcs/INSTALL.md#configure-hadoop),
[GCS core default](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/v2.0.0/gcs/conf/gcs-core-default.xml)
and [GCS core template](https://github.com/GoogleCloudPlatform/bdutil/blob/master/conf/hadoop2/gcs-core-template.xml).

```
"paths" : "gs://billy-bucket/the/data/is/here/data.gz,gs://billy-bucket/the/data/is/here/moredata.gz,gs://billy-bucket/the/data/is/here/evenmoredata.gz"
```

```json
"jobProperties" : {
  "fs.gs.impl" : "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
  "fs.AbstractFileSystem.gs.impl" : "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
}
```

#### `granularity`

A type of inputSpec that expects data to be organized in directories according to datetime using the path format: `y=XXXX/m=XX/d=XX/H=XX/M=XX/S=XX` (where date is represented by lowercase and time is represented by uppercase).

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|dataGranularity|String|Specifies the granularity to expect the data at, e.g. hour means to expect directories `y=XXXX/m=XX/d=XX/H=XX`.|yes|
|inputFormat|String|Specifies the Hadoop InputFormat class to use. e.g. `org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat` |no|
|inputPath|String|Base path to append the datetime path to.|yes|
|filePattern|String|Pattern that files should match to be included.|yes|
|pathFormat|String|Joda datetime format for each directory. Default value is `"'y'=yyyy/'m'=MM/'d'=dd/'H'=HH"`, or see [Joda documentation](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html)|no|

For example, if the sample config were run with the interval 2012-06-01/2012-06-02, it would expect data at the paths:

```
s3n://billy-bucket/the/data/is/here/y=2012/m=06/d=01/H=00
s3n://billy-bucket/the/data/is/here/y=2012/m=06/d=01/H=01
...
s3n://billy-bucket/the/data/is/here/y=2012/m=06/d=01/H=23
```

#### `dataSource`

This is a type of `inputSpec` that reads data already stored inside Druid. This is used to allow "re-indexing" data and for "delta-ingestion" described later in `multi` type inputSpec.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String.|This should always be 'dataSource'.|yes|
|ingestionSpec|JSON object.|Specification of Druid segments to be loaded. See below.|yes|
|maxSplitSize|Number|Enables combining multiple segments into single Hadoop InputSplit according to size of segments. With -1, druid calculates max split size based on user specified number of map task(mapred.map.tasks or mapreduce.job.maps). By default, one split is made for one segment. maxSplitSize is specified in bytes.|no|
|useNewAggs|Boolean|If "false", then list of aggregators in "metricsSpec" of hadoop indexing task must be same as that used in original indexing task while ingesting raw data. Default value is "false". This field can be set to "true" when "inputSpec" type is "dataSource" and not "multi" to enable arbitrary aggregators while reindexing. See below for "multi" type support for delta-ingestion.|no|

Here is what goes inside `ingestionSpec`:

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|dataSource|String|Druid dataSource name from which you are loading the data.|yes|
|intervals|List|A list of strings representing ISO-8601 Intervals.|yes|
|segments|List|List of segments from which to read data from, by default it is obtained automatically. You can obtain list of segments to put here by making a POST query to Coordinator at url /druid/coordinator/v1/metadata/datasources/segments?full with list of intervals specified in the request payload, e.g. ["2012-01-01T00:00:00.000/2012-01-03T00:00:00.000", "2012-01-05T00:00:00.000/2012-01-07T00:00:00.000"]. You may want to provide this list manually in order to ensure that segments read are exactly same as they were at the time of task submission, task would fail if the list provided by the user does not match with state of database when the task actually runs.|no|
|filter|JSON|See [Filters](../querying/filters.md)|no|
|dimensions|Array of String|Name of dimension columns to load. By default, the list will be constructed from parseSpec. If parseSpec does not have an explicit list of dimensions then all the dimension columns present in stored data will be read.|no|
|metrics|Array of String|Name of metric columns to load. By default, the list will be constructed from the "name" of all the configured aggregators.|no|
|ignoreWhenNoSegments|boolean|Whether to ignore this ingestionSpec if no segments were found. Default behavior is to throw error when no segments were found.|no|

For example

```json
"ioConfig" : {
  "type" : "hadoop",
  "inputSpec" : {
    "type" : "dataSource",
    "ingestionSpec" : {
      "dataSource": "wikipedia",
      "intervals": ["2014-10-20T00:00:00Z/P2W"]
    }
  },
  ...
}
```

#### `multi`

This is a composing inputSpec to combine other inputSpecs. This inputSpec is used for delta ingestion. You can also use a `multi` inputSpec to combine data from multiple dataSources. However, each particular dataSource can only be specified one time.
Note that, "useNewAggs" must be set to default value false to support delta-ingestion.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|children|Array of JSON objects|List of JSON objects containing other inputSpecs.|yes|

For example:

```json
"ioConfig" : {
  "type" : "hadoop",
  "inputSpec" : {
    "type" : "multi",
    "children": [
      {
        "type" : "dataSource",
        "ingestionSpec" : {
          "dataSource": "wikipedia",
          "intervals": ["2012-01-01T00:00:00.000/2012-01-03T00:00:00.000", "2012-01-05T00:00:00.000/2012-01-07T00:00:00.000"],
          "segments": [
            {
              "dataSource": "test1",
              "interval": "2012-01-01T00:00:00.000/2012-01-03T00:00:00.000",
              "version": "v2",
              "loadSpec": {
                "type": "local",
                "path": "/tmp/index1.zip"
              },
              "dimensions": "host",
              "metrics": "visited_sum,unique_hosts",
              "shardSpec": {
                "type": "none"
              },
              "binaryVersion": 9,
              "size": 2,
              "identifier": "test1_2000-01-01T00:00:00.000Z_3000-01-01T00:00:00.000Z_v2"
            }
          ]
        }
      },
      {
        "type" : "static",
        "paths": "/path/to/more/wikipedia/data/"
      }
    ]
  },
  ...
}
```

It is STRONGLY RECOMMENDED to provide list of segments in `dataSource` inputSpec explicitly so that your delta ingestion task is idempotent. You can obtain that list of segments by making following call to the Coordinator.
POST `/druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments?full`
Request Body: [interval1, interval2,...] for example ["2012-01-01T00:00:00.000/2012-01-03T00:00:00.000", "2012-01-05T00:00:00.000/2012-01-07T00:00:00.000"]

## `tuningConfig`

The tuningConfig is optional and default parameters will be used if no tuningConfig is specified.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|workingPath|String|The working path to use for intermediate results (results between Hadoop jobs).|Only used by the [Command-line Hadoop indexer](#cli). The default is '/tmp/druid-indexing'. This field must be null otherwise.|
|version|String|The version of created segments. Ignored for HadoopIndexTask unless useExplicitVersion is set to true|no (default == datetime that indexing starts at)|
|partitionsSpec|Object|A specification of how to partition each time bucket into segments. Absence of this property means no partitioning will occur. See [`partitionsSpec`](#partitionsspec) below.|no (default == 'hashed')|
|maxRowsInMemory|Integer|The number of rows to aggregate before persisting. Note that this is the number of post-aggregation rows which may not be equal to the number of input events due to roll-up. This is used to manage the required JVM heap size. Normally user does not need to set this, but depending on the nature of data, if rows are short in terms of bytes, user may not want to store a million rows in memory and this value should be set.|no (default == 1000000)|
|maxBytesInMemory|Long|The number of bytes to aggregate in heap memory before persisting. Normally this is computed internally and user does not need to set it. This is based on a rough estimate of memory usage and not actual usage. The maximum heap memory usage for indexing is maxBytesInMemory * (2 + maxPendingPersists). Note that `maxBytesInMemory` also includes heap usage of artifacts created from intermediary persists. This means that after every persist, the amount of `maxBytesInMemory` until next persist will decreases, and task will fail when the sum of bytes of all intermediary persisted artifacts exceeds `maxBytesInMemory`.|no (default == One-sixth of max JVM memory)|
|leaveIntermediate|Boolean|Leave behind intermediate files (for debugging) in the workingPath when a job completes, whether it passes or fails.|no (default == false)|
|cleanupOnFailure|Boolean|Clean up intermediate files when a job fails (unless leaveIntermediate is on).|no (default == true)|
|overwriteFiles|Boolean|Override existing files found during indexing.|no (default == false)|
|ignoreInvalidRows|Boolean|DEPRECATED. Ignore rows found to have problems. If false, any exception encountered during parsing will be thrown and will halt ingestion; if true, unparseable rows and fields will be skipped. If `maxParseExceptions` is defined, this property is ignored.|no (default == false)|
|combineText|Boolean|Use CombineTextInputFormat to combine multiple files into a file split. This can speed up Hadoop jobs when processing a large number of small files.|no (default == false)|
|useCombiner|Boolean|Use Hadoop combiner to merge rows at mapper if possible.|no (default == false)|
|jobProperties|Object|A map of properties to add to the Hadoop job configuration, see below for details.|no (default == null)|
|indexSpec|Object|Tune how data is indexed. See [`indexSpec`](index.md#indexspec) on the main ingestion page for more information.|no|
|indexSpecForIntermediatePersists|Object|defines segment storage format options to be used at indexing time for intermediate persisted temporary segments. this can be used to disable dimension/metric compression on intermediate segments to reduce memory required for final merging. however, disabling compression on intermediate segments might increase page cache use while they are used before getting merged into final segment published, see [`indexSpec`](index.md#indexspec) for possible values.|no (default = same as indexSpec)|
|numBackgroundPersistThreads|Integer|The number of new background threads to use for incremental persists. Using this feature causes a notable increase in memory pressure and CPU usage but will make the job finish more quickly. If changing from the default of 0 (use current thread for persists), we recommend setting it to 1.|no (default == 0)|
|forceExtendableShardSpecs|Boolean|Forces use of extendable shardSpecs. Hash-based partitioning always uses an extendable shardSpec. For single-dimension partitioning, this option should be set to true to use an extendable shardSpec. For partitioning, please check [Partitioning specification](#partitionsspec). This option can be useful when you need to append more data to existing dataSource.|no (default = false)|
|useExplicitVersion|Boolean|Forces HadoopIndexTask to use version.|no (default = false)|
|logParseExceptions|Boolean|If true, log an error message when a parsing exception occurs, containing information about the row where the error occurred.|no(default = false)|
|maxParseExceptions|Integer|The maximum number of parse exceptions that can occur before the task halts ingestion and fails. Overrides `ignoreInvalidRows` if `maxParseExceptions` is defined.|no(default = unlimited)|
|useYarnRMJobStatusFallback|Boolean|If the Hadoop jobs created by the indexing task are unable to retrieve their completion status from the JobHistory server, and this parameter is true, the indexing task will try to fetch the application status from `http://<yarn-rm-address>/ws/v1/cluster/apps/<application-id>`, where `<yarn-rm-address>` is the value of `yarn.resourcemanager.webapp.address` in your Hadoop configuration. This flag is intended as a fallback for cases where an indexing task's jobs succeed, but the JobHistory server is unavailable, causing the indexing task to fail because it cannot determine the job statuses.|no (default = true)|

### `jobProperties`

```json
   "tuningConfig" : {
     "type": "hadoop",
     "jobProperties": {
       "<hadoop-property-a>": "<value-a>",
       "<hadoop-property-b>": "<value-b>"
     }
   }
```

Hadoop's [MapReduce documentation](https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml) lists the possible configuration parameters.

With some Hadoop distributions, it may be necessary to set `mapreduce.job.classpath` or `mapreduce.job.user.classpath.first`
to avoid class loading issues. See the [working with different Hadoop versions documentation](../operations/other-hadoop.md)
for more details.

## `partitionsSpec`

Segments are always partitioned based on timestamp (according to the granularitySpec) and may be further partitioned in
some other way depending on partition type. Druid supports two types of partitioning strategies: `hashed` (based on the
hash of all dimensions in each row), and `single_dim` (based on ranges of a single dimension).

Hashed partitioning is recommended in most cases, as it will improve indexing performance and create more uniformly
sized data segments relative to single-dimension partitioning.

### Hash-based partitioning

```json
  "partitionsSpec": {
     "type": "hashed",
     "targetRowsPerSegment": 5000000
   }
```

Hashed partitioning works by first selecting a number of segments, and then partitioning rows across those segments
according to the hash of all dimensions in each row. The number of segments is determined automatically based on the
cardinality of the input set and a target partition size.

The configuration options are:

|Field|Description|Required|
|--------|-----------|---------|
|type|Type of partitionSpec to be used.|"hashed"|
|targetRowsPerSegment|Target number of rows to include in a partition, should be a number that targets segments of 500MB\~1GB. Defaults to 5000000 if `numShards` is not set.|either this or `numShards`|
|targetPartitionSize|Deprecated. Renamed to `targetRowsPerSegment`. Target number of rows to include in a partition, should be a number that targets segments of 500MB\~1GB.|either this or `numShards`|
|maxRowsPerSegment|Deprecated. Renamed to `targetRowsPerSegment`. Target number of rows to include in a partition, should be a number that targets segments of 500MB\~1GB.|either this or `numShards`|
|numShards|Specify the number of partitions directly, instead of a target partition size. Ingestion will run faster, since it can skip the step necessary to select a number of partitions automatically.|either this or `maxRowsPerSegment`|
|partitionDimensions|The dimensions to partition on. Leave blank to select all dimensions. Only used with `numShards`, will be ignored when `targetRowsPerSegment` is set.|no|
|partitionFunction|A function to compute hash of partition dimensions. See [Hash partition function](#hash-partition-function)|`murmur3_32_abs`|no|

##### Hash partition function

In hash partitioning, the partition function is used to compute hash of partition dimensions. The partition dimension
values are first serialized into a byte array as a whole, and then the partition function is applied to compute hash of
the byte array.
Druid currently supports only one partition function.

|name|description|
|----|-----------|
|`murmur3_32_abs`|Applies an absolute value function to the result of [`murmur3_32`](https://guava.dev/releases/16.0/api/docs/com/google/common/hash/Hashing.html#murmur3_32()).|

### Single-dimension range partitioning

```json
  "partitionsSpec": {
     "type": "single_dim",
     "targetRowsPerSegment": 5000000
   }
```

Single-dimension range partitioning works by first selecting a dimension to partition on, and then separating that dimension
into contiguous ranges. Each segment will contain all rows with values of that dimension in that range. For example,
your segments may be partitioned on the dimension "host" using the ranges "a.example.com" to "f.example.com" and
"f.example.com" to "z.example.com". By default, the dimension to use is determined automatically, although you can
override it with a specific dimension.

The configuration options are:

|Field|Description|Required|
|--------|-----------|---------|
|type|Type of partitionSpec to be used.|"single_dim"|
|targetRowsPerSegment|Target number of rows to include in a partition, should be a number that targets segments of 500MB\~1GB.|yes|
|targetPartitionSize|Deprecated. Renamed to `targetRowsPerSegment`. Target number of rows to include in a partition, should be a number that targets segments of 500MB\~1GB.|no|
|maxRowsPerSegment|Maximum number of rows to include in a partition. Defaults to 50% larger than the `targetRowsPerSegment`.|no|
|maxPartitionSize|Deprecated. Use `maxRowsPerSegment` instead. Maximum number of rows to include in a partition. Defaults to 50% larger than the `targetPartitionSize`.|no|
|partitionDimension|The dimension to partition on. Leave blank to select a dimension automatically.|no|
|assumeGrouped|Assume that input data has already been grouped on time and dimensions. Ingestion will run faster, but may choose sub-optimal partitions if this assumption is violated.|no|

## Remote Hadoop clusters

If you have a remote Hadoop cluster, make sure to include the folder holding your configuration `*.xml` files in your Druid `_common` configuration folder.

If you are having dependency problems with your version of Hadoop and the version compiled with Druid, please see [these docs](../operations/other-hadoop.md).

## Elastic MapReduce

If your cluster is running on Amazon Web Services, you can use Elastic MapReduce (EMR) to index data
from S3. To do this:

- Create a persistent, [long-running cluster](http://docs.aws.amazon.com/ElasticMapReduce/latest/ManagementGuide/emr-plan-longrunning-transient).
- When creating your cluster, enter the following configuration. If you're using the wizard, this
should be in advanced mode under "Edit software settings":

```
classification=yarn-site,properties=[mapreduce.reduce.memory.mb=6144,mapreduce.reduce.java.opts=-server -Xms2g -Xmx2g -Duser.timezone=UTC -Dfile.encoding=UTF-8 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps,mapreduce.map.java.opts=758,mapreduce.map.java.opts=-server -Xms512m -Xmx512m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps,mapreduce.task.timeout=1800000]
```

- Follow the instructions under
[Configure for connecting to Hadoop](../tutorials/cluster.md#hadoop) using the XML files from `/etc/hadoop/conf`
on your EMR master.

## Kerberized Hadoop clusters

By default druid can use the existing TGT kerberos ticket available in local kerberos key cache.
Although TGT ticket has a limited life cycle,
therefore you need to call `kinit` command periodically to ensure validity of TGT ticket.
To avoid this extra external cron job script calling `kinit` periodically,
 you can provide the principal name and keytab location and druid will do the authentication transparently at startup and job launching time.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.hadoop.security.kerberos.principal`|`druid@EXAMPLE.COM`| Principal user name |empty|
|`druid.hadoop.security.kerberos.keytab`|`/etc/security/keytabs/druid.headlessUser.keytab`|Path to keytab file|empty|

### Loading from S3 with EMR

- In the `jobProperties` field in the `tuningConfig` section of your Hadoop indexing task, add:

```
"jobProperties" : {
   "fs.s3.awsAccessKeyId" : "YOUR_ACCESS_KEY",
   "fs.s3.awsSecretAccessKey" : "YOUR_SECRET_KEY",
   "fs.s3.impl" : "org.apache.hadoop.fs.s3native.NativeS3FileSystem",
   "fs.s3n.awsAccessKeyId" : "YOUR_ACCESS_KEY",
   "fs.s3n.awsSecretAccessKey" : "YOUR_SECRET_KEY",
   "fs.s3n.impl" : "org.apache.hadoop.fs.s3native.NativeS3FileSystem",
   "io.compression.codecs" : "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.SnappyCodec"
}
```

Note that this method uses Hadoop's built-in S3 filesystem rather than Amazon's EMRFS, and is not compatible
with Amazon-specific features such as S3 encryption and consistent views. If you need to use these
features, you will need to make the Amazon EMR Hadoop JARs available to Druid through one of the
mechanisms described in the [Using other Hadoop distributions](#using-other-hadoop-distributions) section.

## Using other Hadoop distributions

Druid works out of the box with many Hadoop distributions.

If you are having dependency conflicts between Druid and your version of Hadoop, you can try
searching for a solution in the [Druid user groups](https://groups.google.com/forum/#!forum/druid-user), or reading the
Druid [Different Hadoop Versions](../operations/other-hadoop.md) documentation.

<a name="cli"></a>

## Command line (non-task) version

To run:

```
java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/*:<hadoop_config_dir> org.apache.druid.cli.Main index hadoop <spec_file>
```

### Options

- "--coordinate" - provide a version of Apache Hadoop to use. This property will override the default Hadoop coordinates. Once specified, Apache Druid will look for those Hadoop dependencies from the location specified by `druid.extensions.hadoopDependenciesDir`.
- "--no-default-hadoop" - don't pull down the default hadoop version

### Spec file

The spec file needs to contain a JSON object where the contents are the same as the "spec" field in the Hadoop index task. See [Hadoop Batch Ingestion](../ingestion/hadoop.md) for details on the spec format.

In addition, a `metadataUpdateSpec` and `segmentOutputPath` field needs to be added to the ioConfig:

```
      "ioConfig" : {
        ...
        "metadataUpdateSpec" : {
          "type":"mysql",
          "connectURI" : "jdbc:mysql://localhost:3306/druid",
          "password" : "diurd",
          "segmentTable" : "druid_segments",
          "user" : "druid"
        },
        "segmentOutputPath" : "/MyDirectory/data/index/output"
      },
```

and a `workingPath` field needs to be added to the tuningConfig:

```
  "tuningConfig" : {
   ...
    "workingPath": "/tmp",
    ...
  }
```

#### Metadata Update Job Spec

This is a specification of the properties that tell the job how to update metadata such that the Druid cluster will see the output segments and load them.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|"metadata" is the only value available.|yes|
|connectURI|String|A valid JDBC url to metadata storage.|yes|
|user|String|Username for db.|yes|
|password|String|password for db.|yes|
|segmentTable|String|Table to use in DB.|yes|

These properties should parrot what you have configured for your [Coordinator](../design/coordinator.md).

#### segmentOutputPath Config

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|segmentOutputPath|String|the path to dump segments into.|yes|

#### workingPath Config

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|workingPath|String|the working path to use for intermediate results (results between Hadoop jobs).|no (default == '/tmp/druid-indexing')|

Please note that the command line Hadoop indexer doesn't have the locking capabilities of the indexing service, so if you choose to use it,
you have to take caution to not override segments created by real-time processing (if you that a real-time pipeline set up).
