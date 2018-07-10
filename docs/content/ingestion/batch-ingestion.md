---
layout: doc_page
---

# Batch Data Ingestion

Druid can load data from static files through a variety of methods described here.

## Hadoop-based Batch Ingestion

Hadoop-based batch ingestion in Druid is supported via a Hadoop-ingestion task. These tasks can be posted to a running instance  
of a Druid [overlord](../design/indexing-service.html). A sample task is shown below:

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
|spec|A Hadoop Index Spec. See [Batch Ingestion](../ingestion/batch-ingestion.html)|yes|
|hadoopDependencyCoordinates|A JSON array of Hadoop dependency coordinates that Druid will use, this property will override the default Hadoop coordinates. Once specified, Druid will look for those Hadoop dependencies from the location specified by `druid.extensions.hadoopDependenciesDir`|no|
|classpathPrefix|Classpath that will be pre-appended for the peon process.|no|

### DataSchema

This field is required.

See [Ingestion](../ingestion/index.html)

### IOConfig

This field is required.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|This should always be 'hadoop'.|yes|
|inputSpec|Object|a specification of where to pull the data in from. See below.|yes|
|segmentOutputPath|String|the path to dump segments into.|yes|
|metadataUpdateSpec|Object|a specification of how to update the metadata for the druid cluster these segments belong to.|yes|

#### InputSpec specification

There are multiple types of inputSpecs:

##### `static`

Is a type of inputSpec where a static path to where the data files are located is passed.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|paths|Array of String|A String of input paths indicating where the raw data is located.|yes|

For example, using the static input paths:

```
"paths" : "s3n://billy-bucket/the/data/is/here/data.gz, s3n://billy-bucket/the/data/is/here/moredata.gz, s3n://billy-bucket/the/data/is/here/evenmoredata.gz"
```

##### `granularity`

Is a type of inputSpec that expects data to be laid out in a specific path format. Specifically, it expects it to be segregated by day in this directory format `y=XXXX/m=XX/d=XX/H=XX/M=XX/S=XX` (dates are represented by lowercase, time is represented by uppercase).

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|dataGranularity|String|specifies the granularity to expect the data at, e.g. hour means to expect directories `y=XXXX/m=XX/d=XX/H=XX`.|yes|
|inputPath|String|Base path to append the expected time path to.|yes|
|filePattern|String|Pattern that files should match to be included.|yes|
|pathFormat|String|Joda date-time format for each directory. Default value is `"'y'=yyyy/'m'=MM/'d'=dd/'H'=HH"`, or see [Joda documentation](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html)|no|

For example, if the sample config were run with the interval 2012-06-01/2012-06-02, it would expect data at the paths

```
s3n://billy-bucket/the/data/is/here/y=2012/m=06/d=01/H=00
s3n://billy-bucket/the/data/is/here/y=2012/m=06/d=01/H=01
...
s3n://billy-bucket/the/data/is/here/y=2012/m=06/d=01/H=23
```

##### `dataSource`

Read Druid segments. See [here](../ingestion/update-existing-data.html) for more information.

##### `multi`

Read multiple sources of data. See [here](../ingestion/update-existing-data.html) for more information.

### TuningConfig

The tuningConfig is optional and default parameters will be used if no tuningConfig is specified.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|workingPath|String|the working path to use for intermediate results (results between Hadoop jobs).|no (default == '/tmp/druid-indexing')|
|version|String|The version of created segments.|no (default == datetime that indexing starts at)|
|leaveIntermediate|Boolean|leave behind files in the workingPath when job completes or fails (debugging tool).|no (default == false)|
|partitionsSpec|Object|a specification of how to partition each time bucket into segments, absence of this property means no partitioning will occur.More details below.|no (default == 'hashed'|
|maxRowsInMemory|Integer|The number of rows to aggregate before persisting. This number is the post-aggregation rows, so it is not equivalent to the number of input events, but the number of aggregated rows that those events result in. This is used to manage the required JVM heap size.|no (default == 5 million)|
|cleanupOnFailure|Boolean|Cleans up intermediate files when the job fails as opposed to leaving them around for debugging.|no (default == true)|
|overwriteFiles|Boolean|Override existing files found during indexing.|no (default == false)|
|ignoreInvalidRows|Boolean|Ignore rows found to have problems.|no (default == false)|
|useCombiner|Boolean|Use hadoop combiner to merge rows at mapper if possible.|no (default == false)|
|jobProperties|Object|a map of properties to add to the Hadoop job configuration.|no (default == null)|
|buildV9Directly|Boolean|Whether to build v9 index directly instead of building v8 index and convert it to v9 format|no (default = false)|
|numBackgroundPersistThreads|Integer|The number of new background threads to use for incremental persists. Using this feature causes a notable increase in memory pressure and cpu usage, but will make the job finish more quickly. If changing from the default of 0 (use current thread for persists), we recommend setting it to 1.|no (default == 0)|

### Partitioning specification

Segments are always partitioned based on timestamp (according to the granularitySpec) and may be further partitioned in
some other way depending on partition type. Druid supports two types of partitioning strategies: "hashed" (based on the
hash of all dimensions in each row), and "dimension" (based on ranges of a single dimension).

Hashed partitioning is recommended in most cases, as it will improve indexing performance and create more uniformly
sized data segments relative to single-dimension partitioning.

#### Hash-based partitioning

```json
  "partitionsSpec": {
     "type": "hashed",
     "targetPartitionSize": 5000000
   }
```

Hashed partitioning works by first selecting a number of segments, and then partitioning rows across those segments
according to the hash of all dimensions in each row. The number of segments is determined automatically based on the
cardinality of the input set and a target partition size.

The configuration options are:

|property|description|required?|
|--------|-----------|---------|
|type|type of partitionSpec to be used |"hashed"|
|targetPartitionSize|target number of rows to include in a partition, should be a number that targets segments of 500MB\~1GB.|either this or numShards|
|numShards|specify the number of partitions directly, instead of a target partition size. Ingestion will run faster, since it can skip the step necessary to select a number of partitions automatically.|either this or targetPartitionSize|

#### Single-dimension partitioning

```json
  "partitionsSpec": {
     "type": "dimension",
     "targetPartitionSize": 5000000
   }
```

Single-dimension partitioning works by first selecting a dimension to partition on, and then separating that dimension
into contiguous ranges. Each segment will contain all rows with values of that dimension in that range. For example,
your segments may be partitioned on the dimension "host" using the ranges "a.example.com" to "f.example.com" and
"f.example.com" to "z.example.com". By default, the dimension to use is determined automatically, although you can
override it with a specific dimension.

The configuration options are:

|property|description|required?|
|--------|-----------|---------|
|type|type of partitionSpec to be used |"dimension"|
|targetPartitionSize|target number of rows to include in a partition, should be a number that targets segments of 500MB\~1GB.|yes|
|maxPartitionSize|maximum number of rows to include in a partition. Defaults to 50% larger than the targetPartitionSize.|no|
|partitionDimension|the dimension to partition on. Leave blank to select a dimension automatically.|no|
|assumeGrouped|assume input data has already been grouped on time and dimensions. Ingestion will run faster, but can choose suboptimal partitions if the assumption is violated.|no|

### Remote Hadoop Cluster

If you have a remote Hadoop cluster, make sure to include the folder holding your configuration `*.xml` files in your Druid `_common` configuration folder.  
If you having dependency problems with your version of Hadoop and the version compiled with Druid, please see [these docs](../operations/other-hadoop.html).

### Using Elastic MapReduce

If your cluster is running on Amazon Web Services, you can use Elastic MapReduce (EMR) to index data
from S3. To do this:

- Create a persistent, [long-running cluster](http://docs.aws.amazon.com/ElasticMapReduce/latest/ManagementGuide/emr-plan-longrunning-transient.html).
- When creating your cluster, enter the following configuration. If you're using the wizard, this
should be in advanced mode under "Edit software settings".

```
classification=yarn-site,properties=[mapreduce.reduce.memory.mb=6144,mapreduce.reduce.java.opts=-server -Xms2g -Xmx2g -Duser.timezone=UTC -Dfile.encoding=UTF-8 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps,mapreduce.map.java.opts=758,mapreduce.map.java.opts=-server -Xms512m -Xmx512m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps,mapreduce.task.timeout=1800000]
```

- Follow the instructions under "[Configure Hadoop for data
loads](cluster.html#configure-cluster-for-hadoop-data-loads)" using the XML files from
`/etc/hadoop/conf` on your EMR master.

#### Loading from S3 with EMR

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

Note that this method uses Hadoop's builtin S3 filesystem rather than Amazon's EMRFS, and is not compatible
with Amazon-specific features such as S3 encryption and consistent views. If you need to use those
features, you will need to make the Amazon EMR Hadoop JARs available to Druid through one of the
mechanisms described in the [Using other Hadoop distributions](#using-other-hadoop-distributions) section.

## Using other Hadoop distributions

Druid works out of the box with many Hadoop distributions.

If you are having dependency conflicts between Druid and your version of Hadoop, you can try
searching for a solution in the [Druid user groups](https://groups.google.com/forum/#!forum/druid-
user), or reading the Druid [Different Hadoop Versions](..//operations/other-hadoop.html) documentation.

## Command Line Hadoop Indexer

If you don't want to use a full indexing service to use Hadoop to get data into Druid, you can also use the standalone command line Hadoop indexer. 
See [here](../ingestion/command-line-hadoop-indexer.html) for more info.

## IndexTask-based Batch Ingestion

If you do not want to have a dependency on Hadoop for batch ingestion, you can also use the index task. This task will be much slower and less scalable than the Hadoop-based method. See [here](../ingestion/tasks.html)for more info.   

Having Problems?
----------------
Getting data into Druid can definitely be difficult for first time users. Please don't hesitate to ask questions in our IRC channel or on our [google groups page](https://groups.google.com/forum/#!forum/druid-user).
