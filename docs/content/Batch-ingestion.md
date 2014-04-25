---
layout: doc_page
---

# Batch Data Ingestion
There are two choices for batch data ingestion to your Druid cluster, you can use the [Indexing service](Indexing-Service.html) or you can use the `HadoopDruidIndexer`.

Which should I use?
-------------------

The [Indexing service](Indexing-Service.html) is a node that can run as part of your Druid cluster and can accomplish a number of different types of indexing tasks. Even if all you care about is batch indexing, it provides for the encapsulation of things like the [database](MySQL.html) that is used for segment metadata and other things, so that your indexing tasks do not need to include such information. The indexing service was created such that external systems could programmatically interact with it and run periodic indexing tasks. Long-term, the indexing service is going to be the preferred method of ingesting data.

The `HadoopDruidIndexer` runs hadoop jobs in order to separate and index data segments. It takes advantage of Hadoop as a job scheduling and distributed job execution platform. It is a simple method if you already have Hadoop running and don’t want to spend the time configuring and deploying the [Indexing service](Indexing-Service.html) just yet.

Batch Ingestion using the HadoopDruidIndexer
--------------------------------------------

The HadoopDruidIndexer can be run like so:

```
java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/*:<hadoop_config_path> io.druid.cli.Main index hadoop <config_file>
```

The interval is the [ISO8601 interval](http://en.wikipedia.org/wiki/ISO_8601#Time_intervals) of the data you are processing. The config\_file is a path to a file (the "specFile") that contains JSON and an example looks like:

```json
{
  "dataSource": "the_data_source",
  "timestampSpec" : {
    "column": "ts",
    "format": "<iso, millis, posix, auto or any Joda time format>"
  },
  "dataSpec": {
    "format": "<csv, tsv, or json>",
    "columns": [
      "ts",
      "column_1",
      "column_2",
      "column_3",
      "column_4",
      "column_5"
    ],
    "dimensions": [
      "column_1",
      "column_2",
      "column_3"
    ]
  },
  "granularitySpec": {
    "type": "uniform",
    "intervals": [
      "<ISO8601 interval:http:\/\/en.wikipedia.org\/wiki\/ISO_8601#Time_intervals>"
    ],
    "gran": "day"
  },
  "pathSpec": {
    "type": "static",
    "paths" : "example/path/data.gz,example/path/moredata.gz"
  },
  "rollupSpec": {
    "aggs": [
      {
        "type": "count",
        "name": "event_count"
      },
      {
        "type": "doubleSum",
        "fieldName": "column_4",
        "name": "revenue"
      },
      {
        "type": "longSum",
        "fieldName": "column_5",
        "name": "clicks"
      }
    ],
    "rollupGranularity": "minute"
  },
  "workingPath": "\/tmp\/path\/on\/hdfs",
  "segmentOutputPath": "s3n:\/\/billy-bucket\/the\/segments\/go\/here",
  "leaveIntermediate": "false",
  "partitionsSpec": {
    "type": "hashed"
    "targetPartitionSize": 5000000
  },
  "metadataUpdateSpec": {
    "type": "db",
    "connectURI": "jdbc:mysql:\/\/localhost:7980\/test_db",
    "user": "username",
    "password": "passmeup",
    "segmentTable": "segments"
  }
}
```

### Hadoop Index Config

|property|description|required?|
|--------|-----------|---------|
|dataSource|name of the dataSource the data will belong to|yes|
|timestampSpec|includes the column that is to be used as the timestamp column and the format of the timestamps; auto = either iso or millis, Joda time formats can be found [here](http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html).|yes|
|dataSpec|a specification of the data format and an array that names all of the columns in the input data.|yes|
|dimensions|the columns that are to be used as dimensions.|yes|
|granularitySpec|the time granularity and interval to chunk segments up into.|yes|
|pathSpec|a specification of where to pull the data in from|yes|
|rollupSpec|a specification of the rollup to perform while processing the data|yes|
|workingPath|the working path to use for intermediate results (results between Hadoop jobs).|yes|
|segmentOutputPath|the path to dump segments into.|yes|
|leaveIntermediate|leave behind files in the workingPath when job completes or fails (debugging tool).|no|
|partitionsSpec|a specification of how to partition each time bucket into segments, absence of this property means no partitioning will occur.|no|
|metadataUpdateSpec|a specification of how to update the metadata for the druid cluster these segments belong to.|yes|

### Path specification

There are multiple types of path specification:

##### `static`

Is a type of data loader where a static path to where the data files are located is passed.

|property|description|required?|
|--------|-----------|---------|
|paths|A String of input paths indicating where the raw data is located.|yes|

For example, using the static input paths:

```
"paths" : "s3n://billy-bucket/the/data/is/here/data.gz, s3n://billy-bucket/the/data/is/here/moredata.gz, s3n://billy-bucket/the/data/is/here/evenmoredata.gz"
```

##### `granularity`

Is a type of data loader that expects data to be laid out in a specific path format. Specifically, it expects it to be segregated by day in this directory format `y=XXXX/m=XX/d=XX/H=XX/M=XX/S=XX` (dates are represented by lowercase, time is represented by uppercase).

|property|description|required?|
|--------|-----------|---------|
|dataGranularity|specifies the granularity to expect the data at, e.g. hour means to expect directories `y=XXXX/m=XX/d=XX/H=XX`.|yes|
|inputPath|Base path to append the expected time path to.|yes|
|filePattern|Pattern that files should match to be included.|yes|

For example, if the sample config were run with the interval 2012-06-01/2012-06-02, it would expect data at the paths

```
s3n://billy-bucket/the/data/is/here/y=2012/m=06/d=01/H=00
s3n://billy-bucket/the/data/is/here/y=2012/m=06/d=01/H=01
...
s3n://billy-bucket/the/data/is/here/y=2012/m=06/d=01/H=23
```

### Rollup specification

The indexing process has the ability to roll data up as it processes the incoming data. If data has already been summarized, summarizing it again will produce the same results so either way is not a problem. This specifies how that rollup should take place.

|property|description|required?|
|--------|-----------|---------|
|aggs|specifies a list of aggregators to aggregate for each bucket (a bucket is defined by the tuple of the truncated timestamp and the dimensions). Aggregators available here are the same as available when querying.|yes|
|rollupGranularity|The granularity to use when truncating incoming timestamps for bucketization.|yes|

### Partitioning specification

Segments are always partitioned based on timestamp (according to the granularitySpec) and may be further partitioned in some other way depending on partition type.
Druid supports two types of partitions spec - singleDimension and hashed.

In SingleDimension partition type data is partitioned based on the values in that dimension.
For example, data for a day may be split by the dimension "last\_name" into two segments: one with all values from A-M and one with all values from N-Z.

In hashed partition type, the number of partitions is determined based on the targetPartitionSize and cardinality of input set and the data is partitioned based on the hashcode of the row.

It is recommended to use Hashed partition as it is more efficient than singleDimension since it does not need to determine the dimension for creating partitions.
Hashing also gives better distribution of data resulting in equal sized partitions and improving query performance

To use this druid to automatically determine optimal partitions indexer must be given a target partition size. It can then find a good set of partition ranges on its own.

#### Configuration for disabling auto-sharding and creating Fixed number of partitions
 Druid can be configured to NOT run determine partitions and create a fixed number of shards by specifying numShards in hashed partitionsSpec.
 e.g This configuration will skip determining optimal partitions and always create 4 shards for every segment granular interval

```json
  "partitionsSpec": {
     "type": "hashed"
     "numShards": 4
   }
```

|property|description|required?|
|--------|-----------|---------|
|type|type of partitionSpec to be used |no, default : singleDimension|
|targetPartitionSize|target number of rows to include in a partition, should be a number that targets segments of 700MB\~1GB.|yes|
|partitionDimension|the dimension to partition on. Leave blank to select a dimension automatically.|no|
|assumeGrouped|assume input data has already been grouped on time and dimensions. This is faster, but can choose suboptimal partitions if the assumption is violated.|no|
|numShards|provides a way to manually override druid-auto sharding and specify the number of shards to create for each segment granular interval.It is only supported by hashed partitionSpec and targetPartitionSize must be set to -1|no|

### Updater job spec

This is a specification of the properties that tell the job how to update metadata such that the Druid cluster will see the output segments and load them.

|property|description|required?|
|--------|-----------|---------|
|type|"db" is the only value available.|yes|
|connectURI|A valid JDBC url to MySQL.|yes|
|user|Username for db.|yes|
|password|password for db.|yes|
|segmentTable|Table to use in DB.|yes|

These properties should parrot what you have configured for your [Coordinator](Coordinator.html).

Batch Ingestion Using the Indexing Service
------------------------------------------

Batch ingestion for the indexing service is done by submitting a [Hadoop Index Task](Tasks.html). The indexing service can be started by issuing:

```
java -Xmx2g -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/*:config/overlord io.druid.cli.Main server overlord
```

This will start up a very simple local indexing service. For more complex deployments of the indexing service, see [here](Indexing-Service.html).

The schema of the Hadoop Index Task contains a task "type" and a Hadoop Index Config. A sample Hadoop index task is shown below:

```json
{
  "type" : "index_hadoop",
  "config": {
    "dataSource" : "example",
    "timestampSpec" : {
      "column" : "timestamp",
      "format" : "auto"
    },
    "dataSpec" : {
      "format" : "json",
      "dimensions" : ["dim1","dim2","dim3"]
    },
    "granularitySpec" : {
      "type" : "uniform",
      "gran" : "DAY",
      "intervals" : [ "2013-08-31/2013-09-01" ]
    },
    "pathSpec" : {
      "type" : "static",
      "paths" : "data.json"
    },
    "targetPartitionSize" : 5000000,
    "rollupSpec" : {
      "aggs": [{
          "type" : "count",
          "name" : "count"
        }, {
          "type" : "doubleSum",
          "name" : "added",
          "fieldName" : "added"
        }, {
          "type" : "doubleSum",
          "name" : "deleted",
          "fieldName" : "deleted"
        }, {
          "type" : "doubleSum",
          "name" : "delta",
          "fieldName" : "delta"
      }],
      "rollupGranularity" : "none"
    }
  }
```

|property|description|required?|
|--------|-----------|---------|
|type|This should be "index_hadoop".|yes|
|config|A Hadoop Index Config (see above).|yes|
|hadoopCoordinates|The Maven `<groupId>:<artifactId>:<version>` of Hadoop to use. The default is "org.apache.hadoop:hadoop-core:1.0.3".|no|

The Hadoop Index Config submitted as part of an Hadoop Index Task is identical to the Hadoop Index Config used by the `HadoopBatchIndexer` except that three fields must be omitted: `segmentOutputPath`, `workingPath`, `metadataUpdateSpec`. The Indexing Service takes care of setting these fields internally.

To run the task:

```
curl -X 'POST' -H 'Content-Type:application/json' -d @example_index_hadoop_task.json localhost:8087/druid/indexer/v1/task
```

If the task succeeds, you should see in the logs of the indexing service:

```
2013-10-16 16:38:31,945 INFO [pool-6-thread-1] io.druid.indexing.overlord.exec.TaskConsumer - Task SUCCESS: HadoopIndexTask...
```

Having Problems?
----------------
Getting data into Druid can definitely be difficult for first time users. Please don't hesitate to ask questions in our IRC channel or on our [google groups page](https://groups.google.com/forum/#!forum/druid-development).
