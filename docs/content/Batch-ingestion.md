---
layout: doc_page
---
Batch Data Ingestion
====================

There are two choices for batch data ingestion to your Druid cluster, you can use the [Indexing service](Indexing-service.html) or you can use the `HadoopDruidIndexerMain`. This page describes how to use the `HadoopDruidIndexerMain`.

Which should I use?
-------------------

The [Indexing service](Indexing-service.html) is a node that can run as part of your Druid cluster and can accomplish a number of different types of indexing tasks. Even if all you care about is batch indexing, it provides for the encapsulation of things like the Database that is used for segment metadata and other things, so that your indexing tasks do not need to include such information. Long-term, the indexing service is going to be the preferred method of ingesting data.

The `HadoopDruidIndexerMain` runs hadoop jobs in order to separate and index data segments. It takes advantage of Hadoop as a job scheduling and distributed job execution platform. It is a simple method if you already have Hadoop running and don’t want to spend the time configuring and deploying the [Indexing service](Indexing service.html) just yet.

HadoopDruidIndexer
------------------

Located at `com.metamx.druid.indexer.HadoopDruidIndexerMain` can be run like

```
java -cp hadoop_config_path:druid_indexer_selfcontained_jar_path com.metamx.druid.indexer.HadoopDruidIndexerMain  <config_file>
```

The interval is the [ISO8601 interval](http://en.wikipedia.org/wiki/ISO_8601#Time_intervals) of the data you are processing. The config\_file is a path to a file (the "specFile") that contains JSON and an example looks like:

```
{
  "dataSource": "the_data_source",
  "timestampColumn": "ts",
  "timestampFormat": "<iso, millis, posix, auto or any Joda time format>",
  "dataSpec": {
    "format": "<csv, tsv, or json>",
    "columns": ["ts", "column_1", "column_2", "column_3", "column_4", "column_5"],
    "dimensions": ["column_1", "column_2", "column_3"]
  },
  "granularitySpec": {
    "type":"uniform",
    "intervals":["<ISO8601 interval:http://en.wikipedia.org/wiki/ISO_8601#Time_intervals>"],
    "gran":"day"
  },
  "pathSpec": { "type": "granularity",
                "dataGranularity": "hour",
                "inputPath": "s3n://billy-bucket/the/data/is/here",
                "filePattern": ".*" },
  "rollupSpec": { "aggs": [
                    { "type": "count", "name":"event_count" },
                    { "type": "doubleSum", "fieldName": "column_4", "name": "revenue" },
                    { "type": "longSum", "fieldName" : "column_5", "name": "clicks" }
                  ],
                  "rollupGranularity": "minute"},
  "workingPath": "/tmp/path/on/hdfs",
  "segmentOutputPath": "s3n://billy-bucket/the/segments/go/here",
  "leaveIntermediate": "false",
  "partitionsSpec": {
    "targetPartitionSize": 5000000
  },
  "updaterJobSpec": {
    "type":"db",
    "connectURI":"jdbc:mysql://localhost:7980/test_db",
    "user":"username",
    "password":"passmeup",
    "segmentTable":"segments"
  }
}
```

### Hadoop indexer config

|property|description|required?|
|--------|-----------|---------|
|dataSource|name of the dataSource the data will belong to|yes|
|timestampColumn|the column that is to be used as the timestamp column|yes|
|timestampFormat|the format of timestamps; auto = either iso or millis, Joda time formats:http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html|yes|
|dataSpec|a specification of the data format and an array that names all of the columns in the input data|yes|
|dimensions|the columns that are to be used as dimensions|yes|
|granularitySpec|the time granularity and interval to chunk segments up into|yes|
|pathSpec|a specification of where to pull the data in from|yes|
|rollupSpec|a specification of the rollup to perform while processing the data|yes|
|workingPath|the working path to use for intermediate results (results between Hadoop jobs)|yes|
|segmentOutputPath|the path to dump segments into|yes|
|leaveIntermediate|leave behind files in the workingPath when job completes or fails (debugging tool)|no|
|partitionsSpec|a specification of how to partition each time bucket into segments, absence of this property means no partitioning will occur|no|
|updaterJobSpec|a specification of how to update the metadata for the druid cluster these segments belong to|yes|
|registererers|a list of serde handler classnames|no|

### Path specification

There are multiple types of path specification:

##### `granularity`

Is a type of data loader that expects data to be laid out in a specific path format. Specifically, it expects it to be segregated by day in this directory format `y=XXXX/m=XX/d=XX/H=XX/M=XX/S=XX` (dates are represented by lowercase, time is represented by uppercase).

|property|description|required?|
|--------|-----------|---------|
|dataGranularity|specifies the granularity to expect the data at, e.g. hour means to expect directories `y=XXXX/m=XX/d=XX/H=XX`|yes|
|inputPath|Base path to append the expected time path to|yes|
|filePattern|Pattern that files should match to be included|yes|

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
|rollupGranularity|The granularity to use when truncating incoming timestamps for bucketization|yes|

### Partitioning specification

Segments are always partitioned based on timestamp (according to the granularitySpec) and may be further partitioned in some other way. For example, data for a day may be split by the dimension "last\_name" into two segments: one with all values from A-M and one with all values from N-Z.

To use this option, the indexer must be given a target partition size. It can then find a good set of partition ranges on its own.

|property|description|required?|
|--------|-----------|---------|
|targetPartitionSize|target number of rows to include in a partition, should be a number that targets segments of 700MB\~1GB.|yes|
|partitionDimension|the dimension to partition on. Leave blank to select a dimension automatically.|no|
|assumeGrouped|assume input data has already been grouped on time and dimensions. This is faster, but can choose suboptimal partitions if the assumption is violated.|no|

### Updater job spec

This is a specification of the properties that tell the job how to update metadata such that the Druid cluster will see the output segments and load them.

|property|description|required?|
|--------|-----------|---------|
|type|"db" is the only value available|yes|
|connectURI|a valid JDBC url to MySQL|yes|
|user|username for db|yes|
|password|password for db|yes|
|segmentTable|table to use in DB|yes|

These properties should parrot what you have configured for your [Master](Master.html).
