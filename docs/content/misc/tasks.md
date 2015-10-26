---
layout: doc_page
---
# Tasks
Tasks are run on middle managers and always operate on a single data source. Tasks are submitted using [POST requests](../design/indexing-service.html).

There are several different types of tasks.

Segment Creation Tasks
----------------------

### Index Task

The Index Task is a simpler variation of the Index Hadoop task that is designed to be used for smaller data sets. The task executes within the indexing service and does not require an external Hadoop setup to use. The grammar of the index task is as follows:

```json
{
  "type" : "index",
  "spec" : {
    "dataSchema" : {
      "dataSource" : "wikipedia",
      "parser" : {
        "type" : "string",
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
      "type" : "index",
      "firehose" : {
        "type" : "local",
        "baseDir" : "examples/indexing/",
        "filter" : "wikipedia_data.json"
       }
    },
    "tuningConfig" : {
      "type" : "index",
      "targetPartitionSize" : -1,
      "rowFlushBoundary" : 0,
      "numShards": 1
    }
  }
}
```

#### Task Properties

|property|description|required?|
|--------|-----------|---------|
|type|The task type, this should always be "index".|yes|
|id|The task ID. If this is not explicitly specified, Druid generates the task ID using the name of the task file and date-time stamp. |no|
|spec|The ingestion spec. See below for more details. |yes|

#### DataSchema

This field is required.

See [Ingestion](../ingestion/index.html)

#### IOConfig

This field is required. You can specify a type of [Firehose](../ingestion/firehose.html) here.

#### TuningConfig

The tuningConfig is optional and default parameters will be used if no tuningConfig is specified. See below for more details.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|The task type, this should always be "index".|None.|yes|
|targetPartitionSize|Used in sharding. Determines how many rows are in each segment. Set this to -1 to use numShards instead for sharding.|5000000|no|
|rowFlushBoundary|Used in determining when intermediate persist should occur to disk.|500000|no|
|numShards|Directly specify the number of shards to create. You can skip the intermediate persist step if you specify the number of shards you want and set targetPartitionSize=-1.|null|no|
|indexSpec|defines segment storage format options to be used at indexing time, see [IndexSpec](#indexspec)|null|no|

#### IndexSpec

The indexSpec defines segment storage format options to be used at indexing
time, such as bitmap type, and column compression formats.

The indexSpec is optional and default parameters will be used if not specified.

|property|description|possible values|default|required?|
|--------|-----------|---------------|-------|---------|
|bitmap|type of bitmap compression to use for inverted indices.|`"concise"`, `"roaring"`|`"concise"` or the value of `druid.processing.bitmap.type`, if specified|no|
|dimensionCompression|compression format for dimension columns (currently only affects single-value dimensions, multi-value dimensions are always uncompressed)|`"uncompressed"`, `"lz4"`, `"lzf"`|`"lz4"`|no|
|metricCompression|compression format for metric columns, defaults to LZ4|`"lz4"`, `"lzf"`|`"lz4"`|no|

### Hadoop Index Task

The Hadoop Index Task is used to index larger data sets that require the parallelization and processing power of a Hadoop cluster.

```
{
  "type" : "index_hadoop",
  "spec": <Hadoop index spec>
}
```

|property|description|required?|
|--------|-----------|---------|
|type|The task type, this should always be "index_hadoop".|yes|
|spec|A Hadoop Index Spec. See [Batch Ingestion](../ingestion/batch-ingestion.html)|yes|
|hadoopDependencyCoordinates|A JSON array of Hadoop dependency coordinates that Druid will use, this property will override the default Hadoop coordinates. Once specified, Druid will look for those Hadoop dependencies from the location specified by `druid.extensions.hadoopDependenciesDir`|no|
|classpathPrefix|Classpath that will be pre-appended for the peon process.|no|

The Hadoop Index Config submitted as part of an Hadoop Index Task is identical to the Hadoop Index Config used by the `HadoopDruidIndexer` except that three fields must be omitted: `segmentOutputPath`, `workingPath`, `metadataUpdateSpec`. The Indexing Service takes care of setting these fields internally.

Note: Before using Hadoop Index Task, please make sure to include Hadoop dependencies so that Druid knows where to pick them up during runtime, see [Include Hadoop Dependencies](../operations/other-hadoop.html).
Druid uses hadoop-client 2.3.0 as the default Hadoop version, you can get it from the released Druid tarball(under folder ```hadoop_druid_dependencies```) or use [pull-deps](../pull-deps.html).

#### Using your own Hadoop distribution

Druid is compiled against Apache hadoop-client 2.3.0. However, if you happen to use a different flavor of Hadoop that is API compatible with hadoop-client 2.3.0, you should first make sure Druid knows where to pick it up, then you should only have to change the `hadoopDependencyCoordinates` property to point to the list of maven artifact used by your distribution. For non-API compatible versions and more information, please see [here](../operations/other-hadoop.html).

#### Resolving dependency conflicts running HadoopIndexTask

Currently, the HadoopIndexTask creates a single classpath to run the HadoopDruidIndexerJob, which can lead to version conflicts between various dependencies of Druid, extension modules, and Hadoop's own dependencies.

The Hadoop index task will put Druid's dependencies first on the classpath, followed by any extensions dependencies, and any Hadoop dependencies last.

If you are having trouble with any extensions in HadoopIndexTask, it may be the case that Druid, or one of its dependencies, depends on a different version of a library than what you are using as part of your extensions, but Druid's version overrides the one in your extension. In that case you probably want to build your own Druid version and override the offending library by adding an explicit dependency to the pom.xml of each druid sub-module that depends on it.

### Realtime Index Task

The indexing service can also run real-time tasks. These tasks effectively transform a middle manager into a real-time node. We introduced real-time tasks as a way to programmatically add new real-time data sources without needing to manually add nodes. We recommend you use the library [tranquility](https://github.com/druid-io/tranquility) to programmatically manage generating real-time index tasks. The grammar for the real-time task is as follows:

```json
{
  "type": "index_realtime",
  "id": "example",
  "resource": {
    "availabilityGroup": "someGroup",
    "requiredCapacity": 1
  },
  "spec": {
    "dataSchema": {
      "dataSource": "wikipedia",
      "parser": {
        "type": "string",
        "parseSpec": {
          "format": "json",
          "timestampSpec": {
            "column": "timestamp",
            "format": "iso"
          },
          "dimensionsSpec": {
            "dimensions": [
              "page",
              "language",
              "user",
              "unpatrolled",
              "newPage",
              "robot",
              "anonymous",
              "namespace",
              "continent",
              "country",
              "region",
              "city"
            ],
            "dimensionExclusions": [

            ],
            "spatialDimensions": [

            ]
          }
        }
      },
      "metricsSpec": [
        {
          "type": "count",
          "name": "count"
        },
        {
          "type": "doubleSum",
          "name": "added",
          "fieldName": "added"
        },
        {
          "type": "doubleSum",
          "name": "deleted",
          "fieldName": "deleted"
        },
        {
          "type": "doubleSum",
          "name": "delta",
          "fieldName": "delta"
        }
      ],
      "granularitySpec": {
        "type": "uniform",
        "segmentGranularity": "DAY",
        "queryGranularity": "NONE"
      }
    },
    "ioConfig": {
      "type": "realtime",
      "firehose": {
        "type": "kafka-0.8",
        "consumerProps": {
          "zookeeper.connect": "zk_connect_string",
          "zookeeper.connection.timeout.ms" : "15000",
          "zookeeper.session.timeout.ms" : "15000",
          "zookeeper.sync.time.ms" : "5000",
          "group.id": "consumer-group",
          "fetch.message.max.bytes" : "1048586",
          "auto.offset.reset": "largest",
          "auto.commit.enable": "false"
        },
        "feed": "your_kafka_topic"
      }
    },
    "tuningConfig": {
      "type": "realtime",
      "maxRowsInMemory": 500000,
      "intermediatePersistPeriod": "PT10m",
      "windowPeriod": "PT10m",
      "rejectionPolicy": {
        "type": "serverTime"
      }
    }
  }
}
```

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|id|String|The ID of the task.|No|
|Resource|JSON object|Used for high availability purposes.|No|
|availabilityGroup|String|An uniqueness identifier for the task. Tasks with the same availability group will always run on different middle managers. Used mainly for replication. |yes|
|requiredCapacity|Integer|How much middle manager capacity this task will take.|yes|

For schema, windowPeriod, segmentGranularity, and other configuration information, see [Realtime Ingestion](../ingestion/realtime-ingestion.html). For firehose configuration, see [Firehose](../ingestion/firehose.html).


Segment Merging Tasks
---------------------

### Append Task

Append tasks append a list of segments together into a single segment (one after the other). The grammar is:

```json
{
    "type": "append",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "segments": <JSON list of DataSegment objects to append>
}
```

### Merge Task

Merge tasks merge a list of segments together. Any common timestamps are merged. The grammar is:

```json
{
    "type": "merge",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "aggregations": <list of aggregators>,
    "segments": <JSON list of DataSegment objects to merge>
}
```

Segment Destroying Tasks
------------------------

### Delete Task

Delete tasks create empty segments with no data. The grammar is:

```json
{
    "type": "delete",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "segments": <JSON list of DataSegment objects to delete>
}
```

### Kill Task

Kill tasks delete all information about a segment and removes it from deep storage. Killable segments must be disabled (used==0) in the Druid segment table. The available grammar is:

```json
{
    "type": "kill",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "interval" : <all_segments_in_this_interval_will_die!>
}
```

Misc. Tasks
-----------

### Version Converter Task
The convert task suite takes active segments and will recompress them using a new IndexSpec. This is handy when doing activities like migrating from Concise to Roaring, or adding dimension compression to old segments.

Upon success the new segments will have the same version as the old segment with `_converted` appended. A convert task may be run against the same interval for the same datasource multiple times. Each execution will append another `_converted` to the version for the segments

There are two types of conversion tasks. One is the Hadoop convert task, and the other is the indexing service convert task. The Hadoop convert task runs on a hadoop cluster, and simply leaves a task monitor on the indexing service (similar to the hadoop batch task). The indexing service convert task runs the actual conversion on the indexing service.
####Hadoop Convert Segment Task
```json
{
  "type": "hadoop_convert_segment",
  "dataSource":"some_datasource",
  "interval":"2013/2015",
  "indexSpec":{"bitmap":{"type":"concise"},"dimensionCompression":"lz4","metricCompression":"lz4"},
  "force": true,
  "validate": false,
  "distributedSuccessCache":"hdfs://some-hdfs-nn:9000/user/jobrunner/cache",
  "jobPriority":"VERY_LOW",
  "segmentOutputPath":"s3n://somebucket/somekeyprefix"
}
```

The values are described below.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`|String|Convert task identifier|Yes: `hadoop_convert_segment`|
|`dataSource`|String|The datasource to search for segments|Yes|
|`interval`|Interval string|The interval in the datasource to look for segments|Yes|
|`indexSpec`|json|The compression specification for the index|Yes|
|`force`|boolean|Forces the convert task to continue even if binary versions indicate it has been updated recently (you probably want to do this)|No|
|`validate`|boolean|Runs validation between the old and new segment before reporting task success|No|
|`distributedSuccessCache`|URI|A location where hadoop should put intermediary files.|Yes|
|`jobPriority`|`org.apache.hadoop.mapred.JobPriority` as String|The priority to set for the hadoop job|No|
|`segmentOutputPath`|URI|A base uri for the segment to be placed. Same format as other places a segment output path is needed|Yes|


####Indexing Service Convert Segment Task
```json
{
  "type": "convert_segment",
  "dataSource":"some_datasource",
  "interval":"2013/2015",
  "indexSpec":{"bitmap":{"type":"concise"},"dimensionCompression":"lz4","metricCompression":"lz4"},
  "force": true,
  "validate": false
}
```

|Field|Type|Description|Required (default)|
|-----|----|-----------|--------|
|`type`|String|Convert task identifier|Yes: `convert_segment`|
|`dataSource`|String|The datasource to search for segments|Yes|
|`interval`|Interval string|The interval in the datasource to look for segments|Yes|
|`indexSpec`|json|The compression specification for the index|Yes|
|`force`|boolean|Forces the convert task to continue even if binary versions indicate it has been updated recently (you probably want to do this)|No (false)|
|`validate`|boolean|Runs validation between the old and new segment before reporting task success|No (true)|

Unlike the hadoop convert task, the indexing service task draws its output path from the indexing service's configuration.
### Noop Task

These tasks start, sleep for a time and are used only for testing. The available grammar is:

```json
{
    "type": "noop",
    "id": <optional_task_id>,
    "interval" : <optional_segment_interval>,
    "runTime" : <optional_millis_to_sleep>,
    "firehose": <optional_firehose_to_test_connect>
}
```

Locking
-------
Once an overlord node accepts a task, a lock is created for the data source and interval specified in the task. Tasks do not need to explicitly release locks, they are released upon task completion. Tasks may potentially release locks early if they desire. Tasks ids are unique by naming them using UUIDs or the timestamp in which the task was created. Tasks are also part of a "task group", which is a set of tasks that can share interval locks.
