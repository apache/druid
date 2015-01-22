---
layout: doc_page
---
# Tasks
Tasks are run on middle managers and always operate on a single data source. Tasks are submitted using [POST requests](Indexing-Service.html).

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
      "numShards": 2
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

See [Ingestion](Ingestion.html)

#### IOConfig

This field is required. You can specify a type of [Firehose](Firehose.html) here.

#### TuningConfig

The tuningConfig is optional and default parameters will be used if no tuningConfig is specified. See below for more details.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|The task type, this should always be "index".|None.||yes|
|targetPartitionSize|Used in sharding. Determines how many rows are in each segment.|5000000|no|
|rowFlushBoundary|Used in determining when intermediate persist should occur to disk.|500000|no|
|numShards|You can skip the intermediate persist step if you specify the number of shards you want and set targetPartitionSize=-1.|null|no|

### Index Hadoop Task

The Hadoop Index Task is used to index larger data sets that require the parallelization and processing power of a Hadoop cluster.

```
{
  "type" : "index_hadoop",
  "config": <Hadoop index config>
}
```

|property|description|required?|
|--------|-----------|---------|
|type|The task type, this should always be "index_hadoop".|yes|
|config|A Hadoop Index Config. See [Batch Ingestion](Batch-ingestion.html)|yes|
|hadoopCoordinates|The Maven \<groupId\>:\<artifactId\>:\<version\> of Hadoop to use. The default is "org.apache.hadoop:hadoop-client:2.3.0".|no|


The Hadoop Index Config submitted as part of an Hadoop Index Task is identical to the Hadoop Index Config used by the `HadoopBatchIndexer` except that three fields must be omitted: `segmentOutputPath`, `workingPath`, `updaterJobSpec`. The Indexing Service takes care of setting these fields internally.

#### Using your own Hadoop distribution

Druid is compiled against Apache hadoop-client 2.3.0. However, if you happen to use a different flavor of hadoop that is API compatible with hadoop-client 2.3.0, you should only have to change the hadoopCoordinates property to point to the maven artifact used by your distribution.

#### Resolving dependency conflicts running HadoopIndexTask

Currently, the HadoopIndexTask creates a single classpath to run the HadoopDruidIndexerJob, which can lead to version conflicts between various dependencies of Druid, extension modules, and Hadoop's own dependencies.

The Hadoop index task will put Druid's dependencies first on the classpath, followed by any extensions dependencies, and any Hadoop dependencies last.

If you are having trouble with any extensions in HadoopIndexTask, it may be the case that Druid, or one of its dependencies, depends on a different version of a library than what you are using as part of your extensions, but Druid's version overrides the one in your extension. In that case you probably want to build your own Druid version and override the offending library by adding an explicit dependency to the pom.xml of each druid sub-module that depends on it.

### Realtime Index Task

The indexing service can also run real-time tasks. These tasks effectively transform a middle manager into a real-time node. We introduced real-time tasks as a way to programmatically add new real-time data sources without needing to manually add nodes. We recommend you use the library [tranquility](https://github.com/metamx/tranquility) to programmatically manage generating real-time index tasks. The grammar for the real-time task is as follows:

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
      }
    },
    "ioConfig": {
      "type": "realtime",
      "firehose": {
        "type": "kafka-0.7.2",
        "consumerProps": {
          "zk.connect": "zk_connect_string",
          "zk.connectiontimeout.ms": "15000",
          "zk.sessiontimeout.ms": "15000",
          "zk.synctime.ms": "5000",
          "groupid": "consumer-group",
          "fetch.size": "1048586",
          "autooffset.reset": "largest",
          "autocommit.enable": "false"
        },
        "feed": "your_kafka_topic"
      },
      "plumber": {
        "type": "realtime"
      }
    },
    "tuningConfig": {
      "type": "realtime",
      "maxRowsInMemory": 500000,
      "intermediatePersistPeriod": "PT10m",
      "windowPeriod": "PT10m",
      "basePersistDirectory": "\/tmp\/realtime\/basePersist",
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

For schema, windowPeriod, segmentGranularity, and other configuration information, see [Realtime Ingestion](Realtime-ingestion.html). For firehose configuration, see [Firehose](Firehose.html).


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

These tasks convert segments from an existing older index version to the latest index version. The available grammar is:

```json
{
    "type": "version_converter",
    "id": <task_id>,
    "groupId" : <task_group_id>,
    "dataSource": <task_datasource>,
    "interval" : <segment_interval>,
    "segment": <JSON DataSegment object to convert>
}
```

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
