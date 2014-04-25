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
  "dataSource" : "example",
  "granularitySpec" : {
    "type" : "uniform",
    "gran" : "DAY",
    "intervals" : [ "2010/2020" ]
  },
  "aggregators" : [ {
     "type" : "count",
     "name" : "count"
   }, {
     "type" : "doubleSum",
     "name" : "value",
     "fieldName" : "value"
  } ],
  "firehose" : {
    "type" : "local",
    "baseDir" : "/tmp/data/json",
    "filter" : "sample_data.json",
    "parser" : {
      "timestampSpec" : {
        "column" : "timestamp"
      },
      "data" : {
        "format" : "json",
        "dimensions" : [ "dim1", "dim2", "dim3" ]
      }
    }
  }
}
```

|property|description|required?|
|--------|-----------|---------|
|type|The task type, this should always be "index".|yes|
|id|The task ID. If this is not explicitly specified, Druid generates the task ID using the name of the task file and date-time stamp. |no|
|granularitySpec|Specifies the segment chunks that the task will process. `type` is always "uniform"; `gran` sets the granularity of the chunks ("DAY" means all segments containing timestamps in the same day), while `intervals` sets the interval that the chunks will cover.|yes|
|spatialDimensions|Dimensions to build spatial indexes over. See [Geographic Queries](GeographicQueries.html).|no|
|aggregators|The metrics to aggregate in the data set. For more info, see [Aggregations](Aggregations.html).|yes|
|indexGranularity|The rollup granularity for timestamps. See [Realtime Ingestion](Realtime-ingestion.html) for more information. |no|
|targetPartitionSize|Used in sharding. Determines how many rows are in each segment.|no|
|firehose|The input source of data. For more info, see [Firehose](Firehose.html).|yes|
|rowFlushBoundary|Used in determining when intermediate persist should occur to disk.|no|

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


The Hadoop Index Config submitted as part of an Hadoop Index Task is identical to the Hadoop Index Config used by the `HadoopBatchIndexer` except that three fields must be omitted: `segmentOutputPath`, `workingPath`, `metadataUpdateSpec`. The Indexing Service takes care of setting these fields internally.

#### Using your own Hadoop distribution

Druid is compiled against Apache hadoop-client 2.3.0. However, if you happen to use a different flavor of hadoop that is API compatible with hadoop-client 2.3.0, you should only have to change the hadoopCoordinates property to point to the maven artifact used by your distribution.

#### Resolving dependency conflicts running HadoopIndexTask

Currently, the HadoopIndexTask creates a single classpath to run the HadoopDruidIndexerJob, which can lead to version conflicts between various dependencies of Druid, extension modules, and Hadoop's own dependencies.

The Hadoop index task will put Druid's dependencies first on the classpath, followed by any extensions dependencies, and any Hadoop dependencies last.

If you are having trouble with any extensions in HadoopIndexTask, it may be the case that Druid, or one of its dependencies, depends on a different version of a library than what you are using as part of your extensions, but Druid's version overrides the one in your extension. In that case you probably want to build your own Druid version and override the offending library by adding an explicit dependency to the pom.xml of each druid sub-module that depends on it.

### Realtime Index Task

The indexing service can also run real-time tasks. These tasks effectively transform a middle manager into a real-time node. We introduced real-time tasks as a way to programmatically add new real-time data sources without needing to manually add nodes. The grammar for the real-time task is as follows:

```json
{
  "type" : "index_realtime",
  "id": "example",
  "resource": {
    "availabilityGroup" : "someGroup",
    "requiredCapacity" : 1
  },
  "schema": {
    "dataSource": "dataSourceName",
    "aggregators": [
      {
        "type": "count",
        "name": "events"
      },
      {
        "type": "doubleSum",
        "name": "outColumn",
        "fieldName": "inColumn"
      }
    ],
    "indexGranularity": "minute",
    "shardSpec": {
      "type": "none"
    }
  },
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
    "feed": "your_kafka_topic",
    "parser": {
      "timestampSpec": {
        "column": "timestamp",
        "format": "iso"
      },
      "data": {
        "format": "json"
      },
      "dimensionExclusions": [
        "value"
      ]
    }
  },
  "fireDepartmentConfig": {
    "maxRowsInMemory": 500000,
    "intermediatePersistPeriod": "PT10m"
  },
  "windowPeriod": "PT10m",
  "segmentGranularity": "hour"
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
    "segments": <JSON list of DataSegment objects to append>
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
    "segments": <JSON list of DataSegment objects to append>
}
```

### Kill Task

Kill tasks delete all information about a segment and removes it from deep storage. Killable segments must be disabled (used==0) in the Druid segment table. The available grammar is:

```json
{
    "type": "kill",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "segments": <JSON list of DataSegment objects to append>
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
