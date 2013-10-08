---
layout: doc_page
---
Tasks are run on middle managers and always operate on a single data source.

There are several different types of tasks.

Segment Creation Tasks
----------------------

#### Index Task

The Index Task is a simpler variation of the Index Hadoop task that is designed to be used for smaller data sets. The task executes within the indexing service and does not require an external Hadoop setup to use. The grammar of the index task is as follows:

```
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
|id|The task ID.|no|
|granularitySpec|See [granularitySpec](Tasks.html#Granularity-Spec)|yes|
|spatialDimensions|Dimensions to build spatial indexes over. See [Spatial-Indexing](Spatial-Indexing.html)|no|
|aggregators|The metrics to aggregate in the data set. For more info, see [Aggregations](Aggregations.html)|yes|
|indexGranularity|The rollup granularity for timestamps.|no|
|targetPartitionSize|Used in sharding. Determines how many rows are in each segment.|no|
|firehose|The input source of data. For more info, see [Firehose](Firehose.html)|yes|
|rowFlushBoundary|Used in determining when intermediate persist should occur to disk.|no|

#### Index Hadoop Task

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
|config|See [Batch Ingestion](Batch-ingestion.html)|yes|

#### Index Realtime Task

Segment Merging Tasks
---------------------

#### Append Task

Append tasks append a list of segments together into a single segment (one after the other). The grammar is:

```
{
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "segments": <JSON list of DataSegment objects to append>
}
```

#### Merge Task

Merge tasks merge a list of segments together. Any common timestamps are merged. The grammar is:

```
{
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "segments": <JSON list of DataSegment objects to append>
}
```

Segment Destroying Tasks
------------------------

#### Delete Task

Delete tasks create empty segments with no data. The grammar is:

```
{
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "segments": <JSON list of DataSegment objects to append>
}
```

#### Kill Task

Kill tasks delete all information about a segment and removes it from deep storage. Killable segments must be disabled (used==0) in the Druid segment table. The available grammar is:

```
{
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "segments": <JSON list of DataSegment objects to append>
}
```

Misc. Tasks
-----------

#### Version Converter Task

These tasks convert segments from an existing older index version to the latest index version. The available grammar is:

```
{
    "id": <task_id>,
    "groupId" : <task_group_id>,
    "dataSource": <task_datasource>,
    "interval" : <segment_interval>,
    "segment": <JSON DataSegment object to convert>
}
```

#### Noop Task

These tasks start, sleep for a time and are used only for testing. The available grammar is:

```
{
    "id": <optional_task_id>,
    "interval" : <optional_segment_interval>,
    "runTime" : <optional_millis_to_sleep>,
    "firehose": <optional_firehose_to_test_connect>
}
```

Locking
-------
Once an overlord node accepts a task, a lock is created for the data source and interval specified in the task. Tasks do not need to explicitly release locks, they are released upon task completion. Tasks may potentially release locks early if they desire. Tasks ids are unique by naming them using UUIDs or the timestamp in which the task was created. Tasks are also part of a "task group", which is a set of tasks that can share interval locks.
