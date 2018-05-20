---
layout: doc_page
---
# Tasks
Tasks are run on middle managers and always operate on a single data source. Tasks are submitted using [POST requests](../design/indexing-service.html).

There are several different types of tasks.

Segment Creation Tasks
----------------------

### Hadoop Index Task

See [batch ingestion](../ingestion/batch-ingestion.html).

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
      "targetPartitionSize" : 5000000,
      "maxRowsInMemory" : 1000000
    }
  }
}
```

#### Task Properties

|property|description|required?|
|--------|-----------|---------|
|type|The task type, this should always be "index".|yes|
|id|The task ID. If this is not explicitly specified, Druid generates the task ID using task type, data source name, interval, and date-time stamp. |no|
|spec|The ingestion spec including the data schema, IOConfig, and TuningConfig. See below for more details. |yes|
|context|Context containing various task configuration parameters. See below for more details.|no|

#### Task Priority

Druid's indexing tasks use locks for atomic data ingestion. Each lock is acquired for the combination of a dataSource and an interval. Once a task acquires a lock, it can write data for the dataSource and the interval of the acquired lock unless the lock is released or preempted. Please see [the below Locking section](#locking)

Each task has a priority which is used for lock acquisition. The locks of higher-priority tasks can preempt the locks of lower-priority tasks if they try to acquire for the same dataSource and interval. If some locks of a task are preempted, the behavior of the preempted task depends on the task implementation. Usually, most tasks finish as failed if they are preempted.

Tasks can have different default priorities depening on their types. Here are a list of default priorities. Higher the number, higher the priority.

|task type|default priority|
|---------|----------------|
|Realtime index task|75|
|Batch index task|50|
|Merge/Append/Compaction task|25|
|Other tasks|0|

You can override the task priority by setting your priority in the task context like below.

```json
"context" : {
  "priority" : 100
}
```

#### DataSchema

This field is required.

See [Ingestion](../ingestion/index.html)

#### IOConfig

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|The task type, this should always be "index".|none|yes|
|firehose|Specify a [Firehose](../ingestion/firehose.html) here.|none|yes|
|appendToExisting|Creates segments as additional shards of the latest version, effectively appending to the segment set instead of replacing it. This will only work if the existing segment set has extendable-type shardSpecs (which can be forced by setting 'forceExtendableShardSpecs' in the tuning config).|false|no|

#### TuningConfig

The tuningConfig is optional and default parameters will be used if no tuningConfig is specified. See below for more details.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|The task type, this should always be "index".|none|yes|
|targetPartitionSize|Used in sharding. Determines how many rows are in each segment.|5000000|no|
|maxRowsInMemory|Used in determining when intermediate persists to disk should occur. Normally user does not need to set this, but depending on the nature of data, if rows are short in terms of bytes, user may not want to store a million rows in memory and this value should be set.|1000000|no|
|maxBytesInMemory|Used in determining when intermediate persists to disk should occur. Normally this is computed internally and user does not need to set it. This value represents number of bytes to aggregate in heap memory before persisting. This is based on a rough estimate of memory usage and not actual usage. The maximum heap memory usage for indexing is maxBytesInMemory * (2 + maxPendingPersists)|1/6 of max JVM memory|no|
|maxTotalRows|Total number of rows in segments waiting for being published. Used in determining when intermediate publish should occur.|150000|no|
|numShards|Directly specify the number of shards to create. If this is specified and 'intervals' is specified in the granularitySpec, the index task can skip the determine intervals/partitions pass through the data. numShards cannot be specified if targetPartitionSize is set.|null|no|
|indexSpec|defines segment storage format options to be used at indexing time, see [IndexSpec](#indexspec)|null|no|
|maxPendingPersists|Maximum number of persists that can be pending but not started. If this limit would be exceeded by a new intermediate persist, ingestion will block until the currently-running persist finishes. Maximum heap memory usage for indexing scales with maxRowsInMemory * (2 + maxPendingPersists).|0 (meaning one persist can be running concurrently with ingestion, and none can be queued up)|no|
|forceExtendableShardSpecs|Forces use of extendable shardSpecs. Experimental feature intended for use with the [Kafka indexing service extension](../development/extensions-core/kafka-ingestion.html).|false|no|
|forceGuaranteedRollup|Forces guaranteeing the [perfect rollup](../design/index.html). The perfect rollup optimizes the total size of generated segments and querying time while indexing time will be increased. This flag cannot be used with either `appendToExisting` of IOConfig or `forceExtendableShardSpecs`. For more details, see the below __Segment publishing modes__ section.|false|no|
|reportParseExceptions|If true, exceptions encountered during parsing will be thrown and will halt ingestion; if false, unparseable rows and fields will be skipped.|false|no|
|publishTimeout|Milliseconds to wait for publishing segments. It must be >= 0, where 0 means to wait forever.|0|no|
|segmentWriteOutMediumFactory|Segment write-out medium to use when creating segments. See [Indexing Service Configuration](../configuration/indexing-service.html) page, "SegmentWriteOutMediumFactory" section for explanation and available options.|Not specified, the value from `druid.peon.defaultSegmentWriteOutMediumFactory` is used|no|

#### IndexSpec

The indexSpec defines segment storage format options to be used at indexing time, such as bitmap type and column
compression formats. The indexSpec is optional and default parameters will be used if not specified.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|bitmap|Object|Compression format for bitmap indexes. Should be a JSON object; see below for options.|no (defaults to Concise)|
|dimensionCompression|String|Compression format for dimension columns. Choose from `LZ4`, `LZF`, or `uncompressed`.|no (default == `LZ4`)|
|metricCompression|String|Compression format for metric columns. Choose from `LZ4`, `LZF`, `uncompressed`, or `none`.|no (default == `LZ4`)|
|longEncoding|String|Encoding format for metric and dimension columns with type long. Choose from `auto` or `longs`. `auto` encodes the values using offset or lookup table depending on column cardinality, and store them with variable size. `longs` stores the value as is with 8 bytes each.|no (default == `longs`)|

##### Bitmap types

For Concise bitmaps:

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|Must be `concise`.|yes|

For Roaring bitmaps:

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|Must be `roaring`.|yes|
|compressRunOnSerialization|Boolean|Use a run-length encoding where it is estimated as more space efficient.|no (default == `true`)|

#### Segment publishing modes

While ingesting data using the Index task, it creates segments from the input data and publishes them. For segment publishing, the Index task supports two segment publishing modes, i.e., _bulk publishing mode_ and _incremental publishing mode_ for [perfect rollup and best-effort rollup](./design/index.html), respectively.

In the bulk publishing mode, every segment is published at the very end of the index task. Until then, created segments are stored in the memory and local storage of the node running the index task. As a result, this mode might cause a problem due to limited storage capacity, and is not recommended to use in production.

On the contrary, in the incremental publishing mode, segments are incrementally published, that is they can be published in the middle of the index task. More precisely, the index task collects data and stores created segments in the memory and disks of the node running that task until the total number of collected rows exceeds `maxTotalRows`. Once it exceeds, the index task immediately publishes all segments created until that moment, cleans all published segments up, and continues to ingest remaining data.

To enable bulk publishing mode, `forceGuaranteedRollup` should be set in the TuningConfig. Note that this option cannot be used with either `forceExtendableShardSpecs` of TuningConfig or `appendToExisting` of IOConfig.

Segment Merging Tasks
---------------------

### Append Task

Append tasks append a list of segments together into a single segment (one after the other). The grammar is:

```json
{
    "type": "append",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "segments": <JSON list of DataSegment objects to append>,
    "aggregations": <optional list of aggregators>,
    "context": <task context>
}
```

### Merge Task

Merge tasks merge a list of segments together. Any common timestamps are merged.
If rollup is disabled as part of ingestion, common timestamps are not merged and rows are reordered by their timestamp.

The grammar is:

```json
{
    "type": "merge",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "aggregations": <list of aggregators>,
    "rollup": <whether or not to rollup data during a merge>,
    "segments": <JSON list of DataSegment objects to merge>,
    "context": <task context>
}
```

### Same Interval Merge Task

Same Interval Merge task is a shortcut of merge task, all segments in the interval are going to be merged.

The grammar is:

```json
{
    "type": "same_interval_merge",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "aggregations": <list of aggregators>,
    "rollup": <whether or not to rollup data during a merge>,
    "interval": <DataSegment objects in this interval are going to be merged>,
    "context": <task context>
}
```

### Compaction Task

Compaction tasks merge all segments of the given interval. The syntax is:

```json
{
    "type": "compact",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "interval": <interval to specify segments to be merged>,
    "dimensions" <custom dimensionsSpec>,
    "tuningConfig" <index task tuningConfig>,
    "context": <task context>
}
```

|Field|Description|Required|
|-----|-----------|--------|
|`type`|Task type. Should be `compact`|Yes|
|`id`|Task id|No|
|`dataSource`|dataSource name to be compacted|Yes|
|`interval`|interval of segments to be compacted|Yes|
|`dimensions`|custom dimensionsSpec. compaction task will use this dimensionsSpec if exist instead of generating one. See below for more details.|No|
|`tuningConfig`|[Index task tuningConfig](#tuningconfig)|No|
|`context`|[Task context](#taskcontext)|No|

An example of compaction task is

```json
{
  "type" : "compact",
  "dataSource" : "wikipedia",
  "interval" : "2017-01-01/2018-01-01"
}
```

This compaction task reads _all segments_ of the interval `2017-01-01/2018-01-01` and results in new segments.
Note that intervals of the input segments are merged into a single interval of `2017-01-01/2018-01-01` no matter what the segmentGranularity was.
To controll the number of result segments, you can set `targetPartitionSize` or `numShards`. See [indexTuningConfig](#tuningconfig) for more details.
To merge each day's worth of data into separate segments, you can submit multiple `compact` tasks, one for each day. They will run in parallel.

A compaction task internally generates an `index` task spec for performing compaction work with some fixed parameters.
For example, its `firehose` is always the [ingestSegmentSpec](./firehose.html), and `dimensionsSpec` and `metricsSpec`
include all dimensions and metrics of the input segments by default.

The output segment can have different metadata from the input segments unless all input segments have the same metadata.

- Dimensions: since Druid supports schema change, the dimensions can be different across segments even if they are a part of the same dataSource.
If the input segments have different dimensions, the output segment basically includes all dimensions of the input segments.
However, even if the input segments have the same set of dimensions, the dimension order or the data type of dimensions can be different. For example, the data type of some dimensions can be
changed from `string` to primitive types, or the order of dimensions can be changed for better locality (See [Partitioning](batch-ingestion.html#partitioning-specification)).
In this case, the dimensions of recent segments precede that of old segments in terms of data types and the ordering.
This is because more recent segments are more likely to have the new desired order and data types. If you want to use
your own ordering and types, you can specify a custom `dimensionsSpec` in the compaction task spec.
- Roll-up: the output segment is rolled up only when `rollup` is set for all input segments.
See [Roll-up](../design/index.html#roll-up) for more details. 
You can check that your segments are rolled up or not by using [Segment Metadata Queries](../querying/segmentmetadataquery.html#analysistypes).

Segment Destroying Tasks
------------------------

### Kill Task

Kill tasks delete all information about a segment and removes it from deep storage. Killable segments must be disabled (used==0) in the Druid segment table. The available grammar is:

```json
{
    "type": "kill",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "interval" : <all_segments_in_this_interval_will_die!>,
    "context": <task context>
}
```

Misc. Tasks
-----------

### Version Converter Task
The convert task suite takes active segments and will recompress them using a new IndexSpec. This is handy when doing activities like migrating from Concise to Roaring, or adding dimension compression to old segments.

Upon success the new segments will have the same version as the old segment with `_converted` appended. A convert task may be run against the same interval for the same datasource multiple times. Each execution will append another `_converted` to the version for the segments

There are two types of conversion tasks. One is the Hadoop convert task, and the other is the indexing service convert task. The Hadoop convert task runs on a hadoop cluster, and simply leaves a task monitor on the indexing service (similar to the hadoop batch task). The indexing service convert task runs the actual conversion on the indexing service.

#### Hadoop Convert Segment Task
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


#### Indexing Service Convert Segment Task
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

Task Context
------------

The task context is used for various task configuration parameters. The following parameters apply to all task types.

|property|default|description|
|--------|-------|-----------|
|taskLockTimeout|300000|task lock timeout in millisecond. For more details, see [the below Locking section](#locking).|
|priority|Different based on task types. See [Task Priority](#task-priority).|Task priority|

<div class="note caution">
When a task acquires a lock, it sends a request via HTTP and awaits until it receives a response containing the lock acquisition result.
As a result, an HTTP timeout error can occur if `taskLockTimeout` is greater than `druid.server.http.maxIdleTime` of overlords.
</div>

Locking
-------

Once an overlord node accepts a task, the task acquires locks for the data source and intervals specified in the task.

There are two lock types, i.e., _shared lock_ and _exclusive lock_.

- A task needs to acquire a shared lock before it reads segments of an interval. Multiple shared locks can be acquired for the same dataSource and interval. Shared locks are always preemptable, but they don't preempt each other.
- A task needs to acquire an exclusive lock before it writes segments for an interval. An exclusive lock is also preemptable except while the task is publishing segments.

Each task can have different lock priorities. The locks of higher-priority tasks can preempt the locks of lower-priority tasks. The lock preemption works based on _optimistic locking_. When a lock is preempted, it is not notified to the owner task immediately. Instead, it's notified when the owner task tries to acquire the same lock again. (Note that lock acquisition is idempotent unless the lock is preempted.) In general, tasks don't compete for acquiring locks because they usually targets different dataSources or intervals.

A task writing data into a dataSource must acquire exclusive locks for target intervals. Note that exclusive locks are still preemptable. That is, they also be able to be preempted by higher priority locks unless they are _publishing segments_ in a critical section. Once publishing segments is finished, those locks become preemptable again.

Tasks do not need to explicitly release locks, they are released upon task completion. Tasks may potentially release 
locks early if they desire. Task ids are unique by naming them using UUIDs or the timestamp in which the task was created. 
Tasks are also part of a "task group", which is a set of tasks that can share interval locks.
