---
layout: doc_page
---
# Tasks
Tasks are run on middle managers and always operate on a single data source. Tasks are submitted using [POST requests](../design/indexing-service.html).

There are several different types of tasks.

Segment Creation Tasks
----------------------

### Native Batch Indexing Task

See [Native batch ingestion](../ingestion/native-batch.html).

### Hadoop Index Task

See [Hadoop batch ingestion](../ingestion/hadoop.html).

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
