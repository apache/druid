---
layout: doc_page
---

# Compaction Task

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
|`tuningConfig`|[Index task tuningConfig](../ingestion/native-batch.html#tuningconfig)|No|
|`context`|[Task context](../ingestion/locking-and-priority.html#task-context)|No|

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
To control the number of result segments, you can set `targetPartitionSize` or `numShards`. See [indexTuningConfig](../ingestion/native-batch.html#tuningconfig) for more details.
To merge each day's worth of data into separate segments, you can submit multiple `compact` tasks, one for each day. They will run in parallel.

A compaction task internally generates an `index` task spec for performing compaction work with some fixed parameters.
For example, its `firehose` is always the [ingestSegmentSpec](./firehose.html#ingestsegmentfirehose), and `dimensionsSpec` and `metricsSpec`
include all dimensions and metrics of the input segments by default.

Compaction tasks will exit with a failure status code, without doing anything, if the interval you specify has no
data segments loaded in it (or if the interval you specify is empty).

The output segment can have different metadata from the input segments unless all input segments have the same metadata.

- Dimensions: since Druid supports schema change, the dimensions can be different across segments even if they are a part of the same dataSource.
If the input segments have different dimensions, the output segment basically includes all dimensions of the input segments.
However, even if the input segments have the same set of dimensions, the dimension order or the data type of dimensions can be different. For example, the data type of some dimensions can be
changed from `string` to primitive types, or the order of dimensions can be changed for better locality.
In this case, the dimensions of recent segments precede that of old segments in terms of data types and the ordering.
This is because more recent segments are more likely to have the new desired order and data types. If you want to use
your own ordering and types, you can specify a custom `dimensionsSpec` in the compaction task spec.
- Roll-up: the output segment is rolled up only when `rollup` is set for all input segments.
See [Roll-up](../ingestion/index.html#rollup) for more details. 
You can check that your segments are rolled up or not by using [Segment Metadata Queries](../querying/segmentmetadataquery.html#analysistypes).
- Partitioning: The compaction task is a special form of native batch indexing task, so it always uses hash-based partitioning on the full set of dimensions.