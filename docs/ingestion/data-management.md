---
id: data-management
title: "Data management"
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->




## Schema changes

Schemas for datasources can change at any time and Apache Druid supports different schemas among segments.

### Replacing segments

Druid uniquely
identifies segments using the datasource, interval, version, and partition number. The partition number is only visible in the segment id if
there are multiple segments created for some granularity of time. For example, if you have hourly segments, but you
have more data in an hour than a single segment can hold, you can create multiple segments for the same hour. These segments will share
the same datasource, interval, and version, but have linearly increasing partition numbers.

```
foo_2015-01-01/2015-01-02_v1_0
foo_2015-01-01/2015-01-02_v1_1
foo_2015-01-01/2015-01-02_v1_2
```

In the example segments above, the dataSource = foo, interval = 2015-01-01/2015-01-02, version = v1, partitionNum = 0.
If at some later point in time, you reindex the data with a new schema, the newly created segments will have a higher version id.

```
foo_2015-01-01/2015-01-02_v2_0
foo_2015-01-01/2015-01-02_v2_1
foo_2015-01-01/2015-01-02_v2_2
```

Druid batch indexing (either Hadoop-based or IndexTask-based) guarantees atomic updates on an interval-by-interval basis.
In our example, until all `v2` segments for `2015-01-01/2015-01-02` are loaded in a Druid cluster, queries exclusively use `v1` segments.
Once all `v2` segments are loaded and queryable, all queries ignore `v1` segments and switch to the `v2` segments.
Shortly afterwards, the `v1` segments are unloaded from the cluster.

Note that updates that span multiple segment intervals are only atomic within each interval. They are not atomic across the entire update.
For example, you have segments such as the following:

```
foo_2015-01-01/2015-01-02_v1_0
foo_2015-01-02/2015-01-03_v1_1
foo_2015-01-03/2015-01-04_v1_2
```

`v2` segments will be loaded into the cluster as soon as they are built and replace `v1` segments for the period of time the
segments overlap. Before v2 segments are completely loaded, your cluster may have a mixture of `v1` and `v2` segments.

```
foo_2015-01-01/2015-01-02_v1_0
foo_2015-01-02/2015-01-03_v2_1
foo_2015-01-03/2015-01-04_v1_2
```

In this case, queries may hit a mixture of `v1` and `v2` segments.

### Different schemas among segments

Druid segments for the same datasource may have different schemas. If a string column (dimension) exists in one segment but not
another, queries that involve both segments still work. Queries for the segment missing the dimension will behave as if the dimension has only null values.
Similarly, if one segment has a numeric column (metric) but another does not, queries on the segment missing the
metric will generally "do the right thing". Aggregations over this missing metric behave as if the metric were missing.

<a name="compact"></a>

## Compaction and reindexing

Compaction is a type of overwrite operation, which reads an existing set of segments, combines them into a new set with larger but fewer segments, and overwrites the original set with the new compacted set, without changing the data that is stored.

For performance reasons, it is sometimes beneficial to compact a set of segments into a set of larger but fewer segments, as there is some per-segment processing and memory overhead in both the ingestion and querying paths.

Compaction tasks merge all segments of the given interval. The syntax is:

```json
{
    "type": "compact",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "ioConfig": <IO config>,
    "dimensionsSpec" <custom dimensionsSpec>,
    "metricsSpec" <custom metricsSpec>,
    "segmentGranularity": <segment granularity after compaction>,
    "tuningConfig" <parallel indexing task tuningConfig>,
    "context": <task context>
}
```

|Field|Description|Required|
|-----|-----------|--------|
|`type`|Task type. Should be `compact`|Yes|
|`id`|Task id|No|
|`dataSource`|DataSource name to be compacted|Yes|
|`ioConfig`|ioConfig for compaction task. See [Compaction IOConfig](#compaction-ioconfig) for details.|Yes|
|`dimensionsSpec`|Custom dimensionsSpec. Compaction task will use this dimensionsSpec if exist instead of generating one. See below for more details.|No|
|`metricsSpec`|Custom metricsSpec. Compaction task will use this metricsSpec if specified rather than generating one.|No|
|`segmentGranularity`|If this is set, compactionTask will change the segment granularity for the given interval. See `segmentGranularity` of [`granularitySpec`](index.md#granularityspec) for more details. See the below table for the behavior.|No|
|`tuningConfig`|[Parallel indexing task tuningConfig](../ingestion/native-batch.md#tuningconfig)|No|
|`context`|[Task context](../ingestion/tasks.md#context)|No|


An example of compaction task is

```json
{
  "type" : "compact",
  "dataSource" : "wikipedia",
  "ioConfig" : {
    "type": "compact",
    "inputSpec": {
      "type": "interval",
      "interval": "2017-01-01/2018-01-01"
    }
  }
}
```

This compaction task reads _all segments_ of the interval `2017-01-01/2018-01-01` and results in new segments.
Since `segmentGranularity` is null, the original segment granularity will be remained and not changed after compaction.
To control the number of result segments per time chunk, you can set [maxRowsPerSegment](../configuration/index.md#compaction-dynamic-configuration) or [numShards](../ingestion/native-batch.md#tuningconfig).
Please note that you can run multiple compactionTasks at the same time. For example, you can run 12 compactionTasks per month instead of running a single task for the entire year.

A compaction task internally generates an `index` task spec for performing compaction work with some fixed parameters.
For example, its `inputSource` is always the [DruidInputSource](native-batch.md#druid-input-source), and `dimensionsSpec` and `metricsSpec`
include all dimensions and metrics of the input segments by default.

Compaction tasks will exit with a failure status code, without doing anything, if the interval you specify has no
data segments loaded in it (or if the interval you specify is empty).

The output segment can have different metadata from the input segments unless all input segments have the same metadata.

- Dimensions: since Apache Druid supports schema change, the dimensions can be different across segments even if they are a part of the same dataSource.
If the input segments have different dimensions, the output segment basically includes all dimensions of the input segments.
However, even if the input segments have the same set of dimensions, the dimension order or the data type of dimensions can be different. For example, the data type of some dimensions can be
changed from `string` to primitive types, or the order of dimensions can be changed for better locality.
In this case, the dimensions of recent segments precede that of old segments in terms of data types and the ordering.
This is because more recent segments are more likely to have the new desired order and data types. If you want to use
your own ordering and types, you can specify a custom `dimensionsSpec` in the compaction task spec.
- Roll-up: the output segment is rolled up only when `rollup` is set for all input segments.
See [Roll-up](../ingestion/index.md#rollup) for more details.
You can check that your segments are rolled up or not by using [Segment Metadata Queries](../querying/segmentmetadataquery.md#analysistypes).


### Compaction IOConfig

The compaction IOConfig requires specifying `inputSpec` as seen below.

|Field|Description|Required|
|-----|-----------|--------|
|`type`|Task type. Should be `compact`|Yes|
|`inputSpec`|Input specification|Yes|

There are two supported `inputSpec`s for now.

The interval `inputSpec` is:

|Field|Description|Required|
|-----|-----------|--------|
|`type`|Task type. Should be `interval`|Yes|
|`interval`|Interval to compact|Yes|

The segments `inputSpec` is:

|Field|Description|Required|
|-----|-----------|--------|
|`type`|Task type. Should be `segments`|Yes|
|`segments`|A list of segment IDs|Yes|


## Adding new data

Druid can insert new data to an existing datasource by appending new segments to existing segment sets. It can also add new data by merging an existing set of segments with new data and overwriting the original set.

Druid does not support single-record updates by primary key.

<a name="update"></a>

## Updating existing data

Once you ingest some data in a dataSource for an interval and create Apache Druid segments, you might want to make changes to
the ingested data. There are several ways this can be done.

### Using lookups

If you have a dimension where values need to be updated frequently, try first using [lookups](../querying/lookups.md). A
classic use case of lookups is when you have an ID dimension stored in a Druid segment, and want to map the ID dimension to a
human-readable String value that may need to be updated periodically.

### Reingesting data

If lookup-based techniques are not sufficient, you will need to reingest data into Druid for the time chunks that you
want to update. This can be done using one of the [batch ingestion methods](index.md#batch) in overwrite mode (the
default mode). It can also be done using [streaming ingestion](index.md#streaming), provided you drop data for the
relevant time chunks first.

If you do the reingestion in batch mode, Druid's atomic update mechanism means that queries will flip seamlessly from
the old data to the new data.

We recommend keeping a copy of your raw data around in case you ever need to reingest it.

### With Hadoop-based ingestion

This section assumes you understand how to do batch ingestion using Hadoop. See
[Hadoop batch ingestion](./hadoop.md) for more information. Hadoop batch-ingestion can be used for reindexing and delta ingestion.

Druid uses an `inputSpec` in the `ioConfig` to know where the data to be ingested is located and how to read it.
For simple Hadoop batch ingestion, `static` or `granularity` spec types allow you to read data stored in deep storage.

There are other types of `inputSpec` to enable reindexing and delta ingestion.

### Reindexing with Native Batch Ingestion

This section assumes you understand how to do batch ingestion without Hadoop using [native batch indexing](../ingestion/native-batch.md). Native batch indexing uses an `inputSource` to know where and how to read the input data. You can use the [`DruidInputSource`](native-batch.md#druid-input-source) to read data from segments inside Druid. You can use Parallel task (`index_parallel`) for all native batch reindexing tasks. Increase the `maxNumConcurrentSubTasks` to accommodate the amount of data your are reindexing. See [Capacity planning](native-batch.md#capacity-planning).

<a name="delete"></a>

## Deleting data

Druid supports permanent deletion of segments that are in an "unused" state (see the
[Segment lifecycle](../design/architecture.md#segment-lifecycle) section of the Architecture page).

The Kill Task deletes unused segments within a specified interval from metadata storage and deep storage.

For more information, please see [Kill Task](../ingestion/tasks.md#kill).

Permanent deletion of a segment in Apache Druid has two steps:

1. The segment must first be marked as "unused". This occurs when a segment is dropped by retention rules, and when a user manually disables a segment through the Coordinator API.
2. After segments have been marked as "unused", a Kill Task will delete any "unused" segments from Druid's metadata store as well as deep storage.

For documentation on retention rules, please see [Data Retention](../operations/rule-configuration.md).

For documentation on disabling segments using the Coordinator API, please see the
[Coordinator Datasources API](../operations/api-reference.md#coordinator-datasources) reference.

A data deletion tutorial is available at [Tutorial: Deleting data](../tutorials/tutorial-delete-data.md)

## Kill Task

Kill tasks delete all information about a segment and removes it from deep storage. Segments to kill must be unused (used==0) in the Druid segment table. The available grammar is:

```json
{
    "type": "kill",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "interval" : <all_segments_in_this_interval_will_die!>,
    "context": <task context>
}
```

## Retention

Druid supports retention rules, which are used to define intervals of time where data should be preserved, and intervals where data should be discarded.

Druid also supports separating Historical processes into tiers, and the retention rules can be configured to assign data for specific intervals to specific tiers.

These features are useful for performance/cost management; a common use case is separating Historical processes into a "hot" tier and a "cold" tier.

For more information, please see [Load rules](../operations/rule-configuration.md).
