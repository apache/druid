---
layout: doc_page
title: "Compaction Task"
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

# Compaction Task

Compaction tasks merge all segments of the given interval. The syntax is:

```json
{
    "type": "compact",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "interval": <interval to specify segments to be merged>,
    "dimensions" <custom dimensionsSpec>,
    "keepSegmentGranularity": <true or false>,
    "segmentGranularity": <segment granularity after compaction>,
    "targetCompactionSizeBytes": <target size of compacted segments>
    "tuningConfig" <index task tuningConfig>,
    "context": <task context>
}
```

|Field|Description|Required|
|-----|-----------|--------|
|`type`|Task type. Should be `compact`|Yes|
|`id`|Task id|No|
|`dataSource`|DataSource name to be compacted|Yes|
|`interval`|Interval of segments to be compacted|Yes|
|`dimensionsSpec`|Custom dimensionsSpec. Compaction task will use this dimensionsSpec if exist instead of generating one. See below for more details.|No|
|`metricsSpec`|Custom metricsSpec. Compaction task will use this metricsSpec if specified rather than generating one.|No|
|`segmentGranularity`|If this is set, compactionTask will change the segment granularity for the given interval. See [segmentGranularity of Uniform Granularity Spec](./ingestion-spec.html#uniform-granularity-spec) for more details. See the below table for the behavior.|No|
|`keepSegmentGranularity`|Deprecated. Please use `segmentGranularity` instead. See the below table for its behavior.|No|
|`targetCompactionSizeBytes`|Target segment size after comapction. Cannot be used with `maxRowsPerSegment`, `maxTotalRows`, and `numShards` in tuningConfig.|No|
|`tuningConfig`|[Index task tuningConfig](../ingestion/native_tasks.html#tuningconfig)|No|
|`context`|[Task context](../ingestion/locking-and-priority.html#task-context)|No|

### Used segmentGranularity based on `segmentGranularity` and `keepSegmentGranularity`

|SegmentGranularity|keepSegmentGranularity|Used SegmentGranularity|
|------------------|----------------------|-----------------------|
|Non-null|True|Error|
|Non-null|False|Given segmentGranularity|
|Non-null|Null|Given segmentGranularity|
|Null|True|Original segmentGranularity|
|Null|False|ALL segmentGranularity. All events will fall into the single time chunk.|
|Null|Null|Original segmentGranularity|

An example of compaction task is

```json
{
  "type" : "compact",
  "dataSource" : "wikipedia",
  "interval" : "2017-01-01/2018-01-01"
}
```

This compaction task reads _all segments_ of the interval `2017-01-01/2018-01-01` and results in new segments.
Since both `segmentGranularity` and `keepSegmentGranularity` are null, the original segment granularity will be remained and not changed after compaction.
To control the number of result segments per time chunk, you can set [maxRowsPerSegment](../configuration/index.html#compaction-dynamic-configuration) or [numShards](../ingestion/native_tasks.html#tuningconfig).
Please note that you can run multiple compactionTasks at the same time. For example, you can run 12 compactionTasks per month instead of running a single task for the entire year.

A compaction task internally generates an `index` task spec for performing compaction work with some fixed parameters.
For example, its `firehose` is always the [ingestSegmentSpec](./firehose.html#ingestsegmentfirehose), and `dimensionsSpec` and `metricsSpec`
include all dimensions and metrics of the input segments by default.

Compaction tasks will exit with a failure status code, without doing anything, if the interval you specify has no
data segments loaded in it (or if the interval you specify is empty).

The output segment can have different metadata from the input segments unless all input segments have the same metadata.

- Dimensions: since Apache Druid (incubating) supports schema change, the dimensions can be different across segments even if they are a part of the same dataSource.
If the input segments have different dimensions, the output segment basically includes all dimensions of the input segments.
However, even if the input segments have the same set of dimensions, the dimension order or the data type of dimensions can be different. For example, the data type of some dimensions can be
changed from `string` to primitive types, or the order of dimensions can be changed for better locality.
In this case, the dimensions of recent segments precede that of old segments in terms of data types and the ordering.
This is because more recent segments are more likely to have the new desired order and data types. If you want to use
your own ordering and types, you can specify a custom `dimensionsSpec` in the compaction task spec.
- Roll-up: the output segment is rolled up only when `rollup` is set for all input segments.
See [Roll-up](../ingestion/index.html#rollup) for more details. 
You can check that your segments are rolled up or not by using [Segment Metadata Queries](../querying/segmentmetadataquery.html#analysistypes).
