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
Within the context of this topic data management refers to Apache Druid's data maintenance capabilities for existing datasources. There are several options to help you keep your data relevant and to help your Druid cluster remain performant. For example updating, reingesting, adding lookups, reindexing, or deleting data.

In addition to the tasks covered on this page, you can also use segment compaction to improve the layout of your existing data. Refer to [Segment optimization](../operations/segment-optimization.md) to see if compaction will help in your environment. For an overview and steps to configure manual compaction tasks, see [Compaction](./compaction.md). 

## Adding new data to existing datasources

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

This section assumes you understand how to do batch ingestion without Hadoop using [native batch indexing](../ingestion/native-batch.md). Native batch indexing uses an `inputSource` to know where and how to read the input data. You can use the [`DruidInputSource`](./native-batch-input-source.md) to read data from segments inside Druid. You can use Parallel task (`index_parallel`) for all native batch reindexing tasks. Increase the `maxNumConcurrentSubTasks` to accommodate the amount of data your are reindexing. See [Capacity planning](native-batch.md#capacity-planning).

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

The kill task deletes all information about segments and removes them from deep storage. Segments to kill must be unused (used==0) in the Druid segment table.

The available grammar is:

```json
{
    "type": "kill",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "interval" : <all_segments_in_this_interval_will_die!>,
    "markAsUnused": <true|false>,
    "context": <task context>
}
```

If `markAsUnused` is true (default is false), the kill task will first mark any segments within the specified interval as unused, before deleting the unused segments within the interval.

**WARNING!** The kill task permanently removes all information about the affected segments from the metadata store and deep storage. These segments cannot be recovered after the kill task runs, this operation cannot be undone. 

## Retention

Druid supports retention rules, which are used to define intervals of time where data should be preserved, and intervals where data should be discarded.

Druid also supports separating Historical processes into tiers, and the retention rules can be configured to assign data for specific intervals to specific tiers.

These features are useful for performance/cost management; a common use case is separating Historical processes into a "hot" tier and a "cold" tier.

For more information, please see [Load rules](../operations/rule-configuration.md).

## Learn more
See the following topics for more information:
- [Compaction](./compaction.md) for an overview and steps to configure manual compaction tasks.
- [Segments](../design/segments.md) for information on how Druid handles segment versioning.
