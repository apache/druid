---
id: delete
title: "Data deletion"
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

## By time range, manually

Apache Druid stores data [partitioned by time chunk](../design/architecture.md#datasources-and-segments) and supports
deleting data for time chunks by dropping segments. This is a fast, metadata-only operation.

Deletion by time range happens in two steps:

1. Segments to be deleted must first be marked as ["unused"](../design/architecture.md#segment-lifecycle). This can
   happen when a segment is dropped by a [drop rule](../operations/rule-configuration.md) or when you manually mark a
   segment unused through the Coordinator API or web console. This is a soft delete: the data is not available for
   querying, but the segment files remains in deep storage, and the segment records remains in the metadata store.
2. Once a segment is marked "unused", you can use a [`kill` task](#kill-task) to permanently delete the segment file from
   deep storage and remove its record from the metadata store. This is a hard delete: the data is unrecoverable unless
   you have a backup.

For documentation on disabling segments using the Coordinator API, see the
[Coordinator API reference](../operations/api-reference.md#coordinator-datasources).

A data deletion tutorial is available at [Tutorial: Deleting data](../tutorials/tutorial-delete-data.md).

## By time range, automatically

Druid supports [load and drop rules](../operations/rule-configuration.md), which are used to define intervals of time
where data should be preserved, and intervals where data should be discarded. Data that falls under a drop rule is
marked unused, in the same manner as if you [manually mark that time range unused](#by-time-range-manually). This is a
fast, metadata-only operation.

Data that is dropped in this way is marked unused, but remains in deep storage. To permanently delete it, use a
[`kill` task](#kill-task).

## Specific records

Druid supports deleting specific records using [reindexing](update.md#reindex) with a filter. The filter specifies which
data remains after reindexing, so it must be the inverse of the data you want to delete. Because segments must be
rewritten to delete data in this way, it can be a time-consuming operation.

For example, to delete records where `userName` is `'bob'` with native batch indexing, use a
[`transformSpec`](../ingestion/ingestion-spec.md#transformspec) with filter `{"type": "not", "field": {"type":
"selector", "dimension": "userName", "value": "bob"}}`.

To delete the same records using SQL, use [REPLACE](../multi-stage-query/concepts.md#replace) with `WHERE userName <> 'bob'`.

To reindex using [native batch](../ingestion/native-batch.md), use the [`druid` input
source](../ingestion/native-batch-input-source.md#druid-input-source). If needed,
[`transformSpec`](../ingestion/ingestion-spec.md#transformspec) can be used to filter or modify data during the
reindexing job. To reindex with SQL, use [`REPLACE <table> OVERWRITE`](../multi-stage-query/reference.md#replace)
with `SELECT ... FROM <table>`. (Druid does not have `UPDATE` or `ALTER TABLE` statements.) Any SQL SELECT query can be
used to filter, modify, or enrich the data during the reindexing job.

Data that is deleted in this way is marked unused, but remains in deep storage. To permanently delete it, use a [`kill`
task](#kill-task).

## Entire table

Deleting an entire table works the same way as [deleting part of a table by time range](#by-time-range-manually). First,
mark all segments unused using the Coordinator API or web console. Then, optionally, delete it permanently using a
[`kill` task](#kill-task).

<a name="kill-task"></a>

## Permanently (`kill` task)

Data that has been overwritten or soft-deleted still remains as segments that have been marked unused. You can use a
`kill` task to permanently delete this data.

The available grammar is:

```json
{
    "type": "kill",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "interval" : <all_unused_segments_in_this_interval_will_die!>,
    "context": <task context>
}
```

**WARNING:** The `kill` task permanently removes all information about the affected segments from the metadata store and
deep storage. This operation cannot be undone.
