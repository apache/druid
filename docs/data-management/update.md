---
id: update
title: "Data updates"
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

## Overwrite

Apache Druid stores data [partitioned by time chunk](../design/storage.md) and supports
overwriting existing data using time ranges. Data outside the replacement time range is not touched. Overwriting of
existing data is done using the same mechanisms as [batch ingestion](../ingestion/index.md#batch).

For example:

- [Native batch](../ingestion/native-batch.md) with `appendToExisting: false`, and `intervals` set to a specific
  time range, overwrites data for that time range.
- [SQL `REPLACE <table> OVERWRITE [ALL | WHERE ...]`](../multi-stage-query/reference.md#replace) overwrites data for
  the entire table or for a specified time range.

In both cases, Druid's atomic update mechanism ensures that queries will flip seamlessly from the old data to the new
data on a time-chunk-by-time-chunk basis.

Ingestion and overwriting cannot run concurrently for the same time range of the same datasource. While an overwrite job
is ongoing for a particular time range of a datasource, new ingestions for that time range are queued up. Ingestions for
other time ranges proceed as normal. Read-only queries also proceed as normal, using the pre-existing version of the
data.

:::info
 Druid does not support single-record updates by primary key.
:::

## Reindex

Reindexing is an [overwrite of existing data](#overwrite) where the source of new data is the existing data itself. It
is used to perform schema changes, repartition data, filter out unwanted data, enrich existing data, and so on. This
behaves just like any other [overwrite](#overwrite) with regard to atomic updates and locking.

With [native batch](../ingestion/native-batch.md), use the [`druid` input
source](../ingestion/input-sources.md#druid-input-source). If needed,
[`transformSpec`](../ingestion/ingestion-spec.md#transformspec) can be used to filter or modify data during the
reindexing job.

With SQL, use [`REPLACE <table> OVERWRITE`](../multi-stage-query/reference.md#replace) with `SELECT ... FROM <table>`.
(Druid does not have `UPDATE` or `ALTER TABLE` statements.) Any SQL SELECT query can be used to filter,
modify, or enrich the data during the reindexing job.

## Rolled-up datasources

Rolled-up datasources can be effectively updated using appends, without rewrites. When you append a row that has an
identical set of dimensions to an existing row, queries that use aggregation operators automatically combine those two
rows together at query time.

[Compaction](compaction.md) or [automatic compaction](automatic-compaction.md) can be used to physically combine these
matching rows together later on, by rewriting segments in the background.

## Lookups

If you have a dimension where values need to be updated frequently, try first using [lookups](../querying/lookups.md). A
classic use case of lookups is when you have an ID dimension stored in a Druid segment, and want to map the ID dimension to a
human-readable string that may need to be updated periodically.
