---
id: concepts
title: "SQL-based ingestion concepts"
sidebar_label: "Key concepts"
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

:::info
 This page describes SQL-based batch ingestion using the [`druid-multi-stage-query`](../multi-stage-query/index.md)
 extension, new in Druid 24.0. Refer to the [ingestion methods](../ingestion/index.md#batch) table to determine which
 ingestion method is right for you.
:::

## Multi-stage query task engine

The `druid-multi-stage-query` extension adds a multi-stage query (MSQ) task engine that executes SQL statements as batch
tasks in the indexing service, which execute on [Middle Managers](../design/architecture.md#druid-services).
[INSERT](reference.md#insert) and [REPLACE](reference.md#replace) tasks publish
[segments](../design/storage.md) just like [all other forms of batch
ingestion](../ingestion/index.md#batch). Each query occupies at least two task slots while running: one controller task,
and at least one worker task. As an experimental feature, the MSQ task engine also supports running SELECT queries as
batch tasks. The behavior and result format of plain SELECT (without INSERT or REPLACE) is subject to change.

You can execute SQL statements using the MSQ task engine through the **Query** view in the [web
console](../operations/web-console.md) or through the [`/druid/v2/sql/task` API](../api-reference/sql-ingestion-api.md).

For more details on how SQL queries are executed using the MSQ task engine, see [multi-stage query
tasks](#multi-stage-query-tasks).

## SQL extensions

To support ingestion, additional SQL functionality is available through the MSQ task engine.

<a name="extern"></a>

### Read external data with `EXTERN`

Query tasks can access external data through the `EXTERN` function, using any native batch [input
source](../ingestion/input-sources.md) and [input format](../ingestion/data-formats.md#input-format).

`EXTERN` can read multiple files in parallel across different worker tasks. However, `EXTERN` does not split individual
files across multiple worker tasks. If you have a small number of very large input files, you can increase query
parallelism by splitting up your input files.

For more information about the syntax, see [`EXTERN`](./reference.md#extern-function).

See also the set of SQL-friendly input-source-specific table functions which may be more convenient
than `EXTERN`.

<a name="insert"></a>

### Load data with `INSERT`

`INSERT` statements can create a new datasource or append to an existing datasource. In Druid SQL, unlike standard SQL,
there is no syntactical difference between creating a table and appending data to a table. Druid does not include a
`CREATE TABLE` statement.

Nearly all `SELECT` capabilities are available for `INSERT ... SELECT` queries. Certain exceptions are listed on the [Known
issues](./known-issues.md#select-statement) page.

`INSERT` statements acquire a shared lock to the target datasource. Multiple `INSERT` statements can run at the same time,
for the same datasource, if your cluster has enough task slots.

Like all other forms of [batch ingestion](../ingestion/index.md#batch), each `INSERT` statement generates new segments and
publishes them at the end of its run. For this reason, it is best suited to loading data in larger batches. Do not use
`INSERT` statements to load data in a sequence of microbatches; for that, use [streaming
ingestion](../ingestion/index.md#streaming) instead.

When deciding whether to use `REPLACE` or `INSERT`, keep in mind that segments generated with `REPLACE` can be pruned
with dimension-based pruning but those generated with `INSERT` cannot. For more information about the requirements
for dimension-based pruning, see [Clustering](#clustering).

For more information about the syntax, see [INSERT](./reference.md#insert).

<a name="replace"></a>

### Overwrite data with REPLACE

`REPLACE` statements can create a new datasource or overwrite data in an existing datasource. In Druid SQL, unlike
standard SQL, there is no syntactical difference between creating a table and overwriting data in a table. Druid does
not include a `CREATE TABLE` statement.

`REPLACE` uses an [OVERWRITE clause](reference.md#replace-specific-time-ranges) to determine which data to overwrite. You
can overwrite an entire table, or a specific time range of a table. When you overwrite a specific time range, that time
range must align with the granularity specified in the `PARTITIONED BY` clause.

`REPLACE` statements acquire an exclusive write lock to the target time range of the target datasource. No other ingestion
or compaction operations may proceed for that time range while the task is running. However, ingestion and compaction
operations may proceed for other time ranges.

Nearly all `SELECT` capabilities are available for `REPLACE ... SELECT` queries. Certain exceptions are listed on the [Known
issues](./known-issues.md#select-statement) page.

For more information about the syntax, see [REPLACE](./reference.md#replace).

When deciding whether to use `REPLACE` or `INSERT`, keep in mind that segments generated with `REPLACE` can be pruned
with dimension-based pruning but those generated with `INSERT` cannot. For more information about the requirements
for dimension-based pruning, see [Clustering](#clustering).

### Write to an external destination with `EXTERN`

Query tasks can write data to an external destination through the `EXTERN` function, when it is used with the `INTO`
clause, such as `INSERT INTO EXTERN(...)`. The EXTERN function takes arguments that specify where to write the files.
The format can be specified using an `AS` clause.

For more information about the syntax, see [`EXTERN`](./reference.md#extern-function).

### Primary timestamp

Druid tables always include a primary timestamp named `__time`.

It is common to set a primary timestamp by using [date and time
functions](../querying/sql-scalar.md#date-and-time-functions); for example: `TIME_FORMAT("timestamp", 'yyyy-MM-dd
HH:mm:ss') AS __time`.

The `__time` column is used for [partitioning by time](#partitioning-by-time). If you use `PARTITIONED BY ALL` or
`PARTITIONED BY ALL TIME`, partitioning by time is disabled. In these cases, you do not need to include a `__time`
column in your `INSERT` statement. However, Druid still creates a `__time` column in your Druid table and sets all
timestamps to 1970-01-01 00:00:00.

For more information, see [Primary timestamp](../ingestion/schema-model.md#primary-timestamp).

<a name="partitioning"></a>

### Partitioning by time

`INSERT` and `REPLACE` statements require the `PARTITIONED BY` clause, which determines how time-based partitioning is done.
In Druid, data is split into one or more segments per time chunk, defined by the PARTITIONED BY granularity.

Partitioning by time is important for three reasons:

1. Queries that filter by `__time` (SQL) or `intervals` (native) are able to use time partitioning to prune the set of
   segments to consider.
2. Certain data management operations, such as overwriting and compacting existing data, acquire exclusive write locks
   on time partitions. Finer-grained partitioning allows finer-grained exclusive write locks.
3. Each segment file is wholly contained within a time partition. Too-fine-grained partitioning may cause a large number
   of small segments, which leads to poor performance.

`PARTITIONED BY HOUR` and `PARTITIONED BY DAY` are the most common choices to balance these considerations. `PARTITIONED
BY ALL` is suitable if your dataset does not have a [primary timestamp](#primary-timestamp).

For more information about the syntax, see [PARTITIONED BY](./reference.md#partitioned-by).

### Clustering

Within each time chunk defined by [time partitioning](#partitioning-by-time), data can be further split by the optional
[CLUSTERED BY](reference.md#clustered-by) clause.

For example, suppose you ingest 100 million rows per hour using `PARTITIONED BY HOUR` and `CLUSTERED BY hostName`. The
ingestion task will generate segments of roughly 3 million rows — the default value of
[`rowsPerSegment`](reference.md#context-parameters) — with lexicographic ranges of `hostName`s grouped into segments.

Clustering is important for two reasons:

1. Lower storage footprint due to improved locality, and therefore improved compressibility.
2. Better query performance due to dimension-based segment pruning, which removes segments from consideration when they
   cannot possibly contain data matching a query's filter. This speeds up filters like `x = 'foo'` and `x IN ('foo',
   'bar')`.

To activate dimension-based pruning, these requirements must be met:

- Segments were generated by a `REPLACE` statement, not an `INSERT` statement.
- All `CLUSTERED BY` columns are single-valued string columns.

If these requirements are _not_ met, Druid still clusters data during ingestion but will not be able to perform
dimension-based segment pruning at query time. You can tell if dimension-based segment pruning is possible by using the
`sys.segments` table to inspect the `shard_spec` for the segments generated by an ingestion query. If they are of type
`range` or `single`, then dimension-based segment pruning is possible. Otherwise, it is not. The shard spec type is also
available in the **Segments** view under the **Partitioning** column.

For more information about syntax, see [`CLUSTERED BY`](./reference.md#clustered-by).

### Rollup

[Rollup](../ingestion/rollup.md) is a technique that pre-aggregates data during ingestion to reduce the amount of data
stored. Intermediate aggregations are stored in the generated segments, and further aggregation is done at query time.
This reduces storage footprint and improves performance, often dramatically.

To perform ingestion with rollup:

1. Use `GROUP BY`. The columns in the `GROUP BY` clause become dimensions, and aggregation functions become metrics.
2. Set [`finalizeAggregations: false`](reference.md#context-parameters) in your context. This causes aggregation
   functions to write their internal state to the generated segments, instead of the finalized end result, and enables
   further aggregation at query time.
3. See [ARRAY types](../querying/arrays.md#sql-based-ingestion-with-rollup) for information about ingesting `ARRAY` columns
4. See [multi-value dimensions](../querying/multi-value-dimensions.md#sql-based-ingestion-with-rollup) for information to ingest multi-value VARCHAR columns

When you do all of these things, Druid understands that you intend to do an ingestion with rollup, and it writes
rollup-related metadata into the generated segments. Other applications can then use [`segmentMetadata`
queries](../querying/segmentmetadataquery.md) to retrieve rollup-related information.

The following [aggregation functions](../querying/sql-aggregations.md) are supported for rollup at ingestion time:
`COUNT` (but switch to `SUM` at query time), `SUM`, `MIN`, `MAX`, `EARLIEST` and `EARLIEST_BY`,
`LATEST` and `LATEST_BY`, `APPROX_COUNT_DISTINCT`, `APPROX_COUNT_DISTINCT_BUILTIN`,
`APPROX_COUNT_DISTINCT_DS_HLL`, `APPROX_COUNT_DISTINCT_DS_THETA`, and `DS_QUANTILES_SKETCH` (but switch to
`APPROX_QUANTILE_DS` at query time). Do not use `AVG`; instead, use `SUM` and `COUNT` at ingest time and compute the
quotient at query time.

For an example, see [INSERT with rollup example](examples.md#insert-with-rollup).

## Multi-stage query tasks

### Execution flow

When you execute a SQL statement using the task endpoint [`/druid/v2/sql/task`](../api-reference/sql-ingestion-api.md#submit-a-query), the following
happens:

1. The Broker plans your SQL query into a native query, as usual.

2. The Broker wraps the native query into a task of type `query_controller`
   and submits it to the indexing service.

3. The Broker returns the task ID to you and exits.

4. The controller task launches some number of worker tasks determined by
   the `maxNumTasks` and `taskAssignment` [context parameters](./reference.md#context-parameters). You can set these settings individually for each query.

5. Worker tasks of type `query_worker` execute the query.

6. If the query is a `SELECT` query, the worker tasks send the results
   back to the controller task, which writes them into its task report.
   If the query is an INSERT or REPLACE query, the worker tasks generate and
   publish new Druid segments to the provided datasource.

### Parallelism

The [`maxNumTasks`](./reference.md#context-parameters) query parameter determines the maximum number of tasks your
query will use, including the one `query_controller` task. Generally, queries perform better with more workers. The
lowest possible value of `maxNumTasks` is two (one worker and one controller). Do not set this higher than the number of
free slots available in your cluster; doing so will result in a [TaskStartTimeout](reference.md#error_TaskStartTimeout)
error.

When [reading external data](#extern), EXTERN can read multiple files in parallel across
different worker tasks. However, EXTERN does not split individual files across multiple worker tasks. If you have a
small number of very large input files, you can increase query parallelism by splitting up your input files.

The `druid.worker.capacity` server property on each [Middle Manager](../design/architecture.md#druid-services)
determines the maximum number of worker tasks that can run on each server at once. Worker tasks run single-threaded,
which also determines the maximum number of processors on the server that can contribute towards multi-stage queries.

### Memory usage

Increasing the amount of available memory can improve performance in certain cases:

- Segment generation becomes more efficient when data doesn't spill to disk as often.
- Sorting stage output data becomes more efficient since available memory affects the
  number of required sorting passes.

Worker tasks use both JVM heap memory and off-heap ("direct") memory.

On Peons launched by Middle Managers, the bulk of the JVM heap (75%, less any space used by
[lookups](../querying/lookups.md)) is split up into two bundles of equal size: one processor bundle and one worker
bundle. Each one comprises 37.5% of the available JVM heap, less any space used by [lookups](../querying/lookups.md).

Depending on the type of query, controller and worker tasks may use sketches for determining partition boundaries.
The heap footprint of these sketches is capped at 10% of available memory, or 300 MB, whichever is lower.

The processor memory bundle is used for query processing and segment generation. Each processor bundle must also
provides space to buffer I/O between stages. Specifically, each downstream stage requires 1 MB of buffer space for each
upstream worker. For example, if you have 100 workers running in stage 0, and stage 1 reads from stage 0, then each
worker in stage 1 requires 1M * 100 = 100 MB of memory for frame buffers.

The worker memory bundle is used for sorting stage output data prior to shuffle. Workers can sort more data than fits in
memory; in this case, they will switch to using disk.

Worker tasks also use off-heap ("direct") memory. Set the amount of direct memory available (`-XX:MaxDirectMemorySize`)
to at least `(druid.processing.numThreads + 1) * druid.processing.buffer.sizeBytes`. Increasing the amount of direct
memory available beyond the minimum does not speed up processing.

### Disk usage

Worker tasks use local disk for four purposes:

- Temporary copies of input data. Each temporary file is deleted before the next one is read. You only need
  enough temporary disk space to store one input file at a time per task.
- Temporary data related to segment generation. You only need enough temporary disk space to store one segments' worth
  of data at a time per task. This is generally less than 2 GB per task.
- External sort of data prior to shuffle. Requires enough space to store a compressed copy of the entire output dataset
  for a task.
- Storing stage output data during a shuffle. Requires enough space to store a compressed copy of the entire output
  dataset for a task.

Workers use the task working directory, given by
[`druid.indexer.task.baseDir`](../configuration/index.md#additional-peon-configuration), for these items. It is
important that this directory has enough space available for these purposes.
