---
id: query-execution
title: "Query execution"
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

> This document describes how Druid executes [native queries](querying.md), but since [Druid SQL](sql.md) queries
> are translated to native queries, this document applies to the SQL runtime as well. Refer to the SQL
> [Query translation](sql.md#query-translation) page for information about how SQL queries are translated to native
> queries.

Druid's approach to query execution varies depending on the kind of [datasource](datasource.md) you are querying.

## Datasource type

### `table`

Queries that operate directly on [table datasources](datasource.md#table) are executed using a scatter-gather approach
led by the Broker process. The process looks like this:

1. The Broker identifies which [segments](../design/segments.md) are relevant to the query based on the `"intervals"`
parameter. Segments are always partitioned by time, so any segment whose interval overlaps the query interval is
potentially relevant.

2. The Broker may additionally further prune the segment list based on the `"filter"`, if the input data was partitioned
by range using the [`single_dim` partitionsSpec](../ingestion/native-batch.md#partitionsspec), and if the filter matches
the dimension used for partitioning.

3. The Broker, having pruned the list of segments for the query, forwards the query to data servers (like Historicals
and tasks running on MiddleManagers) that are currently serving those segments.

4. For all query types except [Scan](scan-query.md), data servers process each segment in parallel and generate partial
results for each segment. The specific processing that is done depends on the query type. These partial results may be
cached if [query caching](caching.md) is enabled. For Scan queries, segments are processed in order by a single thread,
and results are not cached.

5. The Broker receives partial results from each data server, merges them into the final result set, and returns them
to the caller. For Timeseries and Scan queries, and for GroupBy queries where there is no sorting, the Broker is able to
do this in a streaming fashion. Otherwise, the Broker fully computes the result set before returning anything.

### `lookup`

Queries that operate directly on [lookup datasources](datasource.md#lookup) (without a join) are executed on the Broker
that received the query, using its local copy of the lookup. All registered lookup tables are preloaded in-memory on the
Broker. The query runs single-threaded.

Execution of queries that use lookups as right-hand inputs to a join are executed in a way that depends on their
"base" (bottom-leftmost) datasource, as described in the [join](#join) section below.

### `union`

Queries that operate directly on [union datasources](datasource.md#union) are split up on the Broker into a separate
query for each table that is part of the union. Each of these queries runs separately, and the Broker merges their
results together.

### `inline`

Queries that operate directly on [inline datasources](datasource.md#inline) are executed on the Broker that received the
query. The query runs single-threaded.

Execution of queries that use inline datasources as right-hand inputs to a join are executed in a way that depends on
their "base" (bottom-leftmost) datasource, as described in the [join](#join) section below.

### `query`

[Query datasources](datasource.md#query) are subqueries. Each subquery is executed as if it was its own query and
the results are brought back to the Broker. Then, the Broker continues on with the rest of the query as if the subquery
was replaced with an inline datasource.

In most cases, subquery results are fully buffered in memory on the Broker before the rest of the query proceeds,
meaning subqueries execute sequentially. The total number of rows buffered across all subqueries of a given query
in this way cannot exceed the [`druid.server.http.maxSubqueryRows` property](../configuration/index.md).

There is one exception: if the outer query and all subqueries are the [groupBy](groupbyquery.md) type, then subquery
results can be processed in a streaming fashion and the `druid.server.http.maxSubqueryRows` limit does not apply.

### `join`

[Join datasources](datasource.md#join) are handled using a broadcast hash-join approach.

1. The Broker executes any subqueries that are inputs the join, as described in the [query](#query) section, and
replaces them with inline datasources.

2. The Broker flattens a join tree, if present, into a "base" datasource (the bottom-leftmost one) and other leaf
datasources (the rest).

3. Query execution proceeds using the same structure that the base datasource would use on its own. If the base
datasource is a [table](#table), segments are pruned based on `"intervals"` as usual, and the query is executed on the
cluster by forwarding it to all relevant data servers in parallel. If the base datasource is a [lookup](#lookup) or
[inline](#inline) datasource (including an inline datasource that was the result of inlining a subquery), the query is
executed on the Broker itself. The base query cannot be a union, because unions are not currently supported as inputs to
a join.

4. Before beginning to process the base datasource, the server(s) that will execute the query first inspect all the
non-base leaf datasources to determine if a new hash table needs to be built for the upcoming hash join. Currently,
lookups do not require new hash tables to be built (because they are preloaded), but inline datasources do.

5. Query execution proceeds again using the same structure that the base datasource would use on its own, with one
addition: while processing the base datasource, Druid servers will use the hash tables built from the other join inputs
to produce the join result row-by-row, and query engines will operate on the joined rows rather than the base rows.
