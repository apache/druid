---
id: index
title: SQL-based ingestion overview and syntax
sidebar_label: Overview and syntax
description: Introduces multi-stage query architecture and its task engine
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

> SQL-based ingestion using the multi-stage query task engine is our recommended solution starting in Druid 24.0. Alternative ingestion solutions, such as native batch and Hadoop-based ingestion systems, will still be supported. We recommend you read all [known issues](./msq-known-issues.md) and test the feature in a development environment before rolling it out in production. Using the multi-stage query task engine with `SELECT` statements that do not write to a datasource is experimental.

SQL-based ingestion for Apache Druid uses a distributed multi-stage query architecture, which includes a query engine called the multi-stage query task engine (MSQ task engine). The MSQ task engine extends Druid's query capabilities, so you can write queries that reference [external data](#read-external-data) as well as perform ingestion with SQL [INSERT](#insert-data) and [REPLACE](#replace-data). Essentially, you can perform SQL-based ingestion instead of using JSON ingestion specs that Druid's native ingestion uses.

The MSQ task engine excels at executing queries that can get bottlenecked at the Broker when using Druid's native SQL engine. When you submit queries, the MSQ task engine splits them into stages and automatically exchanges data between stages. Each stage is parallelized to run across multiple data servers at once, simplifying performance.


## MSQ task engine features

In its current state, the MSQ task engine enables you to do the following:

- Read external data at query time using EXTERN.
- Execute batch ingestion jobs by writing SQL queries using INSERT and REPLACE. You no longer need to generate a JSON-based ingestion spec.
- Transform and rewrite existing tables using SQL.
- Perform multi-dimension range partitioning reliably, which leads to more evenly distributed segment sizes and better performance.

The MSQ task engine has additional features that can be used as part of a proof of concept or demo, but don't use or rely on the following features for any meaningful use cases, especially production ones:

- Execute heavy-weight queries and return large numbers of rows.
- Execute queries that exchange large amounts of data between servers, like exact count distinct of high-cardinality fields.


## Load the extension

For new clusters that use 24.0 or later, the multi-stage query extension is loaded by default. If you want to add the extension to an existing cluster, add the extension `druid-multi-stage-query` to `druid.extensions.loadlist` in your `common.runtime.properties` file.

For more information about how to load an extension, see [Loading extensions](../development/extensions.md#loading-extensions).

To use EXTERN, you need READ permission on the resource named "EXTERNAL" of the resource type "EXTERNAL". If you encounter a 403 error when trying to use EXTERN, verify that you have the correct permissions.

## MSQ task engine query syntax

You can submit queries to the MSQ task engine through the **Query** view in the Druid console or through the API. The Druid console is a good place to start because you can preview a query before you run it. You can also experiment with many of the [context parameters](./msq-reference.md#context-parameters) through the UI. Once you're comfortable with submitting queries through the Druid console, [explore using the API to submit a query](./msq-api.md#submit-a-query).

If you encounter an issue after you submit a query, you can learn more about what an error means from the [limits](./msq-concepts.md#limits) and [errors](./msq-concepts.md#error-codes). 

Queries for the MSQ task engine involve three primary functions:

- EXTERN to query external data
- INSERT INTO ... SELECT to insert data, such as data from an external source
- REPLACE to replace existing datasources, partially or fully, with query results

For information about the syntax for queries, see [SQL syntax](./msq-reference.md#sql-syntax).

### Read external data

Query tasks can access external data through the EXTERN function. When using EXTERN, keep in mind that  large files do not get split across different worker tasks. If you have fewer input files than worker tasks, you can increase query parallelism by splitting up your input files such that you have at least one input file per worker task.

You can use the EXTERN function anywhere a table is expected in the following form: `TABLE(EXTERN(...))`. You can use external data with SELECT, INSERT, and REPLACE queries.

The following query reads external data:

```sql
SELECT
  *
FROM TABLE(
  EXTERN(
    '{"type": "http", "uris": ["https://static.imply.io/data/wikipedia.json.gz"]}',
    '{"type": "json"}',
    '[{"name": "timestamp", "type": "string"}, {"name": "page", "type": "string"}, {"name": "user", "type": "string"}]'
  )
)
LIMIT 100
``` 

For more information about the syntax, see [EXTERN](./msq-reference.md#extern).

### Insert data

With the MSQ task engine, Druid can use the results of a query task to create a new datasource or to append to an existing datasource. Syntactically, there is no difference between the two. These operations use the INSERT INTO ... SELECT syntax.

All SELECT capabilities are available for INSERT queries. However, the MSQ task engine does not include all the existing SQL query features of Druid. See [Known issues](./msq-known-issues.md) for a list of capabilities that aren't available.

The following example query inserts data from an external source into a table named `w000` and partitions it by day:

```sql
INSERT INTO w000
SELECT
  TIME_PARSE("timestamp") AS __time,
  "page",
  "user"
FROM TABLE(
  EXTERN(
    '{"type": "http", "uris": ["https://static.imply.io/data/wikipedia.json.gz"]}',
    '{"type": "json"}',
    '[{"name": "timestamp", "type": "string"}, {"name": "page", "type": "string"}, {"name": "user", "type": "string"}]'
  )
)
PARTITIONED BY DAY
```

For more information about the syntax, see [INSERT](./msq-reference.md#insert).

### Replace data 

The syntax for REPLACE is similar to INSERT. All SELECT functionality is available for REPLACE queries.
Note that the MSQ task engine does not yet implement all native Druid query features.
For details, see [Known issues](./msq-known-issues.md).

When working with REPLACE queries, keep the following in mind:

- The intervals generated as a result of the OVERWRITE WHERE query must align with the granularity specified in the PARTITIONED BY clause.
- OVERWRITE WHERE queries only support the `__time` column.

For more information about the syntax, see [REPLACE](./msq-reference.md#replace).

The following examples show how to replace data in a table.

#### REPLACE all data

You can replace all the data in a table by using REPLACE INTO ... OVERWRITE ALL SELECT:

```sql
REPLACE INTO w000
OVERWRITE ALL
SELECT
  TIME_PARSE("timestamp") AS __time,
  "page",
  "user"
FROM TABLE(
  EXTERN(
    '{"type": "http", "uris": ["https://static.imply.io/data/wikipedia.json.gz"]}',
    '{"type": "json"}',
    '[{"name": "timestamp", "type": "string"}, {"name": "page", "type": "string"}, {"name": "user", "type": "string"}]'
  )
)
PARTITIONED BY DAY
```

#### REPLACE some data

You can replace some of the data in a table by using REPLACE INTO ... OVERWRITE WHERE ... SELECT:

```sql
REPLACE INTO w000
OVERWRITE WHERE __time >= TIMESTAMP '2019-08-25' AND __time < TIMESTAMP '2019-08-28'
SELECT
  TIME_PARSE("timestamp") AS __time,
  "page",
  "user"
FROM TABLE(
  EXTERN(
    '{"type": "http", "uris": ["https://static.imply.io/data/wikipedia.json.gz"]}',
    '{"type": "json"}',
    '[{"name": "timestamp", "type": "string"}, {"name": "page", "type": "string"}, {"name": "user", "type": "string"}]'
  )
)
PARTITIONED BY DAY
```

## Adjust query behavior

In addition to the basic functions, you can further modify your query behavior to control how your queries run or what your results look like. You can control how your queries behave by changing the following:

### Primary timestamp

Druid tables always include a primary timestamp named `__time`, so your ingestion query should generally include a column named `__time`. 

The following formats are supported for `__time` in the source data:
- ISO 8601 with 'T' separator, such as "2000-01-01T01:02:03.456"
- Milliseconds since Unix epoch (00:00:00 UTC on January 1, 1970)

The `__time` column is used for time-based partitioning, such as `PARTITIONED BY DAY`.

If you use `PARTITIONED BY ALL` or `PARTITIONED BY ALL TIME`, time-based
partitioning is disabled. In these cases, your ingestion query doesn't need
to include a `__time` column. However, Druid still creates a `__time` column 
in your Druid table and sets all timestamps to 1970-01-01 00:00:00.

For more information, see [Primary timestamp](../ingestion/data-model.md#primary-timestamp).

### PARTITIONED BY

INSERT and REPLACE queries require the PARTITIONED BY clause, which determines how time-based partitioning is done. In Druid, data is split into segments, one or more per time chunk defined by the PARTITIONED BY granularity. A good general rule is to adjust the granularity so that each segment contains about five million rows. Choose a granularity based on your ingestion rate. For example, if you ingest a million rows per day, PARTITION BY DAY is good. If you ingest a million rows an hour, choose PARTITION BY HOUR instead.

Using the clause provides the following benefits:

- Better query performance due to time-based segment pruning, which removes segments from
   consideration when they do not contain any data for a query's time filter.
- More efficient data management, as data can be rewritten for each time partition individually
   rather than the whole table.

You can use the following arguments for PARTITIONED BY:

- Time unit: `HOUR`, `DAY`, `MONTH`, or `YEAR`. Equivalent to `FLOOR(__time TO TimeUnit)`.
- `TIME_FLOOR(__time, 'granularity_string')`, where granularity_string is an ISO 8601 period like
  'PT1H'. The first argument must be `__time`.
- `FLOOR(__time TO TimeUnit)`, where `TimeUnit` is any unit supported by the [FLOOR function](../querying/sql-scalar.md#date-and-time-functions). The first
  argument must be `__time`.
- `ALL` or `ALL TIME`, which effectively disables time partitioning by placing all data in a single
  time chunk. To use LIMIT or OFFSET at the outer level of your INSERT or REPLACE query, you must set PARTITIONED BY to ALL or ALL TIME.

You can use the following ISO 8601 periods for `TIME_FLOOR`:

- PT1S
- PT1M
- PT5M
- PT10M
- PT15M
- PT30M
- PT1H
- PT6H
- P1D
- P1W
- P1M
- P3M
- P1Y


### CLUSTERED BY

Data is first divided by the PARTITION BY clause. Data can be further split by the CLUSTERED BY clause. For example, suppose you ingest 100 M rows per hour and use `PARTITIONED BY HOUR` as your time partition. You then divide up the data further by adding `CLUSTERED BY hostName`. The result is segments of about 5 million rows, with like `hostNames` grouped within the same segment.

Using CLUSTERED BY has the following benefits:

- Lower storage footprint due to combining similar data into the same segments, which improves
   compressibility.
- Better query performance due to dimension-based segment pruning, which removes segments from
   consideration when they cannot possibly contain data matching a query's filter.

For dimension-based segment pruning to be effective, your queries should meet the following conditions:

- All CLUSTERED BY columns are single-valued string columns
- Use a REPLACE query for ingestion

Druid still clusters data during ingestion if these conditions aren't met but won't perform dimension-based segment pruning at query time. That means if you use an INSERT query for ingestion or have numeric columns or multi-valued string columns, dimension-based segment pruning doesn't occur at query time.

You can tell if dimension-based segment pruning is possible by using the `sys.segments` table to
inspect the `shard_spec` for the segments generated by an ingestion query. If they are of type
`range` or `single`, then dimension-based segment pruning is possible. Otherwise, it is not. The
shard spec type is also available in the **Segments** view under the **Partitioning**
column.

You can use the following filters for dimension-based segment pruning:

- Equality to string literals, like `x = 'foo'` or `x IN ('foo', 'bar')`.
- Comparison to string literals, like `x < 'foo'` or other comparisons involving `<`, `>`, `<=`, or `>=`.

This differs from multi-dimension range based partitioning in classic batch ingestion where both
string and numeric columns support Broker-level pruning. With SQL-based batch ingestion,
only string columns support Broker-level pruning.

It is okay to mix time partitioning with secondary partitioning. For example, you can
combine `PARTITIONED BY HOUR` with `CLUSTERED BY channel` to perform
time partitioning by hour and secondary partitioning by channel within each hour.

### GROUP BY

A query's GROUP BY clause determines how data is rolled up. The expressions in the GROUP BY clause become
dimensions, and aggregation functions become metrics.

### Ingest-time aggregations

When performing rollup using aggregations, it is important to use aggregators
that return nonfinalized state. This allows you to perform further rollups
at query time. To achieve this, set `finalizeAggregations: false` in your
ingestion query context.

Check out the [INSERT with rollup example query](./msq-example-queries.md#insert-with-rollup) to see this feature in
action.

Druid needs information for aggregating measures of different segments to compact. For example, to aggregate `count("col") as example_measure`, Druid needs to sum the value of `example_measure`
across the segments. This information is stored inside the metadata of the segment. For the SQL-based ingestion, Druid only populates the
aggregator information of a column in the segment metadata when:

- The INSERT or REPLACE query has an outer GROUP BY clause.
- The following context parameters are set for the query context: `finalizeAggregations: false` and `groupByEnableMultiValueUnnesting: false`

The following table lists query-time aggregations for SQL-based ingestion:

|Query-time aggregation|Notes|
|----------------------|-----|
|SUM|Use unchanged at ingest time.|
|MIN|Use unchanged at ingest time.|
|MAX|Use unchanged at ingest time.|
|AVG|Use SUM and COUNT at ingest time. Switch to quotient of SUM at query time.|
|COUNT|Use unchanged at ingest time, but switch to SUM at query time.|
|COUNT(DISTINCT expr)|If approximate, use APPROX_COUNT_DISTINCT at ingest time.<br /><br />If exact, you cannot use an ingest-time aggregation. Instead, `expr` must be stored as-is. Add it to the SELECT and GROUP BY lists.|
|EARLIEST(expr)<br /><br />(numeric form)|Not supported.|
|EARLIEST(expr, maxBytes)<br /><br />(string form)|Use unchanged at ingest time.|
|LATEST(expr)<br /><br />(numeric form)|Not supported.|
|LATEST(expr, maxBytes)<br /><br />(string form)|Use unchanged at ingest time.|
|APPROX_COUNT_DISTINCT|Use unchanged at ingest time.|
|APPROX_COUNT_DISTINCT_BUILTIN|Use unchanged at ingest time.|
|APPROX_COUNT_DISTINCT_DS_HLL|Use unchanged at ingest time.|
|APPROX_COUNT_DISTINCT_DS_THETA|Use unchanged at ingest time.|
|APPROX_QUANTILE|Not supported. Deprecated; use APPROX_QUANTILE_DS instead.|
|APPROX_QUANTILE_DS|Use DS_QUANTILES_SKETCH at ingest time. Continue using APPROX_QUANTILE_DS at query time.|
|APPROX_QUANTILE_FIXED_BUCKETS|Not supported.|

### Multi-value dimensions

By default, multi-value dimensions are not ingested as expected when rollup is enabled because the
GROUP BY operator unnests them instead of leaving them as arrays. This is [standard behavior](../querying/sql-data-types.md#multi-value-strings) for GROUP BY but it is generally not desirable behavior for ingestion.

To address this:

- When using GROUP BY with data from EXTERN, wrap any string type fields from EXTERN that may be
  multi-valued in `MV_TO_ARRAY`.
- Set `groupByEnableMultiValueUnnesting: false` in your query context to ensure that all multi-value
  strings are properly converted to arrays using `MV_TO_ARRAY`. If any strings aren't
  wrapped in `MV_TO_ARRAY`, the query reports an error that includes the message "Encountered
  multi-value dimension x that cannot be processed with groupByEnableMultiValueUnnesting set to false."

For an example, see [INSERT with rollup example query](./msq-example-queries.md#insert-with-rollup).

### Context parameters

Context parameters can control things such as how many tasks get launched or what happens if there's a malformed record.

For a full list of context parameters and how they affect a query, see [Context parameters](./msq-reference.md#context-parameters).

## Next steps

* [Understand how the multi-stage query architecture works](./msq-concepts.md) by reading about the concepts behind it and its processes.
* [Explore the Query view](../operations/druid-console.md) to learn about the UI tools that can help you get started.