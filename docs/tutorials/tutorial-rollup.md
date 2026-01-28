---
id: tutorial-rollup
title: Aggregate data with rollup
sidebar_label: Aggregate data with rollup
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


Apache Druid&circledR; can summarize raw data at ingestion time using a process known as "rollup." [Rollup](../multi-stage-query/concepts.md#rollup) is a first-level aggregation operation over a selected set of columns that reduces the size of stored data.

This tutorial demonstrates how to apply rollup during ingestion and highlights its effects during query execution. The examples in the tutorial use the [multi-stage query (MSQ)](../multi-stage-query/index.md) task engine to execute SQL statements.

## Prerequisites

Before proceeding, download Druid as described in [Quickstart (local)](./index.md) and have it running on your local machine. You don't need to load any data into the Druid cluster.

You should be familiar with data querying in Druid. If you haven't already, go through the [Query data](../tutorials/tutorial-query.md) tutorial first. 


## Load the example data

For this tutorial, you use a small sample of network flow event data representing IP traffic.
The data contains packet and byte counts from a source IP address to a destination IP address.

```json
{"timestamp":"2018-01-01T01:01:35Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":20,"bytes":9024}
{"timestamp":"2018-01-01T01:01:51Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":255,"bytes":21133}
{"timestamp":"2018-01-01T01:01:59Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":11,"bytes":5780}
{"timestamp":"2018-01-01T01:02:14Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":38,"bytes":6289}
{"timestamp":"2018-01-01T01:02:29Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":377,"bytes":359971}
{"timestamp":"2018-01-01T01:03:29Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":49,"bytes":10204}
{"timestamp":"2018-01-02T21:33:14Z","srcIP":"7.7.7.7", "dstIP":"8.8.8.8","packets":38,"bytes":6289}
{"timestamp":"2018-01-02T21:33:45Z","srcIP":"7.7.7.7", "dstIP":"8.8.8.8","packets":123,"bytes":93999}
{"timestamp":"2018-01-02T21:35:45Z","srcIP":"7.7.7.7", "dstIP":"8.8.8.8","packets":12,"bytes":2818}
```

Load the sample dataset using the [`INSERT INTO`](../multi-stage-query/reference.md#insert) statement and the [`EXTERN`](../multi-stage-query/reference.md#extern-function) function to ingest the data inline. In the [Druid web console](../operations/web-console.md), go to the **Query** view and run the following query:

```sql
INSERT INTO "rollup_tutorial"
WITH "inline_data" AS (
  SELECT *
  FROM TABLE(EXTERN('{
    "type":"inline",
    "data":"{\"timestamp\":\"2018-01-01T01:01:35Z\",\"srcIP\":\"1.1.1.1\",\"dstIP\":\"2.2.2.2\",\"packets\":20,\"bytes\":9024}\n{\"timestamp\":\"2018-01-01T01:02:14Z\",\"srcIP\":\"1.1.1.1\",\"dstIP\":\"2.2.2.2\",\"packets\":38,\"bytes\":6289}\n{\"timestamp\":\"2018-01-01T01:01:59Z\",\"srcIP\":\"1.1.1.1\",\"dstIP\":\"2.2.2.2\",\"packets\":11,\"bytes\":5780}\n{\"timestamp\":\"2018-01-01T01:01:51Z\",\"srcIP\":\"1.1.1.1\",\"dstIP\":\"2.2.2.2\",\"packets\":255,\"bytes\":21133}\n{\"timestamp\":\"2018-01-01T01:02:29Z\",\"srcIP\":\"1.1.1.1\",\"dstIP\":\"2.2.2.2\",\"packets\":377,\"bytes\":359971}\n{\"timestamp\":\"2018-01-01T01:03:29Z\",\"srcIP\":\"1.1.1.1\",\"dstIP\":\"2.2.2.2\",\"packets\":49,\"bytes\":10204}\n{\"timestamp\":\"2018-01-02T21:33:14Z\",\"srcIP\":\"7.7.7.7\",\"dstIP\":\"8.8.8.8\",\"packets\":38,\"bytes\":6289}\n{\"timestamp\":\"2018-01-02T21:33:45Z\",\"srcIP\":\"7.7.7.7\",\"dstIP\":\"8.8.8.8\",\"packets\":123,\"bytes\":93999}\n{\"timestamp\":\"2018-01-02T21:35:45Z\",\"srcIP\":\"7.7.7.7\",\"dstIP\":\"8.8.8.8\",\"packets\":12,\"bytes\":2818}"}', 
    '{"type":"json"}')) 
    EXTEND ("timestamp" VARCHAR, "srcIP" VARCHAR, "dstIP" VARCHAR, "packets" BIGINT, "bytes" BIGINT)
)
SELECT
  FLOOR(TIME_PARSE("timestamp") TO MINUTE) AS __time,
  "srcIP",
  "dstIP",
  SUM("bytes") AS "bytes",
  SUM("packets") AS "packets",
  COUNT(*) AS "count"
FROM "inline_data"
GROUP BY 1, 2, 3
PARTITIONED BY DAY
```

Note the following aspects of the ingestion statement:
* You transform the timestamp field using the `FLOOR` function to round timestamps down to the minute.
* You group by the dimensions `timestamp`, `srcIP`, and `dstIP`.
* You create the `bytes` and `packets` metrics, which are summed from their respective input fields.
* You also create the `count` metric that records the number of rows that get rolled-up per each row in the datasource.

With rollup, Druid combines rows with identical timestamp and dimension values after the timestamp truncation. Druid computes and stores the metric values using the specified aggregation function over each set of rolled-up rows.

After the ingestion completes, you can query the data.

## Query the example data

In the web console, open a new tab in the **Query** view. Run the following query to view the ingested data:

```sql
SELECT * FROM "rollup_tutorial"
```

Returns the following:

| `__time` | `srcIP` | `dstIP` | `bytes` | `count` | `packets` |
| -- | -- | -- | -- | -- | -- |
| `2018-01-01T01:01:00.000Z` | `1.1.1.1` | `2.2.2.2` | `35,937` | `3` | `286` |
| `2018-01-01T01:02:00.000Z` | `1.1.1.1` | `2.2.2.2` | `366,260` | `2` | `415` |
| `2018-01-01T01:03:00.000Z` | `1.1.1.1` | `2.2.2.2` | `10,204` | `1` | `49` |
| `2018-01-02T21:33:00.000Z` | `7.7.7.7` | `8.8.8.8` | `100,288` | `2` | `161` |
| `2018-01-02T21:35:00.000Z` | `7.7.7.7` | `8.8.8.8` | `2,818` | `1` | `12` |

Notice there are only five rows as opposed to the nine rows in the example data. In the next section, you explore the components of the rolled-up rows.

## View rollup in action

Consider the three events in the original input data that occur over the course of minute `2018-01-01T01:01`:

```json
{"timestamp":"2018-01-01T01:01:35Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":20,"bytes":9024}
{"timestamp":"2018-01-01T01:01:51Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":255,"bytes":21133}
{"timestamp":"2018-01-01T01:01:59Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":11,"bytes":5780}
```

Druid combines the three rows into one during rollup:

| `__time` | `srcIP` | `dstIP` | `bytes` | `count` | `packets` |
| -- | -- | -- | -- | -- | -- |
| `2018-01-01T01:01:00.000Z` | `1.1.1.1` | `2.2.2.2` | `35,937` | `3` | `286` |

Before the grouping occurs, the `FLOOR(TIME_PARSE("timestamp") TO MINUTE)` expression buckets (floors) the timestamp column of the original input by minute.

The input rows are grouped because they have the same values for their dimension columns `{timestamp, srcIP, dstIP}`. The metric columns calculate the sum aggregation of the grouped rows for `packets` and `bytes`. The `count` metric shows how many rows from the original input data contributed to the final rolled-up row.

Now, consider the two events in the original input data that occur over the course of minute `2018-01-01T01:02`:

```json
{"timestamp":"2018-01-01T01:02:14Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":38,"bytes":6289}
{"timestamp":"2018-01-01T01:02:29Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":377,"bytes":359971}
```

The rows are grouped into the following during rollup:

| `__time` | `srcIP` | `dstIP` | `bytes` | `count` | `packets` |
| -- | -- | -- | -- | -- | -- |
| `2018-01-01T01:02:00.000Z` | `1.1.1.1` | `2.2.2.2` | `366,260` | `2` | `415` |

In the original input data, only one event occurs over the course of minute `2018-01-01T01:03`:

```json
{"timestamp":"2018-01-01T01:03:29Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":49,"bytes":10204}
```

Therefore, no rollup takes place:

| `__time` | `srcIP` | `dstIP` | `bytes` | `count` | `packets` |
| -- | -- | -- | -- | -- | -- |
| `2018-01-01T01:03:00.000Z` | `1.1.1.1` | `2.2.2.2` | `10,204` | `1` | `49` |


## Learn more

See the following topics for more information:

* [Data rollup](../ingestion/rollup.md) for suggestions and best practices when performing rollup.
* [SQL-based ingestion concepts](../multi-stage-query/concepts.md#rollup) for information on rollup using SQL-based ingestion.
* [SQL-based ingestion query examples](../multi-stage-query/examples.md#insert-with-rollup) for another example of data rollup.  
* [Druid schema model](../ingestion/schema-model.md) to learn about the primary timestamp, dimensions, and metrics.
