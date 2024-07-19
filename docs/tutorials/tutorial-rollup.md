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


Apache Druid can summarize raw data at ingestion time using a process we refer to as "rollup". Rollup is a first-level aggregation operation over a selected set of columns that reduces the size of stored data.

This tutorial will demonstrate the effects of rollup on an example dataset.

For this tutorial, we'll assume you've already downloaded Druid as described in
the [single-machine quickstart](index.md) and have it running on your local machine. The examples in the tutorial use the [multi-stage query](../multi-stage-query/) (MSQ) task engine to execute SQL statements.

It will also be helpful to have finished [Load a file](../tutorials/tutorial-batch.md) and [Query data](../tutorials/tutorial-query.md) tutorials.

## Example data

For this tutorial, we'll use a small sample of network flow event data, representing packet and byte counts for traffic from a source to a destination IP address that occurred within a particular second.

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

Note that we have `srcIP` and `dstIP` defined as dimensions, a longSum metric is defined for the `packets` and `bytes` columns, and the `queryGranularity` has been defined as `minute`.

We will see how these definitions are used after we load this data.

## Load the example data

Load a sample dataset using INSERT and EXTERN functions. The EXTERN function lets you read external data or write to an external location.

In the Druid web console, go to the Query view and run the following query:

```sql
INSERT INTO "rollup_tutorial"
WITH "inline_data" AS (
  SELECT *
  FROM TABLE(EXTERN('{
    "type":"inline",
    "data":"{\"timestamp\":\"2018-01-01T01:01:35Z\",\"srcIP\":\"1.1.1.1\",\"dstIP\":\"2.2.2.2\",\"packets\":20,\"bytes\":9024}\n{\"timestamp\":\"2018-01-01T01:02:14Z\",\"srcIP\":\"1.1.1.1\",\"dstIP\":\"2.2.2.2\",\"packets\":38,\"bytes\":6289}\n{\"timestamp\":\"2018-01-01T01:01:59Z\",\"srcIP\":\"1.1.1.1\",\"dstIP\":\"2.2.2.2\",\"packets\":11,\"bytes\":5780}\n{\"timestamp\":\"2018-01-01T01:01:51Z\",\"srcIP\":\"1.1.1.1\",\"dstIP\":\"2.2.2.2\",\"packets\":255,\"bytes\":21133}\n{\"timestamp\":\"2018-01-01T01:02:29Z\",\"srcIP\":\"1.1.1.1\",\"dstIP\":\"2.2.2.2\",\"packets\":377,\"bytes\":359971}\n{\"timestamp\":\"2018-01-01T01:03:29Z\",\"srcIP\":\"1.1.1.1\",\"dstIP\":\"2.2.2.2\",\"packets\":49,\"bytes\":10204}\n{\"timestamp\":\"2018-01-02T21:33:14Z\",\"srcIP\":\"7.7.7.7\",\"dstIP\":\"8.8.8.8\",\"packets\":38,\"bytes\":6289}\n{\"timestamp\":\"2018-01-02T21:33:45Z\",\"srcIP\":\"7.7.7.7\",\"dstIP\":\"8.8.8.8\",\"packets\":123,\"bytes\":93999}\n{\"timestamp\":\"2018-01-02T21:35:45Z\",\"srcIP\":\"7.7.7.7\",\"dstIP\":\"8.8.8.8\",\"packets\":12,\"bytes\":2818}"}', '{"type":"json"}')) EXTEND ("timestamp" VARCHAR, "srcIP" VARCHAR, "dstIP" VARCHAR, "packets" BIGINT, "bytes" BIGINT)
)
SELECT
  FLOOR(TIME_PARSE("timestamp") TO MINUTE) AS __time,
  "srcIP",
  "dstIP",
  SUM("bytes") AS "bytes",
  COUNT(*) AS "count",
  SUM("packets") AS "packets"
FROM "inline_data"
GROUP BY 1, 2, 3
PARTITIONED BY DAY
```

After the script completes, we will query the data.

## Query the example data

Open a new tab in the Query view and run the following query to see what data was ingested:

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


Let's look at the three events in the original input data that occurred during `2018-01-01T01:01`:

```json
{"timestamp":"2018-01-01T01:01:35Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":20,"bytes":9024}
{"timestamp":"2018-01-01T01:01:51Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":255,"bytes":21133}
{"timestamp":"2018-01-01T01:01:59Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":11,"bytes":5780}
```

These three rows have been "rolled up" into the following row:

| `__time` | `srcIP` | `dstIP` | `bytes` | `count` | `packets` |
| -- | -- | -- | -- | -- | -- |
| `2018-01-01T01:01:00.000Z` | `1.1.1.1` | `2.2.2.2` | `35,937` | `3` | `286` |

The input rows have been grouped by the timestamp and dimension columns `{timestamp, srcIP, dstIP}` with sum aggregations on the metric columns `packets` and `bytes`.

Before the grouping occurs, the timestamps of the original input data are bucketed/floored by minute, due to the `"queryGranularity":"minute"` setting in the ingestion spec.

Likewise, these two events that occurred during `2018-01-01T01:02` have been rolled up:

```json
{"timestamp":"2018-01-01T01:02:14Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":38,"bytes":6289}
{"timestamp":"2018-01-01T01:02:29Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":377,"bytes":359971}
```

| `__time` | `srcIP` | `dstIP` | `bytes` | `count` | `packets` |
| -- | -- | -- | -- | -- | -- |
| `2018-01-01T01:02:00.000Z` | `1.1.1.1` | `2.2.2.2` | `366,260` | `2` | `415` |

For the last event recording traffic between 1.1.1.1 and 2.2.2.2, no rollup took place, because this was the only event that occurred during `2018-01-01T01:03`:

```json
{"timestamp":"2018-01-01T01:03:29Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":49,"bytes":10204}
```

| `__time` | `srcIP` | `dstIP` | `bytes` | `count` | `packets` |
| -- | -- | -- | -- | -- | -- |
| `2018-01-01T01:03:00.000Z` | `1.1.1.1` | `2.2.2.2` | `10,204` | `1` | `49` |

Note that the `count` metric shows how many rows in the original input data contributed to the final "rolled up" row.

