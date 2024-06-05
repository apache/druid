---
id: tutorial-latest-by
title: Query for latest values
sidebar_label: Query for latest data
description: How to use LATEST_BY or deltas for up-to-date values.
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

This tutorial describes strategies in Apache Druid for use cases that might be handled by UPSERT in other databases. You can use the LATEST_BY aggregation at query time or "deltas" for numeric dimensions at insert time.

The [Update data](./tutorial-update-data.md) tutorial demonstrates how to use batch operations to update data according to the timestamp, including UPSERT cases. However, with streaming data, you can potentially use LATEST_BY or deltas to satisfy requirements otherwise handled with updates.

## Prerequisites

Before you follow the steps in this tutorial, download Druid as described in the [Local quickstart](index.md) and have it running on your local machine. You don't need to load any data into the Druid cluster.

You should be familiar with data querying in Druid. If you haven't already, go through the [Query data](../tutorials/tutorial-query.md) tutorial first.

## Use LATEST_BY to retrieve updated values

Sometimes, you want to read the latest value of one dimension or measure in relation to another dimension. In a transactional database, you might maintain dimensions or measures using UPSERT, but in Druid you can append all updates or changes during ingestion. The LATEST_BY function lets you get the most recent value for the dimension with the following type of query:

```sql
SELECT dimension,
       LATEST_BY(changed_dimension, updated_timestamp)
FROM my_table
GROUP BY 1
```

In this example `update_timestamp` represents the reference timestamp to use to evaluate the "latest" value. This could be `__time` or another timestamp.

For example, consider the following table of events that log the total number of points for a user:

| `__time` |  `user_id`| `points`|
| --- | --- | --- |
| `2024-01-01T01:00:00.000Z`|`funny_bunny1`| 10 |
| `2024-01-01T01:05:00.000Z`|`funny_bunny1`| 30 |
| `2024-01-01T02:00:00.000Z`|`funny_bunny1`| 35 |
| `2024-01-01T02:00:00.000Z`|`silly_monkey2`| 30 |
| `2024-01-01T02:05:00.000Z`|`silly_monkey2`| 55 |
| `2024-01-01T03:00:00.000Z`|`funny_bunny1`| 40 |

<details>
<summary>Insert sample data</summary>

In the Druid web console, navigate to the **Query** view and run the following query to insert sample data:

```sql
REPLACE INTO "latest_by_tutorial1" OVERWRITE ALL
WITH "ext" AS (
  SELECT *
  FROM TABLE(
    EXTERN(
     '{"type":"inline","data":"{\"timestamp\":\"2024-01-01T01:00:00Z\",\"user_id\":\"funny_bunny1\", \"points\":10}\n{\"timestamp\":\"2024-01-01T01:05:00Z\",\"user_id\":\"funny_bunny1\", \"points\":30}\n{\"timestamp\": \"2024-01-01T02:00:00Z\",\"user_id\":\"funny_bunny1\", \"points\":35}\n{\"timestamp\":\"2024-01-01T02:00:00Z\",\"user_id\":\"silly_monkey2\", \"points\":30}\n{\"timestamp\":\"2024-01-01T02:05:00Z\",\"user_id\":\"silly_monkey2\", \"points\":55}\n{\"timestamp\":\"2024-01-01T03:00:00Z\",\"user_id\":\"funny_bunny1\", \"points\":40}"}',
     '{"type":"json"}'
    )
  ) EXTEND ("timestamp" VARCHAR, "user_id" VARCHAR, "points" BIGINT)
)
SELECT
  TIME_PARSE("timestamp") AS "__time",
  "user_id",
  "points"
FROM "ext"
PARTITIONED BY DAY
```
</details>

Run the following query to retrieve the most recent `points` value for each `user_id`:

```sql
SELECT user_id,
     LATEST_BY("points", "__time") AS latest_points
FROM latest_by_tutorial1
GROUP BY 1
```

The results are as follows:

|`user_id`|`total_points`|
| --- | --- |
|`silly_monkey2`| 55 |
|`funny_bunny1`| 40 |

In the example, the values increase each time, but this method works even if the values fluctuate.

You can use this query shape as a subquery for additional processing. However, if there are many values for `user_id`, the query can be expensive.

If you want to track the latest value at different times within a larger granularity time frame, you need an additional timestamp to record update times. This allows Druid to track the latest version. Consider the following data that represents points for various users updated within an hour time frame. `__time` is hour granularity, but `updated_timestamp` is minute granularity:

| `__time` | `updated_timestamp` | `user_id`| `points`|
| --- | --- | --- | --- |
| `2024-01-01T01:00:00.000Z`| `2024-01-01T01:00:00.000Z`|`funny_bunny1`| 10 |
|`2024-01-01T01:00:00.000Z`| `2024-01-01T01:05:00.000Z`|`funny_bunny1`| 30 |
|`2024-01-01T02:00:00.000Z`| `2024-01-01T02:00:00.000Z`|`funny_bunny1`| 35 |
|`2024-01-01T02:00:00.000Z`|`2024-01-01T02:00:00.000Z`|`silly_monkey2`| 30 |
|`2024-01-01T02:00:00.000Z`| `2024-01-01T02:05:00.000Z`|`silly_monkey2`| 55 |
|`2024-01-01T03:00:00.000Z`| `2024-01-01T03:00:00.000Z`|`funny_bunny1`| 40 |

<details>
<summary>Insert sample data</summary>

Open a new tab in the **Query** view and run the following query to insert sample data:

```sql
REPLACE INTO "latest_by_tutorial2" OVERWRITE ALL
WITH "ext" AS (
  SELECT *
  FROM TABLE(
    EXTERN(
     '{"type":"inline","data":"{\"timestamp\":\"2024-01-01T01:00:00Z\",\"updated_timestamp\":\"2024-01-01T01:00:00Z\",\"user_id\":\"funny_bunny1\", \"points\":10}\n{\"timestamp\":\"2024-01-01T01:05:00Z\",\"updated_timestamp\":\"2024-01-01T01:05:00Z\",\"user_id\":\"funny_bunny1\", \"points\":30}\n{\"timestamp\": \"2024-01-01T02:00:00Z\",\"updated_timestamp\":\"2024-01-01T02:00:00Z\",\"user_id\":\"funny_bunny1\", \"points\":35}\n{\"timestamp\":\"2024-01-01T02:00:00Z\",\"updated_timestamp\":\"2024-01-01T02:00:00Z\",\"user_id\":\"silly_monkey2\", \"points\":30}\n{\"timestamp\":\"2024-01-01T02:00:00Z\",\"updated_timestamp\":\"2024-01-01T02:05:00Z\",\"user_id\":\"silly_monkey2\", \"points\":55}\n{\"timestamp\":\"2024-01-01T03:00:00Z\",\"updated_timestamp\":\"2024-01-01T03:00:00Z\",\"user_id\":\"funny_bunny1\", \"points\":40}"}',
     '{"type":"json"}'
    )
  ) EXTEND ("timestamp" VARCHAR, "updated_timestamp" VARCHAR, "user_id" VARCHAR, "points" BIGINT)
)
SELECT
  TIME_PARSE("timestamp") AS "__time",
  "updated_timestamp",
  "user_id",
  "points"
FROM "ext"
PARTITIONED BY DAY
```
</details>


Run the following query to retrieve the latest points value by user for each hour:

```sql
SELECT FLOOR("__time" TO HOUR) AS "hour_time",
      "user_id",
       LATEST_BY("points", TIME_PARSE(updated_timestamp)) AS "latest_points_hour"
FROM latest_by_tutorial2
GROUP BY 1,2
```

The results are as follows:

| `hour_time` | `user_id` | `latest_points_hour`|
|---|---|---|
|`2024-01-01T01:00:00.000Z`|`funny_bunny1`|20|
|`2024-01-01T02:00:00.000Z`|`funny_bunny1`|5|
|`2024-01-01T02:00:00.000Z`|`silly_monkey2`|25|
|`2024-01-01T03:00:00.000Z`|`funny_bunny1`|10|

LATEST_BY is an aggregation function. While it's very efficient when there are not many update rows matching a dimension, such as `user_id`, it scans all matching rows with the same dimension. For dimensions with numerous updates, such as when a user plays a game a million times, and the updates don't arrive in a timely order, Druid processes all rows matching the `user_id` to find the row with the max timestamp to provide the latest data. 

For instance, if updates constitute 1-5 percent of your data, you'll get good query performance. If updates constitute 50 percent or more of your data, your queries will be slow.

To mitigate this, you can set up a periodic batch ingestion job that re-indexes modified data into a new datasource for direct querying without grouping to reduce the cost of these queries by pre-computing and storing the latest values. Note that your view of the latest data will not be up to date until the next refresh happens.
 
Alternatively, you can perform ingestion-time aggregation using LATEST_BY and append updates with streaming ingestion into a rolled up datasource. Appending into a time chunk adds new segments and does not perfectly roll up data, so rows may be partial rather than complete rollups, and you may have multiple partially rolled up rows. In this case, you still need to use the GROUP BY query for correct querying of the rolled up data source. You can tune automatic compaction to significantly reduce the number of stale rows and improve your performance.

## Use delta values and aggregation for updated values

Instead of appending the latest total value in your events, you can log the change in value with each event and use the aggregator you usually use. This method may allow you to avoid a level of aggregation and grouping in your queries.

For most applications, you can send the event data directly to Druid without pre-processing. For example, when sending impression counts to Druid, don't send the total impression count since yesterday, send just the recent impression count. You can then aggregate the total in Druid during query. Druid is optimized for adding up a lot of rows, so this might be counterintuitive to people who are familiar with batching or pre-aggregating data.

For example, consider a datasource with a measure column `y` that you aggregate with SUM, grouped by another dimension `x`. If you want to update the value of `y` from 3 to 2, then insert -1 for `y`. This way the aggregation `SUM(y)` is correct for any queries grouped by `x`. This may offer a significant performance advantage but the trade off is that the aggregation has to always be a SUM.

In other cases, the updates to the data may already be deltas to the original, and so the data engineering required to append the updates would be simple. The same performance impact mitigation applies as in the previous example: use rollup at ingestion time combined with ongoing automatic compaction.

For example, consider the following table of events that logs the number of points gained or lost for a user during a period of time:

| `__time` |  `user_id`| `delta`|
| --- | --- | --- |
| `2024-01-01T01:00:00.000Z`|`funny_bunny1`| 10 |
| `2024-01-01T01:05:00.000Z`|`funny_bunny1`| 10 |
| `2024-01-01T02:00:00.000Z`|`funny_bunny1`| 5 |
| `2024-01-01T02:00:00.000Z`|`silly_monkey2`| 30 |
| `2024-01-01T02:05:00.000Z`|`silly_monkey2`| -5 |
| `2024-01-01T03:00:00.000Z`|`funny_bunny1`| 10 |

<details>
<summary>Insert sample data</summary>

Open a new tab in the **Query** view and run the following query to insert sample data:

```sql
REPLACE INTO "delta_tutorial" OVERWRITE ALL
WITH "ext" AS (
  SELECT *
  FROM TABLE(
    EXTERN(
     '{"type":"inline","data":"{\"timestamp\":\"2024-01-01T01:00:00Z\",\"user_id\":\"funny_bunny1\", \"points\":10}\n{\"timestamp\":\"2024-01-01T01:05:00Z\",\"user_id\":\"funny_bunny1\", \"points\":10}\n{\"timestamp\": \"2024-01-01T02:00:00Z\",\"user_id\":\"funny_bunny1\", \"points\":5}\n{\"timestamp\":\"2024-01-01T02:00:00Z\",\"user_id\":\"silly_monkey2\", \"points\":30}\n{\"timestamp\":\"2024-01-01T02:05:00Z\",\"user_id\":\"silly_monkey2\", \"points\":-5}\n{\"timestamp\":\"2024-01-01T03:00:00Z\",\"user_id\":\"funny_bunny1\", \"points\":10}"}',
     '{"type":"json"}'
    )
  ) EXTEND ("timestamp" VARCHAR, "user_id" VARCHAR, "points" BIGINT)
)
SELECT
  TIME_PARSE("timestamp") AS "__time",
  "user_id",
  "points" AS "delta"
FROM "ext"
PARTITIONED BY DAY
```

</details>

The following query returns the same points per hour as the second LATEST_BY example:

```sql
SELECT FLOOR("__time" TO HOUR) as "hour_time",
       "user_id",
       SUM("delta") AS "latest_points_hour"
FROM "delta_tutorial"
GROUP BY 1,2
```

## Learn more

See the following topics for more information:

* [Update data](./tutorial-update-data.md) for a tutorial on updating data in Druid.
* [Data updates](../data-management/update.md) for an overview of updating data in Druid.
