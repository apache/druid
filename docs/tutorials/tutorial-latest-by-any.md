---
id: tutorial-latest-by-any
title: Query for latest values and deduplicated data
sidebar_label: Query for latest and deduplicated data
description: How to use LATEST_BY or deltas for up-to-date values and ANY for deduplication.
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

This tutorial describes potential uses for LATEST_BY, deltas, and ANY aggregations to solve certain UPSERT and deduplication use cases for Apache Druid.

The [Update data](./tutorial-update-data.md) tutorial demonstrates how to use batch operations to updadate data according to the timestamp, including UPSERT cases. However, with streaming data, you can potentially use LATEST_BY or deltas to satisfy requirements for updates.

Additionally, the ANY function can solve for cases where you would otherwise want to perform deduplication.

## Prerequisites

Before you follow the steps in this tutorial, download Druid as described in the [Local quickstart](index.md) and have it running on your local machine. You don't need to load any data into the Druid cluster.

You should be familiar with data querying in Druid. If you haven't already, go through the [Query data](../tutorials/tutorial-query.md) tutorial first.

## Use LATEST_BY to get the current value for a field

Sometimes you want to read the latest value of a measure `my-measure` for a corresponding dimension `my-diimension`. Instead of updating your data or using UPSERT, if you append all updates during ingestion, then you can preform the following type of query:

```sql
SELECT my_dimession,
       LATEST_BY(my_measure, update_timestamp)
FROM my_table
GROUP BY 1
```

For example, consider the following table of events that log the total number of points for a user:

| `__time` |  `user_id`| `total_points`|
| --- | --- | --- | --- |
| `2024-01-01T01:00:00.000Z`|`funny_bunny1`| 10 |
| `2024-01-01T01:05:00.000Z`|`funny_bunny1`| 30 |
| `2024-01-01T02:00:00.000Z`|`funny_bunny1`| 35 |
| `2024-01-01T02:00:00.000Z`|`creepy_monkey2`| 30 |
| `2024-01-01T02:05:00.000Z`|`creepy_monkey2`| 55 |
| `2024-01-01T03:00:00.000Z`|`funny_bunny1`| 40 |

<details>
<summary>Insert sample data</summary>

```sql
REPLACE INTO "latest_by_tutorial1" OVERWRITE ALL
WITH "ext" AS (
  SELECT *
  FROM TABLE(
    EXTERN(
     '{"type":"inline","data":"{\"timestamp\":\"2024-01-01T01:00:00Z\",\"user_id\":\"funny_bunny1\", \"points\":10}\n{\"timestamp\":\"2024-01-01T01:05:00Z\",\"user_id\":\"funny_bunny1\", \"points\":30}\n{\"timestamp\": \"2024-01-01T02:00:00Z\",\"user_id\":\"funny_bunny1\", \"points\":35}\n{\"timestamp\":\"2024-01-01T02:00:00Z\",\"user_id\":\"creepy_monkey2\", \"points\":30}\n{\"timestamp\":\"2024-01-01T02:05:00Z\",\"user_id\":\"creepy_monkey2\", \"points\":55}\n{\"timestamp\":\"2024-01-01T03:00:00Z\",\"user_id\":\"funny_bunny1\", \"points\":40}"}',
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

The following query gives us the latest points value for each user_id. In the example, the values increase each time, but this method works if the values fluctuate:

```sql
SELECT user_id,
     LATEST_BY("points", "__time") AS latest_points
FROM latest_by_tutorial1
GROUP BY 1
```

This method requires an additional timestamp to track the update times so that Druid can track the latest version.

You can use this query shape as a subquery to do additional processing. However, if there a lot of values for "my_dimesion", the query can be expensive.

Consider the following data that represents points for various users:



The following query demonstrates how to query for the latest points value by user for each hour:

```sql
SELECT FLOOR("__time" TO HOUR) AS "hour_time",
      "user_id",
       LATEST_BY("points", TIME_PARSE(updated_timestamp)) AS latest_points_hour
FROM latest_by_tutorial
GROUP BY 1,2
```

The results are as follows:

| `hour_time` | `user_id` | `latest_points_hour`|
|---|---|---|
|`2024-01-01T01:00:00.000Z`|`funny_bunny1`|20|
|`2024-01-01T02:00:00.000Z`|`funny_bunny1`|5|
|`2024-01-01T02:00:00.000Z`|`creepy_monkey2`|25|
|`2024-01-01T03:00:00.000Z`|`funny_bunny1`|10|

In the resulting `update_tutorial` datasource, individual rows are uniquely identified by `__time`, `animal`, and `number`.
To view the results, open a new tab and run the following query:

```sql
SELECT * FROM "update_tutorial"
```

<details>
<summary> View the results</summary>

| `__time` | `animal` | `number`|
| -- | -- | -- |
| `2024-01-01T01:01:35.000Z`| `lion`| 300 |
| `2024-01-01T05:01:35.000Z`| `mongoose`| 737 |
| `2024-01-01T06:01:35.000Z`| `snake`| 1234 |
| `2024-01-01T07:01:35.000Z`| `octopus`| 115 |
| `2024-01-02T01:01:35.000Z`| `opossum`| 300 |
| `2024-01-02T05:01:35.000Z`| `skunk`| 737 |
| `2024-01-02T06:01:35.000Z`| `iguana`| 1234 |
| `2024-01-02T07:01:35.000Z`| `seahorse`| 115 |

</details>

The results contain records for eight animals over two days.

## Overwrite all data

You can use the REPLACE function with OVERWRITE ALL to replace the entire datasource with new data while dropping the old data.

In the web console, open a new tab and run the following query to overwrite timestamp data for the entire `update_tutorial` datasource:

```sql
REPLACE INTO "update_tutorial" OVERWRITE ALL
WITH "ext" AS (SELECT *
FROM TABLE(
  EXTERN(
    '{"type":"inline","data":"{\"timestamp\":\"2024-01-02T07:01:35Z\",\"animal\":\"octopus\", \"number\":115}\n{\"timestamp\":\"2024-01-02T05:01:35Z\",\"animal\":\"mongoose\", \"number\":737}\n{\"timestamp\":\"2024-01-02T06:01:35Z\",\"animal\":\"snake\", \"number\":1234}\n{\"timestamp\":\"2024-01-02T01:01:35Z\",\"animal\":\"lion\", \"number\":300}\n{\"timestamp\":\"2024-01-03T07:01:35Z\",\"animal\":\"seahorse\", \"number\":115}\n{\"timestamp\":\"2024-01-03T05:01:35Z\",\"animal\":\"skunk\", \"number\":737}\n{\"timestamp\":\"2024-01-03T06:01:35Z\",\"animal\":\"iguana\", \"number\":1234}\n{\"timestamp\":\"2024-01-03T01:01:35Z\",\"animal\":\"opossum\", \"number\":300}"}',
    '{"type":"json"}'
  )
) EXTEND ("timestamp" VARCHAR, "animal" VARCHAR, "number" BIGINT))
SELECT
  TIME_PARSE("timestamp") AS "__time",
  "animal",
  "number"
FROM "ext"
PARTITIONED BY DAY
```

<details>
<summary> View the results</summary>

| `__time` | `animal` | `number`|
| -- | -- | -- |
| `2024-01-02T01:01:35.000Z`| `lion`| 300 |
| `2024-01-02T05:01:35.000Z`| `mongoose`| 737 |
| `2024-01-02T06:01:35.000Z`| `snake`| 1234 |
| `2024-01-02T07:01:35.000Z`| `octopus`| 115 |
| `2024-01-03T01:01:35.000Z`| `opossum`| 300 |
| `2024-01-03T05:01:35.000Z`| `skunk`| 737 |
| `2024-01-03T06:01:35.000Z`| `iguana`| 1234 |
| `2024-01-03T07:01:35.000Z`| `seahorse`| 115 |

</details>

Note that the values in the `__time` column have changed to one day later.

## Overwrite records for a specific time range

You can use the REPLACE function to overwrite a specific time range of a datasource. When you overwrite a specific time range, that time range must align with the granularity specified in the PARTITIONED BY clause.

In the web console, open a new tab and run the following query to insert a new row and update specific rows. Note that the OVERWRITE WHERE clause tells the query to only update records for the date 2024-01-03.

```sql
REPLACE INTO "update_tutorial" 
  OVERWRITE WHERE "__time" >= TIMESTAMP'2024-01-03 00:00:00' AND "__time" < TIMESTAMP'2024-01-04 00:00:00'
WITH "ext" AS (SELECT *
FROM TABLE(
  EXTERN(
    '{"type":"inline","data":"{\"timestamp\":\"2024-01-03T01:01:35Z\",\"animal\":\"tiger\", \"number\":300}\n{\"timestamp\":\"2024-01-03T07:01:35Z\",\"animal\":\"seahorse\", \"number\":500}\n{\"timestamp\":\"2024-01-03T05:01:35Z\",\"animal\":\"polecat\", \"number\":626}\n{\"timestamp\":\"2024-01-03T06:01:35Z\",\"animal\":\"iguana\", \"number\":300}\n{\"timestamp\":\"2024-01-03T01:01:35Z\",\"animal\":\"flamingo\", \"number\":999}"}',
    '{"type":"json"}'
  )
) EXTEND ("timestamp" VARCHAR, "animal" VARCHAR, "number" BIGINT))
SELECT
  TIME_PARSE("timestamp") AS "__time",
  "animal",
  "number"
FROM "ext"
PARTITIONED BY DAY
```

<details>
<summary> View the results</summary>

| `__time` | `animal` | `number`|
| -- | -- | -- |
| `2024-01-02T01:01:35.000Z`| `lion`| 300 |
| `2024-01-02T05:01:35.000Z`| `mongoose`| 737 |
| `2024-01-02T06:01:35.000Z`| `snake`| 1234 |
| `2024-01-02T07:01:35.000Z`| `octopus`| 115 |
| `2024-01-03T01:01:35.000Z`| `flamingo`| 999 |
| `2024-01-03T01:01:35.000Z`| `tiger`| 300 |
| `2024-01-03T05:01:35.000Z`| `polecat`| 626 |
| `2024-01-03T06:01:35.000Z`| `iguana`| 300 |
| `2024-01-03T07:01:35.000Z`| `seahorse`| 500 |

</details>

Note the changes in the resulting datasource:

* There is now a new row called `flamingo`.
* The `opossum` row has the value `tiger`.
* The `skunk` row has the value `polecat`.
* The `iguana` and `seahorse` rows have different numbers.

## Update a row using partial segment overshadowing

In Druid, you can overlay older data with newer data for the entire segment or portions of the segment within a particular partition.
This capability is called [overshadowing](../ingestion/tasks.md#overshadowing-between-segments).

You can use partial overshadowing to update a single row by adding a smaller time granularity segment on top of the existing data.
It's a less common variation on a more common approach where you replace the entire time chunk.

The following example demonstrates how update data using partial overshadowing with mixed segment granularity.  
Note the following important points about the example:

* The query updates a single record for a specific `number` row.
* The original datasource uses DAY segment granularity.
* The new data segment is at HOUR granularity and represents a time range that's smaller than the existing data.
* The OVERWRITE WHERE and WHERE TIME_IN_INTERVAL clauses specify the destination where the update occurs and the source of the update, respectively.
* The query replaces everything within the specified interval. To update only a subset of data in that interval, you have to carry forward all records, changing only what you want to change. You can accomplish that by using the [CASE](../querying/sql-functions.md#case) function in the SELECT list.

```sql
REPLACE INTO "update_tutorial"
   OVERWRITE
       WHERE "__time" >= TIMESTAMP'2024-01-03 05:00:00' AND "__time" < TIMESTAMP'2024-01-03 06:00:00'
SELECT 
   "__time", 
   "animal", 
   CAST(486 AS BIGINT) AS "number"
FROM "update_tutorial" 
WHERE TIME_IN_INTERVAL("__time", '2024-01-03T05:01:35Z/PT1S')
PARTITIONED BY FLOOR(__time TO HOUR)
```

<details>
<summary> View the results</summary>

| `__time` | `animal` | `number`|
| -- | -- | -- |
| `2024-01-02T01:01:35.000Z`| `lion`| 300 |
| `2024-01-02T05:01:35.000Z`| `mongoose`| 737 |
| `2024-01-02T06:01:35.000Z`| `snake`| 1234 |
| `2024-01-02T07:01:35.000Z`| `octopus`| 115 |
| `2024-01-03T01:01:35.000Z`| `flamingo`| 999 |
| `2024-01-03T01:01:35.000Z`| `tiger`| 300 |
| `2024-01-03T05:01:35.000Z`| `polecat`| 486 |
| `2024-01-03T06:01:35.000Z`| `iguana`| 300 |
| `2024-01-03T07:01:35.000Z`| `seahorse`| 500 |

</details>

Note that the `number` for `polecat` has changed from 626 to 486.

When you perform partial segment overshadowing multiple times, you can create segment fragmentation that could affect query performance. Use [compaction](../data-management/compaction.md) to correct any fragmentation.

## Learn more

See the following topics for more information:

* [Data updates](../data-management/update.md) for an overview of updating data in Druid.
* [Load files with SQL-based ingestion](../tutorials/tutorial-msq-extern.md) for generating a query that references externally hosted data.
* [Overwrite data with REPLACE](../multi-stage-query/concepts.md#overwrite-data-with-replace) for details on how the MSQ task engine executes SQL REPLACE queries.