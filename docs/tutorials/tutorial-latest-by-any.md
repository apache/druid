---
id: tutorial-update-data
title: Update data
sidebar_label: Update data
description: Learn how to update data in Apache Druid.
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

Apache Druid stores data and indexes in [segment files](../design/segments.md) partitioned by time.
After Druid creates a segment, its contents can't be modified.
You can either replace data for the whole segment, or, in some cases, overshadow a portion of the segment data.

In Druid, use time ranges to specify the data you want to update, as opposed to a primary key or dimensions often used in transactional databases. Data outside the specified replacement time range remains unaffected.
You can use this Druid functionality to perform data updates, inserts, and deletes, similar to UPSERT functionality for transactional databases.

This tutorial shows you how to use the Druid SQL [REPLACE](../multi-stage-query/reference.md#replace) function with the OVERWRITE clause to update existing data.

The tutorial walks you through the following use cases:

* [Overwrite all data](#overwrite-all-data)
* [Overwrite records for a specific time range](#overwrite-records-for-a-specific-time-range)
* [Update a row using partial segment overshadowing](#update-a-row-using-partial-segment-overshadowing)

All examples use the [multi-stage query (MSQ)](../multi-stage-query/index.md) task engine to executes SQL statements.

## Prerequisites

Before you follow the steps in this tutorial, download Druid as described in [Quickstart (local)](index.md) and have it running on your local machine. You don't need to load any data into the Druid cluster.

You should be familiar with data querying in Druid. If you haven't already, go through the [Query data](../tutorials/tutorial-query.md) tutorial first.

## Load sample data

Load a sample dataset using [REPLACE](../multi-stage-query/reference.md#replace) and [EXTERN](../multi-stage-query/reference.md#extern-function) functions.
In Druid SQL, the REPLACE function can create a new [datasource](../design/storage.md) or update an existing datasource.

In the Druid [web console](../operations/web-console.md), go to the **Query** view and run the following query:

```sql
REPLACE INTO "update_tutorial" OVERWRITE ALL
WITH "ext" AS (
  SELECT *
  FROM TABLE(
    EXTERN(
     '{"type":"inline","data":"{\"timestamp\":\"2024-01-01T07:01:35Z\",\"animal\":\"octopus\", \"number\":115}\n{\"timestamp\":\"2024-01-01T05:01:35Z\",\"animal\":\"mongoose\", \"number\":737}\n{\"timestamp\":\"2024-01-01T06:01:35Z\",\"animal\":\"snake\", \"number\":1234}\n{\"timestamp\":\"2024-01-01T01:01:35Z\",\"animal\":\"lion\", \"number\":300}\n{\"timestamp\":\"2024-01-02T07:01:35Z\",\"animal\":\"seahorse\", \"number\":115}\n{\"timestamp\":\"2024-01-02T05:01:35Z\",\"animal\":\"skunk\", \"number\":737}\n{\"timestamp\":\"2024-01-02T06:01:35Z\",\"animal\":\"iguana\", \"number\":1234}\n{\"timestamp\":\"2024-01-02T01:01:35Z\",\"animal\":\"opossum\", \"number\":300}"}',
     '{"type":"json"}'
    )
  ) EXTEND ("timestamp" VARCHAR, "animal" VARCHAR, "number" BIGINT)
)
SELECT
  TIME_PARSE("timestamp") AS "__time",
  "animal",
  "number"
FROM "ext"
PARTITIONED BY DAY

```

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