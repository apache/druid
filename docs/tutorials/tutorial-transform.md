---
id: tutorial-transform
title: Transform input data
sidebar_label: Transform input data
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


This tutorial demonstrates how to transform input data during ingestion.

## Prerequisite

Before proceeding, download Apache Druid&circledR; as described in [Quickstart (local)](./index.md) and have it running on your local machine. You don't need to load any data into the Druid cluster.

You should be familiar with data querying in Druid. If you haven't already, go through the [Query data](../tutorials/tutorial-query.md) tutorial first.

## Sample data

For this tutorial, you use the following sample data:

```json
{"timestamp":"2018-01-01T07:01:35Z", "animal":"octopus", "location":1, "number":100}
{"timestamp":"2018-01-01T05:01:35Z", "animal":"mongoose", "location":2,"number":200}
{"timestamp":"2018-01-01T06:01:35Z", "animal":"snake", "location":3, "number":300}
{"timestamp":"2018-01-01T01:01:35Z", "animal":"lion", "location":4, "number":300}
```

## Transform data during ingestion

Load the sample dataset using the [`INSERT INTO`](../multi-stage-query/reference.md#insert) statement and the [`EXTERN`](../multi-stage-query/reference.md#extern-function) function to ingest the data inline. In the [Druid web console](../operations/web-console.md), go to the **Query** view and run the following query:

```sql
INSERT INTO "transform_tutorial"
WITH "ext" AS (
  SELECT *
  FROM TABLE(EXTERN('{"type":"inline","data":"{\"timestamp\":\"2018-01-01T07:01:35Z\",\"animal\":\"octopus\",  \"location\":1, \"number\":100}\n{\"timestamp\":\"2018-01-01T05:01:35Z\",\"animal\":\"mongoose\", \"location\":2,\"number\":200}\n{\"timestamp\":\"2018-01-01T06:01:35Z\",\"animal\":\"snake\", \"location\":3, \"number\":300}\n{\"timestamp\":\"2018-01-01T01:01:35Z\",\"animal\":\"lion\", \"location\":4, \"number\":300}"}', '{"type":"json"}')) EXTEND ("timestamp" VARCHAR, "animal" VARCHAR, "location" BIGINT, "number" BIGINT)
)
SELECT
  TIME_PARSE("timestamp") AS "__time",
  TEXTCAT('super-', "animal") AS "animal",
  "location",
  "number",
  "number" * 3 AS "triple-number"
FROM "ext"
WHERE (TEXTCAT('super-', "animal") = 'super-mongoose' OR "location" = 3 OR "number" = 100)
PARTITIONED BY DAY
```

In the `SELECT` clause, you specify the following transformations:
* `animal`: prepends "super-" to the values in the `animal` column using the [`TEXTCAT`](../querying/sql-functions.md#textcat) function. Note that it only ingests the transformed data.
* `triple-number`: multiplies the `number` column by three and stores the results in a column named `triple-number`. Note that the query ingests both the original and the transformed data.

Additionally, the `WHERE` clause applies the following three OR operators so that the query only ingests the rows where at least one of the following conditions is `true`:

* `TEXTCAT('super-', "animal")` matches "super-mongoose"
* `location` matches 3
* `number` matches 100

Once a row passes the filter, the ingestion job applies the transformations. In this example, the filter selects the first three rows because each row meets at least one of the required OR conditions. For the selected rows, the ingestion job ingests the transformed `animal` column, the `location` column, and both the original `number` and the transformed `triple-number` column. The "lion" row doesn't meet any of the conditions, so it is not ingested or transformed.

## Query the transformed data

In the web console, open a new tab in the **Query** view. Run the following query to view the ingested data:

```sql
SELECT * FROM "transform_tutorial"
```

Returns the following:

| `__time` | `animal` | `location` | `number` | `triple-number` | 
| -- | -- | -- | -- | -- |
| `2018-01-01T05:01:35.000Z` | `super-mongoose` | `2` | `200` | `600` |
| `2018-01-01T06:01:35.000Z` | `super-snake` | `3` | `300` | `900` |
| `2018-01-01T07:01:35.000Z` | `super-octopus` | `1` |  `100` | `300` |

Notice how the "lion" row is missing, and how the other three rows that were ingested have transformations applied to them.

## Learn more

See the following topics for more information:

* [All functions](../querying/sql-functions.md) for a list of functions that can be used to transform data. 
* [Transform spec reference](../ingestion/ingestion-spec.md#transformspec) to learn more about transforms in JSON-based batch ingestion.
* [WHERE clause](../querying/sql.md#where) to learn how to specify filters in Druid SQL.