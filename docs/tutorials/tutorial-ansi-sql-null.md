---
id: tutorial-ansi-sql-null
title: Null handling tutorial
sidebar_label: Handling null values
description: Introduction to three-valued null handling
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

This tutorial introduces the basic concepts of three-valued null handling in Apache Druid.

## Prerequisites

Before starting this tutorial, download and run Apache Druid on your local machine as described in
the [single-machine quickstart](index.md).

The tutorial assumes you are familiar with using the [Query view](./tutorial-sql-query-view.md) to ingest and query data.

## Load data with null values

The tutorial loads some data with null values for various data types as follows:

```json
{"date": "1/1/2024 1:02:00","title": "example_1","string_value": "some_value","numeric_value": 1,"boolean_value_1": true,"boolean_value_2": "1"}
{"date": "1/1/2024 1:03:00","title": "example_2","string_value": "another_value","numeric_value": 2,"boolean_value_1": false,"boolean_value_2": "0"}
{"date": "1/1/2024 1:04:00","title": "example_3","": "", "numeric_value": null, "boolean_value_1": null,"boolean_value_2":null}
```

Note the following about the input data:
- An empty string value `""` serves as null for VARCHAR/string, but the BIGINT and BOOLEAN columns use `null`.
- The two boolean columns demonstrate the use of `true/false` and `0/1` as boolean input values. 

Run the following query in the Druid Console to load the data:

```sql
REPLACE INTO "null_data_example" OVERWRITE ALL
WITH "ext" AS (SELECT *
FROM TABLE(
  EXTERN(
    '{"type":"inline","data":"{\"date\": \"1/1/2024 1:02:00\",\"title\": \"example_1\",\"string_value\": \"some_value\",\"numeric_value\": 1,\"boolean_value_1\": true,\"boolean_value_2\": \"1\"}\n{\"date\": \"1/1/2024 1:03:00\",\"title\": \"example_2\",\"string_value\": \"another_value\",\"numeric_value\": 2,\"boolean_value_1\": false,\"boolean_value_2\": \"0\"}\n{\"date\": \"1/1/2024 1:04:00\",\"title\": \"example_3\",\"\": \"\", \"numeric_value\": null, \"boolean_value_1\": \"\",\"boolean_value_2\":null}"}',
    '{"type":"json"}'
  )
) EXTEND ("date" VARCHAR, "title" VARCHAR, "string_value" VARCHAR, "numeric_value" BIGINT, "boolean_value_1" VARCHAR, "boolean_value_2" VARCHAR))
SELECT
  TIME_PARSE("date", 'd/M/yyyy H:mm:ss') AS "__time",
  "title",
  "string_value",
  "numeric_value",
  CAST("boolean_value_1" AS BOOLEAN) AS "boolean_value_1",
  CAST("boolean_value_2" AS BOOLEAN) AS "boolean_value_2"
FROM "ext"
PARTITIONED BY DAY
```

After Druid finishes loading the data, run the following query to see how Druid loads the data:

```sql
SELECT * FROM null_data_example
```

|`__time`|`title`|`string_value`|`numeric_value`|`boolean_value_1`|`boolean_value_2`|
|---|---|---|---|---|---|
|`"2024-01-01T01:02:00.000Z"`|`"example_1`|`"some_value"`|1|`1|1|
|`"2024-01-01T01:03:00.000Z"`|`"example_2`|`"another_value"`|2|0|0|
|`"2024-01-01T01:04:00.000Z"`|`"example_3"`|`null`|`null`|`null`|`null`|

Note the follwoing about the result set:
- Druid stores the empty string and null values for `example_3` as `null`.
- Druid uses `0/1` for both BOOLEAN type columns.

### Compare to two-valued logic

Before the 28.0 release, Apache Druid used two-valued logic for null handling. The following query shows the results for `SELECT * FROM null_data_example` using the legacy logic.

|`__time`|`title`|`string_value`|`numeric_value`|`boolean_value_1`|`boolean_value_2`|
|---|---|---|---|---|---|
|`"2024-01-01T01:02:00.000Z"`|`"example_1`|`"some_value"`|1|1|1|
|`"2024-01-01T01:03:00.000Z"`|`"example_2`|`"another_value"`|2|0|0|
|`"2024-01-01T01:04:00.000Z"`|`"example_3"`|`""`|0|0|0|

Note that the `null` values for the BIGINT type column and for both BOOLEAN type columns is stored as `0`.

## String query example

Using three-valued logic, Druid treats `null` values as "unknown" and does not count them in comparisons.

```sql
SELECT COUNT(*)
FROM "null_data_example2"
WHERE "string_value" != 'some_value'
```

Druid returns 1. The `null` value for `example_3` is excluded.

### Compare to two-valued logic

The same query using two valued logic returns a value of 2.

## Numeric query example

```sql
SELECT COUNT(*)
FROM "null_data_example"
WHERE "numeric_value" < 2
```

Druid returns 1. The `null` value for `example_3` is excluded.

### Compare to two-valued logic

The same query using two-valued logic returns a value of 2.

## Boolean query examples 

```sql
SELECT "boolean_value_1",
COUNT(*) AS count_of
FROM "null_data_example"
GROUP BY 1
```

Druid returns the following:

|`boolean_value_1`|`count_of`|
|---|---|
|`empty`|1|
|FALSE|1|
|TRUE|1|


```sql
SELECT "boolean_value_2",
COUNT(*) AS count_of
FROM "null_data_example"
GROUP BY 1
```

Druid returns the following:

|`boolean_value_1`|`count_of`|
|---|---|
|`null`|1|
|0|1|
|1|1|

### Compare to two-valued logic

These queries return the following under two-valued logic:

|`boolean_value_1`|`count_of`|
|---|---|
|0|2|
|1|1|

|`boolean_value_2`|`count_of`|
|---|---|
|0|2|
|1|1|









