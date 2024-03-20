---
id: tutorial-sql-null
title: Null handling tutorial
sidebar_label: Handling null values
description: Introduction to null handling in Druid
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

This tutorial introduces the basic concepts of null handling for string and numeric columns in Apache Druid.
The tutorial focuses on filters using the logical NOT operation on columns with NULL values.
This applies to both query and ingestion time filtering.

## Prerequisites

Before starting this tutorial, download and run Apache Druid on your local machine as described in
the [single-machine quickstart](index.md).

The tutorial assumes you are familiar with using the [Query view](./tutorial-sql-query-view.md) to ingest and query data.

## Load data with null values

The tutorial loads some data with null values for various data types as follows:

```json
{"date": "1/1/2024 1:02:00","title": "example_1","string_value": "some_value","numeric_value": 1}
{"date": "1/1/2024 1:03:00","title": "example_2","string_value": "another_value","numeric_value": 2}
{"date": "1/1/2024 1:04:00","title": "example_3","string_value": "", "numeric_value": null}
{"date": "1/1/2024 1:05:00","title": "example_4","string_value": null, "numeric_value": null}
```

Run the following query in the Druid Console to load the data:

```sql
REPLACE INTO "inline_data" OVERWRITE ALL
WITH "ext" AS (
  SELECT *
  FROM TABLE(
    EXTERN(
      '{"type":"inline","data":"{\"date\": \"1/1/2024 1:02:00\",\"title\": \"example_1\",\"string_value\": \"some_value\",\"numeric_value\": 1}\n{\"date\": \"1/1/2024 1:03:00\",\"title\": \"example_2\",\"string_value\": \"another_value\",\"numeric_value\": 2}\n{\"date\": \"1/1/2024 1:04:00\",\"title\": \"example_3\",\"string_value\": \"\", \"numeric_value\": null}\n{\"date\": \"1/1/2024 1:05:00\",\"title\": \"example_4\",\"string_value\": null, \"numeric_value\": null}"}',
      '{"type":"json"}'
    )
  ) EXTEND ("date" VARCHAR, "title" VARCHAR, "string_value" VARCHAR, "numeric_value" BIGINT)
)
SELECT
  TIME_PARSE("date", 'd/M/yyyy H:mm:ss') AS "__time",
  "title",
  "string_value",
  "numeric_value"
FROM "ext"
PARTITIONED BY DAY
```

After Druid finishes loading the data, run the following query to see the data in Druid:

```sql
SELECT * FROM null_data_example
```

|`__time`|`title`|`string_value`|`numeric_value`|
|---|---|---|---|---|---|
|`2024-01-01T01:02:00.000Z`|`example_1`|`some_value`|1|
|`2024-01-01T01:03:00.000Z`|`example_2`|`another_value`|2|
|`2024-01-01T01:04:00.000Z`|`example_3`|`empty`|`null`|
|`2024-01-01T01:05:00.000Z`|`example_4`|`null`|`null`|

Note the difference in the empty string value for example 3 and the null string value for example 4.


## String query example

The following query illustrates null handling with strings:

```sql
SELECT COUNT(*)
FROM "null_data_example2"
WHERE "string_value" != 'some_value'
```
Returns 2 for "another_value" and the empty string "". The null value is not counted.

Note that the null value is included in COUNT(*) but not as a count of the values in the column as follows:

```sql
SELECT "string_value",
      COUNT(*) AS count_all_rows,
      COUNT("string_value") AS count_values
FROM "inline_data"
GROUP BY 1
```

Returns the folloaing data:

|`string_value`|`count_all_rows`|`count_values`|
|---|---|---|
|`null`|1|0|
|`empty`|1|1|
|`another_value`|1|1|
|`some_value`|1|1|

## Numeric query example

Using three-valued logic, Druid treats `null` values as "unknown" and does not count them in comparisons.

```sql
SELECT COUNT(*)
FROM "null_data_example"
WHERE "numeric_value" < 2
```

Druid returns 1. The `null` values for examples 3 and 4 are excluded.









