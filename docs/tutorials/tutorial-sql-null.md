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

## Prerequisites

Before starting this tutorial, download and run Apache Druid on your local machine as described in
the [Local quickstart](index.md).

The tutorial assumes you are familiar with using the [Query view](./tutorial-sql-query-view.md) to ingest and query data.

The tutorial also assumes you have not changed any of the default settings for null handling.

## Load data with null values

The sample data for the tutorial contains null values for string and numeric columns as follows:

```json
{"date": "1/1/2024 1:02:00","title": "example_1","string_value": "some_value","numeric_value": 1}
{"date": "1/1/2024 1:03:00","title": "example_2","string_value": "another_value","numeric_value": 2}
{"date": "1/1/2024 1:04:00","title": "example_3","string_value": "", "numeric_value": null}
{"date": "1/1/2024 1:05:00","title": "example_4","string_value": null, "numeric_value": null}
```

Run the following query in the Druid Console to load the data:

```sql
REPLACE INTO "null_example" OVERWRITE ALL
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

After Druid finishes loading the data, run the following query to see the table:

```sql
SELECT * FROM "null_example"
```

Druid returns the following:

|`__time`|`title`|`string_value`|`numeric_value`|
|---|---|---|---|
|`2024-01-01T01:02:00.000Z`|`example_1`|`some_value`|1|
|`2024-01-01T01:03:00.000Z`|`example_2`|`another_value`|2|
|`2024-01-01T01:04:00.000Z`|`example_3`|`empty`|`null`|
|`2024-01-01T01:05:00.000Z`|`example_4`|`null`|`null`|

Note the difference in the empty string value for example 3 and the null string value for example 4.

## String query example

The queries in this section illustrate null handling with strings.
The following query filters rows where the string value is not equal to `some_value`:

```sql
SELECT COUNT(*)
FROM "null_example"
WHERE "string_value" != 'some_value'
```

Druid returns 2 for `another_value` and the empty string `""`. The null value is not counted.

Note that the null value is included in `COUNT(*)` but not as a count of the values in the column as follows:

```sql
SELECT "string_value",
      COUNT(*) AS count_all_rows,
      COUNT("string_value") AS count_values
FROM "inline_data"
GROUP BY 1
```

Druid returns the following:

|`string_value`|`count_all_rows`|`count_values`|
|---|---|---|
|`null`|1|0|
|`empty`|1|1|
|`another_value`|1|1|
|`some_value`|1|1|

Also note that GROUP BY expressions yields distinct entries for `null` and the empty string.

### Filter for empty strings in addition to null

If your queries rely on treating empty strings and null values the same, you can use an OR operator in the filter. For example to select all rows with null values or empty strings:

```sql
SELECT *
FROM "null_example"
WHERE "string_value" IS NULL OR "string_value" = ''
```

Druid returns the following:

|`__time`|`title`|`string_value`|`numeric_value`|
|---|---|---|---|
|`2024-01-01T01:04:00.000Z`|`example_3`|`empty`|`null`|
|`2024-01-01T01:05:00.000Z`|`example_4`|`null`|`null`|

For another example, if you do not want to count empty strings, use a FILTER to exclude them. For example:

```sql
SELECT COUNT("string_value") FILTER(WHERE "string_value" <> '')
FROM "null_example"
```

Druid returns 2. Both the empty string and null values are excluded.

## Numeric query examples

Druid does not count null values in numeric comparisons.

```sql
SELECT COUNT(*)
FROM "null_example"
WHERE "numeric_value" < 2
```

Druid returns 1. The `null` values for examples 3 and 4 are excluded.

Additionally, be aware that null values do not behave as 0. For examples:

```sql
SELECT numeric_value + 1
FROM "null_example"
WHERE "__time" > '2024-01-01 01:04:00.000Z'
```

Druid returns `null` and not 1. One option is to use the COALESCE function for null handling. For example:

```sql
SELECT COALESCE(numeric_value, 0) + 1
FROM "null_example"
WHERE "__time" > '2024-01-01 01:04:00.000Z'
```

In this case, Druid returns 1.

## Ingestion time filtering

The same null handling rules apply at ingestion time.
The following query replaces the example data with data filtered with a WHERE clause:

```sql
REPLACE INTO "null_example" OVERWRITE ALL
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
WHERE "string_value" != 'some_value'
PARTITIONED BY DAY
```

The resulting data set only includes two rows. Druid has filtered out example 1 (`some_value`) and example 4 (`null`):

|`__time`|`title`|`string_value`|`numeric_value`|
|---|---|---|---|
|`2024-01-01T01:03:00.000Z`|`example_2`|`another_value`|2|
|`2024-01-01T01:04:00.000Z`|`example_3`|`empty`|`null`|

## Learn more

See the following for more information:
- [Null values](../querying/sql-data-types.md#null-values)
- "Generating and working with NULL values" notebook at [Learn druid](https://github.com/implydata/learn-druid/)
