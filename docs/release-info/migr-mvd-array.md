---
id: migr-mvd-array
title: "Migration guide: MVDs to arrays"
sidebar_label: MVDs to arrays
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


Druid now supports SQL-compliant [arrays](../querying/arrays.md). We recommend using arrays over [multi-value dimensions](../querying/multi-value-dimensions.md) (MVDs) whenever possible.
For new projects and complex use cases involving multiple data types, use arrays. Use MVDs for specific use cases, such as operating directly on individual elements like regular strings. If your operations involve entire arrays of values, including the ordering of values within a row, use arrays over MVDs.

## Comparison between arrays and MVDs

The following table compares the general behavior between arrays and MVDs.
For specific query differences between arrays and MVDs, see [Querying arrays and MVDs](#querying-arrays-and-mvds).

|  | Array| MVD |
|---|---|---|
| Data types | Supports VARCHAR, BIGINT, and DOUBLE types (ARRAY<STRING\>, ARRAY<LONG\>, ARRAY<DOUBLE\>) | Only supports arrays of strings (VARCHAR) |
| SQL compliance | Behaves like standard SQL arrays with SQL-compliant behavior | Behaves like SQL VARCHAR rather than standard SQL arrays and requires special SQL functions to achieve array-like behavior. See the [examples](#examples). |
| Ingestion | <ul><li>JSON arrays are ingested as Druid arrays</li><li>Managed through the query context parameter `arrayIngestMode` in SQL-based ingestion. Supported options are `array`, `mvd`, and `none`. Note that if you set this mode to `none`, Druid raises an exception if you try to store any type of array.</li></ul> | <ul><li>JSON arrays are ingested as MVDs</li><li>Managed using functions like [ARRAY_TO_MV](../querying/sql-functions.md#array_to_mv) in SQL-based ingestion</li></ul> |
| Filtering and grouping | <ul><li>Filters and groupings match the entire array value</li><li>Can be used as GROUP BY keys, grouping based on the entire array value</li><li>Use the [UNNEST operator](#group-by-array-elements) to group based on individual array elements</li></ul> | <ul><li>Filters match any value within the array</li><li>Grouping generates a group for each individual value, similar to an implicit UNNEST</li></ul> |
| Conversion | Convert an MVD to an array using [MV_TO_ARRAY](../querying/sql-functions.md#mv_to_array) | Convert an array to an MVD using [ARRAY_TO_MV](../querying/sql-functions.md#array_to_mv) |

## Querying arrays and MVDs

In SQL queries, Druid operates on arrays differently than MVDs.
A value in an array column is treated as a single array entity (SQL ARRAY), whereas a value in an MVD column is treated as individual strings (SQL VARCHAR).
This behavior applies even though multiple string values within the same MVD are still stored as a single field in the MVD column.

For example, consider the same value, `['a', 'b', 'c']` ingested into an array column and an MVD column.
In your query, you want to filter results by comparing some value with `['a', 'b', 'c']`.

* For array columns, Druid only returns the row when an equality filter matches the entire array.  
For example: `WHERE "array_column" = ARRAY['a', 'b', 'c']`.

* For MVD columns, Druid returns the row when an equality filter matches any value of the MVD.  
For example, any of the following filters return the row for the query:  
`WHERE "mvd_column" = 'a'`  
`WHERE "mvd_column" = 'b'`  
`WHERE "mvd_column" = 'c'`

Note this difference between arrays and MVDs when you write queries that involve filtering or grouping.

When your query applies both filters and grouping, MVDs may return rows that don't seem to match the filter,
since the grouping occurs after Druid applies the filter. For an example, see [Filter and group by array elements](#filter-and-group-by-array-elements).

## Examples

The following examples highlight a few analogous queries between arrays and MVDs.
For more information and examples, see [Querying arrays](../querying/arrays.md#querying-arrays) and [Querying multi-value dimensions](../querying/multi-value-dimensions.md#querying-multi-value-dimensions).

### Filter by an array element

Filter rows that have a certain value in the array or MVD.

#### Array

```sql
SELECT label, tags
FROM "array_example"
WHERE ARRAY_CONTAINS(tags, 't3')
```

#### MVD

```sql
SELECT label, tags
FROM "mvd_example"
WHERE tags = 't3'
```

### Filter by one or more elements

Filter rows for which the array or MVD contains one or more elements.
Notice that [ARRAY_OVERLAP](../querying/sql-functions.md#array_overlap) checks for any overlapping elements, whereas [ARRAY_CONTAINS](../querying/sql-functions.md#array_contains) in the previous example checks that all elements are included.

#### Array

```sql
SELECT *
FROM "array_example"
WHERE ARRAY_OVERLAP(tags, ARRAY['t1', 't7'])
```

#### MVD

```sql
SELECT *
FROM "mvd_example"
WHERE tags = 't1' OR tags = 't7'
```

### Filter using array equality

Filter rows for which the array or MVD is equivalent to a reference array.

#### Array

```sql
SELECT *
FROM "array_example"
WHERE tags = ARRAY['t1', 't2', 't3']
```

#### MVD

```sql
SELECT *
FROM "mvd_example"
WHERE MV_TO_ARRAY(tags) = ARRAY['t1', 't2', 't3']
```

### Group results by array

Group results by the array or MVD.

#### Array

```sql
SELECT label, tags
FROM "array_example"
GROUP BY 1, 2
```

#### MVD

```sql
SELECT label, MV_TO_ARRAY(tags)
FROM "mvd_example"
GROUP BY 1, 2
```

### Group by array elements

Group results by individual array or MVD elements.

#### Array

```sql
SELECT label, strings
FROM "array_example" CROSS JOIN UNNEST(tags) as u(strings)
GROUP BY 1, 2
```

#### MVD

```sql
SELECT label, tags
FROM "mvd_example"
GROUP BY 1, 2
```

### Filter and group by array elements

Filter rows that have a certain value, then group by elements in the array or MVD.
This example illustrates that while the results of filtering may match between arrays and MVDs,
be aware that MVDs implicitly unnest their values so that results differ when you also apply a GROUP BY.

For example, consider the queries from [Filter by an array element](#filter-by-an-array-element).
Both queries return the following rows:

```json
{"label":"row1","tags":["t1","t2","t3"]}
{"label":"row2","tags":["t3","t4","t5"]}
```

However, adding `GROUP BY 1, 2` to both queries changes the output.
The two queries are now:

```sql
-- Array
SELECT label, tags
FROM "array_example"
WHERE ARRAY_CONTAINS(tags, 't3')
GROUP BY 1, 2

-- MVD
SELECT label, tags
FROM "mvd_example"
WHERE tags = 't3'
GROUP BY 1, 2
```

The array query returns the following:

```json
{"label":"row1","tags":["t1","t2","t3"]}
{"label":"row2","tags":["t3","t4","t5"]}
```

The MVD query returns the following:

```json
{"label":"row1","tags":"t1"}
{"label":"row1","tags":"t2"}
{"label":"row1","tags":"t3"}
{"label":"row2","tags":"t3"}
{"label":"row2","tags":"t4"}
{"label":"row2","tags":"t5"}
```

The MVD results appear to show four extra rows for which `tags` does not equal `t3`.
However, the rows match the filter based on how Druid evaluates equalities for MVDs.

For the equivalent query on MVDs, use the [MV_FILTER_ONLY](../querying/sql-functions.md#mv_filter_only) function:

```sql
SELECT label, MV_FILTER_ONLY(tags, ARRAY['t3'])
FROM "mvd_example"
WHERE tags = 't3'
GROUP BY 1, 2
```


## How to ingest data as arrays

You can ingest arrays in Druid as follows:

* For native batch and streaming ingestion, configure the dimensions in [`dimensionsSpec`](../ingestion/ingestion-spec.md#dimensionsspec).
Within `dimensionsSpec`, set `"useSchemaDiscovery": true`, and use `dimensions` to list the array inputs with type `auto`.  
For an example, see [Ingesting arrays: Native batch and streaming ingestion](../querying/arrays.md#native-batch-and-streaming-ingestion).

* For SQL-based batch ingestion, include the [query context parameter](../multi-stage-query/reference.md#context-parameters) `"arrayIngestMode": "array"` and reference the relevant array type (`VARCHAR ARRAY`, `BIGINT ARRAY`, or `DOUBLE ARRAY`) in the [EXTEND clause](../multi-stage-query/reference.md#extern-function) that lists the column names and data types.
For examples, see [Ingesting arrays: SQL-based ingestion](../querying/arrays.md#sql-based-ingestion).

   As a best practice, always use the ARRAY data type in your input schema. If you want to ingest MVDs, explicitly wrap the string array in [ARRAY_TO_MV](../querying/sql-functions.md#array_to_mv). For an example, see [Multi-value dimensions: SQL-based ingestion](/querying/multi-value-dimensions.md#sql-based-ingestion).

