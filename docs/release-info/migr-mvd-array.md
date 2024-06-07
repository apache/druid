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


Druid now supports SQL-compliant [arrays](../querying/arrays.md), and we recommend that people use arrays over [multi-value dimensions](../querying/multi-value-dimensions.md) (MVDs) whenever possible.
Use arrays for new projects and complex use cases involving multiple data types. Use MVDs for specific use cases, such as operating directly on individual elements like regular strings. If your operations involve complete arrays of values, including the ordering of values within a row, use arrays over MVDs.

## Comparison between arrays and MVDs

The following table compares the general behavior between arrays and MVDs.
For specific query differences between arrays and MVDs, see [Query differences between arrays and MVDs](#query-differences-between-arrays-and-mvds).

|  | Arrays| Multi-value dimensions (MVDs) |
|---|---|---|
| Data types | Supports VARCHAR, BIGINT, and DOUBLE types (ARRAY<STRING\>, ARRAY<LONG\>, ARRAY<DOUBLE\>) | Only supports arrays of strings (VARCHAR) |
| SQL compliance | Behaves like standard SQL arrays with SQL-compliant behavior | Does not behave like standard SQL arrays; requires special SQL functions |
| Ingestion | <ul><li>JSON arrays are ingested as Druid arrays</li><li>Managed through the query context parameter `arrayIngestMode` in SQL-based ingestion (supported options: `array`, `mvd`, `none`). Note that if you set this mode to `none`, Druid raises an exception if you try to store any type of array.</li></ul> | <ul><li>JSON arrays are ingested as multi-value dimensions</li><li>Managed using functions like [ARRAY_TO_MV](../querying/sql-functions.md#array_to_mv) in SQL-based ingestion</li></ul> |
| Filtering and grouping | <ul><li>Filters and groupings match the entire array value</li><li>Can be used as GROUP BY keys, grouping based on the entire array value</li></ul> | <ul><li>Filters match any value within the array</li><li>Grouping generates a group for each individual value, similar to an implicit UNNEST</li></ul> |
| Conversion | Convert an MVD to an array using [MV_TO_ARRAY](../querying/sql-functions.md#mv_to_array) | Convert an array to an MVD using [ARRAY_TO_MV](../querying/sql-functions.md#array_to_mv) |

## Query differences between arrays and MVDs

In SQL queries, Druid operates on arrays differently than MVDs.
A value in an array column is treated as a single array entity (SQL ARRAY), whereas a value in an MVD column is treated as individual strings (SQL VARCHAR).

For example, consider the same value, `['a', 'b', 'c']` ingested into an array column and an MVD column.
In your query, you want to filter results by comparing some value with `['a', 'b', 'c']`.

* For array columns, Druid only returns the row when an equality filter matches the entire array.  
For example: `WHERE "array_column" = ARRAY['a', 'b', 'c']`.

* For MVD columns, Druid returns the row when an equality filter matches any value of the MVD.  
For example, any of the following filters returns the row for the query:  
`WHERE "mvd_column" = 'a'`  
`WHERE "mvd_column" = 'b'`  
`WHERE "mvd_column" = 'c'`

Note this difference between arrays and MVDs when you write queries that involve filtering or grouping.

The following examples highlight a few analogous queries between arrays and MVDs.
For more information and examples, see [Querying arrays](../querying/arrays.md#querying-arrays) and [Querying multi-value dimensions](../querying/multi-value-dimensions.md#querying-multi-value-dimensions).

### Example: an element in array

Filter rows that have a certain value in the array or MVD.

#### Array

```sql
SELECT *
FROM "array_example"
WHERE ARRAY_CONTAINS(tags, 't3')
```

#### MVD

```sql
SELECT *
FROM "mvd_example"
WHERE tags = 't3'
```

### Example: one or more elements in array

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

### Example: group by array elements

Group results by individual array elements.

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

## How to ingest data as arrays

You can ingest arrays in Druid as follows:

* For native batch and streaming ingestion, configure the dimensions in [`dimensionsSpec`](../ingestion/ingestion-spec.md#dimensionsspec).
Within `dimensionsSpec`, set `"useSchemaDiscovery": true`, and use `dimensions` to list the array inputs with type `auto`.  
For an example, see [Ingesting arrays: Native batch and streaming ingestion](../querying/arrays.md#native-batch-and-streaming-ingestion).

* For SQL-based batch ingestion, include the [query context parameter](../multi-stage-query/reference.md#context-parameters) `"arrayIngestMode": "array"` and reference the relevant array type (`VARCHAR ARRAY`, `BIGINT ARRAY`, or `DOUBLE ARRAY`) in the column descriptors.  
For examples, see [Ingesting arrays: SQL-based ingestion](../querying/arrays.md#sql-based-ingestion).

