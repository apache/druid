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
Use arrays for new projects and complex use cases involving multiple data types. Use MVDs for specific use cases, such as arrays of strings.

## Comparison between arrays and MVDs

The following table compares the general behavior between arrays and MVDs.
For specific query differences between arrays and MVDs, see [Query differences between arrays and MVDs](#query-differences-between-arrays-and-mvds).

|  | Arrays| Multi-value dimensions (MVDs) |
|---|---|---|
| Data types | Supports VARCHAR, BIGINT, and DOUBLE types (ARRAY<STRING\>, ARRAY<LONG\>, ARRAY<DOUBLE\>) | Only supports arrays of strings (VARCHAR) |
| SQL compliance | Behaves like standard SQL arrays with SQL-compliant behavior | Does not behave like standard SQL arrays; requires special SQL functions |
| Ingestion | <ul><li>Can be ingested using native batch or streaming ingestion methods</li><li>JSON arrays are ingested as Druid arrays</li><li>Managed through the query context parameter `arrayIngestMode` in SQL-based ingestion (supported options: `array`, `mvd`, `none`). Note that if you set this mode to `none`, Druid raises an exception if you try to store any type of array.</li></ul> | <ul><li>Typically ingested from fields with an array of values</li><li>JSON arrays are ingested as multi-value dimensions</li><li>Managed using functions like [ARRAY_TO_MV](../querying/sql-functions.md#array_to_mv) in SQL-based ingestion</li></ul> |
| Filtering and grouping | <ul><li>Filters and groupings match the entire array value</li><li>Can be used as GROUP BY keys, grouping based on the entire array value</li></ul> | <ul><li>Filters match any value within the array</li><li>Grouping generates a group for each individual value, similar to an implicit UNNEST</li></ul> |
| Conversion | Convert an MVD to an array using [MV_TO_ARRAY](../querying/sql-multivalue-string-functions.md) | Convert an array to an MVD using [ARRAY_TO_MV](../querying/sql-functions.md#array_to_mv) |

### Query differences between arrays and MVDs

Querying an array column returns different results than when querying a MVD column depending on the query type. Review the following table on the differences. Adjust your applications to handle the new output shapes.

| Query type | Array | MVD |
|---|---|---|
| Equality filter | Matches the entire array value | Matches if any value within the array matches the filter |
| Null filter | Matches rows where the entire array value is null | Matches rows where the array is empty (considered as null) but does not match arrays with empty (`“”`) values |
| Range filter | Use [ARRAY_OVERLAP](../querying/sql-functions.md#array_overlap) | Not directly supported |
| Contains filter | Use [ARRAY_CONTAINS](../querying/sql-functions.md#array_contains)| Use WHERE filter |
| Logical expression filters | Behaves like standard ANSI SQL on the entire array value, such as AND, OR, NOT. For example, `WHERE arrayLong = ARRAY[1,2,3] OR arrayLong = ARRAY[4,5,6]` | Matches a row if any value within the array matches the logical condition. For example, `WHERE tags = 't1' OR tags = 't3'` |
| Column comparison filter | Use [ARRAY_OVERLAP](../querying/sql-functions.md#array_overlap) | Matches when the dimensions have any overlapping values. For example, `WHERE tags IN ('t1', 't2')` |
| Behavior with SQL constructs | Follows standard SQL behavior with array functions like [ARRAY_CONTAINS](../querying/sql-functions.md#array_contains), [ARRAY_OVERLAP](../querying/sql-functions.md#array_overlap) | Requires special SQL functions like [MV_FILTER_ONLY](../querying/sql-functions.md#mv_filter_none), [MV_FILTER_NONE](../querying/sql-functions.md#mv_filter_only) for precise filtering |
| Group by entire array | Groups the entire array as a single value | Not supported |
| Group by individual values | Use [UNNEST](../querying/sql.md#unnest) to group by individual array elements | Automatically unnests groups by each individual value in the array |

## How to ingest data as arrays

To store data as arrays, do one of the following in an ingestion job:

* For classic batch and streaming ingestion, specify the column's data type in [`dimensionSpec`](../ingestion/ingestion-spec.md#dimensionsspec) as `array`.

* When you use [schema auto discovery](../ingestion/schema-design.md#type-aware-schema-discovery), Druid automatically ingests arrays as array types. Set `"useSchemaDiscovery": true` in the `dimensionSpec`.

* For [SQL-based batch ingestion](../multi-stage-query/index.md), write a query that generates arrays, and set the context parameter `"arrayIngestMode": "array"`.

