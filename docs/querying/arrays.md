---
id: arrays
title: "Arrays"
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


Apache Druid supports SQL standard `ARRAY` typed columns for `VARCHAR`, `BIGINT`, and `DOUBLE` types (native types `ARRAY<STRING>`, `ARRAY<LONG>`, and `ARRAY<DOUBLE>`). Other more complicated ARRAY types must be stored in [nested columns](nested-columns.md). Druid ARRAY types are distinct from [multi-value dimension](multi-value-dimensions.md), which have significantly different behavior than standard arrays.

This document describes inserting, filtering, and grouping behavior for `ARRAY` typed columns.
Refer to the [Druid SQL data type documentation](sql-data-types.md#arrays) and [SQL array function reference](sql-array-functions.md) for additional details
about the functions available to use with ARRAY columns and types in SQL.

The following sections describe inserting, filtering, and grouping behavior based on the following example data, which includes 3 array typed columns:

```json lines
{"timestamp": "2023-01-01T00:00:00", "label": "row1", "arrayString": ["a", "b"],  "arrayLong":[1, null,3], "arrayDouble":[1.1, 2.2, null]}
{"timestamp": "2023-01-01T00:00:00", "label": "row2", "arrayString": [null, "b"], "arrayLong":null,        "arrayDouble":[999, null, 5.5]}
{"timestamp": "2023-01-01T00:00:00", "label": "row3", "arrayString": [],          "arrayLong":[1, 2, 3],   "arrayDouble":[null, 2.2, 1.1]} 
{"timestamp": "2023-01-01T00:00:00", "label": "row4", "arrayString": ["a", "b"],  "arrayLong":[1, 2, 3],   "arrayDouble":[]}
{"timestamp": "2023-01-01T00:00:00", "label": "row5", "arrayString": null,        "arrayLong":[],          "arrayDouble":null}
```

## Ingesting arrays

### Native batch and streaming ingestion
When using native [batch](../ingestion/native-batch.md) or streaming ingestion such as with [Apache Kafka](../ingestion/kafka-ingestion.md), arrays can be ingested using the [`"auto"`](../ingestion/ingestion-spec.md#dimension-objects) type dimension schema which is shared with [type-aware schema discovery](../ingestion/schema-design.md#type-aware-schema-discovery).

When ingesting from TSV or CSV data, you can specify the array delimiters using the `listDelimiter` field in the `inputFormat`. JSON data must be formatted as a JSON array to be ingested as an array type. JSON data does not require `inputFormat` configuration.

The following shows an example `dimensionsSpec` for native ingestion of the data used in this document:

```
"dimensions": [
  {
    "type": "auto",
    "name": "label"
  },
  {
    "type": "auto",
    "name": "arrayString"
  },
  {
    "type": "auto",
    "name": "arrayLong"
  },
  {
    "type": "auto",
    "name": "arrayDouble"
  }
],
```

### SQL-based ingestion

Arrays can also be inserted with [SQL-based ingestion](../multi-stage-query/index.md) when you include a query context parameter [`"arrayIngestMode":"array"`](../multi-stage-query/reference.md#context-parameters).

For example, to insert the data used in this document:
```sql
REPLACE INTO "array_example" OVERWRITE ALL
WITH "ext" AS (
  SELECT *
  FROM TABLE(
    EXTERN(
      '{"type":"inline","data":"{\"timestamp\": \"2023-01-01T00:00:00\", \"label\": \"row1\", \"arrayString\": [\"a\", \"b\"],  \"arrayLong\":[1, null,3], \"arrayDouble\":[1.1, 2.2, null]}\n{\"timestamp\": \"2023-01-01T00:00:00\", \"label\": \"row2\", \"arrayString\": [null, \"b\"], \"arrayLong\":null,        \"arrayDouble\":[999, null, 5.5]}\n{\"timestamp\": \"2023-01-01T00:00:00\", \"label\": \"row3\", \"arrayString\": [],          \"arrayLong\":[1, 2, 3],   \"arrayDouble\":[null, 2.2, 1.1]} \n{\"timestamp\": \"2023-01-01T00:00:00\", \"label\": \"row4\", \"arrayString\": [\"a\", \"b\"],  \"arrayLong\":[1, 2, 3],   \"arrayDouble\":[]}\n{\"timestamp\": \"2023-01-01T00:00:00\", \"label\": \"row5\", \"arrayString\": null,        \"arrayLong\":[],          \"arrayDouble\":null}"}',
      '{"type":"json"}',
      '[{"name":"timestamp", "type":"STRING"},{"name":"label", "type":"STRING"},{"name":"arrayString", "type":"ARRAY<STRING>"},{"name":"arrayLong", "type":"ARRAY<LONG>"},{"name":"arrayDouble", "type":"ARRAY<DOUBLE>"}]'
    )
  )
)
SELECT
  TIME_PARSE("timestamp") AS "__time",
  "label",
  "arrayString",
  "arrayLong",
  "arrayDouble"
FROM "ext"
PARTITIONED BY DAY
```

### SQL-based ingestion with rollup
These input arrays can also be grouped for rollup:

```sql
REPLACE INTO "array_example_rollup" OVERWRITE ALL
WITH "ext" AS (
  SELECT *
  FROM TABLE(
    EXTERN(
      '{"type":"inline","data":"{\"timestamp\": \"2023-01-01T00:00:00\", \"label\": \"row1\", \"arrayString\": [\"a\", \"b\"],  \"arrayLong\":[1, null,3], \"arrayDouble\":[1.1, 2.2, null]}\n{\"timestamp\": \"2023-01-01T00:00:00\", \"label\": \"row2\", \"arrayString\": [null, \"b\"], \"arrayLong\":null,        \"arrayDouble\":[999, null, 5.5]}\n{\"timestamp\": \"2023-01-01T00:00:00\", \"label\": \"row3\", \"arrayString\": [],          \"arrayLong\":[1, 2, 3],   \"arrayDouble\":[null, 2.2, 1.1]} \n{\"timestamp\": \"2023-01-01T00:00:00\", \"label\": \"row4\", \"arrayString\": [\"a\", \"b\"],  \"arrayLong\":[1, 2, 3],   \"arrayDouble\":[]}\n{\"timestamp\": \"2023-01-01T00:00:00\", \"label\": \"row5\", \"arrayString\": null,        \"arrayLong\":[],          \"arrayDouble\":null}"}',
      '{"type":"json"}',
      '[{"name":"timestamp", "type":"STRING"},{"name":"label", "type":"STRING"},{"name":"arrayString", "type":"ARRAY<STRING>"},{"name":"arrayLong", "type":"ARRAY<LONG>"},{"name":"arrayDouble", "type":"ARRAY<DOUBLE>"}]'
    )
  )
)
SELECT
  TIME_PARSE("timestamp") AS "__time",
  "label",
  "arrayString",
  "arrayLong",
  "arrayDouble",
  COUNT(*) as "count"
FROM "ext"
GROUP BY 1,2,3,4,5
PARTITIONED BY DAY
```


## Querying arrays

### Filtering

All query types, as well as [filtered aggregators](aggregations.md#filtered-aggregator), can filter on array typed columns. Filters follow these rules for array types:

- All filters match against the entire array value for the row
- Native value filters like [equality](filters.md#equality-filter) and [range](filters.md#range-filter) match on entire array values, as do SQL constructs that plan into these native filters
- The [`IS NULL`](filters.md#null-filter) filter will match rows where the entire array value is null
- [Array specific functions](sql-array-functions.md) like `ARRAY_CONTAINS` and `ARRAY_OVERLAP` follow the behavior specified by those functions
- All other filters do not directly support ARRAY types and will result in a query error

#### Example: equality
```sql
SELECT *
FROM "array_example"
WHERE arrayLong = ARRAY[1,2,3]
```

```json lines
{"__time":"2023-01-01T00:00:00.000Z","label":"row3","arrayString":"[]","arrayLong":"[1,2,3]","arrayDouble":"[null,2.2,1.1]"}
{"__time":"2023-01-01T00:00:00.000Z","label":"row4","arrayString":"[\"a\",\"b\"]","arrayLong":"[1,2,3]","arrayDouble":"[]"}
```

#### Example: null
```sql
SELECT *
FROM "array_example"
WHERE arrayLong IS NULL
```

```json lines
{"__time":"2023-01-01T00:00:00.000Z","label":"row2","arrayString":"[null,\"b\"]","arrayLong":null,"arrayDouble":"[999.0,null,5.5]"}
```

#### Example: range
```sql
SELECT *
FROM "array_example"
WHERE arrayString >= ARRAY['a','b']
```

```json lines
{"__time":"2023-01-01T00:00:00.000Z","label":"row1","arrayString":"[\"a\",\"b\"]","arrayLong":"[1,null,3]","arrayDouble":"[1.1,2.2,null]"}
{"__time":"2023-01-01T00:00:00.000Z","label":"row4","arrayString":"[\"a\",\"b\"]","arrayLong":"[1,2,3]","arrayDouble":"[]"}
```

#### Example: ARRAY_CONTAINS
```sql
SELECT *
FROM "array_example"
WHERE ARRAY_CONTAINS(arrayString, 'a')
```

```json lines
{"__time":"2023-01-01T00:00:00.000Z","label":"row1","arrayString":"[\"a\",\"b\"]","arrayLong":"[1,null,3]","arrayDouble":"[1.1,2.2,null]"}
{"__time":"2023-01-01T00:00:00.000Z","label":"row4","arrayString":"[\"a\",\"b\"]","arrayLong":"[1,2,3]","arrayDouble":"[]"}
```

### Grouping

When grouping on an array with SQL or a native [groupBy query](groupbyquery.md), grouping follows standard SQL behavior and groups on the entire array as a single value. The [`UNNEST`](sql.md#unnest) function allows grouping on the individual array elements.

#### Example: SQL grouping query with no filtering
```sql
SELECT label, arrayString
FROM "array_example"
GROUP BY 1,2
```
results in:
```json lines
{"label":"row1","arrayString":"[\"a\",\"b\"]"}
{"label":"row2","arrayString":"[null,\"b\"]"}
{"label":"row3","arrayString":"[]"}
{"label":"row4","arrayString":"[\"a\",\"b\"]"}
{"label":"row5","arrayString":null}
```

#### Example: SQL grouping query with a filter
```sql
SELECT label, arrayString
FROM "array_example"
WHERE arrayLong = ARRAY[1,2,3]
GROUP BY 1,2
```

results:
```json lines
{"label":"row3","arrayString":"[]"}
{"label":"row4","arrayString":"[\"a\",\"b\"]"}
```

#### Example: UNNEST
```sql
SELECT label, strings
FROM "array_example" CROSS JOIN UNNEST(arrayString) as u(strings)
GROUP BY 1,2
```

results:
```json lines
{"label":"row1","strings":"a"}
{"label":"row1","strings":"b"}
{"label":"row2","strings":null}
{"label":"row2","strings":"b"}
{"label":"row4","strings":"a"}
{"label":"row4","strings":"b"}
```

## Differences between arrays and multi-value dimensions
Avoid confusing string arrays with [multi-value dimensions](multi-value-dimensions.md). Arrays and multi-value dimensions are stored in different column types, and query behavior is different. You can use the functions `MV_TO_ARRAY` and `ARRAY_TO_MV` to convert between the two if needed. In general, we recommend using arrays whenever possible, since they are a newer and more powerful feature and have SQL compliant behavior.

Use care during ingestion to ensure you get the type you want.

To get arrays when performing an ingestion using JSON ingestion specs, such as [native batch](../ingestion/native-batch.md) or streaming ingestion such as with [Apache Kafka](../ingestion/kafka-ingestion.md), use dimension type `auto` or enable `useSchemaDiscovery`. When performing a [SQL-based ingestion](../multi-stage-query/index.md), write a query that generates arrays and set the context parameter `"arrayIngestMode": "array"`. Arrays may contain strings or numbers.

To get multi-value dimensions when performing an ingestion using JSON ingestion specs, use dimension type `string` and do not enable `useSchemaDiscovery`. When performing a [SQL-based ingestion](../multi-stage-query/index.md), wrap arrays in [`ARRAY_TO_MV`](multi-value-dimensions.md#sql-based-ingestion), which ensures you get multi-value dimensions in any `arrayIngestMode`. Multi-value dimensions can only contain strings.

You can tell which type you have by checking the `INFORMATION_SCHEMA.COLUMNS` table, using a query like:

```sql
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mytable'
```

Arrays are type `ARRAY`, multi-value strings are type `VARCHAR`.