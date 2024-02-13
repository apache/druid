---
id: multi-value-dimensions
title: "Multi-value dimensions"
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


Apache Druid supports "multi-value" string dimensions. Multi-value string dimensions result from input fields that contain an
array of values instead of a single value, such as the `tags` values in the following JSON array example: 

```
{"timestamp": "2011-01-12T00:00:00.000Z", "tags": ["t1","t2","t3"]} 
```

It is important to be aware that multi-value dimensions are distinct from [array types](arrays.md). While array types behave like standard SQL arrays, multi-value dimensions do not. This document describes the behavior of multi-value dimensions, and some additional details can be found in the [SQL data type documentation](sql-data-types.md#multi-value-strings-behavior).

This document describes inserting, filtering, and grouping behavior for multi-value dimensions. For information about the internal representation of multi-value dimensions, see
[segments documentation](../design/segments.md#multi-value-columns). Examples in this document
are in the form of both [SQL](sql.md) and [native Druid queries](querying.md). Refer to the [Druid SQL documentation](sql-multivalue-string-functions.md) for details
about the functions available for using multi-value string dimensions in SQL.

The following sections describe inserting, filtering, and grouping behavior based on the following example data, which includes a multi-value dimension, `tags`.

```json lines
{"timestamp": "2011-01-12T00:00:00.000Z", "label": "row1", "tags": ["t1","t2","t3"]}
{"timestamp": "2011-01-13T00:00:00.000Z", "label": "row2", "tags": ["t3","t4","t5"]}
{"timestamp": "2011-01-14T00:00:00.000Z", "label": "row3", "tags": ["t5","t6","t7"]}
{"timestamp": "2011-01-14T00:00:00.000Z", "label": "row4", "tags": []}
```

## Ingestion

### Native batch and streaming ingestion
When using native [batch](../ingestion/native-batch.md) or streaming ingestion such as with [Apache Kafka](../ingestion/kafka-ingestion.md), the Druid web console data loader can detect multi-value dimensions and configure the `dimensionsSpec` accordingly.

For TSV or CSV data, you can specify the multi-value delimiters using the `listDelimiter` field in the `inputFormat`. JSON data must be formatted as a JSON array to be ingested as a multi-value dimension. JSON data does not require `inputFormat` configuration.

The following shows an example `dimensionsSpec` for native ingestion of the data used in this document:

```
"dimensions": [
  {
    "type": "string",
    "name": "label"
  },
  {
    "type": "string",
    "name": "tags",
    "multiValueHandling": "SORTED_ARRAY",
    "createBitmapIndex": true
  }
],
```

By default, Druid sorts values in multi-value dimensions. This behavior is controlled by the `SORTED_ARRAY` value of the `multiValueHandling` field. Alternatively, you can specify multi-value handling as:

* `SORTED_SET`: results in the removal of duplicate values
* `ARRAY`: retains the original order of the values

See [Dimension Objects](../ingestion/ingestion-spec.md#dimension-objects) for information on configuring multi-value handling.

### SQL-based ingestion
Multi-value dimensions can also be inserted with [SQL-based ingestion](../multi-stage-query/index.md). The functions `MV_TO_ARRAY` and `ARRAY_TO_MV` can assist in converting `VARCHAR` to `VARCHAR ARRAY` and `VARCHAR ARRAY` into `VARCHAR` respectively. `multiValueHandling` is not available when using the multi-stage query engine to insert data.

For example, to insert the data used in this document:
```sql
REPLACE INTO "mvd_example" OVERWRITE ALL
WITH "ext" AS (
  SELECT *
  FROM TABLE(
    EXTERN(
      '{"type":"inline","data":"{\"timestamp\": \"2011-01-12T00:00:00.000Z\", \"label\": \"row1\", \"tags\": [\"t1\",\"t2\",\"t3\"]}\n{\"timestamp\": \"2011-01-13T00:00:00.000Z\", \"label\": \"row2\", \"tags\": [\"t3\",\"t4\",\"t5\"]}\n{\"timestamp\": \"2011-01-14T00:00:00.000Z\", \"label\": \"row3\", \"tags\": [\"t5\",\"t6\",\"t7\"]}\n{\"timestamp\": \"2011-01-14T00:00:00.000Z\", \"label\": \"row4\", \"tags\": []}"}',
      '{"type":"json"}',
      '[{"name":"timestamp", "type":"STRING"},{"name":"label", "type":"STRING"},{"name":"tags", "type":"ARRAY<STRING>"}]'
    )
  )
)
SELECT
  TIME_PARSE("timestamp") AS "__time",
  "label",
  ARRAY_TO_MV("tags") AS "tags"
FROM "ext"
PARTITIONED BY DAY
```

### SQL-based ingestion with rollup
These input arrays can also be grouped prior to converting into a multi-value dimension:
```sql
REPLACE INTO "mvd_example_rollup" OVERWRITE ALL
WITH "ext" AS (
  SELECT *
  FROM TABLE(
    EXTERN(
      '{"type":"inline","data":"{\"timestamp\": \"2011-01-12T00:00:00.000Z\", \"label\": \"row1\", \"tags\": [\"t1\",\"t2\",\"t3\"]}\n{\"timestamp\": \"2011-01-13T00:00:00.000Z\", \"label\": \"row2\", \"tags\": [\"t3\",\"t4\",\"t5\"]}\n{\"timestamp\": \"2011-01-14T00:00:00.000Z\", \"label\": \"row3\", \"tags\": [\"t5\",\"t6\",\"t7\"]}\n{\"timestamp\": \"2011-01-14T00:00:00.000Z\", \"label\": \"row4\", \"tags\": []}"}',
      '{"type":"json"}',
      '[{"name":"timestamp", "type":"STRING"},{"name":"label", "type":"STRING"},{"name":"tags", "type":"ARRAY<STRING>"}]'
    )
  )
)
SELECT
  TIME_PARSE("timestamp") AS "__time",
  "label",
  ARRAY_TO_MV("tags") AS "tags",
  COUNT(*) AS "count"
FROM "ext"
GROUP BY 1, 2, "tags"
PARTITIONED BY DAY
```

Notice that `ARRAY_TO_MV` is not present in the `GROUP BY` clause since we only wish to coerce the type _after_ grouping.


The `EXTERN` is also able to refer to the `tags` input type as `VARCHAR`, which is also how a query on a Druid table containing a multi-value dimension would specify the type of the `tags` column. If this is the case you must use `MV_TO_ARRAY` since the multi-stage query engine only supports grouping on multi-value dimensions as arrays. So, they must be coerced first. These arrays must then be coerced back into `VARCHAR` in the `SELECT` part of the statement with `ARRAY_TO_MV`.

```sql
REPLACE INTO "mvd_example_rollup" OVERWRITE ALL
WITH "ext" AS (
  SELECT *
  FROM TABLE(
    EXTERN(
      '{"type":"inline","data":"{\"timestamp\": \"2011-01-12T00:00:00.000Z\", \"label\": \"row1\", \"tags\": [\"t1\",\"t2\",\"t3\"]}\n{\"timestamp\": \"2011-01-13T00:00:00.000Z\", \"label\": \"row2\", \"tags\": [\"t3\",\"t4\",\"t5\"]}\n{\"timestamp\": \"2011-01-14T00:00:00.000Z\", \"label\": \"row3\", \"tags\": [\"t5\",\"t6\",\"t7\"]}\n{\"timestamp\": \"2011-01-14T00:00:00.000Z\", \"label\": \"row4\", \"tags\": []}"}',
      '{"type":"json"}'
    )
  ) EXTEND ("timestamp" VARCHAR, "label" VARCHAR, "tags" VARCHAR)
)
SELECT
  TIME_PARSE("timestamp") AS "__time",
  "label",
  ARRAY_TO_MV(MV_TO_ARRAY("tags")) AS "tags",
  COUNT(*) AS "count"
FROM "ext"
GROUP BY 1, 2, MV_TO_ARRAY("tags")
PARTITIONED BY DAY
```

## Querying multi-value dimensions

### Filtering

All query types, as well as [filtered aggregators](aggregations.md#filtered-aggregator), can filter on multi-value
dimensions. Filters follow these rules on multi-value dimensions:

- Value filters (like "selector", "bound", and "in") match a row if any of the values of a multi-value dimension match
  the filter.
- The Column Comparison filter will match a row if the dimensions have any overlap.
- Value filters that match `null` or `""` (empty string) will match empty cells in a multi-value dimension.
- Logical expression filters behave the same way they do on single-value dimensions: "and" matches a row if all
  underlying filters match that row; "or" matches a row if any underlying filters match that row; "not" matches a row
  if the underlying filter does not match the row.
  
The following example illustrates these rules. This query applies an "or" filter to match row1 and row2 of the dataset above, but not row3:

```sql
SELECT *
FROM "mvd_example_rollup"
WHERE tags = 't1' OR tags = 't3'
```

returns
```json lines
{"__time":"2011-01-12T00:00:00.000Z","label":"row1","tags":"[\"t1\",\"t2\",\"t3\"]","count":1}
{"__time":"2011-01-13T00:00:00.000Z","label":"row2","tags":"[\"t3\",\"t4\",\"t5\"]","count":1}
```

Native queries can also perform filtering that would be considered a "contradiction" in SQL, such as this "and" filter which would match only row1 of the dataset above:

```
{
  "type": "and",
  "fields": [
    {
      "type": "selector",
      "dimension": "tags",
      "value": "t1"
    },
    {
      "type": "selector",
      "dimension": "tags",
      "value": "t3"
    }
  ]
}
```

which returns
```json lines
{"__time":"2011-01-12T00:00:00.000Z","label":"row1","tags":"[\"t1\",\"t2\",\"t3\"]","count":1}
```

Multi-value dimensions also consider an empty row as `null`, consider:
```sql
SELECT *
FROM "mvd_example_rollup"
WHERE tags is null
```

which results in:
```json lines
{"__time":"2011-01-14T00:00:00.000Z","label":"row4","tags":null,"count":1}
```

### Grouping

When grouping on a multi-value dimension with SQL or a native [topN](topnquery.md) or [groupBy](groupbyquery.md) queries, _all_ values 
from matching rows will be used to generate one group per value. This behaves similarly to an implicit SQL `UNNEST`
operation. This means it's possible for a query to return more groups than there are rows. For example, a topN on the
dimension `tags` with filter `"t1" AND "t3"` would match only row1, and generate a result with three groups:
`t1`, `t2`, and `t3`.

If you only need to include values that match your filter, you can use the SQL functions [`MV_FILTER_ONLY`/`MV_FILTER_NONE`](sql-multivalue-string-functions.md),
[filtered virtual column](virtual-columns.md#list-filtered-virtual-column), or [filtered dimensionSpec](dimensionspecs.md#filtered-dimensionspecs). This can also improve performance.

#### Example: SQL grouping query with no filtering
```sql
SELECT label, tags
FROM "mvd_example_rollup"
GROUP BY 1,2
```
results in:
```json lines
{"label":"row1","tags":"t1"}
{"label":"row1","tags":"t2"}
{"label":"row1","tags":"t3"}
{"label":"row2","tags":"t3"}
{"label":"row2","tags":"t4"}
{"label":"row2","tags":"t5"}
{"label":"row3","tags":"t5"}
{"label":"row3","tags":"t6"}
{"label":"row3","tags":"t7"}
{"label":"row4","tags":null}
```

#### Example: SQL grouping query with a filter
```sql
SELECT label, tags
FROM "mvd_example_rollup"
WHERE tags = 't3'
GROUP BY 1,2
```

results:
```json lines
{"label":"row1","tags":"t1"}
{"label":"row1","tags":"t2"}
{"label":"row1","tags":"t3"}
{"label":"row2","tags":"t3"}
{"label":"row2","tags":"t4"}
{"label":"row2","tags":"t5"}
```

#### Example: native GroupBy query with no filtering

See [GroupBy querying](groupbyquery.md) for details.

```json
{
  "queryType": "groupBy",
  "dataSource": "test",
  "intervals": [
    "1970-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z"
  ],
  "granularity": {
    "type": "all"
  },
  "dimensions": [
    {
      "type": "default",
      "dimension": "tags",
      "outputName": "tags"
    }
  ],
  "aggregations": [
    {
      "type": "count",
      "name": "count"
    }
  ]
}
```

This query returns the following result:

```json
[
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 1,
      "tags": "t1"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 1,
      "tags": "t2"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 2,
      "tags": "t3"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 1,
      "tags": "t4"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 2,
      "tags": "t5"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 1,
      "tags": "t6"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 1,
      "tags": "t7"
    }
  }
]
```

Notice that original rows are "exploded" into multiple rows and merged.

#### Example: native GroupBy query with a selector query filter

See [query filters](filters.md) for details of selector query filter.

```json
{
  "queryType": "groupBy",
  "dataSource": "test",
  "intervals": [
    "1970-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z"
  ],
  "filter": {
    "type": "selector",
    "dimension": "tags",
    "value": "t3"
  },
  "granularity": {
    "type": "all"
  },
  "dimensions": [
    {
      "type": "default",
      "dimension": "tags",
      "outputName": "tags"
    }
  ],
  "aggregations": [
    {
      "type": "count",
      "name": "count"
    }
  ]
}
```

This query returns the following result:

```json
[
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 1,
      "tags": "t1"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 1,
      "tags": "t2"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 2,
      "tags": "t3"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 1,
      "tags": "t4"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 1,
      "tags": "t5"
    }
  }
]
```

You might be surprised to see "t1", "t2", "t4" and "t5" included in the results. This is because the query filter is
applied on the row before explosion. For multi-value dimensions, a filter for value "t3" would match row1 and row2,
after which exploding is done. For multi-value dimensions, a query filter matches a row if any individual value inside
the multiple values matches the query filter.

#### Example: native GroupBy query with selector query and dimension filters

To solve the problem above and to get only rows for "t3", use a "filtered dimension spec", as in the query below.

See filtered `dimensionSpecs` in [dimensionSpecs](dimensionspecs.md#filtered-dimensionspecs) for details.

```json
{
  "queryType": "groupBy",
  "dataSource": "test",
  "intervals": [
    "1970-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z"
  ],
  "filter": {
    "type": "selector",
    "dimension": "tags",
    "value": "t3"
  },
  "granularity": {
    "type": "all"
  },
  "dimensions": [
    {
      "type": "listFiltered",
      "delegate": {
        "type": "default",
        "dimension": "tags",
        "outputName": "tags"
      },
      "values": ["t3"]
    }
  ],
  "aggregations": [
    {
      "type": "count",
      "name": "count"
    }
  ]
}
```

This query returns the following result:

```json
[
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 2,
      "tags": "t3"
    }
  }
]
```

Note that, for groupBy queries, you could get similar result with a [having spec](having.md) but using a filtered
`dimensionSpec` is much more efficient because that gets applied at the lowest level in the query processing pipeline.
Having specs are applied at the outermost level of groupBy query processing.

## Disable GroupBy on multi-value columns

You can disable the implicit unnesting behavior for groupBy by setting `groupByEnableMultiValueUnnesting: false` in your 
[query context](query-context.md). In this mode, the groupBy engine will return an error instead of completing the query. This is a safety 
feature for situations where you believe that all dimensions are singly-valued and want the engine to reject any 
multi-valued dimensions that were inadvertently included.

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