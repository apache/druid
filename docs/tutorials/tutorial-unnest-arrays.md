---
id: tutorial-unnest-arrays
sidebar_label: "Unnesting arrays"
title: "Unnest arrays within a column"
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

> If you're looking for information about how to unnest `COMPLEX<json>` columns, see [Nested columns](../querying/nested-columns.md).

This tutorial demonstrates how to use the UNNEST function (SQL) or the unnest datasource (native) to unnest multi-value dimensions, data stored in an array. For example, if you have a column named `dim3` with values like `[a,b]` or `[c,d,f]`, you can unnest this data and use it in a subsequent query or output the data to a new column. This new column would have individual rows that contain single values like `a` and `b`. When doing this, be mindful of the following:

- Unnesting data can dramatically increase the total number of rows. 
- You cannot unnest an array within an array. 

You can use the Druid console  or API to unnest data. To start though, you may want to use the Druid console so that viewing the nested and unnested data is easier. 

## Prerequisites 

You need a Druid cluster, such as the [quickstart](./index.md). The cluster does not need any existing datasources. You'll load a basic one as part of this tutorial.

## Load data with nested values

The data you're ingesting contains a handful of rows that resemble the following:

```
t:2000-01-01, m1:1.0, m2:1.0, dim1:, dim2:[a], dim3:[a,b], dim4[x,y]
```

The focus of this tutorial is on the nested array of values in `dim3`.

You can load this data by running a query for SQL-based ingestion or submitting a JSON-based ingestion spec. The example loads data into a table named `nested_data`:

<!--DOCUSAURUS_CODE_TABS-->

<!--SQL-based ingestion-->

```sql
REPLACE INTO nested_data OVERWRITE ALL
SELECT
  TIME_PARSE("t") as __time,
  dim1,
  dim2,
  dim3,
  dim4,
  dim5,
  m1,
  m2
FROM TABLE(
    EXTERN(
    '{"type":"inline","data":"{\"t\":\"2000-01-01\",\"m1\":\"1.0\",\"m2\":\"1.0\",\"dim1\":\"\",\"dim2\":[\"a\"],\"dim3\":[\"a\",\"b\"],\"dim4\":[\"x\",\"y\"],\"dim5\":[\"a\",\"b\"]},\n{\"t\":\"2000-01-02\",\"m1\":\"2.0\",\"m2\":\"2.0\",\"dim1\":\"10.1\",\"dim2\":[],\"dim3\":[\"c\",\"d\"],\"dim4\":[\"e\",\"f\"],\"dim5\":[\"a\",\"b\",\"c\",\"d\"]},\n{\"t\":\"2001-01-03\",\"m1\":\"6.0\",\"m2\":\"6.0\",\"dim1\":\"abc\",\"dim2\":[\"a\"],\"dim3\":[\"k\",\"l\"]},\n{\"t\":\"2001-01-01\",\"m1\":\"4.0\",\"m2\":\"4.0\",\"dim1\":\"1\",\"dim2\":[\"a\"],\"dim3\":[\"g\",\"h\"]},\n{\"t\":\"2001-01-02\",\"m1\":\"5.0\",\"m2\":\"5.0\",\"dim1\":\"def\",\"dim2\":[\"abc\"],\"dim3\":[\"i\",\"j\"]},\n{\"t\":\"2001-01-03\",\"m1\":\"6.0\",\"m2\":\"6.0\",\"dim1\":\"abc\",\"dim2\":[\"a\"],\"dim3\":[\"k\",\"l\"]},\n{\"t\":\"2001-01-02\",\"m1\":\"5.0\",\"m2\":\"5.0\",\"dim1\":\"def\",\"dim2\":[\"abc\"],\"dim3\":[\"m\",\"n\"]}"}',
    '{"type":"json"}',
    '[{"name":"t","type":"string"},{"name":"dim1","type":"string"},{"name":"dim2","type":"string"},{"name":"dim3","type":"string"},{"name":"dim4","type":"string"},{"name":"m1","type":"float"},{"name":"m2","type":"double"}]'
  )
)
PARTITIONED BY YEAR 
```

<!--Ingestion spec-->

```json
{
  "type": "index_parallel",
  "spec": {
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "inline",
        "data":"{\"t\":\"2000-01-01\",\"m1\":\"1.0\",\"m2\":\"1.0\",\"dim1\":\"\",\"dim2\":[\"a\"],\"dim3\":[\"a\",\"b\"],\"dim4\":[\"x\",\"y\"],\"dim5\":[\"a\",\"b\"]},\n{\"t\":\"2000-01-02\",\"m1\":\"2.0\",\"m2\":\"2.0\",\"dim1\":\"10.1\",\"dim2\":[],\"dim3\":[\"c\",\"d\"],\"dim4\":[\"e\",\"f\"],\"dim5\":[\"a\",\"b\",\"c\",\"d\"]},\n{\"t\":\"2001-01-03\",\"m1\":\"6.0\",\"m2\":\"6.0\",\"dim1\":\"abc\",\"dim2\":[\"a\"],\"dim3\":[\"k\",\"l\"]},\n{\"t\":\"2001-01-01\",\"m1\":\"4.0\",\"m2\":\"4.0\",\"dim1\":\"1\",\"dim2\":[\"a\"],\"dim3\":[\"g\",\"h\"]},\n{\"t\":\"2001-01-02\",\"m1\":\"5.0\",\"m2\":\"5.0\",\"dim1\":\"def\",\"dim2\":[\"abc\"],\"dim3\":[\"i\",\"j\"]},\n{\"t\":\"2001-01-03\",\"m1\":\"6.0\",\"m2\":\"6.0\",\"dim1\":\"abc\",\"dim2\":[\"a\"],\"dim3\":[\"k\",\"l\"]},\n{\"t\":\"2001-01-02\",\"m1\":\"5.0\",\"m2\":\"5.0\",\"dim1\":\"def\",\"dim2\":[\"abc\"],\"dim3\":[\"m\",\"n\"]}"
      },
      "inputFormat": {
        "type": "json"
      }
    },
    "tuningConfig": {
      "type": "index_parallel",
      "partitionsSpec": {
        "type": "dynamic"
      }
    },
    "dataSchema": {
      "dataSource": "nested_data",
      "granularitySpec": {
        "type": "uniform",
        "queryGranularity": "NONE",
        "rollup": false,
        "segmentGranularity": "YEAR"
      },
      "timestampSpec": {
        "column": "t",
        "format": "auto"
      },
      "dimensionsSpec": {
        "dimensions": [
          "dim1",
          "dim2",
          "dim3",
          "dim4",
          "dim5"
        ]
      },
      "metricsSpec": [
        {
          "name": "m1",
          "type": "floatSum",
          "fieldName": "m1"
        },
        {
          "name": "m2",
          "type": "doubleSum",
          "fieldName": "m2"
        }
      ]
    }
  }
}
```
<!--END_DOCUSAURUS_CODE_TABS-->

## View the data

Now that the data is loaded, run the following query:

```sql
SELECT * FROM nested_data
```

In the results, notice that the column named `dim3` has nested values like `["a","b"]`.  The example queries that follow unnest `dim3`  and run queries against the unnested records. Depending on the type of queries you write, see either [Unnest using SQL queries](#unnest-using-sql-queries) or [Unnest using native queries](#unnest-using-native-queries).

## Unnest using SQL queries

The following is the general syntax for UNNEST:

```sql
SELECT target_column FROM datasource, UNNEST(source) AS table_alias_name(column_alias_name)
```

For more information about the syntax, see [UNNEST](../querying/sql.md#unnest).

### Unnest inline array

The following query returns a column that unnests the array `[1,2,3]` that is provided inline: 

```sql
SELECT * FROM UNNEST(ARRAY[1,2,3])
```

If you unnest that same inline array  while using a table as the datasource, Druid treats this as a JOIN between a left datasource and a constant datasource. For example:

```sql
SELECT longs FROM nested_data, UNNEST(ARRAY[1,2,3]) as UNNESTED (longs)"
```

### Unnest a single column in a table

The following query returns a column called `d3` from the table `nested_data`. `d3` contains the unnested values from the source column `dim3`:

```sql
SELECT d3 FROM "nested_data", UNNEST(MV_TO_ARRAY(dim3)) as UNNESTED (d3) 
```

Notice the `MV_TO_ARRAY` helper function, which converts the multi-value records in `dim3` to arrays. It is required since `dim3` is a multi-value string dimension. 

If the column you are unnesting is not a string dimension, then you do not need to use the MV_TO_ARRAY helper function.

### Unnest a virtual column

You can unnest into a virtual column (multiple columns treated as one). The following query returns the two source columns and a third virtual column containing the unnested data:

```sql
SELECT dim4,dim5,d45 FROM nested_data, UNNEST(ARRAY[dim4,dim5]) as UNNESTED (d45)
```

This virtual column is the product of the two source columns. Notice how the total number of rows has grown. The table `nested_data` had only 7 rows originally.

Another way to unnest a virtual column is to concatenate them with ARRAY_CONCAT:

```sql
select d45 from nested_data, UNNEST(ARRAY_CONCAT(dim4,dim5)) AS UNNESTED (d45)
```

Decide which method to use based on what your goals are. 

### Unnest a column from a subset of a table

The following query uses only 3 columns from the `nested_data` table as the datasource. From that subset, it unnests the column `dim3` into `d3` and returns `d3`.

```sql
SELECT d3 FROM (select dim1, dim2, dim3 from "nested_data"), UNNEST(MV_TO_ARRAY(dim3)) as UNNESTED (d3)
```

### Unnest with a filter

You can specify which rows to unnest by including a filter in your query. The following query:
* Unnests the records in `dim3` into `d3` 
* Filters based on `dim2`
* Returns the records for  the unnested `d3` that have a `dim2` record that matches the filter

```sql
SELECT d3 FROM (select * from nested_data WHERE dim2 IN ('abc')), UNNEST(MV_TO_ARRAY(dim3)) as UNNESTED (d3)
```

### Unnest and then GROUP BY

The following query unnests `dim3` and then performs a GROUP BY on the output `d3`.

```sql
SELECT d3 FROM nested_data, UNNEST(MV_TO_ARRAY(dim3)) as UNNESTED (d3) GROUP BY d3 
```

You can further transform your results by  including clauses like `ORDER BY d3 DESC` or LIMIT.

## Unnest using native queries

The following section shows examples of how you can use the unnest datasource in queries. They all use the `nested_data` table you created earlier in the tutorial.

### Scan query 

The following native Scan query returns the rows of the datasource and unnests the values in the `dim3` column by using the `unnest` datasource type:

<details><summary>Show the query</summary>

```json
{
  "queryType": "scan",
  "dataSource": {
    "type": "unnest",
    "base": {
      "type": "table",
      "name": "nested_data"
    },
    "column": "dim3",
    "outputName": "unnest-dim3",
    "allowList": []
  },
  "intervals": {
    "type": "intervals",
    "intervals": [
      "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
    ]
  },
  "limit": 100,
  "columns": [
    "__time",
    "dim1",
    "dim2",
    "dim3",
    "m1",
    "m2",
    "unnest-dim3"
  ],
  "legacy": false,
  "granularity": {
    "type": "all"
  },
  "context": {
    "debug": true,
    "useCache": false
  }
}
```
</details>

In the results, notice that there are more rows than before and an additional column named `unnest-dim3`. The values of `unnest-dim3` are the same as the `dim3` column except the nested values are no longer nested and are each a separate record.

With the `dataSource.allowList` parameter, you can unnest a subset of a column. Set the value of `allowList` to `["a","b"]` and run the query again. Only a subset of rows are returned based on the values you allowed.

You can also implement filters. For example, you can add the following to the Scan query to filter results to only rows that have the values `"a"` or `"abc"` in `"dim2"`:

```json
  "filter": {
    "type": "in",
    "dimension": "dim2",
    "values": [
      "a",
      "abc",
      ]
  },
```

### groupBy query

The following query returns an unnested version of the column `dim3` as the column `unnest-dim3` sorted in descending order.

<details><summary>Show the query</summary>

```json
{
  "queryType": "groupBy",
  "dataSource": {
    "type": "unnest",
    "base": "nested_data",
    "column": "dim3",
    "outputName": "unnest-dim3",
    "allowList": []
  },
  "intervals": ["-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"],
  "granularity": "all",
  "dimensions": [
    "unnest-dim3"
  ],
  "limitSpec": {
    "type": "default",
    "columns": [
      {
        "dimension": "unnest-dim3",
        "direction": "descending"
      }
    ],
    "limit": 1001
  },
  "context": {
    "debug": true
  }
}
```

</details>

### topN query

The example topN query unnests `dim3` into the column `unnest-dim3`. The query uses the unnested column as the dimension for the topN query. The results are outputted to a column named `topN-unnest-d3` and are sorted numerically in ascending order based on the column `a0`, an aggregate value representing the minimum of `m1`.

<details><summary>Show the query</summary>

```json
{
  "queryType": "topN",
  "dataSource": {
    "type": "unnest",
    "base": {
      "type": "table",
      "name": "nested_data"
    },
    "column": "dim3",
    "outputName": "unnest-dim3",
    "allowList": null
  },
  "dimension": {
    "type": "default",
    "dimension": "unnest-dim3",
    "outputName": "topN-unnest-d3",
    "outputType": "STRING"
  },
  "metric": {
    "type": "inverted",
    "metric": {
      "type": "numeric",
      "metric": "a0"
    }
  },
  "threshold": 3,
  "intervals": {
    "type": "intervals",
    "intervals": [
      "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
    ]
  },
  "granularity": {
    "type": "all"
  },
  "aggregations": [
    {
      "type": "floatMin",
      "name": "a0",
      "fieldName": "m1"
    }
  ],
  "context": {
    "debug": true
  }
}
```

</details>

### Unnest with a JOIN query

This query joins the `nested_data` table with itself and outputs the unnested data into a new column called `unnest-dim3`.

<details><summary>Show the query</summary>

```json
{
  "queryType": "scan",
  "dataSource": {
    "type": "unnest",
    "base": {
        "type": "join",
        "left": {
          "type": "table",
          "name": "nested_data"
        },
        "right": {
          "type": "query",
          "query": {
            "queryType": "scan",
            "dataSource": {
              "type": "table",
              "name": "nested_data"
            },
            "intervals": {
              "type": "intervals",
              "intervals": [
                "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
              ]
            },
            "virtualColumns": [
              {
                "type": "expression",
                "name": "v0",
                "expression": "\"m2\"",
                "outputType": "FLOAT"
              }
            ],
            "resultFormat": "compactedList",
            "columns": [
              "__time",
              "dim1",
              "dim2",
              "dim3",
              "m1",
              "m2",
              "v0"
            ],
            "legacy": false,
            "context": {
              "sqlOuterLimit": 1001,
              "useNativeQueryExplain": true
            },
            "granularity": {
              "type": "all"
            }
          }
        },
        "rightPrefix": "j0.",
        "condition": "(\"m1\" == \"j0.v0\")",
        "joinType": "INNER"
      },
    "column": "dim3",
    "outputName": "unnest-dim3",
    "allowList": []
    },
  "intervals": {
    "type": "intervals",
    "intervals": [
      "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
    ]
  },
  "resultFormat": "compactedList",
  "limit": 1001,
  "columns": [
    "__time",
    "dim1",
    "dim2",
    "dim3",
    "j0.__time",
    "j0.dim1",
    "j0.dim2",
    "j0.dim3",
    "j0.m1",
    "j0.m2",
    "m1",
    "m2",
    "unnest-dim3"
  ],
  "legacy": false,
  "context": {
    "sqlOuterLimit": 1001,
    "useNativeQueryExplain": true
  },
  "granularity": {
    "type": "all"
  }
}
```

</details>

### Unnest multiple columns 

You can use a single unnest datasource to unnest multiple columns. Be careful when doing this though because it can lead to a very large number of new rows.

### Unnest a virtual column

The `unnest` datasource supports unnesting virtual columns, which is a queryable composite column that can draw data from multiple source columns.

The following query returns the columns `unnest-v0` and `m1`. The `unnest-v0` column is the unnested version of the virtual column `v0`, which contains an array of the `dim2` and `dim3` columns.

<details><summary>Show the query</summary>

```json
{
  "queryType": "scan",
  "dataSource":{
    "type": "unnest",
    "base": {
      "type": "table",
      "name": "nested_data"
    },
    "column": "v0",
    "outputName": "unnest-v0"
  }
  "intervals": {
    "type": "intervals",
    "intervals": [
      "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
    ]
  },
  "virtualColumns": [
    {
      "type": "expression",
      "name": "v0",
      "expression": "array(\"dim4\",\"dim5\")",
      "outputType": "ARRAY<STRING>"
    }
  ],
  "resultFormat": "compactedList",
  "limit": 1001,
  "columns": [
    "unnest-v0",
    "m1"
  ],
  "legacy": false,
  "context": {
    "populateCache": false,
    "queryId": "d273facb-08cc-4de7-ac0b-d0b82173e531",
    "sqlOuterLimit": 1001,
    "sqlQueryId": "d273facb-08cc-4de7-ac0b-d0b82173e531",
    "useCache": false,
    "useNativeQueryExplain": true
  },
  "granularity": {
    "type": "all"
  }
}
```

</details>

#### Unnest two virtual columns

The following query performs two unnests. It unnests `dim4` into a column named `unnest-dim4`. It also performs an unnest on `dim5` and outputs the results to `unnest-dim5`. You can then treat the combination of `unnest-dim3` and `unnest-dim2` as Cartesian products.

When you run the query, pay special attention to how the total number of rows has grown drastically. The source data has 2 rows. The unnested data has 12 rows, (2 x 2) + (2 x 4).

<details><summary>Show the query</summary>

```json
{
  "queryType": "scan",
  "dataSource": {
    "type": "unnest",
    "base": {
      "type": "unnest",
      "base": {
        "type": "table",
        "name": "nested_data"
      },
      "column": "dim4",
      "outputName": "unnest-dim4",
      "allowList": []
    },
    "column": "dim5",
    "outputName": "unnest-dim5",
    "allowList": []
  },
  "intervals": {
    "type": "intervals",
    "intervals": [
      "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
    ]
  },
  "limit": 1000,
  "columns": [
    "__time",
    "dim1",
    "dim2",
    "dim3",
    "dim4",
    "dim5",
    "m1",
    "m2",
    "unnest-dim4",
    "unnest-dim5"
  ],
  "legacy": false,
  "granularity": {
    "type": "all"
  },
  "context": {
    "debug": true,
    "useCache": false
  }
}
```

</details>

### Unnest inline datasource

You can also use the `unnest` datasource to unnest an inline datasource. The following query takes the row `[1,2,3]` in the column `inline_data` that is provided inline within the query and returns it as unnested values in the `output` column:

<details><summary>Show the query</summary>

```json
{
  "queryType": "scan",
  "dataSource": {
    "type": "unnest",
    "base": {
      "type": "inline",
      "columnNames": [
        "inline_data"
      ],
      "columnTypes": [
        "long_array"
      ],
      "rows": [
        [
          [1,2,3]
        ]
      ]
    },
    "column": "inline_data",
    "outputName": "output",
    "allowList": []
  },
  "intervals": {
    "type": "intervals",
    "intervals": [
      "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
    ]
  },
  "resultFormat": "compactedList",
  "limit": 1001,
  "columns": [
    "inline_data",
    "output"
  ],
  "legacy": false,
  "granularity": {
    "type": "all"
  }
}
```

</details>


## Learn more

For more information, see the following:
-  [UNNEST SQL function](../querying/sql.md#unnest)
- [`unnest` in Datasources](../querying/datasource.md#unnest)
