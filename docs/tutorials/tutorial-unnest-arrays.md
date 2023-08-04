---
id: tutorial-unnest-arrays
sidebar_label: "Unnesting arrays"
title: "Unnest arrays within a column"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

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

:::info
 If you're looking for information about how to unnest `COMPLEX<json>` columns, see [Nested columns](../querying/nested-columns.md).
:::

:::info
 The unnest datasource and UNNEST SQL function are [experimental](../development/experimental.md). Their API and behavior are subject
 to change in future releases. It is not recommended to use this feature in production at this time.
:::

This tutorial demonstrates how to use the unnest datasource to unnest a column that has data stored in arrays. For example, if you have a column named `dim3` with values like `[a,b]` or `[c,d,f]`, the unnest datasource can output the data to a new column with individual rows that contain single values like `a` and `b`. When doing this, be mindful of the following:

- Unnesting data can dramatically increase the total number of rows.
- You cannot unnest an array within an array.

You can use the Druid console  or API to unnest data. To start though, you may want to use the Druid console so that viewing the nested and unnested data is easier.

## Prerequisites

You need a Druid cluster, such as the [quickstart](./index.md). The cluster does not need any existing datasources. You'll load a basic one as part of this tutorial.

## Load data with nested values

The data you're ingesting contains a handful of rows that resemble the following:

```
t:2000-01-01, m1:1.0, m2:1.0, dim1:, dim2:[a], dim3:[a,b], dim4:[x,y], dim5:[a,b]
```

The focus of this tutorial is on the nested array of values in `dim3`.

You can load this data by running a query for SQL-based ingestion or submitting a JSON-based ingestion spec. The example loads data into a table named `nested_data`:

<Tabs>

<TabItem value="1" label="SQL-based ingestion">


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
    '[{"name":"t","type":"string"},{"name":"dim1","type":"string"},{"name":"dim2","type":"string"},{"name":"dim3","type":"string"},{"name":"dim4","type":"string"},{"name":"dim5","type":"string"},{"name":"m1","type":"float"},{"name":"m2","type":"double"}]'
  )
)
PARTITIONED BY YEAR
```

</TabItem>
<TabItem value="2" label="Ingestion spec">


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
</TabItem>
</Tabs>

## View the data

Now that the data is loaded, run the following query:

```sql
SELECT * FROM nested_data
```

In the results, notice that the column named `dim3` has nested values like `["a","b"]`.  The example queries that follow unnest `dim3`  and run queries against the unnested records. Depending on the type of queries you write, see either [Unnest using SQL queries](#unnest-using-sql-queries) or [Unnest using native queries](#unnest-using-native-queries).

## Unnest using SQL queries

The following is the general syntax for UNNEST:

```sql
SELECT column_alias_name FROM datasource, UNNEST(source_expression) AS table_alias_name(column_alias_name)
```

In addition, you must supply the following context parameter:

```json
"enableUnnest": "true"
```

For more information about the syntax, see [UNNEST](../querying/sql.md#unnest).

### Unnest a single source expression in a datasource

The following query returns a column called `d3` from the table `nested_data`. `d3` contains the unnested values from the source column `dim3`:

```sql
SELECT d3 FROM "nested_data", UNNEST(MV_TO_ARRAY(dim3)) AS example_table(d3)
```

Notice the MV_TO_ARRAY helper function, which converts the multi-value records in `dim3` to arrays. It is required since `dim3` is a multi-value string dimension.

If the column you are unnesting is not a string dimension, then you do not need to use the MV_TO_ARRAY helper function.

### Unnest a virtual column

You can unnest into a virtual column (multiple columns treated as one). The following query returns the two source columns and a third virtual column containing the unnested data:

```sql
SELECT dim4,dim5,d45 FROM nested_data, UNNEST(ARRAY[dim4,dim5]) AS example_table(d45)
```

The virtual column `d45` is the product of the two source columns. Notice how the total number of rows has grown. The table `nested_data` had only seven rows originally.

Another way to unnest a virtual column is to concatenate them with ARRAY_CONCAT:

```sql
SELECT dim4,dim5,d45 FROM nested_data, UNNEST(ARRAY_CONCAT(dim4,dim5)) AS example_table(d45)
```

Decide which method to use based on what your goals are.

### Unnest multiple source expressions

You can include multiple UNNEST clauses in a single query. Each `UNNEST` clause needs the following:

```sql
UNNEST(source_expression) AS table_alias_name(column_alias_name)
```

The `table_alias_name` and `column_alias_name` for each UNNEST clause should be unique.

The example query returns the following from  the `nested_data` datasource:

- the source columns `dim3`, `dim4`, and `dim5`
- an unnested version of `dim3` aliased to `d3`
- an unnested virtual column composed of `dim4` and `dim5` aliased to `d45`

```sql
SELECT dim3,dim4,dim5,d3,d45 FROM "nested_data", UNNEST(MV_TO_ARRAY("dim3")) AS foo1(d3), UNNEST(ARRAY[dim4,dim5]) AS foo2(d45)
```


### Unnest a column from a subset of a table

The following query uses only three columns from the `nested_data` table as the datasource. From that subset, it unnests the column `dim3` into `d3` and returns `d3`.

```sql
SELECT d3 FROM (SELECT dim1, dim2, dim3 FROM "nested_data"), UNNEST(MV_TO_ARRAY(dim3)) AS example_table(d3)
```

### Unnest with a filter

You can specify which rows to unnest by including a filter in your query. The following query:

* Filters the source expression based on `dim2`
* Unnests the records in `dim3` into `d3`
* Returns the records for  the unnested `d3` that have a `dim2` record that matches the filter

```sql
SELECT d3 FROM (SELECT * FROM nested_data WHERE dim2 IN ('abc')), UNNEST(MV_TO_ARRAY(dim3)) AS example_table(d3)
```

You can also filter the results of an UNNEST clause. The following example unnests the inline array `[1,2,3]` but only returns the rows that match the filter:

```sql
SELECT * FROM UNNEST(ARRAY[1,2,3]) AS example_table(d1) WHERE d1 IN ('1','2')
```

This means that you can run a query like the following where Druid only return rows that meet the following conditions:

- The unnested values of `dim3` (aliased to `d3`) matches `IN ('b', 'd')`
- The value of `m1` is less than 2.

```sql
SELECT * FROM nested_data, UNNEST(MV_TO_ARRAY("dim3")) AS foo(d3) WHERE d3 IN ('b', 'd') and m1 < 2
```

The query only returns a single row since only one row meets the conditions. You can see the results change if you modify the filter.

### Unnest and then GROUP BY

The following query unnests `dim3` and then performs a GROUP BY on the output `d3`.

```sql
SELECT d3 FROM nested_data, UNNEST(MV_TO_ARRAY(dim3)) AS example_table(d3) GROUP BY d3
```

You can further transform your results by  including clauses like `ORDER BY d3 DESC` or LIMIT.

## Unnest using native queries

The following section shows examples of how you can use the unnest datasource in queries. They all use the `nested_data` table you created earlier in the tutorial.

You can use a single unnest datasource to unnest multiple columns. Be careful when doing this though because it can lead to a very large number of new rows.

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
    "virtualColumn": {
      "type": "expression",
      "name": "unnest-dim3",
      "expression": "\"dim3\""
    }
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

You can implement filters. For example, you can add the following to the Scan query to filter results to only rows that have the values `"a"` or `"abc"` in `"dim2"`:

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
    "virtualColumn": {
      "type": "expression",
      "name": "unnest-dim3",
      "expression": "\"dim3\""
    }
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
    "virtualColumn": {
      "type": "expression",
      "name": "unnest-dim3",
      "expression": "\"dim3\""
    },
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
    "virtualColumn": {
      "type": "expression",
      "name": "unnest-dim3",
      "expression": "\"dim3\""
    }
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

### Unnest a virtual column

The `unnest` datasource supports unnesting virtual columns, which is a queryable composite column that can draw data from multiple source columns.

The following query returns the columns `dim45` and `m1`. The `dim45` column is the unnested version of a virtual column that contains an array of the `dim4` and `dim5` columns.

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
    "virtualColumn": {
      "type": "expression",
      "name": "dim45",
      "expression": "array_concat(\"dim4\",\"dim5\")",
      "outputType": "ARRAY<STRING>"
    },
  }
  "intervals": {
    "type": "intervals",
    "intervals": [
      "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
    ]
  },
  "resultFormat": "compactedList",
  "limit": 1001,
  "columns": [
    "dim45",
    "m1"
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

### Unnest a column and a virtual column

The following Scan query unnests the column `dim3` into `d3` and a virtual column composed of `dim4` and `dim5` into the column `d45`. It then returns those source columns and their unnested variants.

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

"virtualColumn": {
        "type": "expression",
        "name": "d3",
        "expression": "\"dim3\"",
        "outputType": "STRING"
      },
    },
    "virtualColumn": {
      "type": "expression",
      "name": "d45",
      "expression": "array(\"dim4\",\"dim5\")",
      "outputType": "ARRAY<STRING>"
    },
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
    "dim3",
    "d3",
    "dim4",
    "dim5",
    "d45"
  ],
  "legacy": false,
  "context": {
    "enableUnnest": "true",
    "queryId": "2618b9ce-6c0d-414e-b88d-16fb59b9c481",
    "sqlOuterLimit": 1001,
    "sqlQueryId": "2618b9ce-6c0d-414e-b88d-16fb59b9c481",
    "useNativeQueryExplain": true
  },
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
