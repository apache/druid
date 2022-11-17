---
id: tutorial-unnest-datasource
sidebar_label: "Using the unnest datasource"
title: "Tutorial: Unnest data in a column"
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

This tutorial demonstrates how to use the unnest datasource to unnest a column that has data stored in arrays. For example, if you have a column named `dim3` with values like `[a,b]` or `[c,d,f]`, the unnest datasource can output the data to a new column with individual rows that contain single values like `a` and `b`. When doing this though, be mindful of the following: 

- Unnesting data can dramatically increase the total number of rows. 
- You cannot unnest an array within an array. 

You can use the Druid console  or API to unnest data. To start though, you may want to use the Druid console so that viewing the nested and unnested data is easier. 

## Prerequisites 

You need a Druid cluster, such as the [micro-quickstart](./index.md). The cluster does not need any existing datasources. You'll load a basic one as part of this tutorial.

## Load data with nested values

You can load this data by running a query for SQL-based ingestion or submitting a JSON-based ingestion spec. The example loads data into a table named `nested_data`

<!--DOCUSAURUS_CODE_TABS-->

<!--SQL-based ingestion-->

```sql
REPLACE INTO nested_data OVERWRITE ALL
SELECT
  TIME_PARSE("t") as __time,
  dim1,
  dim2,
  dim3,
  m1,
  m2
FROM TABLE(
    EXTERN(
    '{"type":"inline","data":"{\"t\":\"2000-01-01\",\"m1\":\"1.0\",\"m2\":\"1.0\",\"dim1\":\"\",\"dim2\":[\"a\"],\"dim3\":[\"a\",\"b\"]},\n{\"t\":\"2000-01-02\",\"m1\":\"2.0\",\"m2\":\"2.0\",\"dim1\":\"10.1\",\"dim2\":[],\"dim3\":[\"c\",\"d\"]},\n{\"t\":\"2000-01-03\",\"m1\":\"3.0\",\"m2\":\"3.0\",\"dim1\":\"2\",\"dim2\":[\"\"],\"dim3\":[\"e\",\"f\"]},\n{\"t\":\"2001-01-01\",\"m1\":\"4.0\",\"m2\":\"4.0\",\"dim1\":\"1\",\"dim2\":[\"a\"],\"dim3\":[\"g\",\"h\"]},\n{\"t\":\"2001-01-02\",\"m1\":\"5.0\",\"m2\":\"5.0\",\"dim1\":\"def\",\"dim2\":[\"abc\"],\"dim3\":[\"i\",\"j\"]},\n{\"t\":\"2001-01-03\",\"m1\":\"6.0\",\"m2\":\"6.0\",\"dim1\":\"abc\",\"dim2\":[\"a\"],\"dim3\":[\"k\",\"l\"]},\n{\"t\":\"2001-01-02\",\"m1\":\"5.0\",\"m2\":\"5.0\",\"dim1\":\"def\",\"dim2\":[\"abc\"],\"dim3\":[\"m\",\"n\"]}"}',
    '{"type":"json"}',
    '[{"name":"t","type":"string"},{"name":"dim1","type":"string"},{"name":"dim2","type":"string"},{"name":"dim3","type":"string"},{"name":"m1","type":"float"},{"name":"m2","type":"double"}]'
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
        "data": "{\"t\":\"2000-01-01\",\"m1\":\"1.0\",\"m2\":\"1.0\",\"dim1\":\"\",\"dim2\":[\"a\"],\"dim3\":[\"a\",\"b\"]},\n{\"t\":\"2000-01-02\",\"m1\":\"2.0\",\"m2\":\"2.0\",\"dim1\":\"10.1\",\"dim2\":[],\"dim3\":[\"c\",\"d\"]},\n{\"t\":\"2000-01-03\",\"m1\":\"3.0\",\"m2\":\"3.0\",\"dim1\":\"2\",\"dim2\":[\"\"],\"dim3\":[\"e\",\"f\"]},\n{\"t\":\"2001-01-01\",\"m1\":\"4.0\",\"m2\":\"4.0\",\"dim1\":\"1\",\"dim2\":[\"a\"],\"dim3\":[\"g\",\"h\"]},\n{\"t\":\"2001-01-02\",\"m1\":\"5.0\",\"m2\":\"5.0\",\"dim1\":\"def\",\"dim2\":[\"abc\"],\"dim3\":[\"i\",\"j\"]},\n{\"t\":\"2001-01-03\",\"m1\":\"6.0\",\"m2\":\"6.0\",\"dim1\":\"abc\",\"dim2\":[\"a\"],\"dim3\":[\"k\",\"l\"]},\n{\"t\":\"2001-01-02\",\"m1\":\"5.0\",\"m2\":\"5.0\",\"dim1\":\"def\",\"dim2\":[\"abc\"],\"dim3\":[\"m\",\"n\"]}"
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
          "dim3"
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
select * FROM nested_data
```

In the results, notice that the column named `dim3` has nested values like `["a","b"]`.  The example queries that follow unnest `dim3`  and run queries, such as scan. 

## Unnest a single column

The following section shows examples of how you can use the unnest datasource in queries. They all use the `nested_data` table you created earlier in the tutorial.

### Scan query 

The following native Scan query  also unnests the values in the `dim3` column by using the `unnest` datasource type:

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
    "outputName": "unnest-dim3"
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

In the results, you'll notice that there are now more rows than before and an additional column named `unnest-dim3`. The values of `unnest-dim3` are the same as the `dim3` column except the nested values have are no longer nested and are each a separate record. 

### Group by query

The following query returns an unnested version of the column `dim3` as the column `unnest-dim3`, which is the `groupBy` dimension for the query too. The results are sorted in descending order. 

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

The example TopN query does the following unnests `dim3` into the column `unnest-dim3` and sorts the records numerically in ascending order based on the column `a0`.

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
    "dimension": "dim2",
    "outputName": "d0",
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

## Unnest multiple columns 

You can use a single unnest datasource to unnest multiple columns. Be careful when doing this though because it can lead to a very large number of new rows.

### Load data with 2 columns of nested values

Ingest this new data into a table called `nested_data2`:

<!--DOCUSAURUS_CODE_TABS-->

<!--SQL-based ingestion-->

```sql
REPLACE INTO "nested_data2" OVERWRITE ALL
SELECT
  TIME_PARSE("t") as __time,
  "dim1",
  "dim2",
  "dim3",
  "m1",
  "m2"
FROM TABLE(
  EXTERN(
    '{"type":"inline","data":"{\"t\":\"2000-01-01\",\"m1\":\"1.0\",\"m2\":\"1.0\",\"dim1\":\"\",\"dim2\":[\"x\",\"y\"],\"dim3\":[\"a\",\"b\"]},\n{\"t\":\"2000-01-02\",\"m1\":\"2.0\",\"m2\":\"2.0\",\"dim1\":\"10.1\",\"dim2\":[\"e\",\"f\"],\"dim3\":[\"a\",\"b\",\"c\",\"d\"]}"}',
    '{"type":"json"}',
    '[{"name":"t","type":"string"},{"name":"dim1","type":"string"},{"name":"dim2","type":"string"},{"name":"dim3","type":"string"},{"name":"m1","type":"float"},{"name":"m2","type":"double"}]'
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
        "data": "{\"t\":\"2000-01-01\",\"m1\":\"1.0\",\"m2\":\"1.0\",\"dim1\":\"\",\"dim2\":[\"x\",\"y\"],\"dim3\":[\"a\",\"b\"]},\n{\"t\":\"2000-01-02\",\"m1\":\"2.0\",\"m2\":\"2.0\",\"dim1\":\"10.1\",\"dim2\":[\"e\",\"f\"],\"dim3\":[\"a\",\"b\",\"c\",\"d\"]}"
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
      "dataSource": "nested_data2",
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
          "dim3"
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


### Unnest `nested_data2`

The following query performs two unnests. It unnests `dim3` into a column named `unnest-dim3`. It also performs an unnest on `dim2` and outputs the results to `unnest-dim2`. You can then treat the combination of `unnest-dim3` and `unnest-dim2` as Cartesian products.

When you run the query, pay special attention to how the total number of rows has grown drastically. The source data has 2 rows. The unnested data has 12, (2 x 2 ) + (2 x 4).

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
        "name": "nested_data2"
      },
      "column": "dim3",
      "outputName": "unnest-dim3",
      "allowList": []
    },
    "column": "dim2",
    "outputName": "unnest-dim2",
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
    "m1",
    "m2",
    "unnest-dim3",
    "unnest-dim2"
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


## Learn more

For more information about `unnest` and other datasources, see [Datasources](../querying/datasource.md#unnest).