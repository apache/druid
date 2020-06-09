---
id: tutorial-transform-spec
title: "Tutorial: Transforming input data"
sidebar_label: "Transforming input data"
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


This tutorial will demonstrate how to use transform specs to filter and transform input data during ingestion.

For this tutorial, we'll assume you've already downloaded Apache Druid as described in
the [single-machine quickstart](index.html) and have it running on your local machine.

It will also be helpful to have finished [Tutorial: Loading a file](../tutorials/tutorial-batch.md) and [Tutorial: Querying data](../tutorials/tutorial-query.md).

## Sample data

We've included sample data for this tutorial at `quickstart/tutorial/transform-data.json`, reproduced here for convenience:

```json
{"timestamp":"2018-01-01T07:01:35Z","animal":"octopus",  "location":1, "number":100}
{"timestamp":"2018-01-01T05:01:35Z","animal":"mongoose", "location":2,"number":200}
{"timestamp":"2018-01-01T06:01:35Z","animal":"snake", "location":3, "number":300}
{"timestamp":"2018-01-01T01:01:35Z","animal":"lion", "location":4, "number":300}
```

## Load data with transform specs

We will ingest the sample data using the following spec, which demonstrates the use of transform specs:

```json
{
  "type" : "index_parallel",
  "spec" : {
    "dataSchema" : {
      "dataSource" : "transform-tutorial",
      "timestampSpec": {
        "column": "timestamp",
        "format": "iso"
      },
      "dimensionsSpec" : {
        "dimensions" : [
          "animal",
          { "name": "location", "type": "long" }
        ]
      },
      "metricsSpec" : [
        { "type" : "count", "name" : "count" },
        { "type" : "longSum", "name" : "number", "fieldName" : "number" },
        { "type" : "longSum", "name" : "triple-number", "fieldName" : "triple-number" }
      ],
      "granularitySpec" : {
        "type" : "uniform",
        "segmentGranularity" : "week",
        "queryGranularity" : "minute",
        "intervals" : ["2018-01-01/2018-01-03"],
        "rollup" : true
      },
      "transformSpec": {
        "transforms": [
          {
            "type": "expression",
            "name": "animal",
            "expression": "concat('super-', animal)"
          },
          {
            "type": "expression",
            "name": "triple-number",
            "expression": "number * 3"
          }
        ],
        "filter": {
          "type":"or",
          "fields": [
            { "type": "selector", "dimension": "animal", "value": "super-mongoose" },
            { "type": "selector", "dimension": "triple-number", "value": "300" },
            { "type": "selector", "dimension": "location", "value": "3" }
          ]
        }
      }
    },
    "ioConfig" : {
      "type" : "index_parallel",
      "inputSource" : {
        "type" : "local",
        "baseDir" : "quickstart/tutorial",
        "filter" : "transform-data.json"
      },
      "inputFormat" : {
        "type" :"json"
      },
      "appendToExisting" : false
    },
    "tuningConfig" : {
      "type" : "index_parallel",
      "maxRowsPerSegment" : 5000000,
      "maxRowsInMemory" : 25000
    }
  }
}
```

In the transform spec, we have two expression transforms:
* `super-animal`: prepends "super-" to the values in the `animal` column. This will override the `animal` column with the transformed version, since the transform's name is `animal`.
* `triple-number`: multiplies the `number` column by 3. This will create a new `triple-number` column. Note that we are ingesting both the original and the transformed column.

Additionally, we have an OR filter with three clauses:
* `super-animal` values that match "super-mongoose"
* `triple-number` values that match 300
* `location` values that match 3

This filter selects the first 3 rows, and it will exclude the final "lion" row in the input data. Note that the filter is applied after the transformation.

Let's submit this task now, which has been included at `quickstart/tutorial/transform-index.json`:

```bash
bin/post-index-task --file quickstart/tutorial/transform-index.json --url http://localhost:8081
```

## Query the transformed data

Let's run `bin/dsql` and issue a `select * from "transform-tutorial";` query to see what was ingested:

```bash
dsql> select * from "transform-tutorial";
┌──────────────────────────┬────────────────┬───────┬──────────┬────────┬───────────────┐
│ __time                   │ animal         │ count │ location │ number │ triple-number │
├──────────────────────────┼────────────────┼───────┼──────────┼────────┼───────────────┤
│ 2018-01-01T05:01:00.000Z │ super-mongoose │     1 │        2 │    200 │           600 │
│ 2018-01-01T06:01:00.000Z │ super-snake    │     1 │        3 │    300 │           900 │
│ 2018-01-01T07:01:00.000Z │ super-octopus  │     1 │        1 │    100 │           300 │
└──────────────────────────┴────────────────┴───────┴──────────┴────────┴───────────────┘
Retrieved 3 rows in 0.03s.
```

The "lion" row has been discarded, the `animal` column has been transformed, and we have both the original and transformed `number` column.
