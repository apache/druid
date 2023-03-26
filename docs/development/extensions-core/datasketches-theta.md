---
id: datasketches-theta
title: "DataSketches Theta Sketch module"
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


This module provides Apache Druid aggregators based on Theta sketch from [Apache DataSketches](https://datasketches.apache.org/) library.
Sketch algorithms are approximate. For more information, see [Accuracy](https://datasketches.apache.org/docs/Theta/ThetaAccuracy.html) in the DataSketches documentation.

At ingestion time, the Theta sketch aggregator creates Theta sketch objects which are stored in Druid segments. Logically speaking, a Theta sketch object can be thought of as a Set data structure. At query time, sketches are read and aggregated (set unioned) together. In the end, by default, you receive the estimate of the number of unique entries in the sketch object. You can use post aggregators to do union, intersection or difference on sketch columns in the same row.

Note that you can use `thetaSketch` aggregator on columns which were not ingested using the same. It will return estimated cardinality of the column. It is recommended to use it at ingestion time as well to make querying faster.

To use this aggregator, make sure you [include](../../development/extensions.md#loading-extensions) the extension in your config file:

```
druid.extensions.loadList=["druid-datasketches"]
```

For additional sketch types supported in Druid, see [DataSketches extension](datasketches-extension.md).

## Aggregator

```json
{
  "type" : "thetaSketch",
  "name" : <output_name>,
  "fieldName" : <metric_name>,
  "isInputThetaSketch": false,
  "size": 16384
 }
```

|Property|Description|Required?|
|--------|-----------|---------|
|`type`|This string should always be "thetaSketch"|yes|
|`name`|String representing the output column to store sketch values.|yes|
|`fieldName`|A string for the name of the aggregator used at ingestion time.|yes|
|`isInputThetaSketch`|Only set this to true at indexing time if your input data contains Theta sketch objects. This applies to cases when you use DataSketches outside of Druid, for example with Pig or Hive, to produce the data to ingest into Druid |no, defaults to false|
|`size`|Must be a power of 2. Internally, size refers to the maximum number of entries sketch object retains. Higher size means higher accuracy but more space to store sketches. After you index with a particular size, Druid persists the sketch in segments. At query time you must use a size greater or equal to the ingested size. See the [DataSketches site](https://datasketches.apache.org/docs/Theta/ThetaSize) for details. The default is recommended for the majority of use cases.|no, defaults to 16384|
|`shouldFinalize`|Return the final double type representing the estimate rather than the intermediate sketch type itself. In addition to controlling the finalization of this aggregator, you can control whether all aggregators are finalized with the query context parameters [`finalize`](../../querying/query-context.md) and [`sqlFinalizeOuterSketches`](../../querying/sql-query-context.md).|no, defaults to `true`|

## Post aggregators

### Sketch estimator

```json
{
  "type"  : "thetaSketchEstimate",
  "name": <output name>,
  "field"  : <post aggregator of type fieldAccess that refers to a thetaSketch aggregator or that of type thetaSketchSetOp>
}
```

### Sketch operations

```json
{
  "type"  : "thetaSketchSetOp",
  "name": <output name>,
  "func": <UNION|INTERSECT|NOT>,
  "fields"  : <array of fieldAccess type post aggregators to access the thetaSketch aggregators or thetaSketchSetOp type post aggregators to allow arbitrary combination of set operations>,
  "size": <16384 by default, must be max of size from sketches in fields input>
}
```

### Sketch summary

This returns a summary of the sketch that can be used for debugging. This is the result of calling toString() method.

```json
{
  "type"  : "thetaSketchToString",
  "name": <output name>,
  "field"  : <post aggregator that refers to a Theta sketch (fieldAccess or another post aggregator)>
}
```



### Constant Theta Sketch 

You can use the constant theta sketch post aggregator to add a Base64-encoded constant theta sketch value for use in other post-aggregators. For example,  `thetaSketchSetOp`.

```json
{
  "type"  : "thetaSketchConstant",
  "name": DESTINATION_COLUMN_NAME,
  "value"  : CONSTANT_SKETCH_VALUE
}
```

### Example using a constant Theta Sketch 

Assume you have a datasource with a variety of a variety of users. Using `filters` and `aggregation`, you generate a theta sketch of all `football fans`.  

A third-party provider has provided a constant theta sketch of all `cricket fans` and you want to `INTERSECT` both cricket fans and football fans in a `post-aggregation` stage to identify users who are interested in both `cricket`. Then you want to use `thetaSketchEstimate` to calculate the number of unique users.

```json
{
   "type":"thetaSketchEstimate",
   "name":"football_cricket_users_count",
   "field":{
      "type":"thetaSketchSetOp",
      "name":"football_cricket_fans_users_theta_sketch",
      "func":"INTERSECT",
      "fields":[
         {
            "type":"fieldAccess",
            "fieldName":"football_fans_users_theta_sketch"
         },
         {
            "type":"thetaSketchConstant",
            "name":"cricket_fans_users_theta_sketch",
            "value":"AgMDAAAazJMCAAAAAACAPzz9j7pWTMdROWGf15uY1nI="
         }
      ]
   }
}
```

## Examples

Assuming, you have a dataset containing (timestamp, product, user_id). You want to answer questions like

How many unique users visited product A?
How many unique users visited both product A and product B?

to answer above questions, you would index your data using following aggregator.

```json
{ "type": "thetaSketch", "name": "user_id_sketch", "fieldName": "user_id" }
```

then, sample query for, How many unique users visited product A?

```json
{
  "queryType": "groupBy",
  "dataSource": "test_datasource",
  "granularity": "ALL",
  "dimensions": [],
  "aggregations": [
    { "type": "thetaSketch", "name": "unique_users", "fieldName": "user_id_sketch" }
  ],
  "filter": { "type": "selector", "dimension": "product", "value": "A" },
  "intervals": [ "2014-10-19T00:00:00.000Z/2014-10-22T00:00:00.000Z" ]
}
```

sample query for, How many unique users visited both product A and B?

```json
{
  "queryType": "groupBy",
  "dataSource": "test_datasource",
  "granularity": "ALL",
  "dimensions": [],
  "filter": {
    "type": "or",
    "fields": [
      {"type": "selector", "dimension": "product", "value": "A"},
      {"type": "selector", "dimension": "product", "value": "B"}
    ]
  },
  "aggregations": [
    {
      "type" : "filtered",
      "filter" : {
        "type" : "selector",
        "dimension" : "product",
        "value" : "A"
      },
      "aggregator" :     {
        "type": "thetaSketch", "name": "A_unique_users", "fieldName": "user_id_sketch"
      }
    },
    {
      "type" : "filtered",
      "filter" : {
        "type" : "selector",
        "dimension" : "product",
        "value" : "B"
      },
      "aggregator" :     {
        "type": "thetaSketch", "name": "B_unique_users", "fieldName": "user_id_sketch"
      }
    }
  ],
  "postAggregations": [
    {
      "type": "thetaSketchEstimate",
      "name": "final_unique_users",
      "field":
      {
        "type": "thetaSketchSetOp",
        "name": "final_unique_users_sketch",
        "func": "INTERSECT",
        "fields": [
          {
            "type": "fieldAccess",
            "fieldName": "A_unique_users"
          },
          {
            "type": "fieldAccess",
            "fieldName": "B_unique_users"
          }
        ]
      }
    }
  ],
  "intervals": [
    "2014-10-19T00:00:00.000Z/2014-10-22T00:00:00.000Z"
  ]
}
```

### Retention analysis example

Suppose you want to answer a question like, "How many unique users performed a specific action in a particular time period and also performed another specific action in a different time period?"

e.g., "How many unique users signed up in week 1, and purchased something in week 2?"

Using the `(timestamp, product, user_id)` example dataset, data would be indexed with the following aggregator, like in the example above:

```json
{ "type": "thetaSketch", "name": "user_id_sketch", "fieldName": "user_id" }
```

The following query expresses:

"Out of the unique users who visited Product A between 10/01/2014 and 10/07/2014, how many visited Product A again in the week of 10/08/2014 to 10/14/2014?"

```json
{
  "queryType": "groupBy",
  "dataSource": "test_datasource",
  "granularity": "ALL",
  "dimensions": [],
  "filter": {
    "type": "or",
    "fields": [
      {"type": "selector", "dimension": "product", "value": "A"}
    ]
  },
  "aggregations": [
    {
      "type" : "filtered",
      "filter" : {
        "type" : "and",
        "fields" : [
          {
            "type" : "selector",
            "dimension" : "product",
            "value" : "A"
          },
          {
            "type" : "interval",
            "dimension" : "__time",
            "intervals" :  ["2014-10-01T00:00:00.000Z/2014-10-07T00:00:00.000Z"]
          }
        ]
      },
      "aggregator" :     {
        "type": "thetaSketch", "name": "A_unique_users_week_1", "fieldName": "user_id_sketch"
      }
    },
    {
      "type" : "filtered",
      "filter" : {
        "type" : "and",
        "fields" : [
          {
            "type" : "selector",
            "dimension" : "product",
            "value" : "A"
          },
          {
            "type" : "interval",
            "dimension" : "__time",
            "intervals" :  ["2014-10-08T00:00:00.000Z/2014-10-14T00:00:00.000Z"]
          }
        ]
      },
      "aggregator" : {
        "type": "thetaSketch", "name": "A_unique_users_week_2", "fieldName": "user_id_sketch"
      }
    },
  ],
  "postAggregations": [
    {
      "type": "thetaSketchEstimate",
      "name": "final_unique_users",
      "field":
      {
        "type": "thetaSketchSetOp",
        "name": "final_unique_users_sketch",
        "func": "INTERSECT",
        "fields": [
          {
            "type": "fieldAccess",
            "fieldName": "A_unique_users_week_1"
          },
          {
            "type": "fieldAccess",
            "fieldName": "A_unique_users_week_2"
          }
        ]
      }
    }
  ],
  "intervals": ["2014-10-01T00:00:00.000Z/2014-10-14T00:00:00.000Z"]
}
```
