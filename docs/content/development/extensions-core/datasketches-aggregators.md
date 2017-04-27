---
layout: doc_page
---

## DataSketches aggregator

Druid aggregators based on [datasketches](http://datasketches.github.io/) library. Note that sketch algorithms are approximate; see details in the "Accuracy" section of the datasketches doc. 
At ingestion time, this aggregator creates the theta sketch objects which get stored in Druid segments. Logically speaking, a theta sketch object can be thought of as a Set data structure. At query time, sketches are read and aggregated (set unioned) together. In the end, by default, you receive the estimate of the number of unique entries in the sketch object. Also, you can use post aggregators to do union, intersection or difference on sketch columns in the same row. 
Note that you can use `thetaSketch` aggregator on columns which were not ingested using same, it will return estimated cardinality of the column. It is recommended to use it at ingestion time as well to make querying faster.

To use the datasketch aggregators, make sure you [include](../../operations/including-extensions.html) the extension in your config file:

```
druid.extensions.loadList=["druid-datasketches"]
```

### Aggregators

```json
{
  "type" : "thetaSketch",
  "name" : <output_name>,
  "fieldName" : <metric_name>,  
  "isInputThetaSketch": false,
  "size": 16384
 }
```

|property|description|required?|
|--------|-----------|---------|
|type|This String should always be "thetaSketch"|yes|
|name|A String for the output (result) name of the calculation.|yes|
|fieldName|A String for the name of the aggregator used at ingestion time.|yes|
|isInputThetaSketch|This should only be used at indexing time if your input data contains theta sketch objects. This would be the case if you use datasketches library outside of Druid, say with Pig/Hive, to produce the data that you are ingesting into Druid |no, defaults to false|
|size|Must be a power of 2. Internally, size refers to the maximum number of entries sketch object will retain. Higher size means higher accuracy but more space to store sketches. Note that after you index with a particular size, druid will persist sketch in segments and you will use size greater or equal to that at query time. See the [DataSketches site](https://datasketches.github.io/docs/Theta/ThetaSize.html) for details. In general, We recommend just sticking to default size. |no, defaults to 16384|

### Post Aggregators

#### Sketch Estimator

```json
{
  "type"  : "thetaSketchEstimate",
  "name": <output name>,
  "field"  : <post aggregator of type fieldAccess that refers to a thetaSketch aggregator or that of type thetaSketchSetOp>
}
```

#### Sketch Operations

```json
{
  "type"  : "thetaSketchSetOp",
  "name": <output name>,
  "func": <UNION|INTERSECT|NOT>,
  "fields"  : <array of fieldAccess type post aggregators to access the thetaSketch aggregators or thetaSketchSetOp type post aggregators to allow arbitrary combination of set operations>,
  "size": <16384 by default, must be max of size from sketches in fields input>
}
```

### Examples

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

#### Retention Analysis Example

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