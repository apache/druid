---
layout: doc_page
---

## DataSketches aggregator

Druid aggregators based on [datasketches](http://datasketches.github.io/) library. Note that sketch algorithms are approximate; see details in the "Accuracy" section of the datasketches doc. 
At ingestion time, this aggregator creates the theta sketch objects which get stored in Druid segments. Logically speaking, a theta sketch object can be thought of as a Set data structure. At query time, sketches are read and aggregated (set unioned) together. In the end, by default, you receive the estimate of the number of unique entries in the sketch object. Also, you can use post aggregators to do union, intersection or difference on sketch columns in the same row. 
Note that you can use `thetaSketch` aggregator on columns which were not ingested using same, it will return estimated cardinality of the column. It is recommended to use it at ingestion time as well to make querying faster.

### Aggregators

```json
{
  "type" : "thetaSketch",
  "name" : <output_name>,
  "fieldName" : <metric_name>,

  //following boolean field is optional. This should only be used at
  //indexing time if your input data contains theta sketch objects.
  //that would be the case if you use datasketches library outside of Druid,
  //say with Pig/Hive, to produce the data that you are ingesting into Druid
  "isInputThetaSketch": false

  //following field is optional, default = 16384. must be a power of 2.
  //Internally, size refers to the maximum number
  //of entries sketch object will retain, higher size would mean higher
  //accuracy but higher space needed to store those sketches.
  //note that after you index with a particular size, druid will persist sketch in segments
  //and you will use size greater or equal to that at query time.
  //See [theta-size](http://datasketches.github.io/docs/ThetaSize.html) for details.
  //In general, We recommend just sticking to default size, which has worked well.
  "size": 16384
 }
```

### Post Aggregators

#### Sketch Estimator

```json
{
  "type"  : "thetaSketchEstimate",
  "name": <output name>,
  "fieldName"  : <the name field value of the thetaSketch aggregator>
}
```

#### Sketch Operations

```json
{
  "type"  : "thetaSketchSetOp",
  "name": <output name>,
  "func": <UNION|INTERSECT|NOT>,
  "fields"  : <the name field value of the thetaSketch aggregators>,
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
