---
layout: doc_page
---

# Tutorial: Transforming input data

This tutorial will demonstrate how to use transform specs to filter and transform input data during ingestion.

For this tutorial, we'll assume you've already downloaded Druid as described in 
the [single-machine quickstart](index.html) and have it running on your local machine.

It will also be helpful to have finished [Tutorial: Loading a file](/docs/VERSION/tutorials/tutorial-batch.html) and [Tutorial: Querying data](/docs/VERSION/tutorials/tutorial-query.html).

## Sample data

We've included sample data for this tutorial at `examples/transform-data.json`, reproduced here for convenience:

```
{"timestamp":"2018-01-01T07:01:35Z","animal":"octopus",  "location":1, "number":100}
{"timestamp":"2018-01-01T05:01:35Z","animal":"mongoose", "location":2,"number":200}
{"timestamp":"2018-01-01T06:01:35Z","animal":"snake", "location":3, "number":300}
{"timestamp":"2018-01-01T01:01:35Z","animal":"lion", "location":4, "number":300}
```

## Load data with transform specs

We will ingest the sample data using the following spec, which demonstrates the use of transform specs:

```
{
  "type" : "index",
  "spec" : {
    "dataSchema" : {
      "dataSource" : "transform-tutorial",
      "parser" : {
        "type" : "string",
        "parseSpec" : {
          "format" : "json",
          "dimensionsSpec" : {
            "dimensions" : [
              "animal",
              { "name": "location", "type": "long" }
            ]
          },
          "timestampSpec": {
            "column": "timestamp",
            "format": "iso"
          }
        }
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
      "type" : "index",
      "firehose" : {
        "type" : "local",
        "baseDir" : "examples/",
        "filter" : "transform-data.json"
      },
      "appendToExisting" : false
    },
    "tuningConfig" : {
      "type" : "index",
      "targetPartitionSize" : 5000000,
      "maxRowsInMemory" : 25000,
      "forceExtendableShardSpecs" : true
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

```
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/transform-index.json http://localhost:8090/druid/indexer/v1/task
```

## Query the transformed data

Let's a `select * from "transform-tutorial";` query to see what was ingested:

```
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/transform-select-sql.json http://localhost:8082/druid/v2/sql
```

```
[
  {
    "__time": "2018-01-01T05:01:00.000Z",
    "animal": "super-mongoose",
    "count": 1,
    "location": 2,
    "number": 200,
    "triple-number": 600
  },
  {
    "__time": "2018-01-01T06:01:00.000Z",
    "animal": "super-snake",
    "count": 1,
    "location": 3,
    "number": 300,
    "triple-number": 900
  },
  {
    "__time": "2018-01-01T07:01:00.000Z",
    "animal": "super-octopus",
    "count": 1,
    "location": 1,
    "number": 100,
    "triple-number": 300
  }
]
```

The "lion" row has been discarded, the `animal` column has been transformed, and we have both the original and transformed `number` column.