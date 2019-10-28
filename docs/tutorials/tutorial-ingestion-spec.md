---
id: tutorial-ingestion-spec
title: "Tutorial: Writing an ingestion spec"
sidebar_label: "Writing an ingestion spec"
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


This tutorial will guide the reader through the process of defining an ingestion spec, pointing out key considerations and guidelines.

For this tutorial, we'll assume you've already downloaded Apache Druid (incubating) as described in
the [single-machine quickstart](index.html) and have it running on your local machine.

It will also be helpful to have finished [Tutorial: Loading a file](../tutorials/tutorial-batch.md), [Tutorial: Querying data](../tutorials/tutorial-query.md), and [Tutorial: Rollup](../tutorials/tutorial-rollup.md).

## Example data

Suppose we have the following network flow data:

* `srcIP`: IP address of sender
* `srcPort`: Port of sender
* `dstIP`: IP address of receiver
* `dstPort`: Port of receiver
* `protocol`: IP protocol number
* `packets`: number of packets transmitted
* `bytes`: number of bytes transmitted
* `cost`: the cost of sending the traffic

```json
{"ts":"2018-01-01T01:01:35Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2", "srcPort":2000, "dstPort":3000, "protocol": 6, "packets":10, "bytes":1000, "cost": 1.4}
{"ts":"2018-01-01T01:01:51Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2", "srcPort":2000, "dstPort":3000, "protocol": 6, "packets":20, "bytes":2000, "cost": 3.1}
{"ts":"2018-01-01T01:01:59Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2", "srcPort":2000, "dstPort":3000, "protocol": 6, "packets":30, "bytes":3000, "cost": 0.4}
{"ts":"2018-01-01T01:02:14Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2", "srcPort":5000, "dstPort":7000, "protocol": 6, "packets":40, "bytes":4000, "cost": 7.9}
{"ts":"2018-01-01T01:02:29Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2", "srcPort":5000, "dstPort":7000, "protocol": 6, "packets":50, "bytes":5000, "cost": 10.2}
{"ts":"2018-01-01T01:03:29Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2", "srcPort":5000, "dstPort":7000, "protocol": 6, "packets":60, "bytes":6000, "cost": 4.3}
{"ts":"2018-01-01T02:33:14Z","srcIP":"7.7.7.7", "dstIP":"8.8.8.8", "srcPort":4000, "dstPort":5000, "protocol": 17, "packets":100, "bytes":10000, "cost": 22.4}
{"ts":"2018-01-01T02:33:45Z","srcIP":"7.7.7.7", "dstIP":"8.8.8.8", "srcPort":4000, "dstPort":5000, "protocol": 17, "packets":200, "bytes":20000, "cost": 34.5}
{"ts":"2018-01-01T02:35:45Z","srcIP":"7.7.7.7", "dstIP":"8.8.8.8", "srcPort":4000, "dstPort":5000, "protocol": 17, "packets":300, "bytes":30000, "cost": 46.3}
```

Save the JSON contents above into a file called `ingestion-tutorial-data.json` in `quickstart/`.

Let's walk through the process of defining an ingestion spec that can load this data.

For this tutorial, we will be using the native batch indexing task. When using other task types, some aspects of the ingestion spec will differ, and this tutorial will point out such areas.

## Defining the schema

The core element of a Druid ingestion spec is the `dataSchema`. The `dataSchema` defines how to parse input data into a set of columns that will be stored in Druid.

Let's start with an empty `dataSchema` and add fields to it as we progress through the tutorial.

Create a new file called `ingestion-tutorial-index.json` in `quickstart/` with the following contents:

```json
"dataSchema" : {}
```

We will be making successive edits to this ingestion spec as we progress through the tutorial.

### Datasource name

The datasource name is specified by the `dataSource` parameter in the `dataSchema`.

```json
"dataSchema" : {
  "dataSource" : "ingestion-tutorial",
}
```

Let's call the tutorial datasource `ingestion-tutorial`.

### Choose a parser

A `dataSchema` has a `parser` field, which defines the parser that Druid will use to interpret the input data.

Since our input data is represented as JSON strings, we'll use a `string` parser with `json` format:

```json
"dataSchema" : {
  "dataSource" : "ingestion-tutorial",
  "parser" : {
    "type" : "string",
    "parseSpec" : {
      "format" : "json"
    }
  }
}
```

### Time column

The `parser` needs to know how to extract the main timestamp field from the input data. When using a `json` type `parseSpec`, the timestamp is defined in a `timestampSpec`.

The timestamp column in our input data is named "ts", containing ISO 8601 timestamps, so let's add a `timestampSpec` with that information to the `parseSpec`:

```json
"dataSchema" : {
  "dataSource" : "ingestion-tutorial",
  "parser" : {
    "type" : "string",
    "parseSpec" : {
      "format" : "json",
      "timestampSpec" : {
        "format" : "iso",
        "column" : "ts"
      }
    }
  }
}
```

### Column types

Now that we've defined the time column, let's look at definitions for other columns.

Druid supports the following column types: String, Long, Float, Double. We will see how these are used in the following sections.

Before we move on to how we define our other non-time columns, let's discuss `rollup` first.

### Rollup

When ingesting data, we must consider whether we wish to use rollup or not.

* If rollup is enabled, we will need to separate the input columns into two categories, "dimensions" and "metrics". "Dimensions" are the grouping columns for rollup, while "metrics" are the columns that will be aggregated.

* If rollup is disabled, then all columns are treated as "dimensions" and no pre-aggregation occurs.

For this tutorial, let's enable rollup. This is specified with a `granularitySpec` on the `dataSchema`.

Note that the `granularitySpec` lies outside of the `parser`. We will revisit the `parser` soon when we define our dimensions and metrics.

```json
"dataSchema" : {
  "dataSource" : "ingestion-tutorial",
  "parser" : {
    "type" : "string",
    "parseSpec" : {
      "format" : "json",
      "timestampSpec" : {
        "format" : "iso",
        "column" : "ts"
      }
    }
  },
  "granularitySpec" : {
    "rollup" : true
  }
}

```

#### Choosing dimensions and metrics

For this example dataset, the following is a sensible split for "dimensions" and "metrics":

* Dimensions: srcIP, srcPort, dstIP, dstPort, protocol
* Metrics: packets, bytes, cost

The dimensions here are a group of properties that identify a unidirectional flow of IP traffic, while the metrics represent facts about the IP traffic flow specified by a dimension grouping.

Let's look at how to define these dimensions and metrics within the ingestion spec.

#### Dimensions

Dimensions are specified with a `dimensionsSpec` inside the `parseSpec`.

```json
"dataSchema" : {
  "dataSource" : "ingestion-tutorial",
  "parser" : {
    "type" : "string",
    "parseSpec" : {
      "format" : "json",
      "timestampSpec" : {
        "format" : "iso",
        "column" : "ts"
      },
      "dimensionsSpec" : {
        "dimensions": [
          "srcIP",
          { "name" : "srcPort", "type" : "long" },
          { "name" : "dstIP", "type" : "string" },
          { "name" : "dstPort", "type" : "long" },
          { "name" : "protocol", "type" : "string" }
        ]
      }
    }
  },
  "granularitySpec" : {
    "rollup" : true
  }
}
```

Each dimension has a `name` and a `type`, where `type` can be "long", "float", "double", or "string".

Note that `srcIP` is a "string" dimension; for string dimensions, it is enough to specify just a dimension name, since "string" is the default dimension type.

Also note that `protocol` is a numeric value in the input data, but we are ingesting it as a "string" column; Druid will coerce the input longs to strings during ingestion.

##### Strings vs. Numerics

Should a numeric input be ingested as a numeric dimension or as a string dimension?

Numeric dimensions have the following pros/cons relative to String dimensions:
* Pros: Numeric representation can result in smaller column sizes on disk and lower processing overhead when reading values from the column
* Cons: Numeric dimensions do not have indices, so filtering on them will often be slower than filtering on an equivalent String dimension (which has bitmap indices)

#### Metrics

Metrics are specified with a `metricsSpec` inside the `dataSchema`:

```json
"dataSchema" : {
  "dataSource" : "ingestion-tutorial",
  "parser" : {
    "type" : "string",
    "parseSpec" : {
      "format" : "json",
      "timestampSpec" : {
        "format" : "iso",
        "column" : "ts"
      },
      "dimensionsSpec" : {
        "dimensions": [
          "srcIP",
          { "name" : "srcPort", "type" : "long" },
          { "name" : "dstIP", "type" : "string" },
          { "name" : "dstPort", "type" : "long" },
          { "name" : "protocol", "type" : "string" }
        ]
      }
    }
  },
  "metricsSpec" : [
    { "type" : "count", "name" : "count" },
    { "type" : "longSum", "name" : "packets", "fieldName" : "packets" },
    { "type" : "longSum", "name" : "bytes", "fieldName" : "bytes" },
    { "type" : "doubleSum", "name" : "cost", "fieldName" : "cost" }
  ],
  "granularitySpec" : {
    "rollup" : true
  }
}
```

When defining a metric, it is necessary to specify what type of aggregation should be performed on that column during rollup.

Here we have defined long sum aggregations on the two long metric columns, `packets` and `bytes`, and a double sum aggregation for the `cost` column.

Note that the `metricsSpec` is on a different nesting level than `dimensionSpec` or `parseSpec`; it belongs on the same nesting level as `parser` within the `dataSchema`.

Note that we have also defined a `count` aggregator. The count aggregator will track how many rows in the original input data contributed to a "rolled up" row in the final ingested data.

### No rollup

If we were not using rollup, all columns would be specified in the `dimensionsSpec`, e.g.:

```json
      "dimensionsSpec" : {
        "dimensions": [
          "srcIP",
          { "name" : "srcPort", "type" : "long" },
          { "name" : "dstIP", "type" : "string" },
          { "name" : "dstPort", "type" : "long" },
          { "name" : "protocol", "type" : "string" },
          { "name" : "packets", "type" : "long" },
          { "name" : "bytes", "type" : "long" },
          { "name" : "srcPort", "type" : "double" }
        ]
      },
```


### Define granularities

At this point, we are done defining the `parser` and `metricsSpec` within the `dataSchema` and we are almost done writing the ingestion spec.

There are some additional properties we need to set in the `granularitySpec`:
* Type of granularitySpec: `uniform` and `arbitrary` are the two supported types. For this tutorial, we will use a `uniform` granularity spec, where all segments have uniform interval sizes (for example, all segments cover an hour's worth of data).
* The segment granularity: what size of time interval should a single segment contain data for? e.g., `DAY`, `WEEK`
* The bucketing granularity of the timestamps in the time column (referred to as `queryGranularity`)

#### Segment granularity

Segment granularity is configured by the `segmentGranularity` property in the `granularitySpec`. For this tutorial, we'll create hourly segments:

```json
"dataSchema" : {
  "dataSource" : "ingestion-tutorial",
  "parser" : {
    "type" : "string",
    "parseSpec" : {
      "format" : "json",
      "timestampSpec" : {
        "format" : "iso",
        "column" : "ts"
      },
      "dimensionsSpec" : {
        "dimensions": [
          "srcIP",
          { "name" : "srcPort", "type" : "long" },
          { "name" : "dstIP", "type" : "string" },
          { "name" : "dstPort", "type" : "long" },
          { "name" : "protocol", "type" : "string" }
        ]
      }
    }
  },
  "metricsSpec" : [
    { "type" : "count", "name" : "count" },
    { "type" : "longSum", "name" : "packets", "fieldName" : "packets" },
    { "type" : "longSum", "name" : "bytes", "fieldName" : "bytes" },
    { "type" : "doubleSum", "name" : "cost", "fieldName" : "cost" }
  ],
  "granularitySpec" : {
    "type" : "uniform",
    "segmentGranularity" : "HOUR",
    "rollup" : true
  }
}
```

Our input data has events from two separate hours, so this task will generate two segments.

#### Query granularity

The query granularity is configured by the `queryGranularity` property in the `granularitySpec`. For this tutorial, let's use minute granularity:

```json
"dataSchema" : {
  "dataSource" : "ingestion-tutorial",
  "parser" : {
    "type" : "string",
    "parseSpec" : {
      "format" : "json",
      "timestampSpec" : {
        "format" : "iso",
        "column" : "ts"
      },
      "dimensionsSpec" : {
        "dimensions": [
          "srcIP",
          { "name" : "srcPort", "type" : "long" },
          { "name" : "dstIP", "type" : "string" },
          { "name" : "dstPort", "type" : "long" },
          { "name" : "protocol", "type" : "string" }
        ]
      }
    }
  },
  "metricsSpec" : [
    { "type" : "count", "name" : "count" },
    { "type" : "longSum", "name" : "packets", "fieldName" : "packets" },
    { "type" : "longSum", "name" : "bytes", "fieldName" : "bytes" },
    { "type" : "doubleSum", "name" : "cost", "fieldName" : "cost" }
  ],
  "granularitySpec" : {
    "type" : "uniform",
    "segmentGranularity" : "HOUR",
    "queryGranularity" : "MINUTE"
    "rollup" : true
  }
}
```

To see the effect of the query granularity, let's look at this row from the raw input data:

```json
{"ts":"2018-01-01T01:03:29Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2", "srcPort":5000, "dstPort":7000, "protocol": 6, "packets":60, "bytes":6000, "cost": 4.3}
```

When this row is ingested with minute queryGranularity, Druid will floor the row's timestamp to minute buckets:

```json
{"ts":"2018-01-01T01:03:00Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2", "srcPort":5000, "dstPort":7000, "protocol": 6, "packets":60, "bytes":6000, "cost": 4.3}
```

#### Define an interval (batch only)

For batch tasks, it is necessary to define a time interval. Input rows with timestamps outside of the time interval will not be ingested.

The interval is also specified in the `granularitySpec`:

```json
"dataSchema" : {
  "dataSource" : "ingestion-tutorial",
  "parser" : {
    "type" : "string",
    "parseSpec" : {
      "format" : "json",
      "timestampSpec" : {
        "format" : "iso",
        "column" : "ts"
      },
      "dimensionsSpec" : {
        "dimensions": [
          "srcIP",
          { "name" : "srcPort", "type" : "long" },
          { "name" : "dstIP", "type" : "string" },
          { "name" : "dstPort", "type" : "long" },
          { "name" : "protocol", "type" : "string" }
        ]
      }
    }
  },
  "metricsSpec" : [
    { "type" : "count", "name" : "count" },
    { "type" : "longSum", "name" : "packets", "fieldName" : "packets" },
    { "type" : "longSum", "name" : "bytes", "fieldName" : "bytes" },
    { "type" : "doubleSum", "name" : "cost", "fieldName" : "cost" }
  ],
  "granularitySpec" : {
    "type" : "uniform",
    "segmentGranularity" : "HOUR",
    "queryGranularity" : "MINUTE",
    "intervals" : ["2018-01-01/2018-01-02"],
    "rollup" : true
  }
}
```

## Define the task type

We've now finished defining our `dataSchema`. The remaining steps are to place the `dataSchema` we created into an ingestion task spec, and specify the input source.

The `dataSchema` is shared across all task types, but each task type has its own specification format. For this tutorial, we will use the native batch ingestion task:

```json
{
  "type" : "index",
  "spec" : {
    "dataSchema" : {
      "dataSource" : "ingestion-tutorial",
      "parser" : {
        "type" : "string",
        "parseSpec" : {
          "format" : "json",
          "timestampSpec" : {
            "format" : "iso",
            "column" : "ts"
          },
          "dimensionsSpec" : {
            "dimensions": [
              "srcIP",
              { "name" : "srcPort", "type" : "long" },
              { "name" : "dstIP", "type" : "string" },
              { "name" : "dstPort", "type" : "long" },
              { "name" : "protocol", "type" : "string" }
            ]
          }
        }
      },
      "metricsSpec" : [
        { "type" : "count", "name" : "count" },
        { "type" : "longSum", "name" : "packets", "fieldName" : "packets" },
        { "type" : "longSum", "name" : "bytes", "fieldName" : "bytes" },
        { "type" : "doubleSum", "name" : "cost", "fieldName" : "cost" }
      ],
      "granularitySpec" : {
        "type" : "uniform",
        "segmentGranularity" : "HOUR",
        "queryGranularity" : "MINUTE",
        "intervals" : ["2018-01-01/2018-01-02"],
        "rollup" : true
      }
    }
  }
}
```

## Define the input source

Now let's define our input source, which is specified in an `ioConfig` object. Each task type has its own type of `ioConfig`. The native batch task uses "firehoses" to read input data, so let's configure a "local" firehose to read the example netflow data we saved earlier:


```json
    "ioConfig" : {
      "type" : "index",
      "firehose" : {
        "type" : "local",
        "baseDir" : "quickstart/",
        "filter" : "ingestion-tutorial-data.json"
      }
    }
```

```json
{
  "type" : "index",
  "spec" : {
    "dataSchema" : {
      "dataSource" : "ingestion-tutorial",
      "parser" : {
        "type" : "string",
        "parseSpec" : {
          "format" : "json",
          "timestampSpec" : {
            "format" : "iso",
            "column" : "ts"
          },
          "dimensionsSpec" : {
            "dimensions": [
              "srcIP",
              { "name" : "srcPort", "type" : "long" },
              { "name" : "dstIP", "type" : "string" },
              { "name" : "dstPort", "type" : "long" },
              { "name" : "protocol", "type" : "string" }
            ]
          }
        }
      },
      "metricsSpec" : [
        { "type" : "count", "name" : "count" },
        { "type" : "longSum", "name" : "packets", "fieldName" : "packets" },
        { "type" : "longSum", "name" : "bytes", "fieldName" : "bytes" },
        { "type" : "doubleSum", "name" : "cost", "fieldName" : "cost" }
      ],
      "granularitySpec" : {
        "type" : "uniform",
        "segmentGranularity" : "HOUR",
        "queryGranularity" : "MINUTE",
        "intervals" : ["2018-01-01/2018-01-02"],
        "rollup" : true
      }
    },
    "ioConfig" : {
      "type" : "index",
      "firehose" : {
        "type" : "local",
        "baseDir" : "quickstart/",
        "filter" : "ingestion-tutorial-data.json"
      }
    }
  }
}
```

## Additional tuning

Each ingestion task has a `tuningConfig` section that allows users to tune various ingestion parameters.

As an example, let's add a `tuningConfig` that sets a target segment size for the native batch ingestion task:

```json
    "tuningConfig" : {
      "type" : "index",
      "maxRowsPerSegment" : 5000000
    }
```

Note that each ingestion task has its own type of `tuningConfig`.

## Final spec

We've finished defining the ingestion spec, it should now look like the following:

```json
{
  "type" : "index",
  "spec" : {
    "dataSchema" : {
      "dataSource" : "ingestion-tutorial",
      "parser" : {
        "type" : "string",
        "parseSpec" : {
          "format" : "json",
          "timestampSpec" : {
            "format" : "iso",
            "column" : "ts"
          },
          "dimensionsSpec" : {
            "dimensions": [
              "srcIP",
              { "name" : "srcPort", "type" : "long" },
              { "name" : "dstIP", "type" : "string" },
              { "name" : "dstPort", "type" : "long" },
              { "name" : "protocol", "type" : "string" }
            ]
          }
        }
      },
      "metricsSpec" : [
        { "type" : "count", "name" : "count" },
        { "type" : "longSum", "name" : "packets", "fieldName" : "packets" },
        { "type" : "longSum", "name" : "bytes", "fieldName" : "bytes" },
        { "type" : "doubleSum", "name" : "cost", "fieldName" : "cost" }
      ],
      "granularitySpec" : {
        "type" : "uniform",
        "segmentGranularity" : "HOUR",
        "queryGranularity" : "MINUTE",
        "intervals" : ["2018-01-01/2018-01-02"],
        "rollup" : true
      }
    },
    "ioConfig" : {
      "type" : "index",
      "firehose" : {
        "type" : "local",
        "baseDir" : "quickstart/",
        "filter" : "ingestion-tutorial-data.json"
      }
    },
    "tuningConfig" : {
      "type" : "index",
      "maxRowsPerSegment" : 5000000
    }
  }
}
```

## Submit the task and query the data

From the apache-druid-{{DRUIDVERSION}} package root, run the following command:

```bash
bin/post-index-task --file quickstart/ingestion-tutorial-index.json --url http://localhost:8081
```

After the script completes, we will query the data.

Let's run `bin/dsql` and issue a `select * from "ingestion-tutorial";` query to see what data was ingested.

```bash
$ bin/dsql
Welcome to dsql, the command-line client for Druid SQL.
Type "\h" for help.
dsql> select * from "ingestion-tutorial";

┌──────────────────────────┬───────┬──────┬───────┬─────────┬─────────┬─────────┬──────────┬─────────┬─────────┐
│ __time                   │ bytes │ cost │ count │ dstIP   │ dstPort │ packets │ protocol │ srcIP   │ srcPort │
├──────────────────────────┼───────┼──────┼───────┼─────────┼─────────┼─────────┼──────────┼─────────┼─────────┤
│ 2018-01-01T01:01:00.000Z │  6000 │  4.9 │     3 │ 2.2.2.2 │    3000 │      60 │ 6        │ 1.1.1.1 │    2000 │
│ 2018-01-01T01:02:00.000Z │  9000 │ 18.1 │     2 │ 2.2.2.2 │    7000 │      90 │ 6        │ 1.1.1.1 │    5000 │
│ 2018-01-01T01:03:00.000Z │  6000 │  4.3 │     1 │ 2.2.2.2 │    7000 │      60 │ 6        │ 1.1.1.1 │    5000 │
│ 2018-01-01T02:33:00.000Z │ 30000 │ 56.9 │     2 │ 8.8.8.8 │    5000 │     300 │ 17       │ 7.7.7.7 │    4000 │
│ 2018-01-01T02:35:00.000Z │ 30000 │ 46.3 │     1 │ 8.8.8.8 │    5000 │     300 │ 17       │ 7.7.7.7 │    4000 │
└──────────────────────────┴───────┴──────┴───────┴─────────┴─────────┴─────────┴──────────┴─────────┴─────────┘
Retrieved 5 rows in 0.12s.

dsql>
```
