---
layout: doc_page
---

# Tutorial: Roll-up

Druid can summarize raw data at ingestion time using a process we refer to as "roll-up". Roll-up is a first-level aggregation operation over a selected set of columns that reduces the size of stored segments.

This tutorial will demonstrate the effects of roll-up on an example dataset.

For this tutorial, we'll assume you've already downloaded Druid as described in 
the [single-machine quickstart](index.html) and have it running on your local machine.

It will also be helpful to have finished [Tutorial: Loading a file](/docs/VERSION/tutorials/tutorial-batch.html) and [Tutorial: Querying data](/docs/VERSION/tutorials/tutorial-query.html).

## Example data

For this tutorial, we'll use a small sample of network flow event data, representing packet and byte counts for traffic from a source to a destination IP address that occurred within a particular second.

```
{"timestamp":"2018-01-01T01:01:35Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":20,"bytes":9024}
{"timestamp":"2018-01-01T01:01:51Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":255,"bytes":21133}
{"timestamp":"2018-01-01T01:01:59Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":11,"bytes":5780}
{"timestamp":"2018-01-01T01:02:14Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":38,"bytes":6289}
{"timestamp":"2018-01-01T01:02:29Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":377,"bytes":359971}
{"timestamp":"2018-01-01T01:03:29Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":49,"bytes":10204}
{"timestamp":"2018-01-02T21:33:14Z","srcIP":"7.7.7.7", "dstIP":"8.8.8.8","packets":38,"bytes":6289}
{"timestamp":"2018-01-02T21:33:45Z","srcIP":"7.7.7.7", "dstIP":"8.8.8.8","packets":123,"bytes":93999}
{"timestamp":"2018-01-02T21:35:45Z","srcIP":"7.7.7.7", "dstIP":"8.8.8.8","packets":12,"bytes":2818}
```

A file containing this sample input data is located at `examples/rollup-data.json`.

We'll ingest this data using the following ingestion task spec, located at `examples/rollup-index.json`.

```
{
  "type" : "index",
  "spec" : {
    "dataSchema" : {
      "dataSource" : "rollup-tutorial",
      "parser" : {
        "type" : "string",
        "parseSpec" : {
          "format" : "json",
          "dimensionsSpec" : {
            "dimensions" : [
              "srcIP",
              "dstIP"
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
        { "type" : "longSum", "name" : "packets", "fieldName" : "packets" },
        { "type" : "longSum", "name" : "bytes", "fieldName" : "bytes" }
      ],
      "granularitySpec" : {
        "type" : "uniform",
        "segmentGranularity" : "week",
        "queryGranularity" : "minute",
        "intervals" : ["2018-01-01/2018-01-03"],
        "rollup" : true
      }
    },
    "ioConfig" : {
      "type" : "index",
      "firehose" : {
        "type" : "local",
        "baseDir" : "examples",
        "filter" : "rollup-data.json"
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

Roll-up has been enabled by setting `"rollup" : true` in the `granularitySpec`.

Note that we have `srcIP` and `dstIP` defined as dimensions, a longSum metric is defined for the `packets` and `bytes` columns, and the `queryGranularity` has been defined as `minute`. 

We will see how these definitions are used after we load this data.

## Load the example data

From the druid-${DRUIDVERSION} package root, run the following command:

```
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/rollup-index.json http://localhost:8090/druid/indexer/v1/task
```

After the data is loaded, we will query the data.

## Query the example data

Let's issue a `select * from "rollup-tutorial";` query to see what data was ingested.

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/rollup-select-sql.json http://localhost:8082/druid/v2/sql
```

The following results will be returned:

```
[
  {
    "__time": "2018-01-01T01:01:00.000Z",
    "bytes": 35937,
    "count": 3,
    "dstIP": "2.2.2.2",
    "packets": 286,
    "srcIP": "1.1.1.1"
  },
  {
    "__time": "2018-01-01T01:02:00.000Z",
    "bytes": 366260,
    "count": 2,
    "dstIP": "2.2.2.2",
    "packets": 415,
    "srcIP": "1.1.1.1"
  },
  {
    "__time": "2018-01-01T01:03:00.000Z",
    "bytes": 10204,
    "count": 1,
    "dstIP": "2.2.2.2",
    "packets": 49,
    "srcIP": "1.1.1.1"
  },
  {
    "__time": "2018-01-02T21:33:00.000Z",
    "bytes": 100288,
    "count": 2,
    "dstIP": "8.8.8.8",
    "packets": 161,
    "srcIP": "7.7.7.7"
  },
  {
    "__time": "2018-01-02T21:35:00.000Z",
    "bytes": 2818,
    "count": 1,
    "dstIP": "8.8.8.8",
    "packets": 12,
    "srcIP": "7.7.7.7"
  }
]
```

Let's look at the three events in the original input data that occurred during `2018-01-01T01:01`:

```
{"timestamp":"2018-01-01T01:01:35Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":20,"bytes":9024}
{"timestamp":"2018-01-01T01:01:51Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":255,"bytes":21133}
{"timestamp":"2018-01-01T01:01:59Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":11,"bytes":5780}
```

These three rows have been "rolled up" into the following row:

```
  {
    "__time": "2018-01-01T01:01:00.000Z",
    "bytes": 35937,
    "count": 3,
    "dstIP": "2.2.2.2",
    "packets": 286,
    "srcIP": "1.1.1.1"
  },
```

The input rows have been grouped by the timestamp and dimension columns `{timestamp, srcIP, dstIP}` with sum aggregations on the metric columns `packets` and `bytes`.

Before the grouping occurs, the timestamps of the original input data are bucketed/floored by minute, due to the `"queryGranularity":"minute"` setting in the ingestion spec.

Likewise, these two events that occurred during `2018-01-01T01:02` have been rolled up:

```
{"timestamp":"2018-01-01T01:02:14Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":38,"bytes":6289}
{"timestamp":"2018-01-01T01:02:29Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":377,"bytes":359971}
```

```
  {
    "__time": "2018-01-01T01:02:00.000Z",
    "bytes": 366260,
    "count": 2,
    "dstIP": "2.2.2.2",
    "packets": 415,
    "srcIP": "1.1.1.1"
  },
```

For the last event recording traffic between 1.1.1.1 and 2.2.2.2, no roll-up took place, because this was the only event that occurred during `2018-01-01T01:03`:

```
{"timestamp":"2018-01-01T01:03:29Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":49,"bytes":10204}
```

```
  {
    "__time": "2018-01-01T01:03:00.000Z",
    "bytes": 10204,
    "count": 1,
    "dstIP": "2.2.2.2",
    "packets": 49,
    "srcIP": "1.1.1.1"
  },
```

Note that the `count` metric shows how many rows in the original input data contributed to the final "rolled up" row.