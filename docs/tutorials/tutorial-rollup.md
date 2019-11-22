---
id: tutorial-rollup
title: "Tutorial: Roll-up"
sidebar_label: "Roll-up"
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


Apache Druid (incubating) can summarize raw data at ingestion time using a process we refer to as "roll-up". Roll-up is a first-level aggregation operation over a selected set of columns that reduces the size of stored data.

This tutorial will demonstrate the effects of roll-up on an example dataset.

For this tutorial, we'll assume you've already downloaded Druid as described in
the [single-machine quickstart](index.html) and have it running on your local machine.

It will also be helpful to have finished [Tutorial: Loading a file](../tutorials/tutorial-batch.md) and [Tutorial: Querying data](../tutorials/tutorial-query.md).

## Example data

For this tutorial, we'll use a small sample of network flow event data, representing packet and byte counts for traffic from a source to a destination IP address that occurred within a particular second.

```json
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

A file containing this sample input data is located at `quickstart/tutorial/rollup-data.json`.

We'll ingest this data using the following ingestion task spec, located at `quickstart/tutorial/rollup-index.json`.

```json
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
        "baseDir" : "quickstart/tutorial",
        "filter" : "rollup-data.json"
      },
      "appendToExisting" : false
    },
    "tuningConfig" : {
      "type" : "index",
      "maxRowsPerSegment" : 5000000,
      "maxRowsInMemory" : 25000
    }
  }
}
```

Roll-up has been enabled by setting `"rollup" : true` in the `granularitySpec`.

Note that we have `srcIP` and `dstIP` defined as dimensions, a longSum metric is defined for the `packets` and `bytes` columns, and the `queryGranularity` has been defined as `minute`.

We will see how these definitions are used after we load this data.

## Load the example data

From the apache-druid-{{DRUIDVERSION}} package root, run the following command:

```bash
bin/post-index-task --file quickstart/tutorial/rollup-index.json --url http://localhost:8081
```

After the script completes, we will query the data.

## Query the example data

Let's run `bin/dsql` and issue a `select * from "rollup-tutorial";` query to see what data was ingested.

```bash
$ bin/dsql
Welcome to dsql, the command-line client for Druid SQL.
Type "\h" for help.
dsql> select * from "rollup-tutorial";
┌──────────────────────────┬────────┬───────┬─────────┬─────────┬─────────┐
│ __time                   │ bytes  │ count │ dstIP   │ packets │ srcIP   │
├──────────────────────────┼────────┼───────┼─────────┼─────────┼─────────┤
│ 2018-01-01T01:01:00.000Z │  35937 │     3 │ 2.2.2.2 │     286 │ 1.1.1.1 │
│ 2018-01-01T01:02:00.000Z │ 366260 │     2 │ 2.2.2.2 │     415 │ 1.1.1.1 │
│ 2018-01-01T01:03:00.000Z │  10204 │     1 │ 2.2.2.2 │      49 │ 1.1.1.1 │
│ 2018-01-02T21:33:00.000Z │ 100288 │     2 │ 8.8.8.8 │     161 │ 7.7.7.7 │
│ 2018-01-02T21:35:00.000Z │   2818 │     1 │ 8.8.8.8 │      12 │ 7.7.7.7 │
└──────────────────────────┴────────┴───────┴─────────┴─────────┴─────────┘
Retrieved 5 rows in 1.18s.

dsql>
```

Let's look at the three events in the original input data that occurred during `2018-01-01T01:01`:

```json
{"timestamp":"2018-01-01T01:01:35Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":20,"bytes":9024}
{"timestamp":"2018-01-01T01:01:51Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":255,"bytes":21133}
{"timestamp":"2018-01-01T01:01:59Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":11,"bytes":5780}
```

These three rows have been "rolled up" into the following row:

```bash
┌──────────────────────────┬────────┬───────┬─────────┬─────────┬─────────┐
│ __time                   │ bytes  │ count │ dstIP   │ packets │ srcIP   │
├──────────────────────────┼────────┼───────┼─────────┼─────────┼─────────┤
│ 2018-01-01T01:01:00.000Z │  35937 │     3 │ 2.2.2.2 │     286 │ 1.1.1.1 │
└──────────────────────────┴────────┴───────┴─────────┴─────────┴─────────┘
```

The input rows have been grouped by the timestamp and dimension columns `{timestamp, srcIP, dstIP}` with sum aggregations on the metric columns `packets` and `bytes`.

Before the grouping occurs, the timestamps of the original input data are bucketed/floored by minute, due to the `"queryGranularity":"minute"` setting in the ingestion spec.

Likewise, these two events that occurred during `2018-01-01T01:02` have been rolled up:

```json
{"timestamp":"2018-01-01T01:02:14Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":38,"bytes":6289}
{"timestamp":"2018-01-01T01:02:29Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":377,"bytes":359971}
```

```bash
┌──────────────────────────┬────────┬───────┬─────────┬─────────┬─────────┐
│ __time                   │ bytes  │ count │ dstIP   │ packets │ srcIP   │
├──────────────────────────┼────────┼───────┼─────────┼─────────┼─────────┤
│ 2018-01-01T01:02:00.000Z │ 366260 │     2 │ 2.2.2.2 │     415 │ 1.1.1.1 │
└──────────────────────────┴────────┴───────┴─────────┴─────────┴─────────┘
```

For the last event recording traffic between 1.1.1.1 and 2.2.2.2, no roll-up took place, because this was the only event that occurred during `2018-01-01T01:03`:

```json
{"timestamp":"2018-01-01T01:03:29Z","srcIP":"1.1.1.1", "dstIP":"2.2.2.2","packets":49,"bytes":10204}
```

```bash
┌──────────────────────────┬────────┬───────┬─────────┬─────────┬─────────┐
│ __time                   │ bytes  │ count │ dstIP   │ packets │ srcIP   │
├──────────────────────────┼────────┼───────┼─────────┼─────────┼─────────┤
│ 2018-01-01T01:03:00.000Z │  10204 │     1 │ 2.2.2.2 │      49 │ 1.1.1.1 │
└──────────────────────────┴────────┴───────┴─────────┴─────────┴─────────┘
```

Note that the `count` metric shows how many rows in the original input data contributed to the final "rolled up" row.
