---
id: tutorial-update-data
title: "Tutorial: Updating existing data"
sidebar_label: "Updating existing data"
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


This tutorial shows you how to update data in a datasource by overwriting existing data and adding new data to the datasource.

## Prerequisites

Before starting this tutorial, download and run Apache Druid on your local machine as described in
the [single-machine quickstart](index.md).

You should also be familiar with the material in the following tutorials:
* [Tutorial: Loading a file](../tutorials/tutorial-batch.md)
* [Tutorial: Querying data](../tutorials/tutorial-query.md)
* [Tutorial: Roll-up](../tutorials/tutorial-rollup.md)

## Load initial data

Load an initial data set to which you will overwrite and append data.

The ingestion spec is located at `quickstart/tutorial/updates-init-index.json`. This spec creates a datasource called `updates-tutorial` and ingests data from `quickstart/tutorial/updates-data.json`.

Submit the ingestion task:

```bash
bin/post-index-task --file quickstart/tutorial/updates-init-index.json --url http://localhost:8081
```

Start the SQL command-line client:
```bash
bin/dsql
```

Run the following SQL query to retrieve data from `updates-tutorial`:

```bash
dsql> SELECT * FROM "updates-tutorial";
┌──────────────────────────┬──────────┬───────┬────────┐
│ __time                   │ animal   │ count │ number │
├──────────────────────────┼──────────┼───────┼────────┤
│ 2018-01-01T01:01:00.000Z │ tiger    │     1 │    100 │
│ 2018-01-01T03:01:00.000Z │ aardvark │     1 │     42 │
│ 2018-01-01T03:01:00.000Z │ giraffe  │     1 │  14124 │
└──────────────────────────┴──────────┴───────┴────────┘
Retrieved 3 rows in 1.42s.
```

The datasource contains three rows of data with an `animal` dimension and a `number` metric.

## Overwrite data

To overwrite the data, submit another task for the same interval but with different input data.

The `quickstart/tutorial/updates-overwrite-index.json` spec performs an overwrite on the `updates-tutorial` datasource.

In the overwrite ingestion spec, notice the following:
* The `intervals` field remains the same: `"intervals" : ["2018-01-01/2018-01-03"]`
* New data is loaded from the local file, `quickstart/tutorial/updates-data2.json`
* `appendToExisting` is set to `false`, indicating an overwrite task

Submit the ingestion task to overwrite the data:

```bash
bin/post-index-task --file quickstart/tutorial/updates-overwrite-index.json --url http://localhost:8081
```

When Druid finishes loading the new segment from this overwrite task, run the SELECT query again.
In the new results, the `tiger` row now has the value `lion`, the `aardvark` row has a different number, and the `giraffe` row has been replaced with a `bear` row.

```bash
dsql> SELECT * FROM "updates-tutorial";
┌──────────────────────────┬──────────┬───────┬────────┐
│ __time                   │ animal   │ count │ number │
├──────────────────────────┼──────────┼───────┼────────┤
│ 2018-01-01T01:01:00.000Z │ lion     │     1 │    100 │
│ 2018-01-01T03:01:00.000Z │ aardvark │     1 │   9999 │
│ 2018-01-01T04:01:00.000Z │ bear     │     1 │    111 │
└──────────────────────────┴──────────┴───────┴────────┘
Retrieved 3 rows in 0.02s.
```

## Combine existing data with new data and overwrite

Now append new data to the `updates-tutorial` datasource from `quickstart/tutorial/updates-data3.json` using the ingestion spec `quickstart/tutorial/updates-append-index.json`.

The spec directs Druid to read from the existing `updates-tutorial` datasource as well as the `quickstart/tutorial/updates-data3.json` file. The task combines data from the two input sources, then overwrites the original data with the new combined data.

Submit that task:

```bash
bin/post-index-task --file quickstart/tutorial/updates-append-index.json --url http://localhost:8081
```

When Druid finishes loading the new segment from this overwrite task, it adds the new rows to the datasource.
Run the SELECT query again. Druid automatically rolls up the data at ingestion time, aggregating the data in the `lion` row:

```bash
dsql> SELECT * FROM "updates-tutorial";
┌──────────────────────────┬──────────┬───────┬────────┐
│ __time                   │ animal   │ count │ number │
├──────────────────────────┼──────────┼───────┼────────┤
│ 2018-01-01T01:01:00.000Z │ lion     │     2 │    400 │
│ 2018-01-01T03:01:00.000Z │ aardvark │     1 │   9999 │
│ 2018-01-01T04:01:00.000Z │ bear     │     1 │    111 │
│ 2018-01-01T05:01:00.000Z │ mongoose │     1 │    737 │
│ 2018-01-01T06:01:00.000Z │ snake    │     1 │   1234 │
│ 2018-01-01T07:01:00.000Z │ octopus  │     1 │    115 │
└──────────────────────────┴──────────┴───────┴────────┘
Retrieved 6 rows in 0.02s.
```

## Append data

Now you append data to the datasource without changing the existing data.
Use the ingestion spec located at `quickstart/tutorial/updates-append-index2.json`.

The spec directs Druid to ingest data from `quickstart/tutorial/updates-data4.json` and append it to the `updates-tutorial` datasource. The property `appendToExisting` is set to `true` in this spec.

Submit the task:

```bash
bin/post-index-task --file quickstart/tutorial/updates-append-index2.json --url http://localhost:8081
```

Druid adds two additional rows after `octopus`. When the task completes, query the data again to see them.
Druid doesn't roll up the new `bear` row with the existing `bear` row because it stored the new data in a separate segment.

```bash
dsql> SELECT * FROM "updates-tutorial";
┌──────────────────────────┬──────────┬───────┬────────┐
│ __time                   │ animal   │ count │ number │
├──────────────────────────┼──────────┼───────┼────────┤
│ 2018-01-01T01:01:00.000Z │ lion     │     2 │    400 │
│ 2018-01-01T03:01:00.000Z │ aardvark │     1 │   9999 │
│ 2018-01-01T04:01:00.000Z │ bear     │     1 │    111 │
│ 2018-01-01T05:01:00.000Z │ mongoose │     1 │    737 │
│ 2018-01-01T06:01:00.000Z │ snake    │     1 │   1234 │
│ 2018-01-01T07:01:00.000Z │ octopus  │     1 │    115 │
│ 2018-01-01T04:01:00.000Z │ bear     │     1 │    222 │
│ 2018-01-01T09:01:00.000Z │ falcon   │     1 │   1241 │
└──────────────────────────┴──────────┴───────┴────────┘
Retrieved 8 rows in 0.02s.
```

Run the following groupBy query to see that the `bear` rows group together at query time:

```bash
dsql> SELECT __time, animal, SUM("count"), SUM("number") FROM "updates-tutorial" GROUP BY __time, animal;
┌──────────────────────────┬──────────┬────────┬────────┐
│ __time                   │ animal   │ EXPR$2 │ EXPR$3 │
├──────────────────────────┼──────────┼────────┼────────┤
│ 2018-01-01T01:01:00.000Z │ lion     │      2 │    400 │
│ 2018-01-01T03:01:00.000Z │ aardvark │      1 │   9999 │
│ 2018-01-01T04:01:00.000Z │ bear     │      2 │    333 │
│ 2018-01-01T05:01:00.000Z │ mongoose │      1 │    737 │
│ 2018-01-01T06:01:00.000Z │ snake    │      1 │   1234 │
│ 2018-01-01T07:01:00.000Z │ octopus  │      1 │    115 │
│ 2018-01-01T09:01:00.000Z │ falcon   │      1 │   1241 │
└──────────────────────────┴──────────┴────────┴────────┘
Retrieved 7 rows in 0.23s.
```
