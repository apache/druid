---
id: tutorial-batch
title: "Load a file"
sidebar_label: "Load files natively"
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


This tutorial demonstrates how to load data into Apache Druid from a file using Apache Druid's native batch ingestion feature.

You initiate data loading in Druid by submitting an *ingestion task* spec to the Druid Overlord. You can write ingestion 
specs by hand or using the _data loader_ built into the web console. 

For production environments, it's likely that you'll want to automate data ingestion. This tutorial starts by showing
you how to submit an ingestion spec directly in the web console, and then introduces ways to ingest batch data that
lend themselves to automation&mdash;from the command line. 

## Loading data with a spec (via console)

The Druid package includes the following sample native batch ingestion task spec at `quickstart/tutorial/wikipedia-index.json`, shown here for convenience,
which has been configured to read the `quickstart/tutorial/wikiticker-2015-09-12-sampled.json.gz` input file:

```json
{
  "type" : "index_parallel",
  "spec" : {
    "dataSchema" : {
      "dataSource" : "wikipedia",
      "dimensionsSpec" : {
        "dimensions" : [
          "channel",
          "cityName",
          "comment",
          "countryIsoCode",
          "countryName",
          "isAnonymous",
          "isMinor",
          "isNew",
          "isRobot",
          "isUnpatrolled",
          "metroCode",
          "namespace",
          "page",
          "regionIsoCode",
          "regionName",
          "user",
          { "name": "added", "type": "long" },
          { "name": "deleted", "type": "long" },
          { "name": "delta", "type": "long" }
        ]
      },
      "timestampSpec": {
        "column": "time",
        "format": "iso"
      },
      "metricsSpec" : [],
      "granularitySpec" : {
        "type" : "uniform",
        "segmentGranularity" : "day",
        "queryGranularity" : "none",
        "intervals" : ["2015-09-12/2015-09-13"],
        "rollup" : false
      }
    },
    "ioConfig" : {
      "type" : "index_parallel",
      "inputSource" : {
        "type" : "local",
        "baseDir" : "quickstart/tutorial/",
        "filter" : "wikiticker-2015-09-12-sampled.json.gz"
      },
      "inputFormat" :  {
        "type": "json"
      },
      "appendToExisting" : false
    },
    "tuningConfig" : {
      "type" : "index_parallel",
      "partitionsSpec": {
        "type": "dynamic"
      },
      "maxRowsInMemory" : 25000
    }
  }
}
```

This spec creates a datasource named "wikipedia".

From the Ingestion view, click the ellipses next to Tasks and choose `Submit JSON task`.

![Tasks view add task](../assets/tutorial-batch-submit-task-01.png "Tasks view add task")

This brings up the spec submission dialog where you can paste the spec above.

![Query view](../assets/tutorial-batch-submit-task-02.png "Query view")

Once the spec is submitted, wait a few moments for the data to load, after which you can query it.


## Loading data from the command line

To load data from the command line, you need to POST an ingestion spec to the Druid Overlord using its task API.

Run the following command from Druid package root:

```bash
curl -X POST http://localhost:8081/druid/indexer/v1/task \
  -H "Content-Type: application/json" \
  -d @quickstart/tutorial/wikipedia-index.json
```
If the submission was successful, Druid will print a task ID similar to the following:

```bash
{"task":"index_parallel_wikipedia_oiemabcp_2026-07-07T16:05:33.242Z"}
```

Wait a few moments for the data to load, then move on to querying it.


## Querying your data

Once the data is loaded, please follow the [query tutorial](../tutorials/tutorial-query.md) to run some example queries on the newly loaded data.


## Cleanup

If you wish to go through any of the other ingestion tutorials, you will need to shut down the cluster and reset the cluster state by removing the contents of the `var` directory under the druid package, as the other tutorials will write to the same "wikipedia" datasource.


## Further reading

For more information on loading batch data, please see [the native batch ingestion documentation](../ingestion/native-batch.md).
