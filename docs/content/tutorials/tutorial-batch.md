---
layout: doc_page
---

# Tutorial: Loading a file

## Getting started

This tutorial demonstrates how to perform a batch file load, using Druid's native batch ingestion.

For this tutorial, we'll assume you've already downloaded Druid as described in 
the [single-machine quickstart](index.html) and have it running on your local machine. You 
don't need to have loaded any data yet.

## Preparing the data and the ingestion task spec

A data load is initiated by submitting an *ingestion task* spec to the Druid overlord. For this tutorial, we'll be loading the sample Wikipedia page edits data.

We have provided an ingestion spec at `examples/wikipedia-index.json`, shown here for convenience,
which has been configured to read the `quickstart/wikiticker-2015-09-12-sampled.json.gz` input file:

```json
{
  "type" : "index",
  "spec" : {
    "dataSchema" : {
      "dataSource" : "wikipedia",
      "parser" : {
        "type" : "string",
        "parseSpec" : {
          "format" : "json",
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
          }
        }
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
      "type" : "index",
      "firehose" : {
        "type" : "local",
        "baseDir" : "quickstart/",
        "filter" : "wikiticker-2015-09-12-sampled.json.gz"
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

This spec will create a datasource named "wikipedia".

## Load batch data

We've included a sample of Wikipedia edits from September 12, 2015 to get you started.

To load this data into Druid, you can submit an *ingestion task* pointing to the file. To submit
this task, POST it to Druid in a new terminal window from the druid-#{DRUIDVERSION} directory:

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/wikipedia-index.json http://localhost:8090/druid/indexer/v1/task
```

Which will print the ID of the task if the submission was successful:

```bash
{"task":"index_wikipedia_2018-06-09T21:30:32.802Z"}
```

To view the status of your ingestion task, go to your overlord console:
[http://localhost:8090/console.html](http://localhost:8090/console.html). You can refresh the console periodically, and after
the task is successful, you should see a "SUCCESS" status for the task.

After your ingestion task finishes, the data will be loaded by historical nodes and available for
querying within a minute or two. You can monitor the progress of loading your data in the
coordinator console, by checking whether there is a datasource "wikipedia" with a blue circle
indicating "fully available": [http://localhost:8081/#/](http://localhost:8081/#/).

![Coordinator console](../tutorials/img/tutorial-batch-01.png "Wikipedia 100% loaded")

## Querying your data

Your data should become fully available within a minute or two. You can monitor this process on 
your Coordinator console at [http://localhost:8081/#/](http://localhost:8081/#/).

Once the data is loaded, please follow the [query tutorial](../tutorials/tutorial-query.html) to run some example queries on the newly loaded data.

## Cleanup

If you wish to go through any of the other ingestion tutorials, you will need to reset the cluster and follow these [reset instructions](index.html#resetting-cluster-state), as the other tutorials will write to the same "wikipedia" datasource.

## Further reading

For more information on loading batch data, please see [the batch ingestion documentation](../ingestion/batch-ingestion.html).
