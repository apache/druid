---
layout: doc_page
---

# Tutorial: Loading Batch Data

In this tutorial, we will learn about batch ingestion (as opposed to real-time ingestion) and how to create segments using the final piece of the Druid Cluster, the [indexing service](Indexing-Service.html). The indexing service is a standalone service that accepts [tasks](Tasks.html) in the form of POST requests. The output of most tasks are segments. The indexing service can be used as a single service for both real-time/streaming and batch ingestion.

The Data
--------
The data source we'll be using is Wikipedia edits. The data schema is:

Dimensions (things to filter on):

```json
"page"
"language"
"user"
"unpatrolled"
"newPage"
"robot"
"anonymous"
"namespace"
"continent"
"country"
"region"
"city"
```

Metrics (things to aggregate over):

```json
"count"
"added"
"delta"
"deleted"
```

Batch Ingestion
---------------

For the purposes of this tutorial, we are going to use our very small and simple Wikipedia data set. This data can directly be ingested via other means as shown in the previous [tutorial](Tutorial%3A-Loading-Your-Data-Part-1.html).

Our data is located at:

```
examples/indexing/wikipedia_data.json
```

The following events should exist in the file:

```json
{"timestamp": "2013-08-31T01:02:33Z", "page": "Gypsy Danger", "language" : "en", "user" : "nuclear", "unpatrolled" : "true", "newPage" : "true", "robot": "false", "anonymous": "false", "namespace":"article", "continent":"North America", "country":"United States", "region":"Bay Area", "city":"San Francisco", "added": 57, "deleted": 200, "delta": -143}
{"timestamp": "2013-08-31T03:32:45Z", "page": "Striker Eureka", "language" : "en", "user" : "speed", "unpatrolled" : "false", "newPage" : "true", "robot": "true", "anonymous": "false", "namespace":"wikipedia", "continent":"Australia", "country":"Australia", "region":"Cantebury", "city":"Syndey", "added": 459, "deleted": 129, "delta": 330}
{"timestamp": "2013-08-31T07:11:21Z", "page": "Cherno Alpha", "language" : "ru", "user" : "masterYi", "unpatrolled" : "false", "newPage" : "true", "robot": "true", "anonymous": "false", "namespace":"article", "continent":"Asia", "country":"Russia", "region":"Oblast", "city":"Moscow", "added": 123, "deleted": 12, "delta": 111}
{"timestamp": "2013-08-31T11:58:39Z", "page": "Crimson Typhoon", "language" : "zh", "user" : "triplets", "unpatrolled" : "true", "newPage" : "false", "robot": "true", "anonymous": "false", "namespace":"wikipedia", "continent":"Asia", "country":"China", "region":"Shanxi", "city":"Taiyuan", "added": 905, "deleted": 5, "delta": 900}
{"timestamp": "2013-08-31T12:41:27Z", "page": "Coyote Tango", "language" : "ja", "user" : "stringer", "unpatrolled" : "true", "newPage" : "false", "robot": "true", "anonymous": "false", "namespace":"wikipedia", "continent":"Asia", "country":"Japan", "region":"Kanto", "city":"Tokyo", "added": 1, "deleted": 10, "delta": -9}
```

#### Set Up a Druid Cluster

To index the data, we are going to need an indexing service, a historical node, and a coordinator node.

Note: If Zookeeper and MySQL aren't running, you'll have to start them again as described in [The Druid Cluster](Tutorial%3A-The-Druid-Cluster.html).

To start the Indexing Service:

```bash
java -Xmx2g -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath config/_common:config/overlord:lib/*:<hadoop_config_path> io.druid.cli.Main server overlord
```

To start the Coordinator Node:

```bash
java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath config/_common:config/coordinator:lib/* io.druid.cli.Main server coordinator
```

To start the Historical Node:

```bash
java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath config/_common:config/historical:lib/* io.druid.cli.Main server historical
```

#### Index the Data

There are two ways we can load the data, depending on the data volume. The simplest method of loading data is to use the [Index Task](Tasks.html). Index tasks can load batch data without any external dependencies. They are however, slow when the data volume exceeds 1G.

#### Index Task

To index the data and build a Druid segment, we are going to need to submit a task to the indexing service. This task should already exist:

```
examples/indexing/wikipedia_index_task.json
```

Open up the file to see the following:

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
          "timestampSpec" : {
            "column" : "timestamp",
            "format" : "auto"
          },
          "dimensionsSpec" : {
            "dimensions": ["page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city"],
            "dimensionExclusions" : [],
            "spatialDimensions" : []
          }
        }
      },
      "metricsSpec" : [
        {
          "type" : "count",
          "name" : "count"
        },
        {
          "type" : "doubleSum",
          "name" : "added",
          "fieldName" : "added"
        },
        {
          "type" : "doubleSum",
          "name" : "deleted",
          "fieldName" : "deleted"
        },
        {
          "type" : "doubleSum",
          "name" : "delta",
          "fieldName" : "delta"
        }
      ],
      "granularitySpec" : {
        "type" : "uniform",
        "segmentGranularity" : "DAY",
        "queryGranularity" : "NONE",
        "intervals" : [ "2013-08-31/2013-09-01" ]
      }
    },
    "ioConfig" : {
      "type" : "index",
      "firehose" : {
        "type" : "local",
        "baseDir" : "examples/indexing/",
        "filter" : "wikipedia_data.json"
       }
    },
    "tuningConfig" : {
      "type" : "index",
      "targetPartitionSize" : 0,
      "rowFlushBoundary" : 0
    }
  }
}
```

Okay, so what is happening here? The "type" field indicates the type of task we plan to run. In this case, it is a simple "index" task. The "parseSpec" indicates how we plan to figure out what the timestamp and dimension columns are. The "granularitySpec" indicates that we are building a daily segment for 2013-08-31 to 2013-09-01 and the minimum queryGranularity will be millisecond (NONE). Next, the "metricsSpec" indicate which fields in our data set we plan to build metric columns for. The "fieldName" corresponds to the metric name in the raw data. The "name" corresponds to what our metric column is actually going to be called in the segment. Finally, we have a local "firehose" that is going to read data from disk. We tell the firehose where our data is located and the types of files we are looking to ingest. In our case, we only have a single data file.

Let's send our task to the indexing service now:

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/indexing/wikipedia_index_task.json localhost:8090/druid/indexer/v1/task
```

Issuing the request should return a task ID like so:

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/indexing/wikipedia_index_task.json localhost:8090/druid/indexer/v1/task
{"task":"index_wikipedia_2013-10-09T21:30:32.802Z"}
```

In your indexing service logs, you should see the following:

```bash
2013-10-09 21:41:41,150 INFO [qtp300448720-21] io.druid.indexing.overlord.HeapMemoryTaskStorage - Inserting task index_wikipedia_2013-10-09T21:41:41.147Z with status: TaskStatus{id=index_wikipedia_2013-10-09T21:41:41.147Z, status=RUNNING, duration=-1}
2013-10-09 21:41:41,151 INFO [qtp300448720-21] io.druid.indexing.overlord.TaskLockbox - Created new TaskLockPosse: TaskLockPosse{taskLock=TaskLock{groupId=index_wikipedia_2013-10-09T21:41:41.147Z, dataSource=wikipedia, interval=2013-08-31T00:00:00.000Z/2013-09-01T00:00:00.000Z, version=2013-10-09T21:41:41.151Z}, taskIds=[]}
...
013-10-09 21:41:41,215 INFO [pool-6-thread-1] io.druid.indexing.overlord.ForkingTaskRunner - Logging task index_wikipedia_2013-10-09T21:41:41.147Z_generator_2013-08-31T00:00:00.000Z_2013-09-01T00:00:00.000Z_0 output to: /tmp/persistent/index_wikipedia_2013-10-09T21:41:41.147Z_generator_2013-08-31T00:00:00.000Z_2013-09-01T00:00:00.000Z_0/b5099fdb-d6b0-4b81-9053-b2af70336a7e/log
2013-10-09 21:41:45,017 INFO [qtp300448720-22] io.druid.indexing.common.actions.LocalTaskActionClient - Performing action for task[index_wikipedia_2013-10-09T21:41:41.147Z_generator_2013-08-31T00:00:00.000Z_2013-09-01T00:00:00.000Z_0]: LockListAction{}

````

After a few seconds, the task should complete and you should see in the indexing service logs:

```bash
2013-10-09 21:41:45,765 INFO [pool-6-thread-1] io.druid.indexing.overlord.exec.TaskConsumer - Received SUCCESS status for task: IndexGeneratorTask{id=index_wikipedia_2013-10-09T21:41:41.147Z_generator_2013-08-31T00:00:00.000Z_2013-09-01T00:00:00.000Z_0, type=index_generator, dataSource=wikipedia, interval=Optional.of(2013-08-31T00:00:00.000Z/2013-09-01T00:00:00.000Z)}
```

Congratulations! The segment has completed building. Once a segment is built, a segment metadata entry is created in your metadata storage table. The coordinator compares what is in the segment metadata table with what is in the cluster. A new entry in the metadata table will cause the coordinator to load the new segment in a minute or so.

You should see the following logs on the coordinator:

```bash
2013-10-09 21:41:54,368 INFO [Coordinator-Exec--0] io.druid.server.coordinator.helper.DruidCoordinatorLogger - [_default_tier] : Assigned 1 segments among 1 servers
2013-10-09 21:41:54,369 INFO [Coordinator-Exec--0] io.druid.server.coordinator.helper.DruidCoordinatorLogger - Load Queues:
2013-10-09 21:41:54,369 INFO [Coordinator-Exec--0] io.druid.server.coordinator.helper.DruidCoordinatorLogger - Server[localhost:8083, historical, _default_tier] has 1 left to load, 0 left to drop, 4,477 bytes queued, 4,477 bytes served.
```

These logs indicate that the coordinator has assigned our new segment to the historical node to download and serve. If you look at the historical node logs, you should see:

```bash
2013-10-09 21:41:54,369 INFO [ZkCoordinator-0] io.druid.server.coordination.ZkCoordinator - Loading segment wikipedia_2013-08-31T00:00:00.000Z_2013-09-01T00:00:00.000Z_2013-10-09T21:41:41.151Z
2013-10-09 21:41:54,369 INFO [ZkCoordinator-0] io.druid.segment.loading.LocalDataSegmentPuller - Unzipping local file[/tmp/druid/localStorage/wikipedia/2013-08-31T00:00:00.000Z_2013-09-01T00:00:00.000Z/2013-10-09T21:41:41.151Z/0/index.zip] to [/tmp/druid/indexCache/wikipedia/2013-08-31T00:00:00.000Z_2013-09-01T00:00:00.000Z/2013-10-09T21:41:41.151Z/0]
2013-10-09 21:41:54,370 INFO [ZkCoordinator-0] io.druid.utils.CompressionUtils - Unzipping file[/tmp/druid/localStorage/wikipedia/2013-08-31T00:00:00.000Z_2013-09-01T00:00:00.000Z/2013-10-09T21:41:41.151Z/0/index.zip] to [/tmp/druid/indexCache/wikipedia/2013-08-31T00:00:00.000Z_2013-09-01T00:00:00.000Z/2013-10-09T21:41:41.151Z/0]
2013-10-09 21:41:54,380 INFO [ZkCoordinator-0] io.druid.server.coordination.SingleDataSegmentAnnouncer - Announcing segment[wikipedia_2013-08-31T00:00:00.000Z_2013-09-01T00:00:00.000Z_2013-10-09T21:41:41.151Z] to path[/druid/servedSegments/localhost:8083/wikipedia_2013-08-31T00:00:00.000Z_2013-09-01T00:00:00.000Z_2013-10-09T21:41:41.151Z]
```

Once the segment is announced the segment is queryable. Now you should be able to query the data.

Issuing a [TimeBoundaryQuery](TimeBoundaryQuery.html) should yield:

```json
[ {
  "timestamp" : "2013-08-31T01:02:33.000Z",
  "result" : {
    "minTime" : "2013-08-31T01:02:33.000Z",
    "maxTime" : "2013-08-31T12:41:27.000Z"
  }
} ]
```

Console
--------

The indexing service overlord has a console located at:

```bash
localhost:8090/console.html
```

On this console, you can look at statuses and logs of recently submitted and completed tasks.

If you decide to reuse the local firehose to ingest your own data and if you run into problems, you can use the console to read the individual task logs.

Task logs can be stored locally or uploaded to [Deep Storage](Deep-Storage.html). More information about how to configure this is [here](Configuration.html).

Most common data ingestion problems are around timestamp formats and other malformed data issues.

#### Hadoop Index Task

Druid is designed for large data volumes, and most real-world data sets require batch indexing be done through a Hadoop job.

For this tutorial, we used [Hadoop 2.3.0](https://archive.apache.org/dist/hadoop/core/hadoop-2.3.0/). There are many pages on the Internet showing how to set up a single-node (standalone) Hadoop cluster, which is all that's needed for this example.

Before indexing the data, make sure you have a valid Hadoop cluster running. To build our Druid segment, we are going to submit a [Hadoop index task](Tasks.html) to the indexing service. The grammar for the Hadoop index task is very similar to the index task of the last tutorial. The tutorial Hadoop index task should be located at:

```
examples/indexing/wikipedia_index_hadoop_task.json
```

Examining the contents of the file, you should find:

  ```json
  {
    "type" : "index_hadoop",
    "spec" : {
      "dataSchema" : {
        "dataSource" : "wikipedia",
        "parser" : {
          "type" : "string",
          "parseSpec" : {
            "format" : "json",
            "timestampSpec" : {
              "column" : "timestamp",
              "format" : "auto"
            },
            "dimensionsSpec" : {
              "dimensions": ["page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city"],
              "dimensionExclusions" : [],
              "spatialDimensions" : []
            }
          }
        },
        "metricsSpec" : [
          {
            "type" : "count",
            "name" : "count"
          },
          {
            "type" : "doubleSum",
            "name" : "added",
            "fieldName" : "added"
          },
          {
            "type" : "doubleSum",
            "name" : "deleted",
            "fieldName" : "deleted"
          },
          {
            "type" : "doubleSum",
            "name" : "delta",
            "fieldName" : "delta"
          }
        ],
        "granularitySpec" : {
          "type" : "uniform",
          "segmentGranularity" : "DAY",
          "queryGranularity" : "NONE",
          "intervals" : [ "2013-08-31/2013-09-01" ]
        }
      },
      "ioConfig" : {
        "type" : "hadoop",
        "inputSpec" : {
          "type" : "static",
          "paths" : "/MyDirectory/examples/indexing/wikipedia_data.json"
        }
      }
    }
  }
  ```

If you are curious about what all this configuration means, see [here](Tasks.html).

To submit the task:

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/indexing/wikipedia_index_hadoop_task.json localhost:8090/druid/indexer/v1/task
```

After the task is completed, the segment should be assigned to your historical node. You should be able to query the segment.

Next Steps
----------
We demonstrated using the indexing service as a way to ingest data into Druid. Previous versions of Druid used the [HadoopDruidIndexer](Batch-ingestion.html) to ingest batch data. The `HadoopDruidIndexer` still remains a valid option for batch ingestion, however, we recommend using the indexing service as the preferred method of getting batch data into Druid.

Additional Information
----------------------

Getting data into Druid can definitely be difficult for first time users. Please don't hesitate to ask questions in our IRC channel or on our [google groups page](https://groups.google.com/forum/#!forum/druid-user).
