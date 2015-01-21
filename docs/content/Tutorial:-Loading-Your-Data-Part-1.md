---
layout: doc_page
---

# Tutorial: Loading Your Data (Part 1)
In our last [tutorial](Tutorial%3A-The-Druid-Cluster.html), we set up a complete Druid cluster. We created all the Druid dependencies and loaded some batched data. Druid shards data into self-contained chunks known as [segments](Segments.html). Segments are the fundamental unit of storage in Druid and all Druid nodes only understand segments.

In this tutorial, we will learn about batch ingestion (as opposed to real-time ingestion) and how to create segments using the final piece of the Druid Cluster, the [indexing service](Indexing-Service.html). The indexing service is a standalone service that accepts [tasks](Tasks.html) in the form of POST requests. The output of most tasks are segments.

If you are interested more about ingesting your own data into Druid, skip to the next [tutorial](Tutorial%3A-Loading-Your-Data-Part-2.html).

About the data
--------------

The data source we'll be working with is Wikipedia edits once again. The data schema is the same as the previous tutorials:

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
Setting Up
----------

At this point, you should already have Druid downloaded and are comfortable with running a Druid cluster locally. If you are not, see [here](Tutorial%3A-The-Druid-Cluster.html).

Let's start from our usual starting point in the tarball directory.

Segments require data, so before we can build a Druid segment, we are going to need some raw data. Make sure that the following file exists:

```
examples/indexing/wikipedia_data.json
```

Open the file and make sure the following events exist:

```json
{"timestamp": "2013-08-31T01:02:33Z", "page": "Gypsy Danger", "language" : "en", "user" : "nuclear", "unpatrolled" : "true", "newPage" : "true", "robot": "false", "anonymous": "false", "namespace":"article", "continent":"North America", "country":"United States", "region":"Bay Area", "city":"San Francisco", "added": 57, "deleted": 200, "delta": -143}
{"timestamp": "2013-08-31T03:32:45Z", "page": "Striker Eureka", "language" : "en", "user" : "speed", "unpatrolled" : "false", "newPage" : "true", "robot": "true", "anonymous": "false", "namespace":"wikipedia", "continent":"Australia", "country":"Australia", "region":"Cantebury", "city":"Syndey", "added": 459, "deleted": 129, "delta": 330}
{"timestamp": "2013-08-31T07:11:21Z", "page": "Cherno Alpha", "language" : "ru", "user" : "masterYi", "unpatrolled" : "false", "newPage" : "true", "robot": "true", "anonymous": "false", "namespace":"article", "continent":"Asia", "country":"Russia", "region":"Oblast", "city":"Moscow", "added": 123, "deleted": 12, "delta": 111}
{"timestamp": "2013-08-31T11:58:39Z", "page": "Crimson Typhoon", "language" : "zh", "user" : "triplets", "unpatrolled" : "true", "newPage" : "false", "robot": "true", "anonymous": "false", "namespace":"wikipedia", "continent":"Asia", "country":"China", "region":"Shanxi", "city":"Taiyuan", "added": 905, "deleted": 5, "delta": 900}
{"timestamp": "2013-08-31T12:41:27Z", "page": "Coyote Tango", "language" : "ja", "user" : "stringer", "unpatrolled" : "true", "newPage" : "false", "robot": "true", "anonymous": "false", "namespace":"wikipedia", "continent":"Asia", "country":"Japan", "region":"Kanto", "city":"Tokyo", "added": 1, "deleted": 10, "delta": -9}
```

There are five data points spread across the day of 2013-08-31. Talk about big data right? Thankfully, we don't need a ton of data to introduce how batch ingestion works.

In order to ingest and query this data, we are going to need to run a historical node, a coordinator node, and an indexing service to run the batch ingestion.

Note: If Zookeeper and MySQL aren't running, you'll have to start them again as described in [The Druid Cluster](Tutorial%3A-The-Druid-Cluster.html).

#### Starting a Local Indexing Service

The simplest indexing service we can start up is to run an [overlord](Indexing-Service.html) node in local mode. You can do so by issuing:

```bash
java -Xmx2g -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/*:config/overlord io.druid.cli.Main server overlord
```

The overlord configurations should already exist in:

```
config/overlord/runtime.properties
```

The configurations for the overlord node are as follows:

```bash
druid.host=localhost
druid.port=8087
druid.service=overlord

druid.zk.service.host=localhost

druid.extensions.coordinates=["io.druid.extensions:druid-kafka-seven:0.6.171"]

druid.db.connector.connectURI=jdbc:mysql://localhost:3306/druid
druid.db.connector.user=druid
druid.db.connector.password=diurd

druid.selectors.indexing.serviceName=overlord
druid.indexer.queue.startDelay=PT0M
druid.indexer.runner.javaOpts="-server -Xmx256m"
druid.indexer.fork.property.druid.processing.numThreads=1
druid.indexer.fork.property.druid.computation.buffer.size=100000000
```

If you are interested in reading more about these configurations, see [here](Indexing-Service.html).

#### Starting Other Nodes

Just in case you forgot how, let's start up the other nodes we require:

Coordinator node:

```bash
java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/*:config/coordinator io.druid.cli.Main server coordinator
```

Historical node:

```bash
java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/*:config/historical io.druid.cli.Main server historical
```

Note: Historical, real-time and broker nodes share the same query interface. Hence, we do not explicitly need a broker node for this tutorial. All queries can go against the historical node directly.

Once all the nodes are up and running, we are ready to index some data.

Indexing the Data
-----------------

To index the data and build a Druid segment, we are going to need to submit a task to the indexing service. This task should already exist:

```
examples/indexing/wikipedia_index_task.json
```

Open up the file to see the following:

```json
{
  "type" : "index",
  "dataSource" : "wikipedia",
  "granularitySpec" : {
    "type" : "uniform",
    "gran" : "DAY",
    "intervals" : [ "2013-08-31/2013-09-01" ]
  },
  "aggregators" : [{
     "type" : "count",
     "name" : "count"
    }, {
     "type" : "doubleSum",
     "name" : "added",
     "fieldName" : "added"
    }, {
     "type" : "doubleSum",
     "name" : "deleted",
     "fieldName" : "deleted"
    }, {
     "type" : "doubleSum",
     "name" : "delta",
     "fieldName" : "delta"
  }],
  "firehose" : {
    "type" : "local",
    "baseDir" : "examples/indexing",
    "filter" : "wikipedia_data.json",
    "parser" : {
      "timestampSpec" : {
        "column" : "timestamp"
      },
      "data" : {
        "format" : "json",
        "dimensions" : ["page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city"]
      }
    }
  }
}
```

Okay, so what is happening here? The "type" field indicates the type of task we plan to run. In this case, it is a simple "index" task. The "granularitySpec" indicates that we are building a daily segment for 2013-08-31 to 2013-09-01. Next, the "aggregators" indicate which fields in our data set we plan to build metric columns for. The "fieldName" corresponds to the metric name in the raw data. The "name" corresponds to what our metric column is actually going to be called in the segment. Finally, we have a local "firehose" that is going to read data from disk. We tell the firehose where our data is located and the types of files we are looking to ingest. In our case, we only have a single data file.

Let's send our task to the indexing service now:

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/indexing/wikipedia_index_task.json localhost:8087/druid/indexer/v1/task
```

Issuing the request should return a task ID like so:

```bash
$ curl -X 'POST' -H 'Content-Type:application/json' -d @examples/indexing/wikipedia_index_task.json localhost:8087/druid/indexer/v1/task
{"task":"index_wikipedia_2013-10-09T21:30:32.802Z"}
$
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

Congratulations! The segment has completed building. Once a segment is built, a segment metadata entry is created in your MySQL table. The coordinator compares what is in the segment metadata table with what is in the cluster. A new entry in the metadata table will cause the coordinator to load the new segment in a minute or so.

You should see the following logs on the coordinator:

```bash
2013-10-09 21:41:54,368 INFO [Coordinator-Exec--0] io.druid.server.coordinator.helper.DruidCoordinatorLogger - [_default_tier] : Assigned 1 segments among 1 servers
2013-10-09 21:41:54,369 INFO [Coordinator-Exec--0] io.druid.server.coordinator.helper.DruidCoordinatorLogger - Load Queues:
2013-10-09 21:41:54,369 INFO [Coordinator-Exec--0] io.druid.server.coordinator.helper.DruidCoordinatorLogger - Server[localhost:8081, historical, _default_tier] has 1 left to load, 0 left to drop, 4,477 bytes queued, 4,477 bytes served.
```

These logs indicate that the coordinator has assigned our new segment to the historical node to download and serve. If you look at the historical node logs, you should see:

```bash
2013-10-09 21:41:54,369 INFO [ZkCoordinator-0] io.druid.server.coordination.ZkCoordinator - Loading segment wikipedia_2013-08-31T00:00:00.000Z_2013-09-01T00:00:00.000Z_2013-10-09T21:41:41.151Z
2013-10-09 21:41:54,369 INFO [ZkCoordinator-0] io.druid.segment.loading.LocalDataSegmentPuller - Unzipping local file[/tmp/druid/localStorage/wikipedia/2013-08-31T00:00:00.000Z_2013-09-01T00:00:00.000Z/2013-10-09T21:41:41.151Z/0/index.zip] to [/tmp/druid/indexCache/wikipedia/2013-08-31T00:00:00.000Z_2013-09-01T00:00:00.000Z/2013-10-09T21:41:41.151Z/0]
2013-10-09 21:41:54,370 INFO [ZkCoordinator-0] io.druid.utils.CompressionUtils - Unzipping file[/tmp/druid/localStorage/wikipedia/2013-08-31T00:00:00.000Z_2013-09-01T00:00:00.000Z/2013-10-09T21:41:41.151Z/0/index.zip] to [/tmp/druid/indexCache/wikipedia/2013-08-31T00:00:00.000Z_2013-09-01T00:00:00.000Z/2013-10-09T21:41:41.151Z/0]
2013-10-09 21:41:54,380 INFO [ZkCoordinator-0] io.druid.server.coordination.SingleDataSegmentAnnouncer - Announcing segment[wikipedia_2013-08-31T00:00:00.000Z_2013-09-01T00:00:00.000Z_2013-10-09T21:41:41.151Z] to path[/druid/servedSegments/localhost:8081/wikipedia_2013-08-31T00:00:00.000Z_2013-09-01T00:00:00.000Z_2013-10-09T21:41:41.151Z]
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
localhost:8087/console.html
```

On this console, you can look at statuses and logs of recently submitted and completed tasks.

If you decide to reuse the local firehose to ingest your own data and if you run into problems, you can use the console to read the individual task logs.

Task logs can be stored locally or uploaded to [Deep Storage](Deep-Storage.html). More information about how to configure this is [here](Configuration.html).

Most common data ingestion problems are around timestamp formats and other malformed data issues.

Next Steps
----------

This tutorial covered ingesting a small batch data set and loading it into Druid. In [Loading Your Data Part 2](Tutorial%3A-Loading-Your-Data-Part-2.html), we will cover how to ingest data using Hadoop for larger data sets.

Note: The index task and local firehose can be used to ingest your own data if the size of that data is relatively small (< 1G). The index task is fairly slow and we highly recommend using the Hadoop Index Task for ingesting larger quantities of data.

Additional Information
----------------------

Getting data into Druid can definitely be difficult for first time users. Please don't hesitate to ask questions in our IRC channel or on our [google groups page](https://groups.google.com/forum/#!forum/druid-development).
