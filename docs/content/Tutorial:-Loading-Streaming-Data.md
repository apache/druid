---
layout: doc_page
---

# Tutorial: Loading Streaming Data
In our last [tutorial](Tutorial%3A-The-Druid-Cluster.html), we set up a complete Druid cluster. We created all the Druid dependencies and ingested streaming data. In this tutorial, we will expand upon what we've done in the first two tutorials.

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

At this point, you should already have Druid downloaded and are comfortable with running a Druid cluster locally. If you are not, see [here](Tutorial%3A-The-Druid-Cluster.html). If Zookeeper and MySQL aren't running, you'll have to start them again as described in [The Druid Cluster](Tutorial%3A-The-Druid-Cluster.html).

With real-world data, we recommend having a message bus such as [Apache Kafka](http://kafka.apache.org/) sit between the data stream and the real-time node. The message bus provides higher availability for production environments. [Firehoses](Firehose.html) are the key abstraction for real-time ingestion.

<a id="set-up-kafka"></a>
#### Setting up Kafka

[KafkaFirehoseFactory](Firehose.html) is how druid communicates with Kafka. Using this [Firehose](Firehose.html) with the right configuration, we can import data into Druid in real-time without writing any code. To load data to a real-time node via Kafka, we'll first need to initialize Zookeeper and Kafka, and then configure and initialize a [Realtime](Realtime.html) node.

The following quick-start instructions for booting a Zookeeper and then Kafka cluster were taken from the [Kafka website](http://kafka.apache.org/07/quickstart.html).

1. Download Apache Kafka 0.7.2 from [http://kafka.apache.org/downloads.html](http://kafka.apache.org/downloads.html)

  ```bash
  wget http://archive.apache.org/dist/kafka/old_releases/kafka-0.7.2-incubating/kafka-0.7.2-incubating-src.tgz
  tar -xvzf kafka-0.7.2-incubating-src.tgz
  cd kafka-0.7.2-incubating-src
  ```

2. Build Kafka

  ```bash
  ./sbt update
  ./sbt package
  ```

3. Boot Kafka

  ```bash
  cat config/zookeeper.properties
  bin/zookeeper-server-start.sh config/zookeeper.properties

  # in a new console
  bin/kafka-server-start.sh config/server.properties
  ```

4. Launch the console producer (so you can type in JSON kafka messages in a bit)

  ```bash
  bin/kafka-console-producer.sh --zookeeper localhost:2181 --topic wikipedia
  ```

  When things are ready, you should see log messages such as:

  ```
  [2013-10-09 22:03:07,802] INFO zookeeper state changed (SyncConnected) (org.I0Itec.zkclient.ZkClient)
  ```

#### Launch a Realtime Node

You should be comfortable starting Druid nodes at this point. If not, it may be worthwhile to revisit the first few tutorials.

1. Real-time nodes can be started with:

  ```bash
  java -Xmx512m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Ddruid.realtime.specFile=examples/indexing/wikipedia.spec -classpath lib/*:config/realtime io.druid.cli.Main server realtime
  ```

2. A realtime.spec should already exist for the data source in the Druid tarball. You should be able to find it at:

  ```bash
  examples/indexing/wikipedia.spec
  ```

  The contents of the file should match:

  ```json
  [
    {
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
        "metricsSpec" : [{
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
        "granularitySpec" : {
          "type" : "uniform",
          "segmentGranularity" : "DAY",
          "queryGranularity" : "NONE",
          "intervals" : [ "2013-08-31/2013-09-01" ]
        }
      },
      "ioConfig" : {
        "type" : "realtime",
        "firehose": {
          "type": "kafka-0.7.2",
          "consumerProps": {
            "zk.connect": "localhost:2181",
            "zk.connectiontimeout.ms": "15000",
            "zk.sessiontimeout.ms": "15000",
            "zk.synctime.ms": "5000",
            "groupid": "druid-example",
            "fetch.size": "1048586",
            "autooffset.reset": "largest",
            "autocommit.enable": "false"
          },
          "feed": "wikipedia"
        },
        "plumber": {
          "type": "realtime"
        }
      },
      "tuningConfig": {
        "type" : "realtime",
        "maxRowsInMemory": 500000,
        "intermediatePersistPeriod": "PT10m",
        "windowPeriod": "PT10m",
        "basePersistDirectory": "\/tmp\/realtime\/basePersist",
        "rejectionPolicy": {
          "type": "messageTime"
        }
      }
    }
  ]
  ```

Note: This config uses a "messageTime" [rejection policy](Plumber.html) which will accept all events and hand off as long as there is a continuous stream of events. In this particular example, hand-off will not actually occur because we only have a few events.

3. Let's copy and paste some data into the Kafka console producer

  ```json
  {"timestamp": "2013-08-31T01:02:33Z", "page": "Gypsy Danger", "language" : "en", "user" : "nuclear", "unpatrolled" : "true", "newPage" : "true", "robot": "false", "anonymous": "false", "namespace":"article", "continent":"North America", "country":"United States", "region":"Bay Area", "city":"San Francisco", "added": 57, "deleted": 200, "delta": -143}
  {"timestamp": "2013-08-31T03:32:45Z", "page": "Striker Eureka", "language" : "en", "user" : "speed", "unpatrolled" : "false", "newPage" : "true", "robot": "true", "anonymous": "false", "namespace":"wikipedia", "continent":"Australia", "country":"Australia", "region":"Cantebury", "city":"Syndey", "added": 459, "deleted": 129, "delta": 330}
  {"timestamp": "2013-08-31T07:11:21Z", "page": "Cherno Alpha", "language" : "ru", "user" : "masterYi", "unpatrolled" : "false", "newPage" : "true", "robot": "true", "anonymous": "false", "namespace":"article", "continent":"Asia", "country":"Russia", "region":"Oblast", "city":"Moscow", "added": 123, "deleted": 12, "delta": 111}
  {"timestamp": "2013-08-31T11:58:39Z", "page": "Crimson Typhoon", "language" : "zh", "user" : "triplets", "unpatrolled" : "true", "newPage" : "false", "robot": "true", "anonymous": "false", "namespace":"wikipedia", "continent":"Asia", "country":"China", "region":"Shanxi", "city":"Taiyuan", "added": 905, "deleted": 5, "delta": 900}
  {"timestamp": "2013-08-31T12:41:27Z", "page": "Coyote Tango", "language" : "ja", "user" : "stringer", "unpatrolled" : "true", "newPage" : "false", "robot": "true", "anonymous": "false", "namespace":"wikipedia", "continent":"Asia", "country":"Japan", "region":"Kanto", "city":"Tokyo", "added": 1, "deleted": 10, "delta": -9}
  ```

  Disclaimer: We recognize the timestamps of these events aren't actually recent.

5. Watch the events as they are ingested by Druid's real-time node:

  ```bash
  ...
  2013-10-10 05:13:18,976 INFO [chief-wikipedia] io.druid.server.coordination.BatchDataSegmentAnnouncer - Announcing segment[wikipedia_2013-08-31T01:00:00.000Z_2013-08-31T02:00:00.000Z_2013-08-31T01:00:00.000Z] at path[/druid/segments/localhost:8083/2013-10-10T05:13:18.972Z0]
  2013-10-10 05:13:18,992 INFO [chief-wikipedia] io.druid.server.coordination.BatchDataSegmentAnnouncer - Announcing segment[wikipedia_2013-08-31T03:00:00.000Z_2013-08-31T04:00:00.000Z_2013-08-31T03:00:00.000Z] at path[/druid/segments/localhost:8083/2013-10-10T05:13:18.972Z0]
  2013-10-10 05:13:18,997 INFO [chief-wikipedia] io.druid.server.coordination.BatchDataSegmentAnnouncer - Announcing segment[wikipedia_2013-08-31T07:00:00.000Z_2013-08-31T08:00:00.000Z_2013-08-31T07:00:00.000Z] at path[/druid/segments/localhost:8083/2013-10-10T05:13:18.972Z0]
  2013-10-10 05:13:19,003 INFO [chief-wikipedia] io.druid.server.coordination.BatchDataSegmentAnnouncer - Announcing segment[wikipedia_2013-08-31T11:00:00.000Z_2013-08-31T12:00:00.000Z_2013-08-31T11:00:00.000Z] at path[/druid/segments/localhost:8083/2013-10-10T05:13:18.972Z0]
  2013-10-10 05:13:19,008 INFO [chief-wikipedia] io.druid.server.coordination.BatchDataSegmentAnnouncer - Announcing segment[wikipedia_2013-08-31T12:00:00.000Z_2013-08-31T13:00:00.000Z_2013-08-31T12:00:00.000Z] at path[/druid/segments/localhost:8083/2013-10-10T05:13:18.972Z0]
  ...
  ```

Issuing a [TimeBoundaryQuery](TimeBoundaryQuery.html) to the real-time node should yield valid results:

```json
[
  {
    "timestamp" : "2013-08-31T01:02:33.000Z",
    "result" : {
      "minTime" : "2013-08-31T01:02:33.000Z",
      "maxTime" : "2013-08-31T12:41:27.000Z"
    }
  }
]
```

#### Advanced Streaming Ingestion

Druid offers an additional method of ingesting streaming data via the indexing service. You may be wondering why a second method is needed. Standalone real-time nodes are sufficient for certain volumes of data and availability tolerances. They pull data from a message queue like Kafka or Rabbit, index data locally, and periodically finalize segments for handoff to historical nodes. They are fairly straightforward to scale, simply taking advantage of the innate scalability of the backing message queue. But they are difficult to make highly available with Kafka, the most popular supported message queue, because its high-level consumer doesn’t provide a way to scale out two replicated consumer groups such that each one gets the same data in the same shard. They also become difficult to manage once you have a lot of them, since every machine needs a unique configuration.

Druid solved the availability problem by switching from a pull-based model to a push-based model; rather than Druid indexers pulling data from Kafka, another process pulls data and pushes the data into Druid. Since with the push based model, we can ensure that the same data makes it into the same shard, we can replicate data. The [indexing service](Indexing-Service.html) encapsulates this functionality, where a task-and-resources model replaces a standalone machine model. In addition to simplifying machine configuration, the model also allows nodes to run in the cloud with an elastic number of machines. If you are interested in this form of real-time ingestion, please check out the client library [Tranquility](https://github.com/metamx/tranquility).

Additional Information
----------------------

Getting data into Druid can definitely be difficult for first time users. Please don't hesitate to ask questions in our IRC channel or on our [google groups page](https://groups.google.com/forum/#!forum/druid-development).

