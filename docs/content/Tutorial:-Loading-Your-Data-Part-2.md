---
layout: doc_page
---

# Tutorial: Loading Your Data (Part 2)
In this tutorial we will cover more advanced/real-world ingestion topics.

Druid can ingest streaming or batch data. Streaming data is ingested via the real-time node, and batch data is ingested via the Hadoop batch indexer. Druid also has a standalone ingestion service called the [indexing service](Indexing-Service.html).

The Data
--------
The data source we'll be using is (surprise!) Wikipedia edits. The data schema is still:

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

Streaming Event Ingestion
-------------------------

With real-world data, we recommend having a message bus such as [Apache Kafka](http://kafka.apache.org/) sit between the data stream and the real-time node. The message bus provides higher availability for production environments. [Firehoses](Firehose.html) are the key abstraction for real-time ingestion.

<a id="set-up-kafka"></a>
#### Setting up Kafka

[KafkaFirehoseFactory](https://github.com/metamx/druid/blob/druid-0.6.49/realtime/src/main/java/com/metamx/druid/realtime/firehose/KafkaFirehoseFactory.java) is how druid communicates with Kafka. Using this [Firehose](Firehose.html) with the right configuration, we can import data into Druid in real-time without writing any code. To load data to a real-time node via Kafka, we'll first need to initialize Zookeeper and Kafka, and then configure and initialize a [Realtime](Realtime.html) node.

Instructions for booting a Zookeeper and then Kafka cluster are available [here](http://kafka.apache.org/07/quickstart.html).

1. Download Apache Kafka 0.7.2 from [http://kafka.apache.org/downloads.html](http://kafka.apache.org/downloads.html)

  ```bash
  wget http://apache.spinellicreations.com/incubator/kafka/kafka-0.7.2-incubating/kafka-0.7.2-incubating-src.tgz
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
  java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Ddruid.realtime.specFile=examples/indexing/wikipedia.spec -classpath lib/*:config/realtime io.druid.cli.Main server realtime
  ```

2. A realtime.spec should already exist for the data source in the Druid tarball. You should be able to find it at:

  ```bash
  examples/indexing/wikipedia.spec
  ```

  The contents of the file should match:

  ```json
  [
    {
      "schema": {
        "dataSource": "wikipedia",
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
        "indexGranularity": "none"
      },
      "config": {
        "maxRowsInMemory": 500000,
        "intermediatePersistPeriod": "PT10m"
      },
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
        "feed": "wikipedia",
        "parser": {
          "timestampSpec": {
            "column": "timestamp"
          },
          "data": {
            "format": "json",
            "dimensions" : ["page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city"]
          }
        }
      },
      "plumber": {
        "type": "realtime",
        "windowPeriod": "PT10m",
        "segmentGranularity": "hour",
        "basePersistDirectory": "\/tmp\/realtime\/basePersist",
        "rejectionPolicy": {
          "type": "none"
        }
      }
    }
  ]
  ```

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
[ {
  "timestamp" : "2013-08-31T01:02:33.000Z",
  "result" : {
    "minTime" : "2013-08-31T01:02:33.000Z",
    "maxTime" : "2013-08-31T12:41:27.000Z"
  }
} ]
```

Batch Ingestion
---------------
Druid is designed for large data volumes, and most real-world data sets require batch indexing be done through a Hadoop job.

The setup for a single node, 'standalone' Hadoop cluster is available [here](http://hadoop.apache.org/docs/stable/single_node_setup.html).

For the purposes of this tutorial, we are going to use our very small and simple Wikipedia data set. This data can directly be ingested via other means as shown in the previous [tutorial](Tutorial%3A-Loading-Your-Data-Part-1), but we are going to use Hadoop here for demonstration purposes.

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

#### Setup a Druid Cluster

To index the data, we are going to need an indexing service, a historical node, and a coordinator node.

To start the Indexing Service:

```bash
java -Xmx2g -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/*:<hadoop_config_path>:config/overlord io.druid.cli.Main server overlord
```

To start the Coordinator Node:

```bash
java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/*:config/coordinator io.druid.cli.Main server coordinator
```

To start the Historical Node:

```bash
java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/*:config/historical io.druid.cli.Main server historical
```

#### Index the Data

Before indexing the data, make sure you have a valid Hadoop cluster running. To build our Druid segment, we are going to submit a [Hadoop index task](Tasks.html) to the indexing service. The grammar for the Hadoop index task is very similar to the index task of the last tutorial. The tutorial Hadoop index task should be located at:

```
examples/indexing/wikipedia_index_hadoop_task.json
```

Examining the contents of the file, you should find:

  ```json
  {
    "type" : "index_hadoop",
    "config": {
      "dataSource" : "wikipedia",
      "timestampSpec" : {
        "column" : "timestamp",
        "format" : "auto"
      },
      "dataSpec" : {
        "format" : "json",
        "dimensions" : ["page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city"]
      },
      "granularitySpec" : {
        "type" : "uniform",
        "gran" : "DAY",
        "intervals" : [ "2013-08-31/2013-09-01" ]
      },
      "pathSpec" : {
        "type" : "static",
        "paths" : "examples/indexing/wikipedia_data.json"
      },
      "targetPartitionSize" : 5000000,
      "rollupSpec" : {
        "aggs": [{
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
        "rollupGranularity" : "none"
      }
    }
  }
  ```

If you are curious about what all this configuration means, see [here](Task.html).

To submit the task:

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/indexing/wikipedia_index_hadoop_task.json localhost:8087/druid/indexer/v1/task
```

After the task is completed, the segment should be assigned to your historical node. You should be able to query the segment.

Next Steps
----------
We demonstrated using the indexing service as a way to ingest data into Druid. Previous versions of Druid used the [HadoopDruidIndexer](Batch-ingestion.html) to ingest batch data. The `HadoopDruidIndexer` still remains a valid option for batch ingestion, however, we recommend using the indexing service as the preferred method of getting batch data into Druid.

For more information on querying, check out this [tutorial](Tutorial%3A-All-About-Queries.html).

Additional Information
----------------------

Getting data into Druid can definitely be difficult for first time users. Please don't hesitate to ask questions in our IRC channel or on our [google groups page](https://groups.google.com/forum/#!forum/druid-development).
