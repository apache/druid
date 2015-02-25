---
layout: doc_page
---

# Loading Streaming Data

In our [last tutorial](Tutorial%3A-The-Druid-Cluster.html), we set up a
complete Druid cluster. We created all the Druid dependencies and ingested
streaming data. In this tutorial, we will expand upon what we've done in the
first two tutorials.

## About the Data

We will be working with the same Wikipedia edits data schema [from out previous
tutorials](http://localhost:4000/content/Tutorial:-A-First-Look-at-Druid.html#about-the-data).

## Set Up

At this point, you should already have Druid downloaded and be comfortable
running a Druid cluster locally. If not, [have a look at our second
tutorial](Tutorial%3A-The-Druid-Cluster.html). If Zookeeper and MySQL are not
running, you will have to start them as described in [The Druid
Cluster](Tutorial%3A-The-Druid-Cluster.html).

With real-world data, we recommend having a message bus such as [Apache
Kafka](http://kafka.apache.org/) sit between the data stream and the real-time
node. The message bus provides higher availability for production environments.
[Firehoses](Firehose.html) are the key abstraction for real-time ingestion.

### Kafka

Druid communicates with Kafka using the
[KafkaFirehoseFactory](Firehose.html). Using this [Firehose](Firehose.html)
with the right configuration, we can import data into Druid in real-time
without writing any code. To load data to a real-time node via Kafka, we'll
first need to initialize Zookeeper and Kafka, and then configure and initialize
a [Realtime](Realtime.html) node.

The following quick-start instructions for booting a Zookeeper and then Kafka
cluster were adapted from the [Apache Kafka quickstart guide](http://kafka.apache.org/documentation.html#quickstart).

1. Download Kafka

    For this tutorial we will [download Kafka 0.8.2]
    (https://www.apache.org/dyn/closer.cgi?path=/kafka/0.8.2.0/kafka_2.10-0.8.2.0.tgz)

    ```bash
    tar -xzf kafka_2.10-0.8.2.0.tgz
    cd kafka_2.10-0.8.2.0
    ```

1. Start Kafka

    First launch ZooKeeper:

    ```bash
    ./bin/zookeeper-server-start.sh config/zookeeper.properties
    ```

    Then start the Kafka server (in a separate console):

    ```bash
    ./bin/kafka-server-start.sh config/server.properties
    ```

1. Create a topic named `wikipedia`

    ```bash
    ./bin/kafka-topics.sh --create --zookeeper localhost:2181 \
      --replication-factor 1 --partitions 1 --topic wikipedia
    ```

1. Launch a console producer for that topic (so we can paste in kafka
   messages in a bit)

    ```bash
    ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic wikipedia
    ```

### Druid Realtime Node

The realtime spec for the data source in this tutorial is available under
`examples/indexing/wikipedia.spec` from the [Druid
download](http://static.druid.io/artifacts/releases/druid-services-0.7.0-bin.tar.gz)

1. Launch the realtime node

    ```bash
    java -Xmx512m -Duser.timezone=UTC -Dfile.encoding=UTF-8         \
         -Ddruid.realtime.specFile=examples/indexing/wikipedia.spec \
         -classpath "config/_common:config/realtime:lib/*"          \
         io.druid.cli.Main server realtime
    ```

1. Copy and paste the following data into the terminal where we launched
   the Kafka console producer above.

    ```json
    {"timestamp": "2013-08-31T01:02:33Z", "page": "Gypsy Danger", "language" : "en", "user" : "nuclear", "unpatrolled" : "true", "newPage" : "true", "robot": "false", "anonymous": "false", "namespace":"article", "continent":"North America", "country":"United States", "region":"Bay Area", "city":"San Francisco", "added": 57, "deleted": 200, "delta": -143}
    {"timestamp": "2013-08-31T03:32:45Z", "page": "Striker Eureka", "language" : "en", "user" : "speed", "unpatrolled" : "false", "newPage" : "true", "robot": "true", "anonymous": "false", "namespace":"wikipedia", "continent":"Australia", "country":"Australia", "region":"Cantebury", "city":"Syndey", "added": 459, "deleted": 129, "delta": 330}
    {"timestamp": "2013-08-31T07:11:21Z", "page": "Cherno Alpha", "language" : "ru", "user" : "masterYi", "unpatrolled" : "false", "newPage" : "true", "robot": "true", "anonymous": "false", "namespace":"article", "continent":"Asia", "country":"Russia", "region":"Oblast", "city":"Moscow", "added": 123, "deleted": 12, "delta": 111}
    {"timestamp": "2013-08-31T11:58:39Z", "page": "Crimson Typhoon", "language" : "zh", "user" : "triplets", "unpatrolled" : "true", "newPage" : "false", "robot": "true", "anonymous": "false", "namespace":"wikipedia", "continent":"Asia", "country":"China", "region":"Shanxi", "city":"Taiyuan", "added": 905, "deleted": 5, "delta": 900}
    {"timestamp": "2013-08-31T12:41:27Z", "page": "Coyote Tango", "language" : "ja", "user" : "stringer", "unpatrolled" : "true", "newPage" : "false", "robot": "true", "anonymous": "false", "namespace":"wikipedia", "continent":"Asia", "country":"Japan", "region":"Kanto", "city":"Tokyo", "added": 1, "deleted": 10, "delta": -9}
    ```

    **Note:** This config uses a [`messageTime` rejection policy](Plumber.html)
    which will accept all events and hand off as long as there is a continuous
    stream of events. In this particular example, hand-off will not actually
    occur because we only have a handful of events.

    Disclaimer: We recognize the timestamps of these events aren't actually recent.

1. Watch the events getting ingested and the real-time node announcing a data
   segment

    ```
    ...
    2015-02-17T23:01:50,220 INFO [chief-wikipedia] io.druid.server.coordination.BatchDataSegmentAnnouncer - Announcing segment[wikipedia_2013-08-31T00:00:00.000Z_2013-09-01T00:00:00.000Z_2013-08-31T00:00:00.000Z] at path[/druid/segments/localhost:8084/2015-02-17T23:01:50.219Z0]
    ...
    ```

1. Issue a query

    Issuing a [TimeBoundaryQuery](TimeBoundaryQuery.html) to the real-time node
    should return some results:

    ```bash
    curl -XPOST -H'Content-type: application/json' \
      "http://localhost:8084/druid/v2/?pretty" \
      -d'{"queryType":"timeBoundary","dataSource":"wikipedia"}'
    ```

    ```json
    [ {
      "timestamp" : "2013-08-31T01:02:33.000Z",
      "result" : {
        "minTime" : "2013-08-31T01:02:33.000Z",
        "maxTime" : "2013-08-31T12:41:27.000Z"
      }
    } ]
    ```

## Advanced Streaming Ingestion

Druid offers an additional method of ingesting streaming data via the indexing service. You may be wondering why a second method is needed. Standalone real-time nodes are sufficient for certain volumes of data and availability tolerances. They pull data from a message queue like Kafka or Rabbit, index data locally, and periodically finalize segments for handoff to historical nodes. They are fairly straightforward to scale, simply taking advantage of the innate scalability of the backing message queue. But they are difficult to make highly available with Kafka, the most popular supported message queue, because its high-level consumer doesn’t provide a way to scale out two replicated consumer groups such that each one gets the same data in the same shard. They also become difficult to manage once you have a lot of them, since every machine needs a unique configuration.

Druid solved the availability problem by switching from a pull-based model to a push-based model; rather than Druid indexers pulling data from Kafka, another process pulls data and pushes the data into Druid. Since with the push based model, we can ensure that the same data makes it into the same shard, we can replicate data. The [indexing service](Indexing-Service.html) encapsulates this functionality, where a task-and-resources model replaces a standalone machine model. In addition to simplifying machine configuration, the model also allows nodes to run in the cloud with an elastic number of machines. If you are interested in this form of real-time ingestion, please check out the client library [Tranquility](https://github.com/metamx/tranquility).

Additional Information
----------------------

Getting data into Druid can definitely be difficult for first time users. Please don't hesitate to ask questions in our IRC channel or on our [google groups page](https://groups.google.com/forum/#!forum/druid-development).

