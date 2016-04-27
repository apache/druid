---
layout: doc_page
---

# Tutorial: Load from Kafka

## Getting started

This tutorial shows you how to load data from Kafka into Druid.

For this tutorial, we'll assume you've already downloaded Druid and Tranquility as described in
the [single-machine quickstart](quickstart.html) and have it running on your local machine. You
don't need to have loaded any data yet.

<div class="note info">
This tutorial will show you how to load data from Kafka into Druid, but Druid additionally supports
a wide variety of batch and streaming loading methods. See the <a href="../ingestion/batch-ingestion.html">Loading files</a>
and <a href="../ingestion/stream-ingestion.html">Loading streams</a> pages for more information about other options,
including from Hadoop, HTTP, Storm, Samza, Spark Streaming, and your own JVM apps.
</div>

## Start Kafka

[Apache Kafka](http://kafka.apache.org/) is a high throughput message bus that works well with
Druid.  For this tutorial, we will use Kafka 0.9.0.0. To download Kafka, issue the following
commands in your terminal:

```bash
curl -O http://www.us.apache.org/dist/kafka/0.9.0.0/kafka_2.11-0.9.0.0.tgz
tar -xzf kafka_2.11-0.9.0.0.tgz
cd kafka_2.11-0.9.0.0
```

Start a Kafka broker by running the following command in a new terminal:

```bash
./bin/kafka-server-start.sh config/server.properties
```

Run this command to create a Kafka topic called *metrics*, to which we'll send data:

```bash
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic metrics
```

## Send example data

Let's launch a console producer for our topic and send some data!

In your Druid directory, generate some metrics by running:

```bash
bin/generate-example-metrics
```

In your Kafka directory, run:

```bash
./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic metrics
```

The *kafka-console-producer* command is now awaiting input. Copy the generated example metrics,
paste them into the *kafka-console-producer* terminal, and press enter. If you like, you can also
paste more messages into the producer, or you can press CTRL-D to exit the console producer.

You can immediately query this data, or you can skip ahead to the
[Loading your own data](#loading-your-own-data) section if you'd like to load your own dataset.

## Querying your data

After sending data, you can immediately query it using any of the
[supported query methods](../querying/querying.html).

## Loading your own data

So far, you've loaded data into Druid from Kafka using an ingestion spec that we've included in the
distribution. Each ingestion spec is designed to work with a particular dataset. You load your own
data types into Imply by writing a custom ingestion spec.

You can write a custom ingestion spec by starting from the bundled configuration in
`conf-quickstart/tranquility/kafka.json` and modifying it for your own needs.

The most important questions are:

  * What should the dataset be called? This is the "dataSource" field of the "dataSchema".
  * Which field should be treated as a timestamp? This belongs in the "column" of the "timestampSpec".
  * Which fields should be treated as dimensions? This belongs in the "dimensions" of the "dimensionsSpec".
  * Which fields should be treated as measures? This belongs in the "metricsSpec".

Let's use a small JSON pageviews dataset in the topic *pageviews* as an example, with records like:

```json
{"time": "2000-01-01T00:00:00Z", "url": "/foo/bar", "user": "alice", "latencyMs": 32}
```

First, create the topic:

```bash
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic pageviews
```

Next, edit `conf-quickstart/tranquility/kafka.json`:

  * Let's call the dataset "pageviews-kafka".
  * The timestamp is the "time" field.
  * Good choices for dimensions are the string fields "url" and "user".
  * Good choices for measures are a count of pageviews, and the sum of "latencyMs". Collecting that
sum when we load the data will allow us to compute an average at query time as well.

You can edit the existing `conf-quickstart/tranquility/kafka.json` file by altering these
sections:

  1. Change the key `"metrics-kafka"` under `"dataSources"` to `"pageviews-kafka"`
  2. Alter these sections under the new `"pageviews-kafka"` key:
  ```json
  "dataSource": "pageviews-kafka"
  ```

  ```json
  "timestampSpec": {
       "format": "auto",
       "column": "time"
  }
  ```

  ```json
  "dimensionsSpec": {
       "dimensions": ["url", "user"]
  }
  ```

  ```json
  "metricsSpec": [
       {"name": "views", "type": "count"},
       {"name": "latencyMs", "type": "doubleSum", "fieldName": "latencyMs"}
  ]
  ```

  ```json
  "properties" : {
       "task.partitions" : "1",
       "task.replicants" : "1",
       "topicPattern" : "pageviews"
  }
  ```

Next, start Druid Kafka ingestion:

```bash
bin/tranquility kafka -configFile ../druid-0.9.0/conf-quickstart/tranquility/kafka.json
```

- If your Tranquility server or Kafka is already running, stop it (CTRL-C) and
start it up again.

Finally, send some data to the Kafka topic. Let's start with these messages:

```json
{"time": "2000-01-01T00:00:00Z", "url": "/foo/bar", "user": "alice", "latencyMs": 32}
{"time": "2000-01-01T00:00:00Z", "url": "/", "user": "bob", "latencyMs": 11}
{"time": "2000-01-01T00:00:00Z", "url": "/foo/bar", "user": "bob", "latencyMs": 45}
```

Druid streaming ingestion requires relatively current messages (relative to a slack time controlled by the
[windowPeriod](../ingestion/stream-ingestion.html#segmentgranularity-and-windowperiod) value), so you should
replace `2000-01-01T00:00:00Z` in these messages with the current time in ISO8601 format. You can
get this by running:

```bash
python -c 'import datetime; print(datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))'
```

Update the timestamps in the JSON above, then copy and paste these messages into this console
producer and press enter:

```bash
./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic pageviews
```

That's it, your data should now be in Druid. You can immediately query it using any of the
[supported query methods](../querying/querying.html).

## Further reading

To read more about loading streams, see our [streaming ingestion documentation](../ingestion/stream-ingestion.html).
