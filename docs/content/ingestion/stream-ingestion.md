---
layout: doc_page
---

# Loading streams

Streams can be ingested in Druid using either [Tranquility](https://github.com/druid-io/tranquility) (a Druid-aware 
client) or the [Kafka Indexing Service](../development/extensions-core/kafka-ingestion.html).

## Tranquility (Stream Push)

If you have a program that generates a stream, then you can push that stream directly into Druid in 
real-time. With this approach, Tranquility is embedded in your data-producing application. 
Tranquility comes with bindings for the 
Storm and Samza stream processors. It also has a direct API that can be used from any JVM-based 
program, such as Spark Streaming or a Kafka consumer.

Tranquility handles partitioning, replication, service discovery, and schema rollover for you, 
seamlessly and without downtime. You only have to define your Druid schema.

For examples and more information, please see the [Tranquility README](https://github.com/druid-io/tranquility).

A tutorial is also available at [Tutorial: Loading stream data using HTTP push](../tutorials/tutorial-tranquility.html).

## Kafka Indexing Service (Stream Pull)

Druid can pulll data from Kafka streams using the [Kafka Indexing Service](../development/extensions-core/kafka-ingestion.html).

The Kafka indexing service enables the configuration of *supervisors* on the Overlord, which facilitate ingestion from
Kafka by managing the creation and lifetime of Kafka indexing tasks. These indexing tasks read events using Kafka's own
partition and offset mechanism and are therefore able to provide guarantees of exactly-once ingestion. They are also
able to read non-recent events from Kafka and are not subject to the window period considerations imposed on other
ingestion mechanisms. The supervisor oversees the state of the indexing tasks to coordinate handoffs, manage failures,
and ensure that the scalability and replication requirements are maintained.

A tutorial is available at [Tutorial: Loading stream data from Kafka](../tutorials/tutorial-kafka.html).