---
layout: doc_page
---

# Druid extensions

Druid implements an extension system that allows for adding functionality at runtime. Extensions
are commonly used to add support for deep storages (like HDFS and S3), metadata stores (like MySQL
and PostgreSQL), new aggregators, new input formats, and so on.

Production clusters will generally use at least two extensions; one for deep storage and one for a
metadata store. Many clusters will also use additional extensions.

## Including extensions

Please see [here](../operations/including-extensions.html). 

## Core extensions

Core extensions are maintained by Druid committers.

|Name|Description|Docs|
|----|-----------|----|
|druid-avro-extensions|Support for data in Apache Avro data format.|[link](../development/extensions-core/avro.html)|
|druid-datasketches|Support for approximate counts and set operations with [DataSketches](http://datasketches.github.io/).|[link](../development/extensions-core/datasketches-aggregators.html)|
|druid-hdfs-storage|HDFS deep storage.|[link](../development/extensions-core/hdfs.html)|
|druid-histogram|Approximate histograms and quantiles aggregator.|[link](../development/extensions-core/approximate-histograms.html)|
|druid-kafka-eight|Kafka ingest firehose (high level consumer).|[link](../development/extensions-core/kafka-eight-firehose.html)|
|druid-kafka-extraction-namespace|Kafka-based namespaced lookup. Requires namespace lookup extension.|[link](../development/extensions-core/kafka-extraction-namespace.html)|
|druid-namespace-lookup|Required module for [lookups](../querying/lookups.html).|[link](../development/extensions-core/namespaced-lookup.html)|
|druid-s3-extensions|Interfacing with data in AWS S3, and using S3 as deep storage.|[link](../development/extensions-core/s3.html)|
|mysql-metadata-storage|MySQL metadata store.|[link](../development/extensions-core/mysql.html)|
|postgresql-metadata-storage|PostgreSQL metadata store.|[link](../development/extensions-core/postgresql.html)|

# Community Extensions

A number of community members have contributed their own extensions to Druid that are not packaged with the default Druid tarball. 
Community extensions are not maintained by Druid committers, although we accept patches from community members using these extensions. 
If you'd like to take on maintenance for a community extension, please post on [druid-development group](https://groups.google.com/forum/#!forum/druid-development) to let us know!    

|Name|Description|Docs|
|----|-----------|----|
|druid-azure-extensions|Microsoft Azure deep storage.|[link](../development/extensions-contrib/azure.html)|
|druid-cassandra-storage|Apache Cassandra deep storage.|[link](../development/extensions-contrib/cassandra.html)|
|druid-cloudfiles-extensions|Rackspace Cloudfiles deep storage and firehose.|[link](../development/extensions-contrib/cloudfiles.html)|
|druid-kafka-eight-simpleConsumer|Kafka ingest firehose (low level consumer).|[link](../development/extensions-contrib/kafka-simple.html)|
|druid-rabbitmq|RabbitMQ firehose.|[link](../development/extensions-contrib/rabbitmq.html)|
|graphite-emitter|Graphite metrics emitter|[link](../development/extensions-contrib/graphite.html)|

## Promoting Community Extension to Core Extension

Please [let us know](https://groups.google.com/forum/#!forum/druid-development) if you'd like an extension to be promoted to core. 
If we see a community extension actively supported by the community, we can promote it to core based on community feedback. 
