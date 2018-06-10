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
|druid-basic-security|Support for Basic HTTP authentication and role-based access control.|[link](../development/extensions-core/druid-basic-security.html)|
|druid-caffeine-cache|A local cache implementation backed by Caffeine.|[link](../development/extensions-core/caffeine-cache.html)|
|druid-datasketches|Support for approximate counts and set operations with [DataSketches](http://datasketches.github.io/).|[link](../development/extensions-core/datasketches-extension.html)|
|druid-hdfs-storage|HDFS deep storage.|[link](../development/extensions-core/hdfs.html)|
|druid-histogram|Approximate histograms and quantiles aggregator.|[link](../development/extensions-core/approximate-histograms.html)|
|druid-kafka-eight|Kafka ingest firehose (high level consumer) for realtime nodes.|[link](../development/extensions-core/kafka-eight-firehose.html)|
|druid-kafka-extraction-namespace|Kafka-based namespaced lookup. Requires namespace lookup extension.|[link](../development/extensions-core/kafka-extraction-namespace.html)|
|druid-kafka-indexing-service|Supervised exactly-once Kafka ingestion for the indexing service.|[link](../development/extensions-core/kafka-ingestion.html)|
|druid-kerberos|Kerberos authentication for druid nodes.|[link](../development/extensions-core/druid-kerberos.html)|
|druid-lookups-cached-global|A module for [lookups](../querying/lookups.html) providing a jvm-global eager caching for lookups. It provides JDBC and URI implementations for fetching lookup data.|[link](../development/extensions-core/lookups-cached-global.html)|
|druid-lookups-cached-single| Per lookup caching module to support the use cases where a lookup need to be isolated from the global pool of lookups |[link](../development/extensions-core/druid-lookups.html)|
|druid-protobuf-extensions| Support for data in Protobuf data format.|[link](../development/extensions-core/protobuf.html)|
|druid-s3-extensions|Interfacing with data in AWS S3, and using S3 as deep storage.|[link](../development/extensions-core/s3.html)|
|druid-stats|Statistics related module including variance and standard deviation.|[link](../development/extensions-core/stats.html)|
|mysql-metadata-storage|MySQL metadata store.|[link](../development/extensions-core/mysql.html)|
|postgresql-metadata-storage|PostgreSQL metadata store.|[link](../development/extensions-core/postgresql.html)|
|simple-client-sslcontext|Simple SSLContext provider module to be used by internal HttpClient talking to other nodes over HTTPS.|[link](../development/extensions-core/simple-client-sslcontext.html)|

# Community Extensions

<div class="note caution">
Community extensions are not maintained by Druid committers, although we accept patches from community members using these extensions. They may not have been as extensively tested as the core extensions.
</div>

A number of community members have contributed their own extensions to Druid that are not packaged with the default Druid tarball.
If you'd like to take on maintenance for a community extension, please post on [dev@druid.apache.org](https://lists.apache.org/list.html?dev@druid.apache.org) to let us know!

All of these community extensions can be downloaded using *pull-deps* with the coordinate io.druid.extensions.contrib:EXTENSION_NAME:LATEST_DRUID_STABLE_VERSION.

|Name|Description|Docs|
|----|-----------|----|
|ambari-metrics-emitter|Ambari Metrics Emitter |[link](../development/extensions-contrib/ambari-metrics-emitter.html)|
|druid-azure-extensions|Microsoft Azure deep storage.|[link](../development/extensions-contrib/azure.html)|
|druid-cassandra-storage|Apache Cassandra deep storage.|[link](../development/extensions-contrib/cassandra.html)|
|druid-cloudfiles-extensions|Rackspace Cloudfiles deep storage and firehose.|[link](../development/extensions-contrib/cloudfiles.html)|
|druid-distinctcount|DistinctCount aggregator|[link](../development/extensions-contrib/distinctcount.html)|
|druid-kafka-eight-simpleConsumer|Kafka ingest firehose (low level consumer).|[link](../development/extensions-contrib/kafka-simple.html)|
|druid-orc-extensions|Support for data in Apache Orc data format.|[link](../development/extensions-contrib/orc.html)|
|druid-parquet-extensions|Support for data in Apache Parquet data format. Requires druid-avro-extensions to be loaded.|[link](../development/extensions-contrib/parquet.html)|
|druid-rabbitmq|RabbitMQ firehose.|[link](../development/extensions-contrib/rabbitmq.html)|
|druid-redis-cache|A cache implementation for Druid based on Redis.|[link](../development/extensions-contrib/redis-cache.html)|
|druid-rocketmq|RocketMQ firehose.|[link](../development/extensions-contrib/rocketmq.html)|
|druid-time-min-max|Min/Max aggregator for timestamp.|[link](../development/extensions-contrib/time-min-max.html)|
|druid-google-extensions|Google Cloud Storage deep storage.|[link](../development/extensions-contrib/google.html)|
|sqlserver-metadata-storage|Microsoft SqlServer deep storage.|[link](../development/extensions-contrib/sqlserver.html)|
|graphite-emitter|Graphite metrics emitter|[link](../development/extensions-contrib/graphite.html)|
|statsd-emitter|StatsD metrics emitter|[link](../development/extensions-contrib/statsd.html)|
|kafka-emitter|Kafka metrics emitter|[link](../development/extensions-contrib/kafka-emitter.html)|
|druid-thrift-extensions|Support thrift ingestion |[link](../development/extensions-contrib/thrift.html)|
|druid-opentsdb-emitter|OpenTSDB metrics emitter |[link](../development/extensions-contrib/opentsdb-emitter.html)|

## Promoting Community Extension to Core Extension

Please post on [dev@druid.apache.org](https://lists.apache.org/list.html?dev@druid.apache.org) if you'd like an extension to be promoted to core.
If we see a community extension actively supported by the community, we can promote it to core based on community feedback.

# Creating your own Extensions

For information how to create your own extension, please see [here](../development/modules.html).
