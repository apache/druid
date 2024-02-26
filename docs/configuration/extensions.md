---
id: extensions
title: "Extensions"
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->


Druid implements an extension system that allows for adding functionality at runtime. Extensions
are commonly used to add support for deep storages (like HDFS and S3), metadata stores (like MySQL
and PostgreSQL), new aggregators, new input formats, and so on.

Production clusters will generally use at least two extensions; one for deep storage and one for a
metadata store. Many clusters will also use additional extensions.

## Core extensions

Core extensions are maintained by Druid committers.

|Name|Description|Docs|
|----|-----------|----|
|druid-avro-extensions|Support for data in Apache Avro data format.|[link](../development/extensions-core/avro.md)|
|druid-azure-extensions|Microsoft Azure deep storage.|[link](../development/extensions-core/azure.md)|
|druid-basic-security|Support for Basic HTTP authentication and role-based access control.|[link](../development/extensions-core/druid-basic-security.md)|
|druid-bloom-filter|Support for providing Bloom filters in druid queries.|[link](../development/extensions-core/bloom-filter.md)|
|druid-datasketches|Support for approximate counts and set operations with [Apache DataSketches](https://datasketches.apache.org/).|[link](../development/extensions-core/datasketches-extension.md)|
|druid-google-extensions|Google Cloud Storage deep storage.|[link](../development/extensions-core/google.md)|
|druid-hdfs-storage|HDFS deep storage.|[link](../development/extensions-core/hdfs.md)|
|druid-histogram|Approximate histograms and quantiles aggregator. Deprecated, please use the [DataSketches quantiles aggregator](../development/extensions-core/datasketches-quantiles.md) from the `druid-datasketches` extension instead.|[link](../development/extensions-core/approximate-histograms.md)|
|druid-kafka-extraction-namespace|Apache Kafka-based namespaced lookup. Requires namespace lookup extension.|[link](../development/extensions-core/kafka-extraction-namespace.md)|
|druid-kafka-indexing-service|Supervised exactly-once Apache Kafka ingestion for the indexing service.|[link](../ingestion/kafka-ingestion.md)|
|druid-kinesis-indexing-service|Supervised exactly-once Kinesis ingestion for the indexing service.|[link](../ingestion/kinesis-ingestion.md)|
|druid-kerberos|Kerberos authentication for druid processes.|[link](../development/extensions-core/druid-kerberos.md)|
|druid-lookups-cached-global|A module for [lookups](../querying/lookups.md) providing a jvm-global eager caching for lookups. It provides JDBC and URI implementations for fetching lookup data.|[link](../development/extensions-core/lookups-cached-global.md)|
|druid-lookups-cached-single| Per lookup caching module to support the use cases where a lookup need to be isolated from the global pool of lookups |[link](../development/extensions-core/druid-lookups.md)|
|druid-multi-stage-query| Support for the multi-stage query architecture for Apache Druid and the multi-stage query task engine.|[link](../multi-stage-query/index.md)|
|druid-orc-extensions|Support for data in Apache ORC data format.|[link](../development/extensions-core/orc.md)|
|druid-parquet-extensions|Support for data in Apache Parquet data format. Requires druid-avro-extensions to be loaded.|[link](../development/extensions-core/parquet.md)|
|druid-protobuf-extensions| Support for data in Protobuf data format.|[link](../development/extensions-core/protobuf.md)|
|druid-ranger-security|Support for access control through Apache Ranger.|[link](../development/extensions-core/druid-ranger-security.md)|
|druid-s3-extensions|Interfacing with data in AWS S3, and using S3 as deep storage.|[link](../development/extensions-core/s3.md)|
|druid-ec2-extensions|Interfacing with AWS EC2 for autoscaling middle managers|UNDOCUMENTED|
|druid-aws-rds-extensions|Support for AWS token based access to AWS RDS DB Cluster.|[link](../development/extensions-core/druid-aws-rds.md)|
|druid-stats|Statistics related module including variance and standard deviation.|[link](../development/extensions-core/stats.md)|
|mysql-metadata-storage|MySQL metadata store.|[link](../development/extensions-core/mysql.md)|
|postgresql-metadata-storage|PostgreSQL metadata store.|[link](../development/extensions-core/postgresql.md)|
|simple-client-sslcontext|Simple SSLContext provider module to be used by Druid's internal HttpClient when talking to other Druid processes over HTTPS.|[link](../development/extensions-core/simple-client-sslcontext.md)|
|druid-pac4j|OpenID Connect authentication for druid processes.|[link](../development/extensions-core/druid-pac4j.md)|
|druid-kubernetes-extensions|Druid cluster deployment on Kubernetes without Zookeeper.|[link](../development/extensions-core/kubernetes.md)|

## Community extensions

:::info
 Community extensions are not maintained by Druid committers, although we accept patches from community members using these extensions. They may not have been as extensively tested as the core extensions.
:::

A number of community members have contributed their own extensions to Druid that are not packaged with the default Druid tarball.
If you'd like to take on maintenance for a community extension, please post on [dev@druid.apache.org](https://lists.apache.org/list.html?dev@druid.apache.org) to let us know!

All of these community extensions can be downloaded using [pull-deps](../operations/pull-deps.md) while specifying a `-c` coordinate option to pull `org.apache.druid.extensions.contrib:{EXTENSION_NAME}:{DRUID_VERSION}`.

|Name|Description|Docs|
|----|-----------|----|
|aliyun-oss-extensions|Aliyun OSS deep storage |[link](../development/extensions-contrib/aliyun-oss-extensions.md)|
|ambari-metrics-emitter|Ambari Metrics Emitter |[link](../development/extensions-contrib/ambari-metrics-emitter.md)|
|druid-cassandra-storage|Apache Cassandra deep storage.|[link](../development/extensions-contrib/cassandra.md)|
|druid-cloudfiles-extensions|Rackspace Cloudfiles deep storage and firehose.|[link](../development/extensions-contrib/cloudfiles.md)|
|druid-compressed-bigdecimal|Compressed Big Decimal Type | [link](../development/extensions-contrib/compressed-big-decimal.md)|
|druid-ddsketch|Support for DDSketch approximate quantiles based on [DDSketch](https://github.com/datadog/sketches-java) | [link](../development/extensions-contrib/ddsketch-quantiles.md)|
|druid-deltalake-extensions|Support for ingesting Delta Lake tables.|[link](../development/extensions-contrib/delta-lake.md)|
|druid-distinctcount|DistinctCount aggregator|[link](../development/extensions-contrib/distinctcount.md)|
|druid-iceberg-extensions|Support for ingesting Iceberg tables.|[link](../development/extensions-contrib/iceberg.md)|
|druid-redis-cache|A cache implementation for Druid based on Redis.|[link](../development/extensions-contrib/redis-cache.md)|
|druid-time-min-max|Min/Max aggregator for timestamp.|[link](../development/extensions-contrib/time-min-max.md)|
|sqlserver-metadata-storage|Microsoft SQLServer deep storage.|[link](../development/extensions-contrib/sqlserver.md)|
|graphite-emitter|Graphite metrics emitter|[link](../development/extensions-contrib/graphite.md)|
|statsd-emitter|StatsD metrics emitter|[link](../development/extensions-contrib/statsd.md)|
|kafka-emitter|Kafka metrics emitter|[link](../development/extensions-contrib/kafka-emitter.md)|
|druid-thrift-extensions|Support thrift ingestion |[link](../development/extensions-contrib/thrift.md)|
|druid-opentsdb-emitter|OpenTSDB metrics emitter |[link](../development/extensions-contrib/opentsdb-emitter.md)|
|materialized-view-selection, materialized-view-maintenance|Materialized View|[link](../development/extensions-contrib/materialized-view.md)|
|druid-moving-average-query|Support for [Moving Average](https://en.wikipedia.org/wiki/Moving_average) and other Aggregate [Window Functions](https://en.wikibooks.org/wiki/Structured_Query_Language/Window_functions) in Druid queries.|[link](../development/extensions-contrib/moving-average-query.md)|
|druid-influxdb-emitter|InfluxDB metrics emitter|[link](../development/extensions-contrib/influxdb-emitter.md)|
|druid-momentsketch|Support for approximate quantile queries using the [momentsketch](https://github.com/stanford-futuredata/momentsketch) library|[link](../development/extensions-contrib/momentsketch-quantiles.md)|
|druid-tdigestsketch|Support for approximate sketch aggregators based on [T-Digest](https://github.com/tdunning/t-digest)|[link](../development/extensions-contrib/tdigestsketch-quantiles.md)|
|gce-extensions|GCE Extensions|[link](../development/extensions-contrib/gce-extensions.md)|
|prometheus-emitter|Exposes [Druid metrics](../operations/metrics.md) for Prometheus server collection (https://prometheus.io/)|[link](../development/extensions-contrib/prometheus.md)|
|kubernetes-overlord-extensions|Support for launching tasks in k8s without Middle Managers|[link](../development/extensions-contrib/k8s-jobs.md)|
|druid-spectator-histogram|Support for efficient approximate percentile queries|[link](../development/extensions-contrib/spectator-histogram.md)|

## Promoting community extensions to core extensions

Please post on [dev@druid.apache.org](https://lists.apache.org/list.html?dev@druid.apache.org) if you'd like an extension to be promoted to core.
If we see a community extension actively supported by the community, we can promote it to core based on community feedback.


For information how to create your own extension, please see [here](../development/modules.md).

## Loading extensions

### Loading core extensions

Apache Druid bundles all [core extensions](../configuration/extensions.md#core-extensions) out of the box.
See the [list of extensions](../configuration/extensions.md#core-extensions) for your options. You
can load bundled extensions by adding their names to your common.runtime.properties
`druid.extensions.loadList` property. For example, to load the postgresql-metadata-storage and
druid-hdfs-storage extensions, use the configuration:

```properties
druid.extensions.loadList=["postgresql-metadata-storage", "druid-hdfs-storage"]
```

These extensions are located in the `extensions` directory of the distribution.

:::info
 Druid bundles two sets of configurations: one for the [quickstart](../tutorials/index.md) and
 one for a [clustered configuration](../tutorials/cluster.md). Make sure you are updating the correct
 `common.runtime.properties` for your setup.
:::

:::info
 Because of licensing, the mysql-metadata-storage extension does not include the required MySQL JDBC driver. For instructions
 on how to install this library, see the [MySQL extension page](../development/extensions-core/mysql.md).
:::

### Loading community extensions

You can also load community and third-party extensions not already bundled with Druid. To do this, first download the extension and
then install it into your `extensions` directory. You can download extensions from their distributors directly, or
if they are available from Maven, the included [pull-deps](../operations/pull-deps.md) can download them for you. To use *pull-deps*,
specify the full Maven coordinate of the extension in the form `groupId:artifactId:version`. For example,
for the (hypothetical) extension *com.example:druid-example-extension:1.0.0*, run:

```shell
java \
  -cp "lib/*" \
  -Ddruid.extensions.directory="extensions" \
  -Ddruid.extensions.hadoopDependenciesDir="hadoop-dependencies" \
  org.apache.druid.cli.Main tools pull-deps \
  --no-default-hadoop \
  -c "com.example:druid-example-extension:1.0.0"
```

You only have to install the extension once. Then, add `"druid-example-extension"` to
`druid.extensions.loadList` in common.runtime.properties to instruct Druid to load the extension.

:::info
 Please make sure all the Extensions related configuration properties listed [here](../configuration/index.md#extensions) are set correctly.
:::

:::info
 The Maven `groupId` for almost every [community extension](../configuration/extensions.md#community-extensions) is `org.apache.druid.extensions.contrib`. The `artifactId` is the name
 of the extension, and the version is the latest Druid stable version.
:::

### Loading extensions from the classpath

If you add your extension jar to the classpath at runtime, Druid will also load it into the system. This mechanism is relatively easy to reason about,
but it also means that you have to ensure that all dependency jars on the classpath are compatible. That is, Druid makes no provisions while using
this method to maintain class loader isolation so you must make sure that the jars on your classpath are mutually compatible.
