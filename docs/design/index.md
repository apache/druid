---
id: index
title: "Introduction to Apache Druid"
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

Apache Druid is a real-time analytics database designed for fast slice-and-dice analytics ("[OLAP](http://en.wikipedia.org/wiki/Online_analytical_processing)" queries) on large data sets. Most often, Druid powers use cases where real-time ingestion, fast query performance, and high uptime are important.

Druid is commonly used as the database backend for GUIs of analytical applications, or for highly-concurrent APIs that need fast aggregations. Druid works best with event-oriented data.

Common application areas for Druid include:

- Clickstream analytics including web and mobile analytics
- Network telemetry analytics including network performance monitoring
- Server metrics storage
- Supply chain analytics including manufacturing metrics
- Application performance metrics
- Digital marketing/advertising analytics
- Business intelligence/OLAP

## Key features of Druid

Druid's core architecture combines ideas from data warehouses, timeseries databases, and logsearch systems. Some of
Druid's key features are:

1. **Columnar storage format.** Druid uses column-oriented storage. This means it only loads the exact columns
needed for a particular query.  This greatly improves speed for queries that retrieve only a few columns. Additionally, to support fast scans and aggregations, Druid optimizes column storage for each column according to its data type.
2. **Scalable distributed system.** Typical Druid deployments span clusters ranging from tens to hundreds of servers. Druid can ingest data at the rate of millions of records per second while retaining trillions of records and maintaining query latencies ranging from the sub-second to a few seconds.
3. **Massively parallel processing.** Druid can process each query in parallel across the entire cluster.
4. **Realtime or batch ingestion.** Druid can ingest data either real-time or in batches. Ingested data is immediately available for
querying.
5. **Self-healing, self-balancing, easy to operate.** As an operator, you add servers to scale out or
remove servers to scale down. The Druid cluster re-balances itself automatically in the background without any downtime. If a
Druid server fails, the system automatically routes data around the damage until the server can be replaced. Druid
is designed to run continuously without planned downtime for any reason. This is true for configuration changes and software
updates.
6. **Cloud-native, fault-tolerant architecture that won't lose data.** After ingestion, Druid safely stores a copy of your data in [deep storage](architecture.md#deep-storage). Deep storage is typically cloud storage, HDFS, or a shared filesystem. You can recover your data from deep storage even in the unlikely case that all Druid servers fail. For a limited failure that affects only a few Druid servers, replication ensures that queries are still possible during system recoveries.
7. **Indexes for quick filtering.** Druid uses [Roaring](https://roaringbitmap.org/) or
[CONCISE](https://arxiv.org/pdf/1004.0403) compressed bitmap indexes to create indexes to enable fast filtering and searching across multiple columns.
8. **Time-based partitioning.** Druid first partitions data by time. You can optionally implement additional partitioning based upon other fields.
Time-based queries only access the partitions that match the time range of the query which leads to significant performance improvements.
9. **Approximate algorithms.** Druid includes algorithms for approximate count-distinct, approximate ranking, and
computation of approximate histograms and quantiles. These algorithms offer bounded memory usage and are often
substantially faster than exact computations. For situations where accuracy is more important than speed, Druid also
offers exact count-distinct and exact ranking.
10. **Automatic summarization at ingest time.** Druid optionally supports data summarization at ingestion time. This
summarization partially pre-aggregates your data, potentially leading to significant cost savings and performance boosts.

## When to use Druid

Druid is used by many companies of various sizes for many different use cases. For more information see
[Powered by Apache Druid](/druid-powered).

Druid is likely a good choice if your use case matches a few of the following:

- Insert rates are very high, but updates are less common.
- Most of your queries are aggregation and reporting queries. For example "group by" queries. You may also have searching and
scanning queries.
- You are targeting query latencies of 100ms to a few seconds.
- Your data has a time component. Druid includes optimizations and design choices specifically related to time.
- You may have more than one table, but each query hits just one big distributed table. Queries may potentially hit more
than one smaller "lookup" table.
- You have high cardinality data columns, e.g. URLs, user IDs, and need fast counting and ranking over them.
- You want to load data from Kafka, HDFS, flat files, or object storage like Amazon S3.

Situations where you would likely _not_ want to use Druid include:

- You need low-latency updates of _existing_ records using a primary key. Druid supports streaming inserts, but not streaming updates. You can perform updates using
background batch jobs.
- You are building an offline reporting system where query latency is not very important.
- You want to do "big" joins, meaning joining one big fact table to another big fact table, and you are okay with these queries
taking a long time to complete.

## Learn more
- Try the Druid [Quickstart](../tutorials/index.md).
- Learn more about Druid components in [Design](../design/architecture.md).
- Read about new features and other details of [Druid Releases](https://github.com/apache/druid/releases).