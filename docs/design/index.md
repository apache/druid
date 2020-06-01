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

## What is Druid?

Apache Druid is a real-time analytics database designed for fast slice-and-dice analytics
("[OLAP](http://en.wikipedia.org/wiki/Online_analytical_processing)" queries) on large data sets. Druid is most often
used as a database for powering use cases where real-time ingest, fast query performance, and high uptime are important.
As such, Druid is commonly used for powering GUIs of analytical applications, or as a backend for highly-concurrent APIs
that need fast aggregations. Druid works best with event-oriented data.

Common application areas for Druid include:

- Clickstream analytics (web and mobile analytics)
- Network telemetry analytics (network performance monitoring)
- Server metrics storage
- Supply chain analytics (manufacturing metrics)
- Application performance metrics
- Digital marketing/advertising analytics
- Business intelligence / OLAP

Druid's core architecture combines ideas from data warehouses, timeseries databases, and logsearch systems. Some of
Druid's key features are:

1. **Columnar storage format.** Druid uses column-oriented storage, meaning it only needs to load the exact columns
needed for a particular query.  This gives a huge speed boost to queries that only hit a few columns. In addition, each
column is stored optimized for its particular data type, which supports fast scans and aggregations.
2. **Scalable distributed system.** Druid is typically deployed in clusters of tens to hundreds of servers, and can
offer ingest rates of millions of records/sec, retention of trillions of records, and query latencies of sub-second to a
few seconds.
3. **Massively parallel processing.** Druid can process a query in parallel across the entire cluster.
4. **Realtime or batch ingestion.** Druid can ingest data either real-time (ingested data is immediately available for
querying) or in batches.
5. **Self-healing, self-balancing, easy to operate.** As an operator, to scale the cluster out or in, simply add or
remove servers and the cluster will rebalance itself automatically, in the background, without any downtime. If any
Druid servers fail, the system will automatically route around the damage until those servers can be replaced. Druid
is designed to run 24/7 with no need for planned downtimes for any reason, including configuration changes and software
updates.
6. **Cloud-native, fault-tolerant architecture that won't lose data.** Once Druid has ingested your data, a copy is
stored safely in [deep storage](architecture.html#deep-storage) (typically cloud storage, HDFS, or a shared filesystem).
Your data can be recovered from deep storage even if every single Druid server fails. For more limited failures affecting
just a few Druid servers, replication ensures that queries are still possible while the system recovers.
7. **Indexes for quick filtering.** Druid uses [Roaring](https://roaringbitmap.org/) or
[CONCISE](https://arxiv.org/pdf/1004.0403) compressed bitmap indexes to create indexes that power fast filtering and
searching across multiple columns.
8. **Time-based partitioning.** Druid first partitions data by time, and can additionally partition based on other fields.
This means time-based queries will only access the partitions that match the time range of the query. This leads to
significant performance improvements for time-based data.
9. **Approximate algorithms.** Druid includes algorithms for approximate count-distinct, approximate ranking, and
computation of approximate histograms and quantiles. These algorithms offer bounded memory usage and are often
substantially faster than exact computations. For situations where accuracy is more important than speed, Druid also
offers exact count-distinct and exact ranking.
10. **Automatic summarization at ingest time.** Druid optionally supports data summarization at ingestion time. This
summarization partially pre-aggregates your data, and can lead to big costs savings and performance boosts.

## When should I use Druid?

Druid is used by many companies of various sizes for many different use cases. Check out the
[Powered by Apache Druid](/druid-powered) page

Druid is likely a good choice if your use case fits a few of the following descriptors:

- Insert rates are very high, but updates are less common.
- Most of your queries are aggregation and reporting queries ("group by" queries). You may also have searching and
scanning queries.
- You are targeting query latencies of 100ms to a few seconds.
- Your data has a time component (Druid includes optimizations and design choices specifically related to time).
- You may have more than one table, but each query hits just one big distributed table. Queries may potentially hit more
than one smaller "lookup" table.
- You have high cardinality data columns (e.g. URLs, user IDs) and need fast counting and ranking over them.
- You want to load data from Kafka, HDFS, flat files, or object storage like Amazon S3.

Situations where you would likely _not_ want to use Druid include:

- You need low-latency updates of _existing_ records using a primary key. Druid supports streaming inserts, but not streaming updates (updates are done using
background batch jobs).
- You are building an offline reporting system where query latency is not very important.
- You want to do "big" joins (joining one big fact table to another big fact table) and you are okay with these queries
taking a long time to complete.
