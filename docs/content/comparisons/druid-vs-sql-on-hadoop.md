---
layout: doc_page
title: "Apache Druid (incubating) vs SQL-on-Hadoop"
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

# Druid vs SQL-on-Hadoop (Impala/Drill/Spark SQL/Presto)

SQL-on-Hadoop engines provide an
execution engine for various data formats and data stores, and
many can be made to push down computations down to Druid, while providing a SQL interface to Druid.

For a direct comparison between the technologies and when to only use one or the other, things basically comes down to your
product requirements and what the systems were designed to do.

Druid was designed to

1. be an always on service
1. ingest data in real-time
1. handle slice-n-dice style ad-hoc queries

SQL-on-Hadoop engines generally sidestep Map/Reduce, instead querying data directly from HDFS or, in some cases, other storage systems.
Some of these engines (including Impala and Presto) can be colocated with HDFS data nodes and coordinate with them to achieve data locality for queries.
What does this mean?  We can talk about it in terms of three general areas

1. Queries
1. Data Ingestion
1. Query Flexibility

### Queries

Druid segments stores data in a custom column format. Segments are scanned directly as part of queries and each Druid server
calculates a set of results that are eventually merged at the Broker level. This means the data that is transferred between servers
are queries and results, and all computation is done internally as part of the Druid servers.

Most SQL-on-Hadoop engines are responsible for query planning and execution for underlying storage layers and storage formats.
They are processes that stay on even if there is no query running (eliminating the JVM startup costs from Hadoop MapReduce).
Some (Impala/Presto) SQL-on-Hadoop engines have daemon processes that can be run where the data is stored, virtually eliminating network transfer costs. There is still
some latency overhead (e.g. serde time) associated with pulling data from the underlying storage layer into the computation layer. We are unaware of exactly
how much of a performance impact this makes.

### Data Ingestion

Druid is built to allow for real-time ingestion of data.  You can ingest data and query it immediately upon ingestion,
the latency between how quickly the event is reflected in the data is dominated by how long it takes to deliver the event to Druid.

SQL-on-Hadoop, being based on data in HDFS or some other backing store, are limited in their data ingestion rates by the
rate at which that backing store can make data available.  Generally, the backing store is the biggest bottleneck for
how quickly data can become available.

### Query Flexibility

Druid's query language is fairly low level and maps to how Druid operates internally. Although Druid can be combined with a high level query
planner such as [Plywood](https://github.com/implydata/plywood) to support most SQL queries and analytic SQL queries (minus joins among large tables),
base Druid is less flexible than SQL-on-Hadoop solutions for generic processing.

SQL-on-Hadoop support SQL style queries with full joins.

## Druid vs Parquet

Parquet is a column storage format that is designed to work with SQL-on-Hadoop engines. Parquet doesn't have a query execution engine, and instead
relies on external sources to pull data out of it.

Druid's storage format is highly optimized for linear scans. Although Druid has support for nested data, Parquet's storage format is much
more hierachical, and is more designed for binary chunking. In theory, this should lead to faster scans in Druid.
