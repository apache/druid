---
layout: doc_page
title: "Apache Druid (incubating) vs Redshift"
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

# Druid vs Redshift

### How does Druid compare to Redshift?

In terms of drawing a differentiation, Redshift started out as ParAccel (Actian), which Amazon is licensing and has since heavily modified.

Aside from potential performance differences, there are some functional differences:

### Real-time data ingestion

Because Druid is optimized to provide insight against massive quantities of streaming data; it is able to load and aggregate data in real-time.

Generally traditional data warehouses including column stores work only with batch ingestion and are not optimal for streaming data in regularly.

### Druid is a read oriented analytical data store

Druid’s write semantics are not as fluid and does not support full joins (we support large table to small table joins). Redshift provides full SQL support including joins and insert/update statements.

### Data distribution model

Druid’s data distribution is segment-based and leverages a highly available "deep" storage such as S3 or HDFS. Scaling up (or down) does not require massive copy actions or downtime; in fact, losing any number of Historical processes does not result in data loss because new Historical processes can always be brought up by reading data from "deep" storage.

To contrast, ParAccel’s data distribution model is hash-based. Expanding the cluster requires re-hashing the data across the nodes, making it difficult to perform without taking downtime. Amazon’s Redshift works around this issue with a multi-step process:

* set cluster into read-only mode
* copy data from cluster to new cluster that exists in parallel
* redirect traffic to new cluster

### Replication strategy

Druid employs segment-level data distribution meaning that more processes can be added and rebalanced without having to perform a staged swap. The replication strategy also makes all replicas available for querying. Replication is done automatically and without any impact to performance.

ParAccel’s hash-based distribution generally means that replication is conducted via hot spares. This puts a numerical limit on the number of nodes you can lose without losing data, and this replication strategy often does not allow the hot spare to help share query load.

### Indexing strategy

Along with column oriented structures, Druid uses indexing structures to speed up query execution when a filter is provided. Indexing structures do increase storage overhead (and make it more difficult to allow for mutation), but they also significantly speed up queries.

ParAccel does not appear to employ indexing strategies.
