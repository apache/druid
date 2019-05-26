---
layout: doc_page
title: "Hadoop-based Batch Ingestion VS Native Batch Ingestion"
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

# Comparison of Batch Ingestion Methods

Apache Druid (incubating) basically supports three types of batch ingestion: Apache Hadoop-based
batch ingestion, native parallel batch ingestion, and native local batch
ingestion. The below table shows what features are supported by each
ingestion method.


|   |Hadoop-based ingestion|Native parallel ingestion|Native local ingestion|
|---|----------------------|-------------------------|----------------------|
| Parallel indexing | Always parallel | Parallel if firehose is splittable | Always sequential |
| Supported indexing modes | Replacing mode | Both appending and replacing modes | Both appending and replacing modes |
| External dependency | Hadoop (it internally submits Hadoop jobs) | No dependency | No dependency |
| Supported [rollup modes](http://druid.io/docs/latest/ingestion/index.html#roll-up-modes) | Perfect rollup | Best-effort rollup | Both perfect and best-effort rollup |
| Supported partitioning methods | [Both Hash-based and range partitioning](http://druid.io/docs/latest/ingestion/hadoop.html#partitioning-specification) | N/A | Hash-based partitioning (when `forceGuaranteedRollup` = true) |
| Supported input locations | All locations accessible via HDFS client or Druid dataSource | All implemented [firehoses](./firehose.html) | All implemented [firehoses](./firehose.html) |
| Supported file formats | All implemented Hadoop InputFormats | Currently text file formats (CSV, TSV, JSON) by default. Additional formats can be added though a [custom extension](../development/modules.html) implementing [`FiniteFirehoseFactory`](https://github.com/apache/incubator-druid/blob/master/core/src/main/java/org/apache/druid/data/input/FiniteFirehoseFactory.java) | Currently text file formats (CSV, TSV, JSON) by default. Additional formats can be added though a [custom extension](../development/modules.html) implementing [`FiniteFirehoseFactory`](https://github.com/apache/incubator-druid/blob/master/core/src/main/java/org/apache/druid/data/input/FiniteFirehoseFactory.java) |
| Saving parse exceptions in ingestion report | Currently not supported | Currently not supported | Supported |
| Custom segment version | Supported, but this is NOT recommended | N/A | N/A |
