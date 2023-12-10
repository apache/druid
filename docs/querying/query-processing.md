---
id: query-processing
title: "Query processing"
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

This topic provides a high-level overview of how Apache Druid distributes and processes queries.

The general flow is as follows:

1. A query enters the [Broker](../design/broker.md) service, which identifies the segments with data that may pertain to that query. The list of segments is always pruned by time, and may also be pruned by other attributes depending on how the datasource is partitioned.
2. The Broker identifies which [Historical](../design/historical.md) and [MiddleManager](../design/middlemanager.md) services are serving those segments and distributes a rewritten subquery to each of the services.
3. The Historical and MiddleManager services execute each subquery and return results to the Broker.
4. The Broker merges the partial results to get the final answer, which it returns to the original caller.

Druid uses time and attribute pruning to minimize the data it must scan for each query.

For filters that are more precise than what the Broker uses for pruning, the [indexing structures](../design/storage.md#indexing-and-handoff) inside each segment allow Historical services to identify matching rows before accessing the data. Once the Historical service knows which rows match a particular query, it only accesses the requires rows and columns.

To maximize query performance, Druid uses the following techniques:

- Pruning the set of segments accessed for a query.
- Within each segment, using indexes to identify which rows must be accessed.
- Within each segment, only reading the specific rows and columns that are relevant to a particular query.

## Learn more

See the following topic for more information:

* [Query execution](../querying/query-execution.md) to learn how Druid services process query statements.