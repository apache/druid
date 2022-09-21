---
id: index
title: SQL-based ingestion
sidebar_label: Overview
description: Introduces multi-stage query architecture and its task engine
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

> This page describes SQL-based batch ingestion using the [`druid-multi-stage-query`](../multi-stage-query/index.md)
> extension, new in Druid 24.0. Refer to the [ingestion methods](../ingestion/index.md#batch) table to determine which
> ingestion method is right for you.

Apache Druid supports SQL-based ingestion using the bundled [`druid-multi-stage-query` extension](#load-the-extension).
This extension adds a [multi-stage query task engine for SQL](concepts.md#multi-stage-query-task-engine) that allows running SQL
[INSERT](concepts.md#insert) and [REPLACE](concepts.md#replace) statements as batch tasks. As an experimental feature,
the task engine also supports running SELECT queries as batch tasks.

Nearly all SELECT capabilities are available in the multi-stage query (MSQ) task engine, with certain exceptions listed on the [Known
issues](./known-issues.md#select) page. This allows great flexibility to apply transformations, filters, JOINs,
aggregations, and so on as part of `INSERT ... SELECT` and `REPLACE ... SELECT` statements. This also allows in-database
transformation: creating new tables based on queries of other tables.

## Vocabulary

- **Controller**: An indexing service task of type `query_controller` that manages
  the execution of a query. There is one controller task per query.

- **Worker**: Indexing service tasks of type `query_worker` that execute a
  query. There can be multiple worker tasks per query. Internally,
  the tasks process items in parallel using their processing pools (up to `druid.processing.numThreads` of execution parallelism
  within a worker task).

- **Stage**: A stage of query execution that is parallelized across
  worker tasks. Workers exchange data with each other between stages.

- **Partition**: A slice of data output by worker tasks. In INSERT or REPLACE
  queries, the partitions of the final stage become Druid segments.

- **Shuffle**: Workers exchange data between themselves on a per-partition basis in a process called
  shuffling. During a shuffle, each output partition is sorted by a clustering key.

## Load the extension

To add the extension to an existing cluster, add `druid-multi-stage-query` to `druid.extensions.loadlist` in your
`common.runtime.properties` file.

For more information about how to load an extension, see [Loading extensions](../development/extensions.md#loading-extensions).

To use [EXTERN](reference.md#extern), you need READ permission on the resource named "EXTERNAL" of the resource type
"EXTERNAL". If you encounter a 403 error when trying to use EXTERN, verify that you have the correct permissions.

## Next steps

* [Read about key concepts](./concepts.md) to learn more about how SQL-based ingestion and multi-stage queries work.
* [Check out the examples](./examples.md) to see SQL-based ingestion in action.
* [Explore the Query view](../operations/web-console.md) to get started in the web console.
