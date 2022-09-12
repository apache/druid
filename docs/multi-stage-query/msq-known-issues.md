---
id: known-issues
title: SQL-based ingestion known issues
sidebar_label: Known issues
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

> SQL-based ingestion using the multi-stage query task engine is our recommended solution starting in Druid 24.0. Alternative ingestion solutions, such as native batch and Hadoop-based ingestion systems, will still be supported. We recommend you read all [known issues](./msq-known-issues.md) and test the feature in a development environment before rolling it out in production. Using the multi-stage query task engine with `SELECT` statements that do not write to a datasource is experimental.

## General query execution

- There's no fault tolerance. If any task fails, the entire query fails. 

- Only one local file system per server is used for stage output data during multi-stage query
  execution. If your servers have multiple local file systems, this causes queries to exhaust
  available disk space earlier than expected. 

- When `msqMaxNumTasks` is higher than the total
  capacity of the cluster, more tasks may be launched than can run at once. This leads to a
  [TaskStartTimeout](./msq-reference.md#context-parameters) error code, as there is never enough capacity to run the query.
  To avoid this, set `msqMaxNumTasks` to a number of tasks that can run simultaneously on your cluster.

- When `msqTaskAssignment` is set to `auto`, the system generates one task per input file for certain splittable
  input sources where file sizes are not known ahead of time. This includes the `http` input source, where the system
  generates one task per URI.

## Memory usage

- INSERT queries can consume excessive memory when using complex types due to inaccurate footprint
  estimation. This can appear as an OutOfMemoryError during the SegmentGenerator stage when using
  sketches. If you run into this issue, try manually lowering the value of the
  [`msqRowsInMemory`](./msq-reference.md#context-parameters) parameter.

- EXTERN loads an entire row group into memory at once when reading from Parquet files. Row groups
  can be up to 1 GB in size, which can lead to excessive heap usage when reading many files in
  parallel. This can appear as an OutOfMemoryError during stages that read Parquet input files. If
  you run into this issue, try using a smaller number of worker tasks or you can increase the heap
  size of your Indexers or of your Middle Manager-launched indexing tasks.

- Ingesting a very long row may consume excessive memory and result in an OutOfMemoryError. If a row is read 
  which requires more memory than is available, the service might throw OutOfMemoryError. If you run into this
  issue, allocate enough memory to be able to store the largest row to the indexer. 

## SELECT queries

- SELECT query results do not include real-time data until it has been published.

- TIMESTAMP types are formatted as numbers rather than ISO8601 timestamp
  strings, which differs from Druid's standard result format. 

- BOOLEAN types are formatted as numbers like `1` and `0` rather
  than `true` or `false`, which differs from Druid's standard result
  format. 

- TopN is not implemented. The context parameter
  `useApproximateTopN` is ignored and always treated as if it
  were `false`. Therefore, topN-shaped queries will
  always run using the groupBy engine. There is no loss of
  functionality, but there may be a performance impact, since
  these queries will run using an exact algorithm instead of an
  approximate one.
- GROUPING SETS is not implemented. Queries that use GROUPING SETS
  will fail.
- The numeric flavors of the EARLIEST and LATEST aggregators do not work properly. Attempting to use the numeric flavors of these aggregators will lead to an error like `java.lang.ClassCastException: class java.lang.Double cannot be cast to class org.apache.druid.collections.SerializablePair`. The string flavors, however, do work properly.

##  INSERT queries

- The [schemaless dimensions](../ingestion/ingestion-spec.md#inclusions-and-exclusions)
feature is not available. All columns and their types must be specified explicitly.

- [Segment metadata queries](../querying/segmentmetadataquery.md)
  on datasources ingested with the Multi-Stage Query Engine will return values for`timestampSpec` that are not usable
  for introspection.

- When INSERT with GROUP BY does the match the criteria mentioned in [GROUP BY](./index.md#group-by),  the multi-stage engine generates segments that Druid's compaction
  functionality is not able to further roll up. This applies to automatic compaction as well as manually
  issued `compact` tasks. Individual queries executed with the multi-stage engine always guarantee
  perfect rollup for their output, so this only matters if you are performing a sequence of INSERT
  queries that each append data to the same time chunk. If necessary, you can compact such data
  using another SQL query instead of a `compact` task.

- When using INSERT with GROUP BY, splitting of large partitions is not currently
  implemented. If a single partition key appears in a
  very large number of rows, an oversized segment will be created.
  You can mitigate this by adding additional columns to your
  partition key. Note that partition splitting _does_ work properly
  when performing INSERT without GROUP BY.

- INSERT with column lists, like
  `INSERT INTO tbl (a, b, c) SELECT ...`, is not implemented.

## EXTERN queries

- EXTERN does not accept `druid` input sources.

## Missing guardrails

- Maximum number of input files. Since there's no limit, the controller can potentially run out of memory tracking all input files

- Maximum amount of local disk space to use for temporary data. No guardrail today means worker tasks may exhaust all available disk space. In this case, you will receive an [UnknownError](./msq-reference.md#error-codes)) with a message including "No space left on device".