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

:::info
 This page describes SQL-based batch ingestion using the [`druid-multi-stage-query`](../multi-stage-query/index.md)
 extension, new in Druid 24.0. Refer to the [ingestion methods](../ingestion/index.md#batch) table to determine which
 ingestion method is right for you.
:::

## Multi-stage query task runtime

- Fault tolerance is partially implemented. Workers get relaunched when they are killed unexpectedly. The controller does not get relaunched if it is killed unexpectedly.

- Worker task stage outputs are stored in the working directory given by `druid.indexer.task.baseDir`. Stages that
generate a large amount of output data may exhaust all available disk space. In this case, the query fails with
an [UnknownError](./reference.md#error_UnknownError) with a message including "No space left on device".

## `SELECT` Statement

- `GROUPING SETS` are not implemented. Queries using these features return a
  [QueryNotSupported](reference.md#error_QueryNotSupported) error.

## `INSERT` and `REPLACE` Statements

- The `INSERT` and `REPLACE` statements with column lists, like `INSERT INTO tbl (a, b, c) SELECT ...`, is not implemented.

- `INSERT ... SELECT` and `REPLACE ... SELECT` insert columns from the `SELECT` statement based on column name. This
differs from SQL standard behavior, where columns are inserted based on position.

- `INSERT` and `REPLACE` do not support all options available in [ingestion specs](../ingestion/ingestion-spec.md),
including the `createBitmapIndex` and `multiValueHandling` [dimension](../ingestion/ingestion-spec.md#dimension-objects)
properties, and the `indexSpec` [`tuningConfig`](../ingestion/ingestion-spec.md#tuningconfig) property.

## `EXTERN` Function

- The [schemaless dimensions](../ingestion/ingestion-spec.md#inclusions-and-exclusions)
  feature is not available. All columns and their types must be specified explicitly using the `signature` parameter
  of the [`EXTERN` function](reference.md#extern-function).

- `EXTERN` with input sources that match large numbers of files may exhaust available memory on the controller task.

- `EXTERN` refers to external files. Use `FROM` to access `druid` input sources.
