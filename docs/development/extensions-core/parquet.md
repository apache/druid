---
id: parquet
title: "Apache Parquet Extension"
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


This Apache Druid module extends [Druid Hadoop based indexing](../../ingestion/hadoop.md) to ingest data directly from offline
Apache Parquet files.

Note: If using the `parquet-avro` parser for Apache Hadoop based indexing, `druid-parquet-extensions` depends on the `druid-avro-extensions` module, so be sure to
 [include  both](../../development/extensions.md#loading-extensions).

The `druid-parquet-extensions` provides the [Parquet input format](../../ingestion/data-formats.md#parquet), the [Parquet Hadoop parser](../../ingestion/data-formats.md#parquet-hadoop-parser),
and the [Parquet Avro Hadoop Parser](../../ingestion/data-formats.md#parquet-avro-hadoop-parser) with `druid-avro-extensions`.
The Parquet input format is available for [native batch ingestion](../../ingestion/native-batch.md)
and the other 2 parsers are for [Hadoop batch ingestion](../../ingestion/hadoop.md).
Please see corresponding docs for details.
