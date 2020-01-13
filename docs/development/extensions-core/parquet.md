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

## Parquet and Native Batch
This extension provides a `parquet` input format which can be used with Druid [native batch ingestion](../../ingestion/native-batch.md).

### Parquet InputFormat

Druid supports Parquet input format. See [Data formats](../../ingestion/data-formats.md#parquet) for details.

## Parquet Hadoop Parser

