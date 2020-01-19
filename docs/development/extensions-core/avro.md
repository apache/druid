---
id: avro
title: "Apache Avro"
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

## Avro extension

This Apache Druid extension enables Druid to ingest and understand the Apache Avro data format. This extension provides 
two Avro Parsers for stream ingestion and Hadoop batch ingestion. 
See [Avro Hadoop Parser](../../ingestion/data-formats.md#avro-hadoop-parser) and [Avro Stream Parser](../../ingestion/data-formats.md#avro-stream-parser)
for more details about how to use these in an ingestion spec.

Make sure to [include](../../development/extensions.md#loading-extensions) `druid-avro-extensions` as an extension.