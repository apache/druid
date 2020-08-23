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

Additionally, it provides an InputFormat for reading Avro OCF files when using
[native batch indexing](../../ingestion/native-batch.md), see [Avro OCF](../../ingestion/data-formats.md#avro-ocf)
for details on how to ingest OCF files.

Make sure to [include](../../development/extensions.md#loading-extensions) `druid-avro-extensions` as an extension.

### Avro Types

Druid supports most Avro types natively, there are however some exceptions which are detailed here.

`union` types which aren't of the form `[null, otherType]` aren't supported at this time.

`bytes` and `fixed` Avro types will be returned by default as base64 encoded strings unless the `binaryAsString` option is enabled on the Avro parser.
This setting will decode these types as UTF-8 strings.

`enum` types will be returned as `string` of the enum symbol.

`record` and `map` types representing nested data can be ingested using [flattenSpec](../../ingestion/data-formats.md#flattenspec) on the parser.

Druid doesn't currently support Avro logical types, they will be ignored and fields will be handled according to the underlying primitive type.