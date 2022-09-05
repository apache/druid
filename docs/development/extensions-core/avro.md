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

This Apache Druid extension enables Druid to ingest and parse the Apache Avro data format as follows:
- [Avro stream input format](../../ingestion/data-formats.md#avro-stream) for Kafka and Kinesis.
- [Avro OCF input format](../../ingestion/data-formats.md#avro-ocf) for native batch ingestion.
- [Avro Hadoop Parser](../../ingestion/data-formats.md#avro-hadoop-parser).

The [Avro Stream Parser](../../ingestion/data-formats.md#avro-stream-parser) is deprecated.

## Load the Avro extension

To use the Avro extension, add the `druid-avro-extensions` to the list of loaded extensions. See [Loading extensions](../../development/extensions.md#loading-extensions) for more information.

## Avro types

Druid supports most Avro types natively. This section describes some  exceptions.

### Unions
Druid has two modes for supporting `union` types.

The default mode treats unions as a single value regardless of the type of data populating the union.

If you want to operate on individual members of a union, set `extractUnionsByType` on the Avro parser. This configuration expands union values into nested objects according to the following rules:
- Primitive types and unnamed complex types are keyed by their type name, such as `int` and `string`.
- Complex named types are keyed by their names, this includes `record`, `fixed`, and `enum`.
- The Avro null type is elided as its value can only ever be null.

This is safe because an Avro union can only contain a single member of each unnamed type and duplicates of the same named type are not allowed. For example, only a single array is allowed, multiple records (or other named types) are allowed as long as each has a unique name.

You can then access the members of the union with a [flattenSpec](../../ingestion/data-formats.md#flattenspec) like you would for other nested types.

### Binary types
The extension returns `bytes` and `fixed` Avro types as base64 encoded strings by default. To decode these types as UTF-8 strings, enable the `binaryAsString` option on the Avro parser.

### Enums
The extension returns `enum` types as `string` of the enum symbol.

### Complex types
You can ingest `record` and `map` types representing nested data with a [flattenSpec](../../ingestion/data-formats.md#flattenspec) on the parser.

### Logical types
Druid does not currently support Avro logical types. It ignores them and handles fields according to the underlying primitive type.
