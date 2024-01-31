---
id: delta-lake
title: "Delta Lake extension"
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

## Delta Lake extension


Delta Lake is an open source storage framework that enables building a
Lakehouse architecture with various compute engines. [DeltaLakeInputSource](../../ingestion/input-sources.md#delta-lake-input-source) lets
you ingest data stored in a Delta Lake table into Apache Druid. To use the Delta Lake extension, add the `druid-deltalake-extensions` to the list of loaded extensions.
See [Loading extensions](../../configuration/extensions.md#loading-extensions) for more information.

The Delta input source reads the configured Delta Lake table and extracts all the underlying delta files in the table's latest snapshot.
These Delta Lake files are versioned Parquet files.

## Version support

The Delta Lake extension uses the Delta Kernel introduced in Delta Lake 3.0.0, which is compatible with Apache Spark 3.5.x.
Older versions are unsupported, so consider upgrading to Delta Lake 3.0.x or higher to use this extension.

## Known limitations

- This extension relies on the Delta Kernel API and can only read from the latest Delta table snapshot.
- Column filtering isn't supported. The extension reads all columns in the configured table.