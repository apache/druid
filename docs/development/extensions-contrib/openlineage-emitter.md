---
id: openlineage-emitter
title: "OpenLineage Emitter"
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

To use this Apache Druid extension, [include](../../configuration/extensions.md#loading-extensions) `openlineage-emitter` in the extensions load list.

## Introduction

This extension emits [OpenLineage](https://openlineage.io) `RunEvent`s for each completed Druid query, enabling data lineage tracking with any OpenLineage-compatible backend such as [Marquez](https://marquezproject.ai).

For SQL queries, the SQL text is parsed to extract input datasources (FROM clauses, JOINs, CTEs) and the output datasource (INSERT INTO). For native queries, table names are resolved from the datasource tree. Native sub-queries spawned by a SQL execution are deduplicated against the SQL-level event.

:::note
SQL table extraction relies on `calcite-core` being on the classpath, which is the case on Broker nodes. Native query lineage is available on all nodes.
:::

## Configuration

All configuration parameters are under `druid.request.logging`.

| Property | Description | Required | Default |
|---|---|---|---|
| `druid.request.logging.type` | Set to `openlineage` to enable this extension. | yes | — |
| `druid.request.logging.namespace` | Namespace used for OpenLineage job and dataset URIs. Typically the Broker URL. | no | `druid://<hostname>` |
| `druid.request.logging.transportType` | Where to send events. `CONSOLE` logs JSON to the Druid log; `HTTP` POSTs to an OpenLineage API endpoint. | no | `CONSOLE` |
| `druid.request.logging.transportUrl` | OpenLineage API endpoint URL. Required when `transportType=HTTP`. | no | — |
| `druid.request.logging.excludedNativeQueryTypes` | Native query types to exclude from lineage emission. Internal broker queries like segment metadata lookups produce noisy, low-value events. | no | `["segmentMetadata", "dataSourceMetadata", "timeBoundary"]` |

### Examples

**Console (development)**

```properties
druid.request.logging.type=openlineage
druid.request.logging.namespace=druid://broker.prod:8082
```

**HTTP (production)**

```properties
druid.request.logging.type=openlineage
druid.request.logging.namespace=druid://broker.prod:8082
druid.request.logging.transportType=HTTP
druid.request.logging.transportUrl=http://marquez:5000/api/v1/lineage
```

**Combined with another logger using the `composing` provider**

```properties
druid.request.logging.type=composing
druid.request.logging.loggerProviders=[{"type":"slf4j"},{"type":"openlineage","namespace":"druid://broker.prod:8082","transportType":"HTTP","transportUrl":"http://marquez:5000/api/v1/lineage"}]
```

## Event structure

Each emitted event follows the [OpenLineage spec](https://openlineage.io/spec/2-0-2/OpenLineage.json) and includes the following facets.

### Run facets

| Facet | Description |
|---|---|
| `processing_engine` | Engine name (`druid`). Standard OpenLineage facet. |
| `druid_query_context` | Query metadata: `identity` (authenticated user), `remoteAddress`, `queryType`, and `nativeQueryIds` (for SQL queries). |
| `druid_query_statistics` | Execution stats: `durationMs`, `bytes`, `planningTimeMs`, `statusCode`. |
| `errorMessage` | Exception message for failed queries. Standard OpenLineage facet. |

### Job facets

| Facet | Description |
|---|---|
| `jobType` | `processingType=BATCH`, `integration=DRUID`, `jobType=QUERY`. Standard OpenLineage facet. |
| `sql` | Raw SQL text. Present on SQL queries only. Standard OpenLineage facet. |
