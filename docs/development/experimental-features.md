---
id: experimental-features
title: "Experimental features"
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

The following features are marked [experimental](./experimental.md) in the Druid docs.

This document includes each page that mentions an experimental feature. To graduate a feature, remove all mentions of its experimental status on all relevant pages.

Note that this document does not track the status of contrib extensions, all of which are considered experimental.

## SQL-based ingestion

- [SQL-based ingestion](../multi-stage-query/index.md)
- [SQL-based ingestion concepts](../multi-stage-query/concepts.md)
- [SQL-based ingestion and multi-stage query task API](../api-reference/sql-ingestion-api.md)

## Indexer service

- [Indexer service](../design/indexer.md)
- [Data server](../design/architecture.md#indexer-service-optional)

## Kubernetes

- [Kubernetes](../development/extensions-core/kubernetes.md)

## Segment locking

- [Configuration reference](../configuration/index.md#overlord-operations)
- [Task reference](../ingestion/tasks.md#locking)
- [Design](../design/storage.md#availability-and-consistency)

## Front coding

- [Ingestion spec reference](../ingestion/ingestion-spec.md#front-coding)

## Other configuration properties

- [Configuration reference](../configuration/index.md)
   - `CLOSED_SEGMENTS_SINKS` mode
