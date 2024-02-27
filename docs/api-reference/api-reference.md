---
id: api-reference
title: API reference
sidebar_label: Overview
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


This topic is an index to the Apache Druid API documentation.

## HTTP APIs
* [Druid SQL queries](./sql-api.md) to submit SQL queries using the Druid SQL API.
* [SQL-based ingestion](./sql-ingestion-api.md) to submit SQL-based batch ingestion requests.
* [JSON querying](./json-querying-api.md) to submit JSON-based native queries.
* [Tasks](./tasks-api.md) to manage data ingestion operations.
* [Supervisors](./supervisor-api.md) to manage supervisors for data ingestion lifecycle and data processing.
* [Retention rules](./retention-rules-api.md) to define and manage data retention rules across datasources.
* [Data management](./data-management-api.md) to manage data segments.
* [Automatic compaction](./automatic-compaction-api.md) to optimize segment sizes after ingestion.
* [Lookups](./lookups-api.md) to manage and modify key-value datasources.
* [Service status](./service-status-api.md) to monitor components within the Druid cluster. 
* [Dynamic configuration](./dynamic-configuration-api.md) to configure the behavior of the Coordinator and Overlord processes.
* [Legacy metadata](./legacy-metadata-api.md) to retrieve datasource metadata.

## Java APIs
* [SQL JDBC driver](./sql-jdbc.md) to connect to Druid and make Druid SQL queries using the Avatica JDBC driver.