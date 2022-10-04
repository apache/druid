---
id: security
title: SQL-based ingestion security
sidebar_label: Security
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

> This page describes SQL-based batch ingestion using the [`druid-multi-stage-query`](../multi-stage-query/index.md)
> extension, new in Druid 24.0. Refer to the [ingestion methods](../ingestion/index.md#batch) table to determine which
> ingestion method is right for you.

All authenticated users can use the multi-stage query task engine (MSQ task engine) through the UI and API if the extension is loaded. However, without additional permissions, users are not able to issue queries that read or write Druid datasources or external data. The permission needed depends on what the user is trying to do.

To submit a query:

- SELECT from a Druid datasource requires the READ DATASOURCE permission on that datasource.
- [INSERT](reference.md#insert) or [REPLACE](reference.md#replace) into a Druid datasource requires the WRITE DATASOURCE
  permission on that datasource.
- [EXTERN](reference.md#extern) requires READ permission on a resource named "EXTERNAL" with type "EXTERNAL". Users without the correct
  permission encounter a 403 error when trying to run queries that include EXTERN.

Once a query is submitted, it executes as a [`query_controller`](concepts.md#execution-flow) task. Query tasks that
users submit to the MSQ task engine are Overlord tasks, so they follow the Overlord's security model. This means that
users with access to the Overlord API can perform some actions even if they didn't submit the query, including
retrieving status or canceling a query. For more information about the Overlord API and the task API, see [APIs for
SQL-based ingestion](./api.md).

To interact with a query through the Overlord API, users need the following permissions:

- INSERT or REPLACE queries: Users must have READ DATASOURCE permission on the output datasource.
- SELECT queries: Users must have read permissions on the `__query_select` datasource, which is a stub datasource that gets created.
