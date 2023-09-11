---
id: dynamic-configuration-api
title: Dynamic configuration API
sidebar_label: Dynamic configuration
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

This document describes the API endpoints to retrieve and manage the dynamic configurations for the [Coordinator](../configuration/index.md#overlord-dynamic-configuration) and [Overlord](../configuration/index.md#dynamic-configuration) in Apache Druid.

## Coordinator dynamic configuration

See [Coordinator Dynamic Configuration](../configuration/index.md#dynamic-configuration) for details.

Note that all _interval_ URL parameters are ISO 8601 strings delimited by a `_` instead of a `/`
as in `2016-06-27_2016-06-28`.

`GET /druid/coordinator/v1/config`

Retrieves current coordinator dynamic configuration.

`GET /druid/coordinator/v1/config/history?interval={interval}&count={count}`

Retrieves history of changes to overlord dynamic configuration. Accepts `interval` and  `count` query string parameters
to filter by interval and limit the number of results respectively.

`POST /druid/coordinator/v1/config`

Update overlord dynamic worker configuration.


## Overlord dynamic configuration

See [Overlord Dynamic Configuration](../configuration/index.md#overlord-dynamic-configuration) for details.

Note that all _interval_ URL parameters are ISO 8601 strings delimited by a `_` instead of a `/`
as in `2016-06-27_2016-06-28`.

`GET /druid/indexer/v1/worker`

Retrieves current overlord dynamic configuration.

`GET /druid/indexer/v1/worker/history?interval={interval}&count={count}`

Retrieves history of changes to overlord dynamic configuration. Accepts `interval` and  `count` query string parameters
to filter by interval and limit the number of results respectively.

`GET /druid/indexer/v1/workers`

Retrieves a list of all the worker nodes in the cluster along with its metadata.

`GET /druid/indexer/v1/scaling`

Retrieves overlord scaling events if auto-scaling runners are in use.

`POST /druid/indexer/v1/worker`

Update overlord dynamic worker configuration.