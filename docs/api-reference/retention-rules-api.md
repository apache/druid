---
id: retention-rules-api
title: Retention rules API
sidebar_label: Retention rules
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

This document describes the API endpoints for managing retention rules in Apache Druid.

## Retention rules

Note that all _interval_ URL parameters are ISO 8601 strings delimited by a `_` instead of a `/` as in `2016-06-27_2016-06-28`.

`GET /druid/coordinator/v1/rules`

Returns all rules as JSON objects for all datasources in the cluster including the default datasource.

`GET /druid/coordinator/v1/rules/{dataSourceName}`

Returns all rules for a specified datasource.

`GET /druid/coordinator/v1/rules/{dataSourceName}?full`

Returns all rules for a specified datasource and includes default datasource.

`GET /druid/coordinator/v1/rules/history?interval=<interval>`

Returns audit history of rules for all datasources. Default value of interval can be specified by setting `druid.audit.manager.auditHistoryMillis` (1 week if not configured) in Coordinator `runtime.properties`.

`GET /druid/coordinator/v1/rules/history?count=<n>`

Returns last `n` entries of audit history of rules for all datasources.

`GET /druid/coordinator/v1/rules/{dataSourceName}/history?interval=<interval>`

Returns audit history of rules for a specified datasource. Default value of interval can be specified by setting `druid.audit.manager.auditHistoryMillis` (1 week if not configured) in Coordinator `runtime.properties`.

`GET /druid/coordinator/v1/rules/{dataSourceName}/history?count=<n>`

Returns last `n` entries of audit history of rules for a specified datasource.

`POST /druid/coordinator/v1/rules/{dataSourceName}`

POST with a list of rules in JSON form to update rules.

Optional Header Parameters for auditing the config change can also be specified.

|Header Param Name| Description | Default |
|----------|-------------|---------|
|`X-Druid-Author`| Author making the config change|`""`|
|`X-Druid-Comment`| Comment describing the change being done|`""`|
