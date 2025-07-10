---
id: migr-subquery-limit
title: "Migration guide: Subquery limit"
sidebar_label: Subquery limit
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

Druid now allows you to set a byte-based limit on subquery size, to prevent brokers from running out of memory when handling large subqueries. 
Druid uses subqueries as joins as well as in common table expressions, such as WITH.

The byte-based subquery limit overrides Druid's row-based subquery limit.

:::info
We recommend that you move towards using byte-based limits starting in Druid 30.0.
:::

For queries that generate a large number of rows (5 million or more), we recommend that you don't use `maxSubqueryBytes` from the outset. 
You can increase `maxSubqueryRows` and then configure the byte-based limit if you find that Druid needs it to process the query.

## Row-based subquery limit

Druid uses the `maxSubqueryRows` property to limit the number of rows Druid returns in a subquery. 
Because this is a row-based limit, it doesn't restrict the overall size of the returned data.

The `maxSubqueryRows` property is set to 100,000 by default.

## Enable a byte-based subquery limit

Set the optional property `maxSubqueryBytes` to set a maximum number of returned bytes. 
This property takes precedence over `maxSubqueryRows`.

## Usage considerations

You can set both `maxSubqueryRows` and `maxSubqueryBytes` at cluster level and override them in individual queries. 
See [Overriding default query context values](../configuration#overriding-default-query-context-values) for more information.

Make sure you enable the Broker monitor `SubqueryCountStatsMonitor` so that Druid emits metrics for subquery statistics.
To do this, add `org.apache.druid.server.metrics.SubqueryCountStatsMonitor` to the `druid.monitoring.monitors` property in your Broker's `runtime.properties` configuration file.
See [Metrics monitors](../configuration/index.md#metrics-monitors-for-each-service) for more information.

## Learn more

See the following topics for more information:

- [Query context](../querying/query-context.md) for information on setting query context parameters.
- [Broker configuration reference](../configuration#guardrails-for-materialization-of-subqueries) for more information on `maxSubqueryRows` and `maxSubqueryBytes`.
