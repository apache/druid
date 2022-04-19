---
id: sql
title: "Druid SQL overview"
sidebar_label: "Druid SQL overview"
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

> Apache Druid supports two query languages: Druid SQL and [native queries](querying.md).
> This document describes the SQL language.

You can query data in Druid datasources using Druid SQL.
Druid uses [Apache Calcite](https://calcite.apache.org/) to parse and plan SQL queries.
Druid translates SQL statements into its native JSON-based query language.
Other than the slight overhead of translating SQL on the Broker, there isn't an
additional performance penalty to using Druid SQL compared to native queries.

Druid SQL planning occurs on the Broker.
Set [Broker runtime properties](../configuration/index.md#sql) to configure the query plan and JDBC querying.

See [Defining SQL permissions](../operations/security-user-auth.md#sql-permissions)
for information on permissions needed to make SQL queries.

## Unsupported features

Druid does not support all SQL features. In particular, the following features are not supported.

- JOIN between native datasources (table, lookup, subquery) and [system tables](sql-metadata-tables.md).
- JOIN conditions that are not an equality between expressions from the left- and right-hand sides.
- JOIN conditions containing a constant value inside the condition.
- JOIN conditions on a column which contains a multi-value dimension.
- OVER clauses, and analytic functions such as `LAG` and `LEAD`.
- ORDER BY for a non-aggregating query, except for `ORDER BY __time` or `ORDER BY __time DESC`, which are supported.
  This restriction only applies to non-aggregating queries; you can ORDER BY any column in an aggregating query.
- DDL and DML.
- Using Druid-specific functions like `TIME_PARSE` and `APPROX_QUANTILE_DS` on [system tables](sql-metadata-tables.md).

Additionally, some Druid native query features are not supported by the SQL language. Some unsupported Druid features
include:

- [Inline datasources](datasource.md#inline).
- [Spatial filters](../development/geo.md).
- [Multi-value dimensions](sql-data-types.md#multi-value-strings) are only partially implemented in Druid SQL. There are known
inconsistencies between their behavior in SQL queries and in native queries due to how they are currently treated by
the SQL planner.
