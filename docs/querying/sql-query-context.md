---
id: sql-query-context
title: "SQL query context"
sidebar_label: "SQL query context"
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

:::info
 Apache&circledR; Druid supports two query languages: Druid SQL and [native queries](querying.md).
 This document describes the SQL language.
:::

In Apache&circledR; Druid, you can control how your [Druid SQL queries](./sql.md) queries run by using query context parameters. The parameters let you adjust aspects of query processing such as using approximations, selecting particular filters, controlling how lookups are executed.

For additional context parameters supported for all query types, refer to [Query context reference](query-context-reference.md). To learn how to set the query context, see [Set query context](../querying/query-context.md).

The following query context parameters apply to Druid SQL only. See the [query context reference](query-context-reference.md#druid-sql-parameters) for details.

<!-- BEGIN GENERATED QUERY CONTEXT PARAMETER TABLE: queries=SQL engines~NATIVE queryTypes= -->

|Parameter|Statements|Description|Default|
|---|---|---|---|
|[`enableJoinLeftTableScanDirect`](query-context-reference.md#enablejoinlefttablescandirect)||This parameter applies to queries with joins.|`false`|
|[`enableTimeBoundaryPlanning`](query-context-reference.md#enabletimeboundaryplanning)||If `true`, Druid converts SQL queries to [time boundary queries](../querying/timeboundaryquery.md) wherever possible.|`false`|
|[`engine`](query-context-reference.md#engine)||SQL engine that runs the query through the `/druid/v2/sql` endpoint.|`native`|
|[`inFunctionExprThreshold`](query-context-reference.md#infunctionexprthreshold)||At or beyond this threshold number of values, SQL `IN` is eligible for execution using the native function `scalar_in_array` rather than an <code>&#124;&#124;</code> of `==`, even if the number of values is below `inFunctionThreshold`.|`2`|
|[`inFunctionThreshold`](query-context-reference.md#infunctionthreshold)||At or beyond this threshold number of values, Druid converts SQL `IN` to [`SCALAR_IN_ARRAY`](../querying/sql-functions.md#scalar_in_array).|`100`|
|[`inSubQueryThreshold`](query-context-reference.md#insubquerythreshold)||At or beyond this threshold number of values, Druid converts SQL `IN` to `JOIN` on an inline table.|`2147483647`|
|[`maxNumericInFilters`](query-context-reference.md#maxnumericinfilters)||Max limit for the amount of numeric values that Druid can compare for a string type dimension when the entire SQL WHERE clause of a query translates only to an [OR](../querying/filters.md#or) of [bound filter](../querying/filters.md#bound-filter).|`druid.sql.planner.maxNumericInFilters`|
|[`maxPlanningTimeMs`](query-context-reference.md#maxplanningtimems)||Maximum wall-clock time, in milliseconds, allowed for planning this query on the Broker.|`0`|
|[`sqlFinalizeOuterSketches`](query-context-reference.md#sqlfinalizeoutersketches)||If `false` (default behavior in Druid 25.0.0 and later), `DS_HLL`, `DS_THETA`, and `DS_QUANTILES_SKETCH` return sketches in query results.|`false`|
|[`sqlPlannerBloat`](query-context-reference.md#sqlplannerbloat)||Calcite parameter which controls whether to merge two Project operators when inlining expressions causes complexity to increase.|`1000`|
|[`sqlPullUpLookup`](query-context-reference.md#sqlpulluplookup)||Whether to consider the [pull-up rewrite](../querying/lookups.md#pull-up) of the `LOOKUP` function during SQL planning.|`true`|
|[`sqlQueryId`](query-context-reference.md#sqlqueryid)||SQL query ID.|auto-generated|
|[`sqlReverseLookup`](query-context-reference.md#sqlreverselookup)||Whether to consider the [reverse-lookup rewrite](../querying/lookups.md#reverse-lookup) of the `LOOKUP` function during SQL planning.|`true`|
|[`sqlReverseLookupThreshold`](query-context-reference.md#sqlreverselookupthreshold)||Maximum size of `IN` filter to create when applying a [reverse-lookup rewrite](../querying/lookups.md#reverse-lookup).|`10000`|
|[`sqlStringifyArrays`](query-context-reference.md#sqlstringifyarrays)||If `true`, Druid serializes result columns with array values as JSON strings in the response instead of arrays.|`true`, except for JDBC connections, where it's always `false`|
|[`sqlTimeCondition`](query-context-reference.md#sqltimecondition)|SELECT|Time condition to apply to all tables that the query reads, except global tables on the right side of a JOIN.|none|
|[`sqlTimeZone`](query-context-reference.md#sqltimezone)||Time zone for a connection.|`druid.sql.planner.sqlTimeZone`|
|[`sqlUseBoundAndSelectors`](query-context-reference.md#sqluseboundandselectors)||If `false` (default behavior in Druid 27.0.0 and later), the SQL planner uses [equality](../querying/filters.md#equality-filter), [null](../querying/filters.md#null-filter), and [range](../querying/filters.md#range-filter) filters instead of [selector](../querying/filters.md#selector-filter) and [bounds](../querying/filters.md#bound-filter).|`false`|
|[`sqlUseExtractionFns`](query-context-reference.md#sqluseextractionfns)||If false, the SQL planner avoids using [`extractionFn`](../querying/dimensionspecs.md#extraction-functions) in favor of using other constructs such as [virtual columns](../querying/virtual-columns.md).|`false`|
|[`useApproximateCountDistinct`](query-context-reference.md#useapproximatecountdistinct)||Whether to use an approximate cardinality algorithm for `COUNT(DISTINCT foo)`.|`druid.sql.planner.useApproximateCountDistinct`|
|[`useApproximateTopN`](query-context-reference.md#useapproximatetopn)||If `true`, Druid converts SQL queries to approximate [TopN queries](../querying/topnquery.md) wherever possible.|`druid.sql.planner.useApproximateTopN`|
|[`useGroupingSetForExactDistinct`](query-context-reference.md#usegroupingsetforexactdistinct)||Whether to use grouping sets to execute queries with multiple exact distinct aggregations.|`druid.sql.planner.useGroupingSetForExactDistinct`|
|[`useLexicographicTopN`](query-context-reference.md#uselexicographictopn)||If `true`, Druid can use [TopN queries](../querying/topnquery.md) with lexicographic dimension ordering.|`druid.sql.planner.useLexicographicTopN`|
|[`useNativeQueryExplain`](query-context-reference.md#usenativequeryexplain)||If `true`, `EXPLAIN PLAN FOR` returns the explain plan as a JSON representation of equivalent native query, else it returns the original version of explain plan generated by Calcite.|`druid.sql.planner.useNativeQueryExplain`|

<!-- END GENERATED QUERY CONTEXT PARAMETER TABLE -->

## Learn more
- [Set query context](../querying/query-context.md) for how to set the query context.
- [Query context reference](query-context-reference.md)  for available query context parameters.
- [Overriding default query context values](../configuration/index.md#overriding-default-query-context-values) for how to set cluster-wide defaults for any query context parameter with `druid.query.default.context.*`.
- [MSQ context parameters](../multi-stage-query/reference.md#context-parameters) for how to set context parameters for Multi-Stage Queries.
