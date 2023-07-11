---
id: data-model
title: "Druid data model"
sidebar_label: Data model
description: Introduces concepts of datasources, primary timestamp, dimensions, and metrics.
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

Druid stores data in datasources, which are similar to tables in a traditional relational database management system (RDBMS). Druid's data model shares  similarities with both relational and timeseries data models.

## Primary timestamp

Druid schemas must always include a primary timestamp. Druid uses the primary timestamp to [partition and sort](./partitioning.md) your data. Druid uses the primary timestamp to rapidly identify and retrieve data within the time range of queries. Druid also uses the primary timestamp column
for time-based [data management operations](../data-management/index.md) such as dropping time chunks, overwriting time chunks, and time-based retention rules.

Druid parses the primary timestamp based on the [`timestampSpec`](./ingestion-spec.md#timestampspec) configuration at ingestion time. Regardless of the source field for the primary timestamp, Druid always stores the timestamp in the `__time` column in your Druid datasource.

You can control other important operations that are based on the primary timestamp in the
[`granularitySpec`](./ingestion-spec.md#granularityspec). If you have more than one timestamp column, you can store the others as
[secondary timestamps](./schema-design.md#secondary-timestamps).

## Dimensions

Dimensions are columns that Druid stores "as-is". You can use dimensions for any purpose. For example, you can group, filter, or apply aggregators to dimensions at query time when necessary.

If you disable [rollup](./rollup.md), then Druid treats the set of
dimensions like a set of columns to ingest. The dimensions behave exactly as you would expect from any database that does not support a rollup feature.

At ingestion time, you configure dimensions in the [`dimensionsSpec`](./ingestion-spec.md#dimensionsspec).

## Metrics

Metrics are columns that Druid stores in an aggregated form. Metrics are most useful when you enable [rollup](rollup.md). If you specify a metric, you can apply an aggregation function to each row during ingestion. This
has the following benefits:

Rollup is a form of aggregation that collapses dimensions while aggregating the values in the metrics, that is, it collapses rows but retains its summary information."
- [Rollup](rollup.md) is a form of aggregation that combines multiple rows with the same timestamp value and dimension values. For example, the [rollup tutorial](../tutorials/tutorial-rollup.md) demonstrates using rollup to collapse netflow data to a single row per `(minute, srcIP, dstIP)` tuple, while retaining aggregate information about total packet and byte counts.
- Druid can compute some aggregators, especially approximate ones, more quickly at query time if they are partially computed at ingestion time, including data that has not been rolled up.

 At ingestion time, you configure Metrics in the [`metricsSpec`](./ingestion-spec.md#metricsspec).
