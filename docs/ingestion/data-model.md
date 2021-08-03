---
id: data-model
title: "Druid data model"
sidebar_label: Data model
description: Introduces concepts of datasources, primary timestamp, dimensions, and metrics.
---

Druid stores data in datasources, which are similar to tables in a traditional relational database management system (RDBMS). Druid's data model shares  similarities with both relational and timeseries data models.

## Primary timestamp

Druid schemas must always include a primary timestamp. Druid uses the primary timestamp to [partition and sort](./partitioning.md) your data. Druid uses the primary timestamp to rapidly identify and retrieve data within the time range of queries. Druid also uses the primary timestamp column
for time-based [data management operations](./data-management.md) such as dropping time chunks, overwriting time chunks, and time-based retention rules.

Druid parses the primary timestamp based on the [`timestampSpec`](./ingestion-spec.md#timestampspec) configuration at ingestion time. Regardless of the source field field for the primary timestamp, Druid always stores the timestamp in the `__time` column in your Druid datasource.

You can control other important operations that are based on the primary timestamp in the
[`granularitySpec`](./ingestion-spec.md#granularityspec). If you have more than one timestamp column, you can store the others as
[secondary timestamps](./schema-design.md#secondary-timestamps).

## Dimensions

Dimensions are columns that Druid stores "as-is". You can use dimensions for any purpose. For example, you can group, filter, or apply aggregators to dimensions at query time in an ad hoc manner.

If you disable [rollup](./rollup.md), then Druid treats the set of
dimensions like a set of columns to ingest. The dimensions behave exactly as you would expect from any database that does not support a rollup feature.

At ingestion time, you configure dimensions in the [`dimensionsSpec`](./ingestion-spec.md#dimensionsspec).

## Metrics

Metrics are columns that Druid stores in an aggregated form. Metrics are most useful when you enable [rollup](rollup.md). If you Specify a metric, you can apply an aggregation function to each row during ingestion. This
has the following benefits:

- With [rollup](rollup.md) enabled, Druid collapses multiple rows into one row even while retaining summary information. For example, the [rollup tutorial](../tutorials/tutorial-rollup.md) demonstrates using rollup to collapse netflow data to a single row per `(minute, srcIP, dstIP)` tuple, while retaining aggregate information about total packet and byte counts.
- Druid can compute some aggregators, especially approximate ones, more quickly at query time if they are partially computed at ingestion time, including data that has not been rolled up.

 At ingestion time, you configure Metrics in the [`metricsSpec`](./ingestion-spec.md#metricsspec).
