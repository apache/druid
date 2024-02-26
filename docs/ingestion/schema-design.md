---
id: schema-design
title: "Schema design tips"
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

## Druid's data model

For general information, check out the documentation on [Druid schema model](./schema-model.md) on the main
ingestion overview page. The rest of this page discusses tips for users coming from other kinds of systems, as well as
general tips and common practices.

* Druid data is stored in [datasources](./schema-model.md), which are similar to tables in a traditional RDBMS.
* Druid datasources can be ingested with or without [rollup](./rollup.md). With rollup enabled, Druid partially aggregates your data during ingestion, potentially reducing its row count, decreasing storage footprint, and improving query performance. With rollup disabled, Druid stores one row for each row in your input data, without any pre-aggregation.
* Every row in Druid must have a timestamp. Data is always partitioned by time, and every query has a time filter. Query results can also be broken down by time buckets like minutes, hours, days, and so on.
* All columns in Druid datasources, other than the timestamp column, are either dimensions or metrics. This follows the [standard naming convention](https://en.wikipedia.org/wiki/Online_analytical_processing#Overview_of_OLAP_systems) of OLAP data.
* Typical production datasources have tens to hundreds of columns.
* [Dimension columns](./schema-model.md#dimensions) are stored as-is, so they can be filtered on, grouped by, or aggregated at query time. They are always single Strings, [arrays of Strings](../querying/multi-value-dimensions.md), single Longs, single Doubles or single Floats.
* [Metric columns](./schema-model.md#metrics) are stored [pre-aggregated](../querying/aggregations.md), so they can only be aggregated at query time (not filtered or grouped by). They are often stored as numbers (integers or floats) but can also be stored as complex objects like [HyperLogLog sketches or approximate quantile sketches](../querying/aggregations.md#approximate-aggregations). Metrics can be configured at ingestion time even when rollup is disabled, but are most useful when rollup is enabled.

## If you're coming from a

### Relational model

(Like Hive or PostgreSQL.)

Druid datasources are generally equivalent to tables in a relational database. Druid [lookups](../querying/lookups.md)
can act similarly to data-warehouse-style dimension tables, but as you'll see below, denormalization is often
recommended if you can get away with it.

Common practice for relational data modeling involves [normalization](https://en.wikipedia.org/wiki/Database_normalization):
the idea of splitting up data into multiple tables such that data redundancy is reduced or eliminated. For example, in a
"sales" table, best-practices relational modeling calls for a "product id" column that is a foreign key into a separate
"products" table, which in turn has "product id", "product name", and "product category" columns. This prevents the
product name and category from needing to be repeated on different rows in the "sales" table that refer to the same
product.

In Druid, on the other hand, it is common to use totally flat datasources that do not require joins at query time. In
the example of the "sales" table, in Druid it would be typical to store "product_id", "product_name", and
"product_category" as dimensions directly in a Druid "sales" datasource, without using a separate "products" table.
Totally flat schemas substantially increase performance, since the need for joins is eliminated at query time. As an
an added speed boost, this also allows Druid's query layer to operate directly on compressed dictionary-encoded data.
Perhaps counter-intuitively, this does _not_ substantially increase storage footprint relative to normalized schemas,
since Druid uses dictionary encoding to effectively store just a single integer per row for string columns.

If necessary, Druid datasources can be partially normalized through the use of [lookups](../querying/lookups.md),
which are the rough equivalent of dimension tables in a relational database. At query time, you would use Druid's SQL
`LOOKUP` function, or native lookup extraction functions, instead of using the JOIN keyword like you would in a
relational database. Since lookup tables impose an increase in memory footprint and incur more computational overhead
at query time, it is only recommended to do this if you need the ability to update a lookup table and have the changes
reflected immediately for already-ingested rows in your main table.

Tips for modeling relational data in Druid:

* Druid datasources do not have primary or unique keys, so skip those.
* Denormalize if possible. If you need to be able to update dimension / lookup tables periodically and have those
changes reflected in already-ingested data, consider partial normalization with [lookups](../querying/lookups.md).
* If you need to join two large distributed tables with each other, you must do this before loading the data into Druid.
Druid does not support query-time joins of two datasources. Lookups do not help here, since a full copy of each lookup
table is stored on each Druid server, so they are not a good choice for large tables.
* Consider whether you want to enable [rollup](#rollup) for pre-aggregation, or whether you want to disable
rollup and load your existing data as-is. Rollup in Druid is similar to creating a summary table in a relational model.

### Time series model

(Like OpenTSDB or InfluxDB.)

Similar to time series databases, Druid's data model requires a timestamp. Druid is not a timeseries database, but
it is a natural choice for storing timeseries data. Its flexible data model allows it to store both timeseries and
non-timeseries data, even in the same datasource.

To achieve best-case compression and query performance in Druid for timeseries data, it is important to partition and
sort by metric name, like timeseries databases often do. See [Partitioning and sorting](./partitioning.md) for more details.

Tips for modeling timeseries data in Druid:

* Druid does not think of data points as being part of a "time series". Instead, Druid treats each point separately
for ingestion and aggregation.
* Create a dimension that indicates the name of the series that a data point belongs to. This dimension is often called
"metric" or "name". Do not get the dimension named "metric" confused with the concept of Druid metrics. Place this
first in the list of dimensions in your "dimensionsSpec" for best performance (this helps because it improves locality;
see [partitioning and sorting](./partitioning.md) below for details).
* Create other dimensions for attributes attached to your data points. These are often called "tags" in timeseries
database systems.
* Create [metrics](../querying/aggregations.md) corresponding to the types of aggregations that you want to be able
to query. Typically this includes "sum", "min", and "max" (in one of the long, float, or double flavors). If you want the ability
to compute percentiles or quantiles, use Druid's [approximate aggregators](../querying/aggregations.md#approximate-aggregations).
* Consider enabling [rollup](./rollup.md), which will allow Druid to potentially combine multiple points into one
row in your Druid datasource. This can be useful if you want to store data at a different time granularity than it is
naturally emitted. It is also useful if you want to combine timeseries and non-timeseries data in the same datasource.
* If you don't know ahead of time what columns you'll want to ingest, use an empty dimensions list to trigger
[automatic detection of dimension columns](#schema-auto-discovery-for-dimensions).

### Log aggregation model

(Like Elasticsearch or Splunk.)

Similar to log aggregation systems, Druid offers inverted indexes for fast searching and filtering. Druid's search
capabilities are generally less developed than these systems, and its analytical capabilities are generally more
developed. The main data modeling differences between Druid and these systems are that when ingesting data into Druid,
you must be more explicit. Druid columns have types specific upfront.

Tips for modeling log data in Druid:

* If you don't know ahead of time what columns to ingest, you can have Druid perform [schema auto-discovery](#schema-auto-discovery-for-dimensions).
* If you have nested data, you can ingest it using the [nested columns](../querying/nested-columns.md) feature or flatten it using a [`flattenSpec`](./ingestion-spec.md#flattenspec).
* Consider enabling [rollup](./rollup.md) if you have mainly analytical use cases for your log data. This will
mean you lose the ability to retrieve individual events from Druid, but you potentially gain substantial compression and
query performance boosts.

## General tips and best practices

### Rollup

Druid can roll up data as it is ingested to minimize the amount of raw data that needs to be stored. This is a form
of summarization or pre-aggregation. For more details, see the [Rollup](./rollup.md) section of the ingestion
documentation.

### Partitioning and sorting

Optimally partitioning and sorting your data can have substantial impact on footprint and performance. For more details,
see the [Partitioning](./partitioning.md) section of the ingestion documentation.

<a name="sketches"></a>

### Sketches for high cardinality columns

When dealing with high cardinality columns like user IDs or other unique IDs, consider using sketches for approximate
analysis rather than operating on the actual values. When you ingest data using a sketch, Druid does not store the
original raw data, but instead stores a "sketch" of it that it can feed into a later computation at query time. Popular
use cases for sketches include count-distinct and quantile computation. Each sketch is designed for just one particular
kind of computation.

In general using sketches serves two main purposes: improving rollup, and reducing memory footprint at
query time.

Sketches improve rollup ratios because they allow you to collapse multiple distinct values into the same sketch. For
example, if you have two rows that are identical except for a user ID (perhaps two users did the same action at the
same time), storing them in a count-distinct sketch instead of as-is means you can store the data in one row instead of
two. You won't be able to retrieve the user IDs or compute exact distinct counts, but you'll still be able to compute
approximate distinct counts, and you'll reduce your storage footprint.

Sketches reduce memory footprint at query time because they limit the amount of data that needs to be shuffled between
servers. For example, in a quantile computation, instead of needing to send all data points to a central location
so they can be sorted and the quantile can be computed, Druid instead only needs to send a sketch of the points. This
can reduce data transfer needs to mere kilobytes.

For details about the sketches available in Druid, see the
[approximate aggregators](../querying/aggregations.md#approximate-aggregations) page.

If you prefer videos, take a look at [Not exactly!](https://www.youtube.com/watch?v=Hpd3f_MLdXo), a conference talk
about sketches in Druid.

### String vs numeric dimensions

If the user wishes to ingest a column as a numeric-typed dimension (Long, Double or Float), it is necessary to specify the type of the column in the `dimensions` section of the `dimensionsSpec`. If the type is omitted, Druid will ingest a column as the default String type.

There are performance tradeoffs between string and numeric columns. Numeric columns are generally faster to group on
than string columns. But unlike string columns, numeric columns don't have indexes, so they can be slower to filter on.
You may want to experiment to find the optimal choice for your use case.

For details about how to configure numeric dimensions, see the [`dimensionsSpec`](./ingestion-spec.md#dimensionsspec) documentation.

### Secondary timestamps

Druid schemas must always include a primary timestamp. The primary timestamp is used for
[partitioning and sorting](./partitioning.md) your data, so it should be the timestamp that you will most often filter on.
Druid is able to rapidly identify and retrieve data corresponding to time ranges of the primary timestamp column.

If your data has more than one timestamp, you can ingest the others as secondary timestamps. The best way to do this
is to ingest them as [long-typed dimensions](./ingestion-spec.md#dimensionsspec) in milliseconds format.
If necessary, you can get them into this format using a [`transformSpec`](./ingestion-spec.md#transformspec) and
[expressions](../querying/math-expr.md) like `timestamp_parse`, which returns millisecond timestamps.

At query time, you can query secondary timestamps with [SQL time functions](../querying/sql-scalar.md#date-and-time-functions)
like `MILLIS_TO_TIMESTAMP`, `TIME_FLOOR`, and others. If you're using native Druid queries, you can use
[expressions](../querying/math-expr.md).

### Nested dimensions

You can ingest and store nested data in a Druid column as a `COMPLEX<json>` data type. See [Nested columns](../querying/nested-columns.md) for more information.

If you want to ingest nested data in a format unsupported by the nested columns feature, you  must use the `flattenSpec` object to flatten it. For example, if you have data of the following form:

```json
{ "foo": { "bar": 3 } }
```

then before indexing it, you should transform it to:

```json
{ "foo_bar": 3 }
```

See the [`flattenSpec`](./ingestion-spec.md#flattenspec) documentation for more details.

<a name="counting"></a>

### Counting the number of ingested events

When rollup is enabled, count aggregators at query time do not actually tell you the number of rows that have been
ingested. They tell you the number of rows in the Druid datasource, which may be smaller than the number of rows
ingested.

In this case, a count aggregator at _ingestion_ time can be used to count the number of events. However, it is important to note
that when you query for this metric, you should use a `longSum` aggregator. A `count` aggregator at query time will return
the number of Druid rows for the time interval, which can be used to determine what the roll-up ratio was.

To clarify with an example, if your ingestion spec contains:

```json
"metricsSpec": [
    { "type": "count", "name": "count" }
]
```

You should query for the number of ingested rows with:

```json
"aggregations": [
    { "type": "longSum", "name": "numIngestedEvents", "fieldName": "count" }
]
```

### Schema auto-discovery for dimensions

Druid can infer the schema for your data in one of two ways:

- [Type-aware schema discovery](#type-aware-schema-discovery) where Druid infers the schema and type for your data. Type-aware schema discovery is available for native batch and streaming ingestion.
- [String-based schema discovery](#string-based-schema-discovery) where all the discovered columns are typed as either native string or multi-value string columns.

#### Type-aware schema discovery

:::info
 Note that using type-aware schema discovery can impact downstream BI tools depending on how they handle ARRAY typed columns.
:::

You can have Druid infer the schema and types for your data partially or fully by setting `dimensionsSpec.useSchemaDiscovery` to `true` and defining some or no dimensions in the dimensions list. 

When performing type-aware schema discovery, Druid can discover all of the columns of your input data (that aren't in
the exclusion list). Druid automatically chooses the most appropriate native Druid type among `STRING`, `LONG`,
`DOUBLE`, `ARRAY<STRING>`, `ARRAY<LONG>`, `ARRAY<DOUBLE>`, or `COMPLEX<json>` for nested data. For input formats with
native boolean types, Druid ingests these values as longs if `druid.expressions.useStrictBooleans` is set to `true`
(the default) or strings if set to `false`. Array typed columns can be queried using
the [array functions](../querying/sql-array-functions.md) or [UNNEST](../querying/sql-functions.md#unnest). Nested
columns can be queried with the [JSON functions](../querying/sql-json-functions.md).

Mixed type columns follow the same rules for schema differences between segments, and present as the _least_ restrictive
type that can represent all values in the column. For example:

- Mixed numeric columns are `DOUBLE`
- If there are any strings present, then the column is a `STRING`
- If there are arrays, then the column becomes an array with the least restrictive element type
- Any nested data or arrays of nested data become `COMPLEX<json>` nested columns.

Grouping, filtering, and aggregating mixed type values will handle these columns as if all values are represented as the
least restrictive type. The exception to this is the scan query, which will return the values in their original mixed
types, but any downstream operations on these values will still coerce them to the common type.

If you're already using string-based schema discovery and want to migrate, see [Migrating to type-aware schema discovery](#migrating-to-type-aware-schema-discovery).

#### String-based schema discovery

If you do not set `dimensionsSpec.useSchemaDiscovery` to `true`, Druid can still use the string-based schema discovery for ingestion if any of the following conditions are met: 

- The dimension list is empty 
- You set `includeAllDimensions` to `true` 

Druid coerces primitives and arrays of primitive types into the native Druid string type. Nested data structures and arrays of nested data structures are ignored and not ingested.

#### Migrating to type-aware schema discovery

If you previously used string-based schema discovery and want to migrate to type-aware schema discovery, do the following:

- Update any queries that use multi-value dimensions (MVDs) to use UNNEST in conjunction with other functions so that no MVD behavior is being relied upon. Type-aware schema discovery generates ARRAY typed columns instead of MVDs, so queries that use any MVD features will fail.
- Be aware of mixed typed inputs and test how type-aware schema discovery handles them. Druid attempts to cast them as the least restrictive type.
- If you notice issues with numeric types, you may need to explicitly cast them. Generally, Druid handles the coercion for you.
- Update your dimension exclusion list and add any nested columns if you want to continue to exclude them. String-based schema discovery automatically ignores nested columns, but type-aware schema discovery will ingest them.

### Including the same column as a dimension and a metric

One workflow with unique IDs is to be able to filter on a particular ID, while still being able to do fast unique counts on the ID column.
If you are not using schema-less dimensions, this use case is supported by setting the `name` of the metric to something different than the dimension.
If you are using schema-less dimensions, the best practice here is to include the same column twice, once as a dimension, and as a `hyperUnique` metric. This may involve
some work at ETL time.

As an example, for schema-less dimensions, repeat the same column:

```json
{ "device_id_dim": 123, "device_id_met": 123 }
```

and in your `metricsSpec`, include:

```json
{ "type": "hyperUnique", "name": "devices", "fieldName": "device_id_met" }
```

`device_id_dim` should automatically get picked up as a dimension.
