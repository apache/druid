---
layout: doc_page
title: "Schema Design"
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

# Schema Design

This page is meant to assist users in designing a schema for data to be ingested in Druid. Druid offers a unique data
modeling system that bears similarity to both relational and timeseries models. The key factors are:

* Druid data is stored in [datasources](index.html#datasources), which are similar to tables in a traditional RDBMS.
* Druid datasources can be ingested with or without [rollup](#rollup). With rollup enabled, Druid partially aggregates your data during ingestion, potentially reducing its row count, decreasing storage footprint, and improving query performance. With rollup disabled, Druid stores one row for each row in your input data, without any pre-aggregation.
* Every row in Druid must have a timestamp. Data is always partitioned by time, and every query has a time filter. Query results can also be broken down by time buckets like minutes, hours, days, and so on.
* All columns in Druid datasources, other than the timestamp column, are either dimensions or metrics. This follows the [standard naming convention](https://en.wikipedia.org/wiki/Online_analytical_processing#Overview_of_OLAP_systems) of OLAP data.
* Typical production datasources have tens to hundreds of columns.
* [Dimension columns](ingestion-spec.html#dimensions) are stored as-is, so they can be filtered on, grouped by, or aggregated at query time. They are always single Strings, [arrays of Strings](../querying/multi-value-dimensions.html), single Longs, single Doubles or single Floats.
* Metric columns are stored [pre-aggregated](../querying/aggregations.html), so they can only be aggregated at query time (not filtered or grouped by). They are often stored as numbers (integers or floats) but can also be stored as complex objects like [HyperLogLog sketches or approximate quantile sketches](../querying/aggregations.html#approx). Metrics can be configured at ingestion time even when rollup is disabled, but are most useful when rollup is enabled.

The rest of this page discusses tips for users coming from other kinds of systems, as well as general tips and
common practices.

## If you're coming from a...

### Relational model

(Like Hive or PostgreSQL.)

Druid datasources are generally equivalent to tables in a relational database. Druid [lookups](../querying/lookups.html)
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

If necessary, Druid datasources can be partially normalized through the use of [lookups](../querying/lookups.html),
which are the rough equivalent of dimension tables in a relational database. At query time, you would use Druid's SQL
`LOOKUP` function, or native lookup extraction functions, instead of using the JOIN keyword like you would in a
relational database. Since lookup tables impose an increase in memory footprint and incur more computational overhead
at query time, it is only recommended to do this if you need the ability to update a lookup table and have the changes
reflected immediately for already-ingested rows in your main table.

Tips for modeling relational data in Druid:

- Druid datasources do not have primary or unique keys, so skip those.
- Denormalize if possible. If you need to be able to update dimension / lookup tables periodically and have those
changes reflected in already-ingested data, consider partial normalization with [lookups](../querying/lookups.html).
- If you need to join two large distributed tables with each other, you must do this before loading the data into Druid.
Druid does not support query-time joins of two datasources. Lookups do not help here, since a full copy of each lookup
table is stored on each Druid server, so they are not a good choice for large tables.
- Consider whether you want to enable [rollup](#rollup) for pre-aggregation, or whether you want to disable
rollup and load your existing data as-is. Rollup in Druid is similar to creating a summary table in a relational model.

### Time series model

(Like OpenTSDB or InfluxDB.)

Similar to time series databases, Druid's data model requires a timestamp. Druid is not a timeseries database, but
it is a natural choice for storing timeseries data. Its flexible data model allows it to store both timeseries and
non-timeseries data, even in the same datasource.

To achieve best-case compression and query performance in Druid for timeseries data, it is important to partition and
sort by metric name, like timeseries databases often do. See [Partitioning and sorting](#partitioning) for more details.

Tips for modeling timeseries data in Druid:

- Druid does not think of data points as being part of a "time series". Instead, Druid treats each point separately
for ingestion and aggregation.
- Create a dimension that indicates the name of the series that a data point belongs to. This dimension is often called
"metric" or "name". Do not get the dimension named "metric" confused with the concept of Druid metrics. Place this
first in the list of dimensions in your "dimensionsSpec" for best performance (this helps because it improves locality;
see [partitioning and sorting](#partitioning) below for details).
- Create other dimensions for attributes attached to your data points. These are often called "tags" in timeseries
database systems.
- Create [metrics](../querying/aggregations.html) corresponding to the types of aggregations that you want to be able
to query. Typically this includes "sum", "min", and "max" (in one of the long, float, or double flavors). If you want to
be able to compute percentiles or quantiles, use Druid's [approximate aggregators](../querying/aggregations.html#approx).
- Consider enabling [rollup](#rollup), which will allow Druid to potentially combine multiple points into one
row in your Druid datasource. This can be useful if you want to store data at a different time granularity than it is
naturally emitted. It is also useful if you want to combine timeseries and non-timeseries data in the same datasource.
- If you don't know ahead of time what columns you'll want to ingest, use an empty dimensions list to trigger
[automatic detection of dimension columns](#schemaless).

### Log aggregation model

(Like Elasticsearch or Splunk.)

Similar to log aggregation systems, Druid offers inverted indexes for fast searching and filtering. Druid's search
capabilities are generally less developed than these systems, and its analytical capabilities are generally more
developed. The main data modeling differences between Druid and these systems are that when ingesting data into Druid,
you must be more explicit. Druid columns have types specific upfront and Druid does not, at this time, natively support
nested data.

Tips for modeling log data in Druid:

- If you don't know ahead of time what columns you'll want to ingest, use an empty dimensions list to trigger
[automatic detection of dimension columns](#schemaless).
- If you have nested data, flatten it using [Druid flattenSpecs](flatten-json.html).
- Consider enabling [rollup](#rollup) if you have mainly analytical use cases for your log data. This will
mean you lose the ability to retrieve individual events from Druid, but you potentially gain substantial compression and
query performance boosts.

## General tips and best practices

<a name="rollup" />

### Rollup

Druid can roll up data as it is ingested to minimize the amount of raw data that needs to be stored. Rollup is
a form of summarization or pre-aggregation. Columns stored in a Druid datasource are split into _dimensions_ and
_measures_. When rollup is enabled, any number of rows that have identical dimensions to each other (including an
identical timestamp after `queryGranularity`-based truncation has been applied) can be collapsed into a single row in
Druid.

In practice, rolling up data can dramatically reduce the size of data that needs to be stored, reducing row counts
by potentially orders of magnitude. This storage reduction does come at a cost: as we roll up data, we lose the ability
to query individual events.

You can measure the rollup ratio of a datasource by comparing the number of rows in Druid with the number of ingested
events. One way to do this is with a [Druid SQL](../querying/sql.html) query like:

```
-- "* 1.0" so we get decimal rather than integer division
SELECT SUM("event_count") / COUNT(*) * 1.0 FROM datasource
```

In this case, `event_count` was a "count" type metric specified at ingestion time. See
[Counting the number of ingested events](#counting) below for more details about how counting works when rollup is
enabled.

Tips for maximizing rollup:

- Generally, the fewer dimensions you have, and the lower the cardinality of your dimensions, the better rollup ratios
you will achieve.
- Use [sketches](#sketches) to avoid storing high cardinality dimensions, which harm rollup ratios.
- Adjusting `queryGranularity` at ingestion time (for example, using `PT5M` instead of `PT1M`) increases the
likelihood of two rows in Druid having matching timestamps, and can improve your rollup ratios.
- It can be beneficial to load the same data into more than one Druid datasource. Some users choose to create a "full"
datasource that has rollup disabled (or enabled, but with a minimal rollup ratio) and an "abbreviated" datasource that
has fewer dimensions and a higher rollup ratio. When queries only involve dimensions in the "abbreviated" set, using
that datasource leads to much faster query times. This can often be done with just a small increase in storage
footprint, since abbreviated datasources tend to be substantially smaller.

For more details about how rollup works and how to configure it, see the [ingestion overview](index.html#rollup).

<a name="partitioning" />

### Partitioning and sorting

Druid always partitions your data by time, but the segments within a particular time chunk may be
[partitioned further](index.html#partitioning) using options that vary based on the ingestion method you have chosen.

In general, partitioning using a particular dimension will improve locality, meaning that rows with the same value
for that dimension are stored together and can be accessed quickly. This gives you better performance when querying that
dimension, including both filtering and grouping on it. Partitioning on a dimension that "naturally" partitions your
data (such as a customer ID) will also tend to improve compression and give you a smaller storage footprint. These
effects will be maximized by putting the partition dimension first in the "dimensions" list of your "dimensionsSpec",
which also tells Druid to sort data segments by that column.

Note that Druid always sorts rows within a segment by timestamp first, even before the first dimension listed in your
dimensionsSpec. This can affect storage footprint and data locality. If you want to truly sort by a dimension, you can
work around this by setting `queryGranularity` equal to `segmentGranularity` in your ingestion spec, and then if you
need finer-granularity timestamps, ingesting your timestamp as a separate long-typed dimension. See
[Secondary timestamps](#secondary-timestamps) below for more information. This limitation may be removed in future
versions of Druid.

For details about how partitioning works and how to configure it, see the [ingestion overview](index.html#partitioning).

<a name="sketches" />

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
[approximate aggregators](../querying/aggregations.html#approx) page.

If you prefer videos, take a look at [Not exactly!](https://www.youtube.com/watch?v=Hpd3f_MLdXo), a conference talk
about sketches in Druid.

<a name="numeric-dimensions" />

### String vs numeric dimensions

If the user wishes to ingest a column as a numeric-typed dimension (Long, Double or Float), it is necessary to specify the type of the column in the `dimensions` section of the `dimensionsSpec`. If the type is omitted, Druid will ingest a column as the default String type.

There are performance tradeoffs between string and numeric columns. Numeric columns are generally faster to group on
than string columns. But unlike string columns, numeric columns don't have indexes, so they can be slower to filter on.
You may want to experiment to find the optimal choice for your use case.

For details about how to configure numeric dimensions, see the
[Dimension Schema](../ingestion/ingestion-spec.html#dimension-schema) page.

<a name="secondary-timestamps" />

### Secondary timestamps

Druid schemas must always include a primary timestamp. The primary timestamp is used for
[partitioning and sorting](#partitioning) your data, so it should be the timestamp that you will most often filter on.
Druid is able to rapidly identify and retrieve data corresponding to time ranges of the primary timestamp column.

If your data has more than one timestamp, you can ingest the others as secondary timestamps. The best way to do this
is to ingest them as [long-typed dimensions](../ingestion/ingestion-spec.html#dimension-schema) in milliseconds format.
If necessary, you can get them into this format using [transform specs](transform-spec.html) and
[expressions](../misc/math-expr.html) like `timestamp_parse`, which returns millisecond timestamps.

At query time, you can query secondary timestamps with [SQL time functions](../querying/sql.html#time-functions)
like `MILLIS_TO_TIMESTAMP`, `TIME_FLOOR`, and others. If you're using native Druid queries, you can use
[expressions](../misc/math-expr.html).

### Nested dimensions

At the time of this writing, Druid does not support nested dimensions. Nested dimensions need to be flattened. For example, 
if you have data of the following form:
 
```
{"foo":{"bar": 3}}
```
 
then before indexing it, you should transform it to:

```
{"foo_bar": 3}
```

Druid is capable of flattening JSON, Avro, or Parquet input data.
Please read about [flattenSpecs](../ingestion/flatten-json.html) for more details.

<a name="counting" />

### Counting the number of ingested events

When rollup is enabled, count aggregators at query time do not actually tell you the number of rows that have been
ingested. They tell you the number of rows in the Druid datasource, which may be smaller than the number of rows
ingested.

In this case, a count aggregator at _ingestion_ time can be used to count the number of events. However, it is important to note
that when you query for this metric, you should use a `longSum` aggregator. A `count` aggregator at query time will return 
the number of Druid rows for the time interval, which can be used to determine what the roll-up ratio was.

To clarify with an example, if your ingestion spec contains:

```
...
"metricsSpec" : [
      {
        "type" : "count",
        "name" : "count"
      },
...
```

You should query for the number of ingested rows with:

```
...
"aggregations": [
    { "type": "longSum", "name": "numIngestedEvents", "fieldName": "count" },
...
```

<a name="schemaless" />

### Schema-less dimensions

If the `dimensions` field is left empty in your ingestion spec, Druid will treat every column that is not the timestamp column, 
a dimension that has been excluded, or a metric column as a dimension.

Note that when using schema-less ingestion, all dimensions will be ingested as String-typed dimensions.

### Including the same column as a dimension and a metric

One workflow with unique IDs is to be able to filter on a particular ID, while still being able to do fast unique counts on the ID column. 
If you are not using schema-less dimensions, this use case is supported by setting the `name` of the metric to something different than the dimension. 
If you are using schema-less dimensions, the best practice here is to include the same column twice, once as a dimension, and as a `hyperUnique` metric. This may involve 
some work at ETL time.

As an example, for schema-less dimensions, repeat the same column:

```
{"device_id_dim":123, "device_id_met":123}
```

and in your `metricsSpec`, include:
 
```
{ "type" : "hyperUnique", "name" : "devices", "fieldName" : "device_id_met" }
```

`device_id_dim` should automatically get picked up as a dimension.
