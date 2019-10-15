---
id: index
title: "Ingestion"
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

## Overview

All data in Druid is organized into _segments_, which are data files that generally have up to a few million rows each.
Loading data in Druid is called _ingestion_ or _indexing_ and consists of reading data from a source system and creating
segments based on that data.

In most ingestion methods, the work of loading data is done by Druid MiddleManager processes. One exception is
Hadoop-based ingestion, where this work is instead done using a Hadoop MapReduce job on YARN (although MiddleManager
processes are still involved in starting and monitoring the Hadoop jobs). Once segments have been generated and stored
in [deep storage](../dependencies/deep-storage.md), they will be loaded by Historical processes. For more details on
how this works under the hood, see the [Storage design](../design/architecture.md#storage-design) section of Druid's design
documentation.

## How to use this documentation

This **page you are currently reading** provides information about universal Druid ingestion concepts, and about
configurations that are common to all [ingestion methods](#ingestion-methods).

The **individual pages for each ingestion method** provide additional information about concepts and configurations
that are unique to each ingestion method.

We recommend reading (or at least skimming) this universal page first, and then referring to the page for the
ingestion method or methods that you have chosen.

## Ingestion methods

The table below lists Druid's most common data ingestion methods, along with comparisons to help you choose
the best one for your situation. Each ingestion method supports its own set of source systems to pull from. For details
about how each method works, as well as configuration properties specific to that method, check out its documentation
page.

### Streaming

The most recommended, and most popular, method of streaming ingestion is the
[Kafka indexing service](../development/extensions-core/kafka-ingestion.md) that reads directly from Kafka. The Kinesis
indexing service also works well if you prefer Kinesis.

This table compares the major available options:

| **Method** | [Kafka](../development/extensions-core/kafka-ingestion.md) | [Kinesis](../development/extensions-core/kinesis-ingestion.md) | [Tranquility](tranquility.md) |
|---|-----|--------------|------------|
| **Supervisor type** | `kafka` | `kinesis` | N/A |
| **How it works** | Druid reads directly from Apache Kafka. | Druid reads directly from Amazon Kinesis. | Tranquility, a library that ships separately from Druid, is used to push data into Druid. |
| **Can ingest late data?** | Yes | Yes | No (late data is dropped based on the `windowPeriod` config) |
| **Exactly-once guarantees?** | Yes | Yes | No |

### Batch

When doing batch loads from files, you should use one-time [tasks](tasks.md), and you have three options: `index`
(native batch; single-task), `index_parallel` (native batch; parallel), or `index_hadoop` (Hadoop-based).

In general, we recommend native batch whenever it meets your needs, since the setup is simpler (it does not depend on
an external Hadoop cluster). However, there are still scenarios where Hadoop-based batch ingestion is the right choice,
especially due to its support for custom partitioning options and reading binary data formats.

This table compares the three available options:

| **Method** | [Native batch (simple)](native-batch.html#simple-task) | [Native batch (parallel)](native-batch.html#parallel-task) | [Hadoop-based](hadoop.html) |
|---|-----|--------------|------------|
| **Task type** | `index` | `index_parallel` | `index_hadoop` |
| **Parallel?** | No. Each task is single-threaded. | Yes, if firehose is splittable and `maxNumConcurrentSubTasks` > 1 in tuningConfig. See [firehose documentation](native-batch.md#firehoses) for details. | Yes, always. |
| **Can append or overwrite?** | Yes, both. | Yes, both. | Overwrite only. |
| **External dependencies** | None. | None. | Hadoop cluster (Druid submits Map/Reduce jobs). |
| **Input locations** | Any [firehose](native-batch.md#firehoses). | Any [firehose](native-batch.md#firehoses). | Any Hadoop FileSystem or Druid datasource. |
| **File formats** | Text file formats (CSV, TSV, JSON). Support for binary formats is coming in a future release. | Text file formats (CSV, TSV, JSON). Support for binary formats is coming in a future release. | Any Hadoop InputFormat. |
| **[Rollup modes](#rollup)** | Perfect if `forceGuaranteedRollup` = true in the [`tuningConfig`](native-batch.md#tuningconfig).| Perfect if `forceGuaranteedRollup` = true in the [`tuningConfig`](native-batch.md#tuningconfig). | Always perfect. |
| **Partitioning options** | Hash-based partitioning is supported when `forceGuaranteedRollup` = true in the [`tuningConfig`](native-batch.md#tuningconfig). | Hash-based partitioning (when `forceGuaranteedRollup` = true). | Hash-based or range-based partitioning via [`partitionsSpec`](hadoop.md#partitionsspec). |

<a name="data-model"></a>

## Druid's data model

### Datasources

Druid data is stored in [datasources](index.html#datasources), which are similar to tables in a traditional RDBMS. Druid
offers a unique data modeling system that bears similarity to both relational and timeseries models.

### Primary timestamp

Druid schemas must always include a primary timestamp. The primary timestamp is used for
[partitioning and sorting](#partitioning) your data. Druid queries are able to rapidly identify and retrieve data
corresponding to time ranges of the primary timestamp column. Druid is also able to use the primary timestamp column
for time-based [data management operations](data-management.html) such as dropping time chunks, overwriting time chunks,
and time-based retention rules.

The primary timestamp is parsed based on the [`timestampSpec`](#timestampspec). In addition, the
[`granularitySpec`](#granularityspec) controls other important operations that are based on the primary timestamp.
Regardless of which input field the primary timestamp is read from, it will always be stored as a column named `__time`
in your Druid datasource.

If you have more than one timestamp column, you can store the others as
[secondary timestamps](schema-design.md#secondary-timestamps).

### Dimensions

Dimensions are columns that are stored as-is and can be used for any purpose. You can group, filter, or apply
aggregators to dimensions at query time in an ad-hoc manner. If you run with [rollup](#rollup) disabled, then the set of
dimensions is simply treated like a set of columns to ingest, and behaves exactly as you would expect from a typical
database that does not support a rollup feature.

Dimensions are configured through the [`dimensionsSpec`](#dimensionsspec).

### Metrics

Metrics are columns that are stored in an aggregated form. They are most useful when [rollup](#rollup) is enabled.
Specifying a metric allows you to choose an aggregation function for Druid to apply to each row during ingestion. This
has two benefits:

1. If [rollup](#rollup) is enabled, multiple rows can be collapsed into one row even while retaining summary
information. In the [rollup tutorial](../tutorials/tutorial-rollup.md), this is used to collapse netflow data to a
single row per `(minute, srcIP, dstIP)` tuple, while retaining aggregate information about total packet and byte counts.
2. Some aggregators, especially approximate ones, can be computed faster at query time even on non-rolled-up data if
they are partially computed at ingestion time.

Metrics are configured through the [`metricsSpec`](#metricsspec).

## Rollup

### What is rollup?

Druid can roll up data as it is ingested to minimize the amount of raw data that needs to be stored. Rollup is
a form of summarization or pre-aggregation. In practice, rolling up data can dramatically reduce the size of data that
needs to be stored, reducing row counts by potentially orders of magnitude. This storage reduction does come at a cost:
as we roll up data, we lose the ability to query individual events.

When rollup is disabled, Druid loads each row as-is without doing any form of pre-aggregation. This mode is similar
to what you would expect from a typical database that does not support a rollup feature.

When rollup is enabled, then any rows that have identical [dimensions](#dimensions) and [timestamp](#primary-timestamp)
to each other (after [`queryGranularity`-based truncation](#granularityspec)) can be collapsed, or _rolled up_, into a
single row in Druid.

By default, rollup is enabled.

### Enabling or disabling rollup

Rollup is controlled by the `rollup` setting in the [`granularitySpec`](#granularityspec). By default, it is `true`
(enabled). Set this to `false` if you want Druid to store each record as-is, without any rollup summarization.

### Example of rollup

For an example of how to configure rollup, and of how the feature will modify your data, check out the
[rollup tutorial](../tutorials/tutorial-rollup.md).

### Maximizing rollup ratio

You can measure the rollup ratio of a datasource by comparing the number of rows in Druid with the number of ingested
events. The higher this number, the more benefit you are gaining from rollup. One way to do this is with a
[Druid SQL](../querying/sql.md) query like:

```sql
SELECT SUM("cnt") / COUNT(*) * 1.0 FROM datasource
```

In this query, `cnt` should refer to a "count" type metric specified at ingestion time. See
[Counting the number of ingested events](schema-design.md#counting) on the "Schema design" page for more details about
how counting works when rollup is enabled.

Tips for maximizing rollup:

- Generally, the fewer dimensions you have, and the lower the cardinality of your dimensions, the better rollup ratios
you will achieve.
- Use [sketches](schema-design.html#sketches) to avoid storing high cardinality dimensions, which harm rollup ratios.
- Adjusting `queryGranularity` at ingestion time (for example, using `PT5M` instead of `PT1M`) increases the
likelihood of two rows in Druid having matching timestamps, and can improve your rollup ratios.
- It can be beneficial to load the same data into more than one Druid datasource. Some users choose to create a "full"
datasource that has rollup disabled (or enabled, but with a minimal rollup ratio) and an "abbreviated" datasource that
has fewer dimensions and a higher rollup ratio. When queries only involve dimensions in the "abbreviated" set, using
that datasource leads to much faster query times. This can often be done with just a small increase in storage
footprint, since abbreviated datasources tend to be substantially smaller.
- If you are using a [best-effort rollup](#best-effort-rollup) ingestion configuration that does not guarantee perfect
rollup, you can potentially improve your rollup ratio by switching to a guaranteed perfect rollup option, or by
[reindexing](data-management.md#compaction-and-reindexing) your data in the background after initial ingestion.

### Best-effort rollup

Some Druid ingestion methods guarantee _perfect rollup_, meaning that input data are perfectly aggregated at ingestion
time. Others offer _best-effort rollup_, meaning that input data might not be perfectly aggregated and thus there could
be multiple segments holding rows with the same timestamp and dimension values.

In general, ingestion methods that offer best-effort rollup do this because they are either parallelizing ingestion
without a shuffling step (which would be required for perfect rollup), or because they are finalizing and publishing
segments before all data for a time chunk has been received, which we call _incremental publishing_. In both of these
cases, records that could theoretically be rolled up may end up in different segments. All types of streaming ingestion
run in this mode.

Ingestion methods that guarantee perfect rollup do it with an additional preprocessing step to determine intervals
and partitioning before the actual data ingestion stage. This preprocessing step scans the entire input dataset, which
generally increases the time required for ingestion, but provides information necessary for perfect rollup.

The following table shows how each method handles rollup:

|Method|How it works|
|------|------------|
|[Native batch](native-batch.html)|`index_parallel` and `index` type may be either perfect or best-effort, based on configuration.|
|[Hadoop](hadoop.html)|Always perfect.|
|[Kafka indexing service](../development/extensions-core/kafka-ingestion.md)|Always best-effort.|
|[Kinesis indexing service](../development/extensions-core/kinesis-ingestion.md)|Always best-effort.|

## Partitioning

### Why partition?

Optimal partitioning and sorting of segments within your datasources can have substantial impact on footprint and
performance.

Druid datasources are always partitioned by time into _time chunks_, and each time chunk contains one or more segments.
This partitioning happens for all ingestion methods, and is based on the `segmentGranularity` parameter of your
ingestion spec's `dataSchema`.

The segments within a particular time chunk may also be partitioned further, using options that vary based on the
ingestion method you have chosen. In general, doing this secondary partitioning using a particular dimension will
improve locality, meaning that rows with the same value for that dimension are stored together and can be accessed
quickly.

You will usually get the best performance and smallest overall footprint by partitioning your data on some "natural"
dimension that you often filter by, if one exists. This will often improve compression - users have reported threefold
storage size decreases - and it also tends to improve query performance as well.

> Partitioning and sorting are best friends! If you do have a "natural" partitioning dimension, you should also consider
> placing it first in the `dimensions` list of your `dimensionsSpec`, which tells Druid to sort rows within each segment
> by that column. This will often improve compression even more, beyond the improvement gained by partitioning alone.
>
> However, note that currently, Druid always sorts rows within a segment by timestamp first, even before the first
> dimension listed in your `dimensionsSpec`. This can prevent dimension sorting from being maximally effective. If
> necessary, you can work around this limitation by setting `queryGranularity` equal to `segmentGranularity` in your
> [`granularitySpec`](#granularityspec), which will set all timestamps within the segment to the same value, and by saving
> your "real" timestamp as a [secondary timestamp](schema-design.md#secondary-timestamps). This limitation may be removed
> in a future version of Druid.

### How to set up partitioning

Not all ingestion methods support an explicit partitioning configuration, and not all have equivalent levels of
flexibility. As of current Druid versions, If you are doing initial ingestion through a less-flexible method (like
Kafka) then you can use [reindexing techniques](data-management.html#compaction-and-reindexing) to repartition your data after it
is initially ingested. This is a powerful technique: you can use it to ensure that any data older than a certain
threshold is optimally partitioned, even as you continuously add new data from a stream.

The following table shows how each ingestion method handles partitioning:

|Method|How it works|
|------|------------|
|[Native batch](native-batch.html)|Configured using [`partitionsSpec`](native-batch.html#partitionsspec) inside the `tuningConfig`.|
|[Hadoop](hadoop.html)|Configured using [`partitionsSpec`](hadoop.html#partitionsspec) inside the `tuningConfig`.|
|[Kafka indexing service](../development/extensions-core/kafka-ingestion.md)|Partitioning in Druid is guided by how your Kafka topic is partitioned. You can also [reindex](data-management.html#compaction-and-reindexing) to repartition after initial ingestion.|
|[Kinesis indexing service](../development/extensions-core/kinesis-ingestion.md)|Partitioning in Druid is guided by how your Kinesis stream is sharded. You can also [reindex](data-management.html#compaction-and-reindexing) to repartition after initial ingestion.|

> Note that, of course, one way to partition data is to load it into separate datasources. This is a perfectly viable
> approach and works very well when the number of datasources does not lead to excessive per-datasource overheads. If
> you go with this approach, then you can ignore this section, since it is describing how to set up partitioning
> _within a single datasource_.
>
> For more details on splitting data up into separate datasources, and potential operational considerations, refer
> to the [Multitenancy considerations](../querying/multitenancy.md) page.

<a name="spec"></a>

## Ingestion specs

No matter what ingestion method you use, data is loaded into Druid using either one-time [tasks](tasks.html) or
ongoing "supervisors" (which run and supervised a set of tasks over time). In any case, part of the task or supervisor
definition is an _ingestion spec_.

Ingestion specs consists of three main components:

- [`dataSchema`](#dataschema), which configures the [datasource name](#datasource), [input row parser](#parser),
   [primary timestamp](#timestampspec), [flattening of nested data](#flattenspec) (if needed),
   [dimensions](#dimensionsspec), [metrics](#metricsspec), and [transforms and filters](#transformspec) (if needed).
- [`ioConfig`](#ioconfig), which tells Druid how to connect to the source system and . For more information, see the
   documentation for each [ingestion method](#ingestion-methods).
- [`tuningConfig`](#tuningconfig), which controls various tuning parameters specific to each
  [ingestion method](#ingestion-methods).

Example ingestion spec for task type "index" (native batch):

```
{
  "type": "index",
  "spec": {
    "dataSchema": {
      "dataSource": "wikipedia",
      "parser": {
        "type": "string",
        "parseSpec": {
          "format": "json",
          "flattenSpec": {
            "useFieldDiscovery": true,
            "fields": [
              { "type": "path", "name": "userId", "expr": "$.user.id" }
            ]
          },
          "timestampSpec": {
            "column": "timestamp",
            "format": "auto"
          },
          "dimensionsSpec": {
            "dimensions": [
              { "type": "string", "page" },
              { "type": "string", "language" },
              { "type": "long", "name": "userId" }
            ]
          }
        }
      },
      "metricsSpec": [
        { "type": "count", "name": "count" },
        { "type": "doubleSum", "name": "bytes_added_sum", "fieldName": "bytes_added" },
        { "type": "doubleSum", "name": "bytes_deleted_sum", "fieldName": "bytes_deleted" }
      ],
      "granularitySpec": {
        "segmentGranularity": "day",
        "queryGranularity": "none",
        "intervals": [
          "2013-08-31/2013-09-01"
        ]
      }
    },
    "ioConfig": {
      "type": "index",
      "firehose": {
        "type": "local",
        "baseDir": "examples/indexing/",
        "filter": "wikipedia_data.json"
      }
    },
    "tuningConfig": {
      "type": "index"
    }
  }
}
```

The specific options supported by these sections will depend on the [ingestion method](#ingestion-methods) you have chosen.
For more examples, refer to the documentation for each ingestion method.

You can also load data visually, without the need to write an ingestion spec, using the "Load data" functionality
available in Druid's [web console](../operations/druid-console.md). Druid's visual data loader supports
[Kafka](../development/extensions-core/kafka-ingestion.md),
[Kinesis](../development/extensions-core/kinesis-ingestion.md), and
[native batch](native-batch.html) mode.

## `dataSchema`

The `dataSchema` is a holder for the following components:

- [datasource name](#datasource), [input row parser](#parser),
   [primary timestamp](#timestampspec), [flattening of nested data](#flattenspec) (if needed),
   [dimensions](#dimensionsspec), [metrics](#metricsspec), and [transforms and filters](#transformspec) (if needed).

An example `dataSchema` is:

```
"dataSchema": {
  "dataSource": "wikipedia",
  "parser": {
    "type": "string",
    "parseSpec": {
      "format": "json",
      "flattenSpec": {
        "useFieldDiscovery": true,
        "fields": [
          { "type": "path", "name": "userId", "expr": "$.user.id" }
        ]
      },
      "timestampSpec": {
        "column": "timestamp",
        "format": "auto"
      },
      "dimensionsSpec": {
        "dimensions": [
          { "type": "string", "page" },
          { "type": "string", "language" },
          { "type": "long", "name": "userId" }
        ]
      }
    }
  },
  "metricsSpec": [
    { "type": "count", "name": "count" },
    { "type": "doubleSum", "name": "bytes_added_sum", "fieldName": "bytes_added" },
    { "type": "doubleSum", "name": "bytes_deleted_sum", "fieldName": "bytes_deleted" }
  ],
  "granularitySpec": {
    "segmentGranularity": "day",
    "queryGranularity": "none",
    "intervals": [
      "2013-08-31/2013-09-01"
    ]
  }
}
```

### `dataSource`

The `dataSource` is located in `dataSchema` → `dataSource` and is simply the name of the
[datasource](../design/architecture.html#datasources-and-segments) that data will be written to. An example
`dataSource` is:

```
"dataSource": "my-first-datasource"
```

### `parser`

The `parser` is located in `dataSchema` → `parser` and is responsible for configuring a wide variety of
items related to parsing input records.

For details about supported data formats, see the ["Data formats" page](data-formats.md).

For details about major components of the `parseSpec`, refer to their subsections:

- [`timestampSpec`](#timestampspec), responsible for configuring the [primary timestamp](#primary-timestamp).
- [`dimensionsSpec`](#dimensionsspec), responsible for configuring [dimensions](#dimensions).
- [`flattenSpec`](#flattenspec), responsible for flattening nested data formats.

An example `parser` is:

```
"parser": {
  "type": "string",
  "parseSpec": {
    "format": "json",
    "flattenSpec": {
      "useFieldDiscovery": true,
      "fields": [
        { "type": "path", "name": "userId", "expr": "$.user.id" }
      ]
    },
    "timestampSpec": {
      "column": "timestamp",
      "format": "auto"
    },
    "dimensionsSpec": {
      "dimensions": [
        { "type": "string", "page" },
        { "type": "string", "language" },
        { "type": "long", "name": "userId" }
      ]
    }
  }
}
```

### `timestampSpec`

The `timestampSpec` is located in `dataSchema` → `parser` → `parseSpec` → `timestampSpec` and is responsible for
configuring the [primary timestamp](#primary-timestamp). An example `timestampSpec` is:

```
"timestampSpec": {
  "column": "timestamp",
  "format": "auto"
}
```

> Conceptually, after input data records are read, Druid applies ingestion spec components in a particular order:
> first [`flattenSpec`](#flattenspec), then [`timestampSpec`](#timestampspec), then [`transformSpec`](#transformspec),
> and finally [`dimensionsSpec`](#dimensionsspec) and [`metricsSpec`](#metricsspec). Keep this in mind when writing
> your ingestion spec.

A `timestampSpec` can have the following components:

|Field|Description|Default|
|-----|-----------|-------|
|column|Input row field to read the primary timestamp from.<br><br>Regardless of the name of this input field, the primary timestamp will always be stored as a column named `__time` in your Druid datasource.|timestamp|
|format|Timestamp format. Options are: <ul><li>`iso`: ISO8601 with 'T' separator, like "2000-01-01T01:02:03.456"</li><li>`posix`: seconds since epoch</li><li>`millis`: milliseconds since epoch</li><li>`micro`: microseconds since epoch</li><li>`nano`: nanoseconds since epoch</li><li>`auto`: automatically detects ISO (either 'T' or space separator) or millis format</li><li>any [Joda DateTimeFormat string](http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html)</li></ul>|auto|
|missingValue|Timestamp to use for input records that have a null or missing timestamp `column`. Should be in ISO8601 format, like `"2000-01-01T01:02:03.456"`, even if you have specified something else for `format`. Since Druid requires a primary timestamp, this setting can be useful for ingesting datasets that do not have any per-record timestamps at all. |none|

### `dimensionsSpec`

The `dimensionsSpec` is located in `dataSchema` → `parser` → `parseSpec` → `dimensionsSpec` and is responsible for
configuring [dimensions](#dimensions). An example `dimensionsSpec` is:

```
"dimensionsSpec" : {
  "dimensions": [
    "page",
    "language",
    { "type": "long", "name": "userId" }
  ],
  "dimensionExclusions" : [],
  "spatialDimensions" : []
}
```

> Conceptually, after input data records are read, Druid applies ingestion spec components in a particular order:
> first [`flattenSpec`](#flattenspec), then [`timestampSpec`](#timestampspec), then [`transformSpec`](#transformspec),
> and finally [`dimensionsSpec`](#dimensionsspec) and [`metricsSpec`](#metricsspec). Keep this in mind when writing
> your ingestion spec.

A `dimensionsSpec` can have the following components:

| Field | Description | Default |
|-------|-------------|---------|
| dimensions | A list of [dimension names or objects](#dimension-objects).<br><br>If this is an empty array, Druid will treat all non-timestamp, non-metric columns that do not appear in `dimensionExclusions` as String-typed dimension columns (see [inclusions and exclusions](#inclusions-and-exclusions) below). | `[]` |
| dimensionExclusions | The names of dimensions to exclude from ingestion. Only names are supported here, not objects. | `[]` |
| spatialDimensions | An array of [spatial dimensions](../development/geo.md). | `[]` |

#### Dimension objects

Each dimension in the `dimensions` list can either be a name or an object. Providing a name is equivalent to providing
a `string` type dimension object with the given name, e.g. `"page"` is equivalent to `{"name": "page", "type": "string"}`.

Dimension objects can have the following components:

| Field | Description | Default |
|-------|-------------|---------|
| type | Either `string`, `long`, `float`, or `double`. | `string` |
| name | The name of the dimension. This will be used as the field name to read from input records, as well as the column name stored in generated segments.<br><br>Note that you can use a [`transformSpec`](#transformspec) if you want to rename columns during ingestion time. | none (required) |
| createBitmapIndex | For `string` typed dimensions, whether or not bitmap indexes should be created for the column in generated segments. Creating a bitmap index requires more storage, but speeds up certain kinds of filtering (especially equality and prefix filtering). Only supported for `string` typed dimensions. | `true` |

#### Inclusions and exclusions

Druid will interpret a `dimensionsSpec` in two possible ways: _normal_ or _schemaless_.

Normal interpretation occurs when either `dimensions` or `spatialDimensions` is non-empty. In this case, the combination of the two lists will be taken as the set of dimensions to be ingested, and `dimensionExclusions` is ignored.

Schemaless interpretation occurs when both `dimensions` and `spatialDimensions` are empty or null. In this case, the set of dimensions is determined in the following way:

1. First, start from the set of all input fields from the [`parser`](#parser) (or the [`flattenSpec`](#flattenspec), if one is being used).
2. Any field listed in `dimensionExclusions` is excluded.
3. The field listed as `column` in the [`timestampSpec`](#timestampspec) is excluded.
4. Any field used as an input to an aggregator from the [metricsSpec](#metricsspec) is excluded.
5. Any field with the same name as an aggregator from the [metricsSpec](#metricsspec) is excluded.
6. All other fields are ingested as `string` typed dimensions with the [default settings](#dimension-objects).

> Note: Fields generated by a [`transformSpec`](#transformspec) are not currently considered candidates for
> schemaless dimension interpretation.

### `flattenSpec`

The `flattenSpec` is located in `dataSchema` → `parser` → `parseSpec` → `flattenSpec` and is responsible for
bridging the gap between potentially nested input data (such as JSON, Avro, etc) and Druid's flat data model.
An example `flattenSpec` is:

```
"flattenSpec": {
  "useFieldDiscovery": true,
  "fields": [
    { "name": "baz", "type": "root" },
    { "name": "foo_bar", "type": "path", "expr": "$.foo.bar" },
    { "name": "first_food", "type": "jq", "expr": ".thing.food[1]" }
  ]
}
```

> Conceptually, after input data records are read, Druid applies ingestion spec components in a particular order:
> first [`flattenSpec`](#flattenspec), then [`timestampSpec`](#timestampspec), then [`transformSpec`](#transformspec),
> and finally [`dimensionsSpec`](#dimensionsspec) and [`metricsSpec`](#metricsspec). Keep this in mind when writing
> your ingestion spec.


Flattening is only supported for [data formats](data-formats.md) that support nesting, including `avro`, `json`, `orc`,
and `parquet`. Flattening is _not_ supported for the `timeAndDims` parseSpec type.

A `flattenSpec` can have the following components:

| Field | Description | Default |
|-------|-------------|---------|
| useFieldDiscovery | If true, interpret all root-level fields as available fields for usage by [`timestampSpec`](#timestampspec), [`transformSpec`](#transformspec), [`dimensionsSpec`](#dimensionsspec), and [`metricsSpec`](#metricsspec).<br><br>If false, only explicitly specified fields (see `fields`) will be available for use. | `true` |
| fields | Specifies the fields of interest and how they are accessed. [See below for details.](#field-flattening-specifications) | `[]` |

#### Field flattening specifications

Each entry in the `fields` list can have the following components:

| Field | Description | Default |
|-------|-------------|---------|
| type | Options are as follows:<br><br><ul><li>`root`, referring to a field at the root level of the record. Only really useful if `useFieldDiscovery` is false.</li><li>`path`, referring to a field using [JsonPath](https://github.com/jayway/JsonPath) notation. Supported by most data formats that offer nesting, including `avro`, `json`, `orc`, and `parquet`.</li><li>`jq`, referring to a field using [jackson-jq](https://github.com/eiiches/jackson-jq) notation. Only supported for the `json` format.</li></ul> | none (required) |
| name | Name of the field after flattening. This name can be referred to by the [`timestampSpec`](#timestampspec), [`transformSpec`](#transformspec), [`dimensionsSpec`](#dimensionsspec), and [`metricsSpec`](#metricsspec).| none (required) |
| expr | Expression for accessing the field while flattening. For type `path`, this should be [JsonPath](https://github.com/jayway/JsonPath). For type `jq`, this should be [jackson-jq](https://github.com/eiiches/jackson-jq) notation. For other types, this parameter is ignored. | none (required for types `path` and `jq`) |

#### Notes on flattening

* For convenience, when defining a root-level field, it is possible to define only the field name, as a string, instead of a JSON object. For example, `{"name": "baz", "type": "root"}` is equivalent to `"baz"`.
* Enabling `useFieldDiscovery` will only autodetect "simple" fields at the root level that correspond to data types that Druid supports. This includes strings, numbers, and lists of strings or numbers. Other types will not be automatically detected, and must be specified explicitly in the `fields` list.
* Duplicate field `name`s are not allowed. An exception will be thrown.
* If `useFieldDiscovery` is enabled, any discovered field with the same name as one already defined in the `fields` list will be skipped, rather than added twice.
* [http://jsonpath.herokuapp.com/](http://jsonpath.herokuapp.com/) is useful for testing `path`-type expressions.
* jackson-jq supports a subset of the full [jq](https://stedolan.github.io/jq/) syntax.  Please refer to the [jackson-jq documentation](https://github.com/eiiches/jackson-jq) for details.

### `metricsSpec`

The `metricsSpec` is located in `dataSchema` → `metricsSpec` and is a list of [aggregators](../querying/aggregations.md)
to apply at ingestion time. This is most useful when [rollup](#rollup) is enabled, since it's how you configure
ingestion-time aggregation.

An example `metricsSpec` is:

```
"metricsSpec": [
  { "type": "count", "name": "count" },
  { "type": "doubleSum", "name": "bytes_added_sum", "fieldName": "bytes_added" },
  { "type": "doubleSum", "name": "bytes_deleted_sum", "fieldName": "bytes_deleted" }
]
```

> Generally, when [rollup](#rollup) is disabled, you should have an empty `metricsSpec` (because without rollup,
> Druid does not do any ingestion-time aggregation, so there is little reason to include an ingestion-time aggregator). However,
> in some cases, it can still make sense to define metrics: for example, if you want to create a complex column as a way of
> pre-computing part of an [approximate aggregation](../querying/aggregations.md#approximate-aggregations), this can only
> be done by defining a metric in a `metricsSpec`.

### `granularitySpec`

The `granularitySpec` is located in `dataSchema` → `granularitySpec` and is responsible for configuring
the following operations:

1. Partitioning a datasource into [time chunks](../design/architecture.html#datasources-and-segments) (via `segmentGranularity`).
2. Truncating the timestamp, if desired (via `queryGranularity`).
3. Specifying which time chunks of segments should be created, for batch ingestion (via `intervals`).
4. Specifying whether ingestion-time [rollup](#rollup) should be used or not (via `rollup`).

Other than `rollup`, these operations are all based on the [primary timestamp](#primary-timestamp).

An example `granularitySpec` is:

```
"granularitySpec": {
  "segmentGranularity": "day",
  "queryGranularity": "none",
  "intervals": [
    "2013-08-31/2013-09-01"
  ],
  "rollup": true
}
```

A `granularitySpec` can have the following components:

| Field | Description | Default |
|-------|-------------|---------|
| type | Either `uniform` or `arbitrary`. In most cases you want to use `uniform`.| `uniform` |
| segmentGranularity | [Time chunking](../design/architecture.html#datasources-and-segments) granularity for this datasource. Multiple segments can be created per time chunk. For example, when set to `day`, the events of the same day fall into the same time chunk which can be optionally further partitioned into multiple segments based on other configurations and input size. Any [granularity](../querying/granularities.md) can be provided here.<br><br>Ignored if `type` is set to `arbitrary`.| `day` |
| queryGranularity | The resolution of timestamp storage within each segment. This must be equal to, or finer, than `segmentGranularity`. This will be the finest granularity that you can query at and still receive sensible results, but note that you can still query at anything coarser than this granularity. E.g., a value of `minute` will mean that records will be stored at minutely granularity, and can be sensibly queried at any multiple of minutes (including minutely, 5-minutely, hourly, etc).<br><br>Any [granularity](../querying/granularities.md) can be provided here. Use `none` to store timestamps as-is, without any truncation.| `none` |
| rollup | Whether to use ingestion-time [rollup](#rollup) or not. | `true` |
| intervals | A list of intervals describing what time chunks of segments should be created. If `type` is set to `uniform`, this list will be broken up and rounded-off based on the `segmentGranularity`. If `type` is set to `arbitrary`, this list will be used as-is.<br><br>If `null` or not provided, batch ingestion tasks will generally determine which time chunks to output based on what timestamps are found in the input data.<br><br>If specified, batch ingestion tasks may be able to skip a determining-partitions phase, which can result in faster ingestion. Batch ingestion tasks may also be able to request all their locks up-front instead of one by one. Batch ingestion tasks will throw away any records with timestamps outside of the specified intervals.<br><br>Ignored for any form of streaming ingestion. | `null` |

### `transformSpec`

The `transformSpec` is located in `dataSchema` → `transformSpec` and is responsible for transforming and filtering
records during ingestion time. It is optional. An example `transformSpec` is:

```
"transformSpec": {
  "transforms": [
    { "type": "expression", "name": "countryUpper", "expression": "upper(country)" }
  ],
  "filter": {
    "type": "selector",
    "dimension": "country",
    "value": "San Serriffe"
  }
}
```

> Conceptually, after input data records are read, Druid applies ingestion spec components in a particular order:
> first [`flattenSpec`](#flattenspec), then [`timestampSpec`](#timestampspec), then [`transformSpec`](#transformspec),
> and finally [`dimensionsSpec`](#dimensionsspec) and [`metricsSpec`](#metricsspec). Keep this in mind when writing
> your ingestion spec.

#### Transforms

The `transforms` list allows you to specify a set of expressions to evaluate on top of input data. Each transform has a
"name" which can be referred to by your `dimensionsSpec`, `metricsSpec`, etc.

If a transform has the same name as a field in an input row, then it will shadow the original field. Transforms that
shadow fields may still refer to the fields they shadow. This can be used to transform a field "in-place".

Transforms do have some limitations. They can only refer to fields present in the actual input rows; in particular,
they cannot refer to other transforms. And they cannot remove fields, only add them. However, they can shadow a field
with another field containing all nulls, which will act similarly to removing the field.

Transforms can refer to the [timestamp](#timestampspec) of an input row by referring to `__time` as part of the expression.
They can also _replace_ the timestamp if you set their "name" to `__time`. In both cases, `__time` should be treated as
a millisecond timestamp (number of milliseconds since Jan 1, 1970 at midnight UTC). Transforms are applied _after_ the
`timestampSpec`.

Druid currently includes one kind of built-in transform, the expression transform. It has the following syntax:

```
{
  "type": "expression",
  "name": "<output name>",
  "expression": "<expr>"
}
```

The `expression` is a [Druid query expression](../misc/math-expr.md).

#### Filter

The `filter` conditionally filters input rows during ingestion. Only rows that pass the filter will be
ingested. Any of Druid's standard [query filters](../querying/filters.md) can be used. Note that within a
`transformSpec`, the `transforms` are applied before the `filter`, so the filter can refer to a transform.

## `ioConfig`

The `ioConfig` influences how data is read from a source system, such as Apache Kafka, Amazon S3, a mounted
filesystem, or any other supported source system. For details, see the documentation provided by each
[ingestion method](#ingestion-methods).

## `tuningConfig`

Tuning properties are specified in a `tuningConfig`, which goes at the top level of an ingestion spec. Some
properties apply to all [ingestion methods](#ingestion-methods), but most are specific to each individual
ingestion method. An example `tuningConfig` that sets all of the shared, common properties to their defaults
is:

```plaintext
"tuningConfig": {
  "type": "<ingestion-method-specific type code>",
  "maxRowsInMemory": 1000000,
  "maxBytesInMemory": <one-sixth of JVM memory>,
  "indexSpec": {
    "bitmap": { "type": "concise" },
    "dimensionCompression": "lz4",
    "metricCompression": "lz4",
    "longEncoding": "longs"
  },
  <other ingestion-method-specific properties>
}
```

|Field|Description|Default|
|-----|-----------|-------|
|type|Each ingestion method has its own tuning type code. You must specify the type code that matches your ingestion method. Common options are `index`, `hadoop`, `kafka`, and `kinesis`.||
|maxRowsInMemory|The maximum number of records to store in memory before persisting to disk. Note that this is the number of rows post-rollup, and so it may not be equal to the number of input records. Ingested records will be persisted to disk when either `maxRowsInMemory` or `maxBytesInMemory` are reached (whichever happens first).|`1000000`|
|maxBytesInMemory|The maximum aggregate size of records, in bytes, to store in the JVM heap before persisting. This is based on a rough estimate of memory usage. Ingested records will be persisted to disk when either `maxRowsInMemory` or `maxBytesInMemory` are reached (whichever happens first).<br /><br />Setting maxBytesInMemory to -1 disables this check, meaning Druid will rely entirely on maxRowsInMemory to control memory usage. Setting it to zero means the default value will be used (one-sixth of JVM heap size).<br /><br />Note that the estimate of memory usage is designed to be an overestimate, and can be especially high when using complex ingest-time aggregators, including sketches. If this causes your indexing workloads to persist to disk too often, you can set maxBytesInMemory to -1 and rely on maxRowsInMemory instead.|One-sixth of max JVM heap size|
|indexSpec|Tune how data is indexed. See below for more information.|See table below|
|Other properties|Each ingestion method has its own list of additional tuning properties. See the documentation for each method for a full list: [Kafka indexing service](../development/extensions-core/kafka-ingestion.md#tuningconfig), [Kinesis indexing service](../development/extensions-core/kinesis-ingestion.md#tuningconfig), [Native batch](native-batch.md#tuningconfig), and [Hadoop-based](hadoop.md#tuningconfig).||

#### `indexSpec`

The `indexSpec` object can include the following properties:

|Field|Description|Default|
|-----|-----------|-------|
|bitmap|Compression format for bitmap indexes. Should be a JSON object with `type` set to `concise` or `roaring`. For type `roaring`, the boolean property `compressRunOnSerialization` (defaults to true) controls whether or not run-length encoding will be used when it is determined to be more space-efficient.|`{"type": "concise"}`|
|dimensionCompression|Compression format for dimension columns. Options are `lz4`, `lzf`, or `uncompressed`.|`lz4`|
|metricCompression|Compression format for metric columns. Options are `lz4`, `lzf`, `uncompressed`, or `none` (which is more efficient than `uncompressed`, but not supported by older versions of Druid).|`lz4`|
|longEncoding|Encoding format for long-typed columns. Applies regardless of whether they are dimensions or metrics. Options are `auto` or `longs`. `auto` encodes the values using offset or lookup table depending on column cardinality, and store them with variable size. `longs` stores the value as-is with 8 bytes each.|`longs`|

Beyond these properties, each ingestion method has its own specific tuning properties. See the documentation for each
[ingestion method](#ingestion-methods) for details.
