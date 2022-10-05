---
id: ingestion-spec
title: Ingestion spec reference
sidebar_label: Ingestion spec
description: Reference for the configuration options in the ingestion spec.
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

All ingestion methods use ingestion tasks to load data into Druid. Streaming ingestion uses ongoing supervisors that run and supervise a set of tasks over time. Native batch and Hadoop-based ingestion use a one-time [task](tasks.md). All types of ingestion use an _ingestion spec_ to configure ingestion.

Ingestion specs consists of three main components:

- [`dataSchema`](#dataschema), which configures the [datasource name](#datasource),
   [primary timestamp](#timestampspec), [dimensions](#dimensionsspec), [metrics](#metricsspec), and [transforms and filters](#transformspec) (if needed).
- [`ioConfig`](#ioconfig), which tells Druid how to connect to the source system and how to parse data. For more information, see the
   documentation for each [ingestion method](./index.md#ingestion-methods).
- [`tuningConfig`](#tuningconfig), which controls various tuning parameters specific to each
  [ingestion method](./index.md#ingestion-methods).

Example ingestion spec for task type `index_parallel` (native batch):

```
{
  "type": "index_parallel",
  "spec": {
    "dataSchema": {
      "dataSource": "wikipedia",
      "timestampSpec": {
        "column": "timestamp",
        "format": "auto"
      },
      "dimensionsSpec": {
        "dimensions": [
          "page",
          "language",
          { "type": "long", "name": "userId" }
        ]
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
      "type": "index_parallel",
      "inputSource": {
        "type": "local",
        "baseDir": "examples/indexing/",
        "filter": "wikipedia_data.json"
      },
      "inputFormat": {
        "type": "json",
        "flattenSpec": {
          "useFieldDiscovery": true,
          "fields": [
            { "type": "path", "name": "userId", "expr": "$.user.id" }
          ]
        }
      }
    },
    "tuningConfig": {
      "type": "index_parallel"
    }
  }
}
```

The specific options supported by these sections will depend on the [ingestion method](./index.md#ingestion-methods) you have chosen.
For more examples, refer to the documentation for each ingestion method.

You can also load data visually, without the need to write an ingestion spec, using the "Load data" functionality
available in Druid's [web console](../operations/web-console.md). Druid's visual data loader supports
[Kafka](../development/extensions-core/kafka-ingestion.md),
[Kinesis](../development/extensions-core/kinesis-ingestion.md), and
[native batch](native-batch.md) mode.

## `dataSchema`

> The `dataSchema` spec has been changed in 0.17.0. The new spec is supported by all ingestion methods
except for _Hadoop_ ingestion. See the [Legacy `dataSchema` spec](#legacy-dataschema-spec) for the old spec.

The `dataSchema` is a holder for the following components:

- [datasource name](#datasource)
- [primary timestamp](#timestampspec)
- [dimensions](#dimensionsspec)
- [metrics](#metricsspec)
- [transforms and filters](#transformspec) (if needed).

An example `dataSchema` is:

```
"dataSchema": {
  "dataSource": "wikipedia",
  "timestampSpec": {
    "column": "timestamp",
    "format": "auto"
  },
  "dimensionsSpec": {
    "dimensions": [
      "page",
      "language",
      { "type": "long", "name": "userId" }
    ]
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
[datasource](../design/architecture.md#datasources-and-segments) that data will be written to. An example
`dataSource` is:

```
"dataSource": "my-first-datasource"
```

### `timestampSpec`

The `timestampSpec` is located in `dataSchema` → `timestampSpec` and is responsible for
configuring the [primary timestamp](./data-model.md#primary-timestamp). An example `timestampSpec` is:

```
"timestampSpec": {
  "column": "timestamp",
  "format": "auto"
}
```

> Conceptually, after input data records are read, Druid applies ingestion spec components in a particular order:
> first [`flattenSpec`](data-formats.md#flattenspec) (if any), then [`timestampSpec`](#timestampspec), then [`transformSpec`](#transformspec),
> and finally [`dimensionsSpec`](#dimensionsspec) and [`metricsSpec`](#metricsspec). Keep this in mind when writing
> your ingestion spec.

A `timestampSpec` can have the following components:

|Field|Description|Default|
|-----|-----------|-------|
|column|Input row field to read the primary timestamp from.<br /><br />Regardless of the name of this input field, the primary timestamp will always be stored as a column named `__time` in your Druid datasource.|timestamp|
|format|Timestamp format. Options are: <ul><li>`iso`: ISO8601 with 'T' separator, like "2000-01-01T01:02:03.456"</li><li>`posix`: seconds since epoch</li><li>`millis`: milliseconds since epoch</li><li>`micro`: microseconds since epoch</li><li>`nano`: nanoseconds since epoch</li><li>`auto`: automatically detects ISO (either 'T' or space separator) or millis format</li><li>any [Joda DateTimeFormat string](http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html)</li></ul>|auto|
|missingValue|Timestamp to use for input records that have a null or missing timestamp `column`. Should be in ISO8601 format, like `"2000-01-01T01:02:03.456"`, even if you have specified something else for `format`. Since Druid requires a primary timestamp, this setting can be useful for ingesting datasets that do not have any per-record timestamps at all. |none|

You can use the timestamp in a expression as `__time` because Druid parses the `timestampSpec` before applying [transforms](#transforms).  You can also set the expression `name` to `__time` to replace the value of the timestamp.

Treat `__time` as a millisecond timestamp: the number of milliseconds since Jan 1, 1970 at midnight UTC.

### `dimensionsSpec`

The `dimensionsSpec` is located in `dataSchema` → `dimensionsSpec` and is responsible for
configuring [dimensions](./data-model.md#dimensions). An example `dimensionsSpec` is:

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
> first [`flattenSpec`](data-formats.md#flattenspec) (if any), then [`timestampSpec`](#timestampspec), then [`transformSpec`](#transformspec),
> and finally [`dimensionsSpec`](#dimensionsspec) and [`metricsSpec`](#metricsspec). Keep this in mind when writing
> your ingestion spec.

A `dimensionsSpec` can have the following components:

| Field                  | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | Default |
|------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `dimensions`           | A list of [dimension names or objects](#dimension-objects). You cannot include the same column in both `dimensions` and `dimensionExclusions`.<br /><br />If `dimensions` and `spatialDimensions` are both null or empty arrays, Druid treats all columns other than timestamp or metrics that do not appear in `dimensionExclusions` as String-typed dimension columns. See [inclusions and exclusions](#inclusions-and-exclusions) for details.<br /><br />As a best practice, put the most frequently filtered dimensions at the beginning of the dimensions list. In this case, it would also be good to consider [`partitioning`](partitioning.md) by those same dimensions.                                                                                                                                                                                                                                  | `[]`    |
| `dimensionExclusions`  | The names of dimensions to exclude from ingestion. Only names are supported here, not objects.<br /><br />This list is only used if the `dimensions` and `spatialDimensions` lists are both null or empty arrays; otherwise it is ignored. See [inclusions and exclusions](#inclusions-and-exclusions) below for details.                                                                                                                                                                                                                                                                                                                                               | `[]`    |
| `spatialDimensions`    | An array of [spatial dimensions](../development/geo.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | `[]`    |
| `includeAllDimensions` | You can set `includeAllDimensions` to true to ingest both explicit dimensions in the `dimensions` field and other dimensions that the ingestion task discovers from input data. In this case, the explicit dimensions will appear first in order that you specify them and the dimensions dynamically discovered will come after. This flag can be useful especially with auto schema discovery using [`flattenSpec`](./data-formats.html#flattenspec). If this is not set and the `dimensions` field is not empty, Druid will ingest only explicit dimensions. If this is not set and the `dimensions` field is empty, all discovered dimensions will be ingested. | false   |

#### Dimension objects

Each dimension in the `dimensions` list can either be a name or an object. Providing a name is equivalent to providing
a `string` type dimension object with the given name, e.g. `"page"` is equivalent to `{"name": "page", "type": "string"}`.

Dimension objects can have the following components:

| Field | Description | Default |
|-------|-------------|---------|
| type | Either `string`, `long`, `float`, `double`, or `json`. | `string` |
| name | The name of the dimension. This will be used as the field name to read from input records, as well as the column name stored in generated segments.<br /><br />Note that you can use a [`transformSpec`](#transformspec) if you want to rename columns during ingestion time. | none (required) |
| createBitmapIndex | For `string` typed dimensions, whether or not bitmap indexes should be created for the column in generated segments. Creating a bitmap index requires more storage, but speeds up certain kinds of filtering (especially equality and prefix filtering). Only supported for `string` typed dimensions. | `true` |
| multiValueHandling | Specify the type of handling for [multi-value fields](../querying/multi-value-dimensions.md). Possible values are `sorted_array`, `sorted_set`, and `array`. `sorted_array` and `sorted_set` order the array upon ingestion. `sorted_set` removes duplicates. `array` ingests data as-is | `sorted_array` |

#### Inclusions and exclusions

Druid will interpret a `dimensionsSpec` in two possible ways: _normal_ or _schemaless_.

Normal interpretation occurs when either `dimensions` or `spatialDimensions` is non-empty. In this case, the combination of the two lists will be taken as the set of dimensions to be ingested, and the list of `dimensionExclusions` will be ignored.

Schemaless interpretation occurs when both `dimensions` and `spatialDimensions` are empty or null. In this case, the set of dimensions is determined in the following way:

1. First, start from the set of all root-level fields from the input record, as determined by the [`inputFormat`](./data-formats.md). "Root-level" includes all fields at the top level of a data structure, but does not included fields nested within maps or lists. To extract these, you must use a [`flattenSpec`](./data-formats.md#flattenspec). All fields of non-nested data formats, such as CSV and delimited text, are considered root-level.
2. If a [`flattenSpec`](./data-formats.md#flattenspec) is being used, the set of root-level fields includes any fields generated by the `flattenSpec`. The `useFieldDiscovery` parameter determines whether the original root-level fields will be retained or discarded.
3. Any field listed in `dimensionExclusions` is excluded.
4. The field listed as `column` in the [`timestampSpec`](#timestampspec) is excluded.
5. Any field used as an input to an aggregator from the [metricsSpec](#metricsspec) is excluded.
6. Any field with the same name as an aggregator from the [metricsSpec](#metricsspec) is excluded.
7. All other fields are ingested as `string` typed dimensions with the [default settings](#dimension-objects).

> Note: Fields generated by a [`transformSpec`](#transformspec) are not currently considered candidates for
> schemaless dimension interpretation.

### `metricsSpec`

The `metricsSpec` is located in `dataSchema` → `metricsSpec` and is a list of [aggregators](../querying/aggregations.md)
to apply at ingestion time. This is most useful when [rollup](./rollup.md) is enabled, since it's how you configure
ingestion-time aggregation.

An example `metricsSpec` is:

```
"metricsSpec": [
  { "type": "count", "name": "count" },
  { "type": "doubleSum", "name": "bytes_added_sum", "fieldName": "bytes_added" },
  { "type": "doubleSum", "name": "bytes_deleted_sum", "fieldName": "bytes_deleted" }
]
```

> Generally, when [rollup](./rollup.md) is disabled, you should have an empty `metricsSpec` (because without rollup,
> Druid does not do any ingestion-time aggregation, so there is little reason to include an ingestion-time aggregator). However,
> in some cases, it can still make sense to define metrics: for example, if you want to create a complex column as a way of
> pre-computing part of an [approximate aggregation](../querying/aggregations.md#approximate-aggregations), this can only
> be done by defining a metric in a `metricsSpec`.

### `granularitySpec`

The `granularitySpec` is located in `dataSchema` → `granularitySpec` and is responsible for configuring
the following operations:

1. Partitioning a datasource into [time chunks](../design/architecture.md#datasources-and-segments) (via `segmentGranularity`).
2. Truncating the timestamp, if desired (via `queryGranularity`).
3. Specifying which time chunks of segments should be created, for batch ingestion (via `intervals`).
4. Specifying whether ingestion-time [rollup](./rollup.md) should be used or not (via `rollup`).

Other than `rollup`, these operations are all based on the [primary timestamp](./data-model.md#primary-timestamp).

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
| type |`uniform`| `uniform` |
| segmentGranularity | [Time chunking](../design/architecture.md#datasources-and-segments) granularity for this datasource. Multiple segments can be created per time chunk. For example, when set to `day`, the events of the same day fall into the same time chunk which can be optionally further partitioned into multiple segments based on other configurations and input size. Any [granularity](../querying/granularities.md) can be provided here. Note that all segments in the same time chunk should have the same segment granularity.| `day` |
| queryGranularity | The resolution of timestamp storage within each segment. This must be equal to, or finer, than `segmentGranularity`. This will be the finest granularity that you can query at and still receive sensible results, but note that you can still query at anything coarser than this granularity. E.g., a value of `minute` will mean that records will be stored at minutely granularity, and can be sensibly queried at any multiple of minutes (including minutely, 5-minutely, hourly, etc).<br /><br />Any [granularity](../querying/granularities.md) can be provided here. Use `none` to store timestamps as-is, without any truncation. Note that `rollup` will be applied if it is set even when the `queryGranularity` is set to `none`. | `none` |
| rollup | Whether to use ingestion-time [rollup](./rollup.md) or not. Note that rollup is still effective even when `queryGranularity` is set to `none`. Your data will be rolled up if they have the exactly same timestamp. | `true` |
| intervals | A list of intervals defining time chunks for segments. Specify interval values using ISO8601 format. For example, `["2021-12-06T21:27:10+00:00/2021-12-07T00:00:00+00:00"]`. If you omit the time, the time defaults to "00:00:00".<br /><br />Druid breaks the list up and rounds off the list values based on the `segmentGranularity`.<br /><br />If `null` or not provided, batch ingestion tasks generally determine which time chunks to output based on the timestamps found in the input data.<br /><br />If specified, batch ingestion tasks may be able to skip a determining-partitions phase, which can result in faster ingestion. Batch ingestion tasks may also be able to request all their locks up-front instead of one by one. Batch ingestion tasks throw away any records with timestamps outside of the specified intervals.<br /><br />Ignored for any form of streaming ingestion. | `null` |

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
> first [`flattenSpec`](data-formats.md#flattenspec) (if any), then [`timestampSpec`](#timestampspec), then [`transformSpec`](#transformspec),
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

Druid currently includes one kind of built-in transform, the expression transform. It has the following syntax:

```
{
  "type": "expression",
  "name": "<output name>",
  "expression": "<expr>"
}
```

The `expression` is a [Druid query expression](../misc/math-expr.md).

> Conceptually, after input data records are read, Druid applies ingestion spec components in a particular order:
> first [`flattenSpec`](data-formats.md#flattenspec) (if any), then [`timestampSpec`](#timestampspec), then [`transformSpec`](#transformspec),
> and finally [`dimensionsSpec`](#dimensionsspec) and [`metricsSpec`](#metricsspec). Keep this in mind when writing
> your ingestion spec.

#### Filter

The `filter` conditionally filters input rows during ingestion. Only rows that pass the filter will be
ingested. Any of Druid's standard [query filters](../querying/filters.md) can be used. Note that within a
`transformSpec`, the `transforms` are applied before the `filter`, so the filter can refer to a transform.

### Legacy `dataSchema` spec

> The `dataSchema` spec has been changed in 0.17.0. The new spec is supported by all ingestion methods
except for _Hadoop_ ingestion. See [`dataSchema`](#dataschema) for the new spec.

The legacy `dataSchema` spec has below two more components in addition to the ones listed in the [`dataSchema`](#dataschema) section above.

- [input row parser](#parser-deprecated), [flattening of nested data](#flattenspec) (if needed)

#### `parser` (Deprecated)

In legacy `dataSchema`, the `parser` is located in the `dataSchema` → `parser` and is responsible for configuring a wide variety of
items related to parsing input records. The `parser` is deprecated and it is highly recommended to use `inputFormat` instead.
For details about `inputFormat` and supported `parser` types, see the ["Data formats" page](data-formats.md).

For details about major components of the `parseSpec`, refer to their subsections:

- [`timestampSpec`](#timestampspec), responsible for configuring the [primary timestamp](./data-model.md#primary-timestamp).
- [`dimensionsSpec`](#dimensionsspec), responsible for configuring [dimensions](./data-model.md#dimensions).
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
        "page",
        "language",
        { "type": "long", "name": "userId" }
      ]
    }
  }
}
```

#### `flattenSpec`

In the legacy `dataSchema`, the `flattenSpec` is located in `dataSchema` → `parser` → `parseSpec` → `flattenSpec` and is responsible for
bridging the gap between potentially nested input data (such as JSON, Avro, etc) and Druid's flat data model.
See [Flatten spec](./data-formats.md#flattenspec) for more details.

## `ioConfig`

The `ioConfig` influences how data is read from a source system, such as Apache Kafka, Amazon S3, a mounted
filesystem, or any other supported source system. The `inputFormat` property applies to all
[ingestion method](./index.md#ingestion-methods) except for Hadoop ingestion. The Hadoop ingestion still
uses the [`parser`](#parser-deprecated) in the legacy `dataSchema`.
The rest of `ioConfig` is specific to each individual ingestion method.
An example `ioConfig` to read JSON data is:

```json
"ioConfig": {
    "type": "<ingestion-method-specific type code>",
    "inputFormat": {
      "type": "json"
    },
    ...
}
```
For more details, see the documentation provided by each [ingestion method](./index.md#ingestion-methods).

## `tuningConfig`

Tuning properties are specified in a `tuningConfig`, which goes at the top level of an ingestion spec. Some
properties apply to all [ingestion methods](./index.md#ingestion-methods), but most are specific to each individual
ingestion method. An example `tuningConfig` that sets all of the shared, common properties to their defaults
is:

```plaintext
"tuningConfig": {
  "type": "<ingestion-method-specific type code>",
  "maxRowsInMemory": 1000000,
  "maxBytesInMemory": <one-sixth of JVM memory>,
  "indexSpec": {
    "bitmap": { "type": "roaring" },
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
|maxBytesInMemory|The maximum aggregate size of records, in bytes, to store in the JVM heap before persisting. This is based on a rough estimate of memory usage. Ingested records will be persisted to disk when either `maxRowsInMemory` or `maxBytesInMemory` are reached (whichever happens first). `maxBytesInMemory` also includes heap usage of artifacts created from intermediary persists. This means that after every persist, the amount of `maxBytesInMemory` until the next persist will decrease. If the sum of bytes of all intermediary persisted artifacts exceeds `maxBytesInMemory` the task fails.<br /><br />Setting `maxBytesInMemory` to -1 disables this check, meaning Druid will rely entirely on `maxRowsInMemory` to control memory usage. Setting it to zero means the default value will be used (one-sixth of JVM heap size).<br /><br />Note that the estimate of memory usage is designed to be an overestimate, and can be especially high when using complex ingest-time aggregators, including sketches. If this causes your indexing workloads to persist to disk too often, you can set `maxBytesInMemory` to -1 and rely on `maxRowsInMemory` instead.|One-sixth of max JVM heap size|
|skipBytesInMemoryOverheadCheck|The calculation of maxBytesInMemory takes into account overhead objects created during ingestion and each intermediate persist. Setting this to true can exclude the bytes of these overhead objects from maxBytesInMemory check.|false|
|indexSpec|Defines segment storage format options to use at indexing time.|See [`indexSpec`](#indexspec) for more information.|
|indexSpecForIntermediatePersists|Defines segment storage format options to use at indexing time for intermediate persisted temporary segments.|See [`indexSpec`](#indexspec) for more information.|
|Other properties|Each ingestion method has its own list of additional tuning properties. See the documentation for each method for a full list: [Kafka indexing service](../development/extensions-core/kafka-supervisor-reference.md#tuningconfig), [Kinesis indexing service](../development/extensions-core/kinesis-ingestion.md#tuningconfig), [Native batch](native-batch.md#tuningconfig), and [Hadoop-based](hadoop.md#tuningconfig).||

#### `indexSpec`

The `indexSpec` object can include the following properties:

|Field|Description|Default|
|-----|-----------|-------|
|bitmap|Compression format for bitmap indexes. Should be a JSON object with `type` set to `roaring` or `concise`. For type `roaring`, the boolean property `compressRunOnSerialization` (defaults to true) controls whether or not run-length encoding will be used when it is determined to be more space-efficient.|`{"type": "roaring"}`|
|dimensionCompression|Compression format for dimension columns. Options are `lz4`, `lzf`, or `uncompressed`.|`lz4`|
|metricCompression|Compression format for primitive type metric columns. Options are `lz4`, `lzf`, `uncompressed`, or `none` (which is more efficient than `uncompressed`, but not supported by older versions of Druid).|`lz4`|
|longEncoding|Encoding format for long-typed columns. Applies regardless of whether they are dimensions or metrics. Options are `auto` or `longs`. `auto` encodes the values using offset or lookup table depending on column cardinality, and store them with variable size. `longs` stores the value as-is with 8 bytes each.|`longs`|

Beyond these properties, each ingestion method has its own specific tuning properties. See the documentation for each
[ingestion method](./index.md#ingestion-methods) for details.
