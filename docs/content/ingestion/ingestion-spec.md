---
layout: doc_page
title: "Ingestion Spec"
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

# Ingestion Spec

A Druid ingestion spec consists of 3 components:

```json
{
  "dataSchema" : {...},
  "ioConfig" : {...},
  "tuningConfig" : {...}
}
```

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| dataSchema | JSON Object | Specifies the the schema of the incoming data. All ingestion specs can share the same dataSchema. | yes |
| ioConfig | JSON Object | Specifies where the data is coming from and where the data is going. This object will vary with the ingestion method. | yes |
| tuningConfig | JSON Object | Specifies how to tune various ingestion parameters. This object will vary with the ingestion method. | no |

# DataSchema

An example dataSchema is shown below:

```json
"dataSchema" : {
  "dataSource" : "wikipedia",
  "parser" : {
    "type" : "string",
    "parseSpec" : {
      "format" : "json",
      "timestampSpec" : {
        "column" : "timestamp",
        "format" : "auto"
      },
      "dimensionsSpec" : {
        "dimensions": [
          "page",
          "language",
          "user",
          "unpatrolled",
          "newPage",
          "robot",
          "anonymous",
          "namespace",
          "continent",
          "country",
          "region",
          "city",
          {
            "type": "long",
            "name": "countryNum"
          },
          {
            "type": "float",
            "name": "userLatitude"
          },
          {
            "type": "float",
            "name": "userLongitude"
          }
        ],
        "dimensionExclusions" : [],
        "spatialDimensions" : []
      }
    }
  },
  "metricsSpec" : [{
    "type" : "count",
    "name" : "count"
  }, {
    "type" : "doubleSum",
    "name" : "added",
    "fieldName" : "added"
  }, {
    "type" : "doubleSum",
    "name" : "deleted",
    "fieldName" : "deleted"
  }, {
    "type" : "doubleSum",
    "name" : "delta",
    "fieldName" : "delta"
  }],
  "granularitySpec" : {
    "segmentGranularity" : "DAY",
    "queryGranularity" : "NONE",
    "intervals" : [ "2013-08-31/2013-09-01" ]
  },
  "transformSpec" : null
}
```

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| dataSource | String | The name of the ingested datasource. Datasources can be thought of as tables. | yes |
| parser | JSON Object | Specifies how ingested data can be parsed. | yes |
| metricsSpec | JSON Object array | A list of [aggregators](../querying/aggregations.html). | yes |
| granularitySpec | JSON Object | Specifies how to create segments and roll up data. | yes |
| transformSpec | JSON Object | Specifes how to filter and transform input data. See [transform specs](../ingestion/transform-spec.html).| no |

## Parser

If `type` is not included, the parser defaults to `string`. For additional data formats, please see our [extensions list](../development/extensions.html).

### String Parser

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `string` in general, or `hadoopyString` when used in a Hadoop indexing job. | no |
| parseSpec | JSON Object | Specifies the format, timestamp, and dimensions of the data. | yes |

### ParseSpec

ParseSpecs serve two purposes:

- The String Parser use them to determine the format (i.e. JSON, CSV, TSV) of incoming rows.
- All Parsers use them to determine the timestamp and dimensions of incoming rows.

If `format` is not included, the parseSpec defaults to `tsv`.

#### JSON ParseSpec

Use this with the String Parser to load JSON.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| format | String | This should say `json`. | no |
| timestampSpec | JSON Object | Specifies the column and format of the timestamp. | yes |
| dimensionsSpec | JSON Object | Specifies the dimensions of the data. | yes |
| flattenSpec | JSON Object | Specifies flattening configuration for nested JSON data. See [Flattening JSON](./flatten-json.html) for more info. | no |

#### JSON Lowercase ParseSpec

<div class="note caution">
The _jsonLowercase_ parser is deprecated and may be removed in a future version of Druid.
</div>

This is a special variation of the JSON ParseSpec that lower cases all the column names in the incoming JSON data. This parseSpec is required if you are updating to Druid 0.7.x from Druid 0.6.x, are directly ingesting JSON with mixed case column names, do not have any ETL in place to lower case those column names, and would like to make queries that include the data you created using 0.6.x and 0.7.x.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| format | String | This should say `jsonLowercase`. | yes |
| timestampSpec | JSON Object | Specifies the column and format of the timestamp. | yes |
| dimensionsSpec | JSON Object | Specifies the dimensions of the data. | yes |

#### CSV ParseSpec

Use this with the String Parser to load CSV. Strings are parsed using the com.opencsv library.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| format | String | This should say `csv`. | yes |
| timestampSpec | JSON Object | Specifies the column and format of the timestamp. | yes |
| dimensionsSpec | JSON Object | Specifies the dimensions of the data. | yes |
| listDelimiter | String | A custom delimiter for multi-value dimensions. | no (default == ctrl+A) |
| columns | JSON array | Specifies the columns of the data. | yes |

#### TSV / Delimited ParseSpec

Use this with the String Parser to load any delimited text that does not require special escaping. By default,
the delimiter is a tab, so this will load TSV.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| format | String | This should say `tsv`. | yes |
| timestampSpec | JSON Object | Specifies the column and format of the timestamp. | yes |
| dimensionsSpec | JSON Object | Specifies the dimensions of the data. | yes |
| delimiter | String | A custom delimiter for data values. | no (default == \t) |
| listDelimiter | String | A custom delimiter for multi-value dimensions. | no (default == ctrl+A) |
| columns | JSON String array | Specifies the columns of the data. | yes |

#### TimeAndDims ParseSpec

Use this with non-String Parsers to provide them with timestamp and dimensions information. Non-String Parsers
handle all formatting decisions on their own, without using the ParseSpec.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| format | String | This should say `timeAndDims`. | yes |
| timestampSpec | JSON Object | Specifies the column and format of the timestamp. | yes |
| dimensionsSpec | JSON Object | Specifies the dimensions of the data. | yes |

### TimestampSpec

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| column | String | The column of the timestamp. | yes |
| format | String | iso, posix, millis, micro, nano, auto or any [Joda time](http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html) format. | no (default == 'auto' |

<a name="dimensions" />

### DimensionsSpec

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| dimensions | JSON array | A list of [dimension schema](#dimension-schema) objects or dimension names. Providing a name is equivalent to providing a String-typed dimension schema with the given name. If this is an empty array, Druid will treat all non-timestamp, non-metric columns that do not appear in "dimensionExclusions" as String-typed dimension columns. | yes |
| dimensionExclusions | JSON String array | The names of dimensions to exclude from ingestion. | no (default == [] |
| spatialDimensions | JSON Object array | An array of [spatial dimensions](../development/geo.html) | no (default == [] |

#### Dimension Schema
A dimension schema specifies the type and name of a dimension to be ingested.

For string columns, the dimension schema can also be used to enable or disable bitmap indexing by setting the
`createBitmapIndex` boolean. By default, bitmap indexes are enabled for all string columns. Only string columns can have
bitmap indexes; they are not supported for numeric columns.

For example, the following `dimensionsSpec` section from a `dataSchema` ingests one column as Long (`countryNum`), two
columns as Float (`userLatitude`, `userLongitude`), and the other columns as Strings, with bitmap indexes disabled
for the `comment` column.

```json
"dimensionsSpec" : {
  "dimensions": [
    "page",
    "language",
    "user",
    "unpatrolled",
    "newPage",
    "robot",
    "anonymous",
    "namespace",
    "continent",
    "country",
    "region",
    "city",
    {
      "type": "string",
      "name": "comment",
      "createBitmapIndex": false
    },
    {
      "type": "long",
      "name": "countryNum"
    },
    {
      "type": "float",
      "name": "userLatitude"
    },
    {
      "type": "float",
      "name": "userLongitude"
    }
  ],
  "dimensionExclusions" : [],
  "spatialDimensions" : []
}
```

## metricsSpec
 The `metricsSpec` is a list of [aggregators](../querying/aggregations.html). If `rollup` is false in the granularity spec, the metrics spec should be an empty list and all columns should be defined in the `dimensionsSpec` instead (without rollup, there isn't a real distinction between dimensions and metrics at ingestion time). This is optional, however.
 
## GranularitySpec

GranularitySpec is to define how to partition a dataSource into [time chunks](../design/index.html#datasources-and-segments).
The default granularitySpec is `uniform`, and can be changed by setting the `type` field.
Currently, `uniform` and `arbitrary` types are supported.

### Uniform Granularity Spec

This spec is used to generated segments with uniform intervals.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| segmentGranularity | string | The granularity to create time chunks at. Multiple segments can be created per time chunk. For example, with 'DAY' `segmentGranularity`, the events of the same day fall into the same time chunk which can be optionally further partitioned into multiple segments based on other configurations and input size. See [Granularity](../querying/granularities.html) for supported granularities.| no (default == 'DAY') |
| queryGranularity | string | The minimum granularity to be able to query results at and the granularity of the data inside the segment. E.g. a value of "minute" will mean that data is aggregated at minutely granularity. That is, if there are collisions in the tuple (minute(timestamp), dimensions), then it will aggregate values together using the aggregators instead of storing individual rows. A granularity of 'NONE' means millisecond granularity. See [Granularity](../querying/granularities.html) for supported granularities.| no (default == 'NONE') |
| rollup | boolean | rollup or not | no (default == true) |
| intervals | string | A list of intervals for the raw data being ingested. Ignored for real-time ingestion. | no. If specified, batch ingestion tasks may skip determining partitions phase which results in faster ingestion. |

### Arbitrary Granularity Spec

This spec is used to generate segments with arbitrary intervals (it tries to create evenly sized segments). This spec is not supported for real-time processing.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| queryGranularity | string | The minimum granularity to be able to query results at and the granularity of the data inside the segment. E.g. a value of "minute" will mean that data is aggregated at minutely granularity. That is, if there are collisions in the tuple (minute(timestamp), dimensions), then it will aggregate values together using the aggregators instead of storing individual rows. A granularity of 'NONE' means millisecond granularity. See [Granularity](../querying/granularities.html) for supported granularities.| no (default == 'NONE') |
| rollup | boolean | rollup or not | no (default == true) |
| intervals | string | A list of intervals for the raw data being ingested. Ignored for real-time ingestion. | no. If specified, batch ingestion tasks may skip determining partitions phase which results in faster ingestion. |

# Transform Spec

Transform specs allow Druid to transform and filter input data during ingestion. See [Transform specs](../ingestion/transform-spec.html)

# IO Config

The IOConfig spec differs based on the ingestion task type.

* Native Batch ingestion: See [Native Batch IOConfig](../ingestion/native_tasks.html#ioconfig)
* Hadoop Batch ingestion: See [Hadoop Batch IOConfig](../ingestion/hadoop.html#ioconfig)
* Kafka Indexing Service: See [Kafka Supervisor IOConfig](../development/extensions-core/kafka-ingestion.html#KafkaSupervisorIOConfig)
* Stream Push Ingestion: Stream push ingestion with Tranquility does not require an IO Config.
* Stream Pull Ingestion (Deprecated): See [Stream pull ingestion](../ingestion/stream-pull.html#ioconfig).

# Tuning Config

The TuningConfig spec differs based on the ingestion task type.

* Native Batch ingestion: See [Native Batch TuningConfig](../ingestion/native_tasks.html#tuningconfig)
* Hadoop Batch ingestion: See [Hadoop Batch TuningConfig](../ingestion/hadoop.html#tuningconfig)
* Kafka Indexing Service: See [Kafka Supervisor TuningConfig](../development/extensions-core/kafka-ingestion.html#KafkaSupervisorTuningConfig)
* Stream Push Ingestion (Tranquility): See [Tranquility TuningConfig](http://static.druid.io/tranquility/api/latest/#com.metamx.tranquility.druid.DruidTuning).
* Stream Pull Ingestion (Deprecated): See [Stream pull ingestion](../ingestion/stream-pull.html#tuningconfig).

# Evaluating Timestamp, Dimensions and Metrics

Druid will interpret dimensions, dimension exclusions, and metrics in the following order:

* Any column listed in the list of dimensions is treated as a dimension.
* Any column listed in the list of dimension exclusions is excluded as a dimension.
* The timestamp column and columns/fieldNames required by metrics are excluded by default.
* If a metric is also listed as a dimension, the metric must have a different name than the dimension name.
