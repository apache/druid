---
layout: doc_page
---

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
        "dimensions": ["page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city"],
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
  }
}
```

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| dataSource | String | The name of the ingested datasource. Datasources can be thought of as tables. | yes |
| parser | JSON Object | Specifies how ingested data can be parsed. | yes |
| metricsSpec | JSON Object array | A list of [aggregators](../querying/aggregations.html). | yes |
| granularitySpec | JSON Object | Specifies how to create segments and roll up data. | yes |

## Parser

If `type` is not included, the parser defaults to `string`.

### String Parser

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `string` in general, or `hadoopyString` when used in a Hadoop indexing job. | no |
| parseSpec | JSON Object | Specifies the format, timestamp, and dimensions of the data. | yes |

### Protobuf Parser

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `protobuf`. | no |
| parseSpec | JSON Object | Specifies the timestamp and dimensions of the data. Should be a timeAndDims parseSpec. | yes |

### Avro Stream Parser

This is for realtime ingestion. Make sure to include `druid-avro-extensions` as an extension.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `avro_stream`. | no |
| avroBytesDecoder | JSON Object | Specifies how to decode bytes to Avro record. | yes |
| parseSpec | JSON Object | Specifies the timestamp and dimensions of the data. Should be a timeAndDims parseSpec. | yes |

For example, using Avro stream parser with schema repo Avro bytes decoder:

```json
"parser" : {
  "type" : "avro_stream",
  "avroBytesDecoder" : {
    "type" : "schema_repo",
    "subjectAndIdConverter" : {
      "type" : "avro_1124",
      "topic" : "${YOUR_TOPIC}"
    },
    "schemaRepository" : {
      "type" : "avro_1124_rest_client",
      "url" : "${YOUR_SCHEMA_REPO_END_POINT}",
    }
  },
  "parseSpec" : {
    "type": "timeAndDims",
    "timestampSpec": <standard timestampSpec>,
    "dimensionsSpec": <standard dimensionsSpec>
  }
}
```

#### Avro Bytes Decoder

If `type` is not included, the avroBytesDecoder defaults to `schema_repo`.

##### SchemaRepo Based Avro Bytes Decoder

This Avro bytes decoder first extract `subject` and `id` from input message bytes, then use them to lookup the Avro schema with which to decode Avro record from bytes. Details can be found in [schema repo](https://github.com/schema-repo/schema-repo) and [AVRO-1124](https://issues.apache.org/jira/browse/AVRO-1124). You will need an http service like schema repo to hold the avro schema. Towards schema registration on the message producer side, you can refer to `io.druid.data.input.AvroStreamInputRowParserTest#testParse()`.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `schema_repo`. | no |
| subjectAndIdConverter | JSON Object | Specifies the how to extract subject and id from message bytes. | yes |
| schemaRepository | JSON Object | Specifies the how to lookup Avro schema from subject and id. | yes |

##### Avro-1124 Subject And Id Converter

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `avro_1124`. | no |
| topic | String | Specifies the topic of your kafka stream. | yes |


##### Avro-1124 Schema Repository

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `avro_1124_rest_client`. | no |
| url | String | Specifies the endpoint url of your Avro-1124 schema repository. | yes |

### Avro Hadoop Parser

This is for batch ingestion using the HadoopDruidIndexer. The `inputFormat` of `inputSpec` in `ioConfig` must be set to `"io.druid.data.input.avro.AvroValueInputFormat"`. You may want to set Avro reader's schema in `jobProperties` in `tuningConfig`, eg: `"avro.schema.path.input.value": "/path/to/your/schema.avsc"` or `"avro.schema.input.value": "your_schema_JSON_object"`, if reader's schema is not set, the schema in Avro object container file will be used, see [Avro specification](http://avro.apache.org/docs/1.7.7/spec.html#Schema+Resolution). Make sure to include "io.druid.extensions:druid-avro-extensions" as an extension.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `avro_hadoop`. | no |
| parseSpec | JSON Object | Specifies the timestamp and dimensions of the data. Should be a timeAndDims parseSpec. | yes |
| fromPigAvroStorage | Boolean | Specifies whether the data file is stored using AvroStorage. | no(default == false) |

For example, using Avro Hadoop parser with custom reader's schema file:

```json
{
  "type" : "index_hadoop",  
  "spec" : {
    "dataSchema" : {
      "dataSource" : "",
      "parser" : {
        "type" : "avro_hadoop",
        "parseSpec" : {
          "type": "timeAndDims",
          "timestampSpec": <standard timestampSpec>,
          "dimensionsSpec": <standard dimensionsSpec>
        }
      }
    },
    "ioConfig" : {
      "type" : "hadoop",
      "inputSpec" : {
        "type" : "static",
        "inputFormat": "io.druid.data.input.avro.AvroValueInputFormat",
        "paths" : ""
      }
    },
    "tuningConfig" : {
       "jobProperties" : {
          "avro.schema.path.input.value" : "/path/to/my/schema.avsc",
      }
    }
  }
}
```

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

This is a special variation of the JSON ParseSpec that lower cases all the column names in the incoming JSON data. This parseSpec is required if you are updating to Druid 0.7.x from Druid 0.6.x, are directly ingesting JSON with mixed case column names, do not have any ETL in place to lower case those column names, and would like to make queries that include the data you created using 0.6.x and 0.7.x.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| format | String | This should say `jsonLowercase`. | yes |
| timestampSpec | JSON Object | Specifies the column and format of the timestamp. | yes |
| dimensionsSpec | JSON Object | Specifies the dimensions of the data. | yes |

#### CSV ParseSpec

Use this with the String Parser to load CSV. Strings are parsed using the net.sf.opencsv library.

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
| format | String | iso, millis, posix, auto or any [Joda time](http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html) format. | no (default == 'auto' |

### DimensionsSpec

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| dimensions | JSON String array | The names of the dimensions. If this is an empty array, Druid will treat all columns that are not timestamp or metric columns as dimension columns. | yes |
| dimensionExclusions | JSON String array | The names of dimensions to exclude from ingestion. | no (default == [] |
| spatialDimensions | JSON Object array | An array of [spatial dimensions](../development/geo.html) | no (default == [] |

## GranularitySpec

The default granularity spec is `uniform`.

### Uniform Granularity Spec

This spec is used to generated segments with uniform intervals.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | string | The type of granularity spec. | no (default == 'uniform') |
| segmentGranularity | string | The granularity to create segments at. | no (default == 'DAY') |
| queryGranularity | string | The minimum granularity to be able to query results at and the granularity of the data inside the segment. E.g. a value of "minute" will mean that data is aggregated at minutely granularity. That is, if there are collisions in the tuple (minute(timestamp), dimensions), then it will aggregate values together using the aggregators instead of storing individual rows. | no (default == 'NONE') |
| intervals | string | A list of intervals for the raw data being ingested. Ignored for real-time ingestion. | yes for batch, no for real-time |

### Arbitrary Granularity Spec

This spec is used to generate segments with arbitrary intervals (it tries to create evenly sized segments). This spec is not supported for real-time processing.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | string | The type of granularity spec. | no (default == 'uniform') |
| queryGranularity | string | The minimum granularity to be able to query results at and the granularity of the data inside the segment. E.g. a value of "minute" will mean that data is aggregated at minutely granularity. That is, if there are collisions in the tuple (minute(timestamp), dimensions), then it will aggregate values together using the aggregators instead of storing individual rows. | no (default == 'NONE') |
| intervals | string | A list of intervals for the raw data being ingested. Ignored for real-time ingestion. | yes for batch, no for real-time |

# IO Config

Stream Push Ingestion: Stream push ingestion with Tranquility does not require an IO Config.
Stream Pull Ingestion: See [Stream pull ingestion](../ingestion/stream-pull.html).
Batch Ingestion: See [Batch ingestion](../ingestion/batch-ingestion.html)

# Tuning Config

Stream Push Ingestion: See [Stream push ingestion](../ingestion/stream-push.html).
Stream Pull Ingestion: See [Stream pull ingestion](../ingestion/stream-pull.html).
Batch Ingestion: See [Batch ingestion](../ingestion/batch-ingestion.html)

# Evaluating Timestamp, Dimensions and Metrics

Druid will interpret dimensions, dimension exclusions, and metrics in the following order:

* Any column listed in the list of dimensions is treated as a dimension.
* Any column listed in the list of dimension exclusions is excluded as a dimension.
* The timestamp column and columns/fieldNames required by metrics are excluded by default.
* If a metric is also listed as a dimension, the metric must have a different name than the dimension name.
