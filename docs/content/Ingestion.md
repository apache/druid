---
layout: doc_page
---

# Ingestion Spec

A Druid ingestion spec consists of 3 components:

```json
{
  "dataSchema" : {...}
  "ioConfig" : {...}
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
| metricsSpec | JSON Object array | A list of [aggregators](Aggregations.html). | yes |
| granularitySpec | JSON Object | Specifies how to create segments and roll up data. | yes |

## Parser

If `type` is not included, the parser defaults to `string`.

### String Parser

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `string`. | no |
| parseSpec | JSON Object | Specifies the format of the data. | yes |

### Protobuf Parser

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `protoBuf`. | no |
| parseSpec | JSON Object | Specifies the format of the data. | yes |

### ParseSpec

If `type` is not included, the parseSpec defaults to `tsv`.

#### JSON ParseSpec

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `json`. | no |
| timestampSpec | JSON Object | Specifies the column and format of the timestamp. | yes |
| dimensionsSpec | JSON Object | Specifies the dimensions of the data. | yes |

#### CSV ParseSpec

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `csv`. | no |
| timestampSpec | JSON Object | Specifies the column and format of the timestamp. | yes |
| dimensionsSpec | JSON Object | Specifies the dimensions of the data. | yes |
| listDelimiter | String | A custom delimiter for multi-value dimensions. | no (default == ctrl+A) |
| columns | JSON array | Specifies the columns of the data. | yes |

#### TSV ParseSpec

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `tsv`. | no |
| timestampSpec | JSON Object | Specifies the column and format of the timestamp. | yes |
| dimensionsSpec | JSON Object | Specifies the dimensions of the data. | yes |
| delimiter | String | A custom delimiter for data values. | no (default == \t) |
| listDelimiter | String | A custom delimiter for multi-value dimensions. | no (default == ctrl+A) |
| columns | JSON String array | Specifies the columns of the data. | yes |

### Timestamp Spec

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| column | String | The column of the timestamp. | yes |
| format | String | iso, millis, posix, auto or any [Joda time](http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html) format. | no (default == 'auto' |

### DimensionsSpec

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| dimensions | JSON String array | The names of the dimensions. | yes |
| dimensionExclusions | JSON String array | The names of dimensions to exclude from ingestion. | no (default == [] |
| spatialDimensions | JSON Object array | An array of [spatial dimensions](GeographicQueries.html) | no (default == [] |

## GranularitySpec

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| segmentGranularity | string | The granularity to create segments at. | no (default == 'DAY') |
| queryGranularity | string | The minimum granularity to be able to query results at and the granularity of the data inside the segment. E.g. a value of "minute" will mean that data is aggregated at minutely granularity. That is, if there are collisions in the tuple (minute(timestamp), dimensions), then it will aggregate values together using the aggregators instead of storing individual rows. | no (default == 'NONE') |
| intervals | string | A list of intervals for the raw data being ingested. Ignored for real-time ingestion. | yes for batch, no for real-time |

# IO Config

Real-time Ingestion: See [Real-time ingestion](Realtime-ingestion.html).
Batch Ingestion: See [Batch ingestion](Batch-ingestion.html)

# Ingestion Spec

Real-time Ingestion: See [Real-time ingestion](Realtime-ingestion.html).
Batch Ingestion: See [Batch ingestion](Batch-ingestion.html)
