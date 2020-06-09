---
id: data-formats
title: "Data formats"
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

Apache Druid can ingest denormalized data in JSON, CSV, or a delimited form such as TSV, or any custom format. While most examples in the documentation use data in JSON format, it is not difficult to configure Druid to ingest any other delimited data.
We welcome any contributions to new formats.

This page lists all default and core extension data formats supported by Druid.
For additional data formats supported with community extensions,
please see our [community extensions list](../development/extensions.md#community-extensions).

## Formatting the Data

The following samples show data formats that are natively supported in Druid:

_JSON_

```json
{"timestamp": "2013-08-31T01:02:33Z", "page": "Gypsy Danger", "language" : "en", "user" : "nuclear", "unpatrolled" : "true", "newPage" : "true", "robot": "false", "anonymous": "false", "namespace":"article", "continent":"North America", "country":"United States", "region":"Bay Area", "city":"San Francisco", "added": 57, "deleted": 200, "delta": -143}
{"timestamp": "2013-08-31T03:32:45Z", "page": "Striker Eureka", "language" : "en", "user" : "speed", "unpatrolled" : "false", "newPage" : "true", "robot": "true", "anonymous": "false", "namespace":"wikipedia", "continent":"Australia", "country":"Australia", "region":"Cantebury", "city":"Syndey", "added": 459, "deleted": 129, "delta": 330}
{"timestamp": "2013-08-31T07:11:21Z", "page": "Cherno Alpha", "language" : "ru", "user" : "masterYi", "unpatrolled" : "false", "newPage" : "true", "robot": "true", "anonymous": "false", "namespace":"article", "continent":"Asia", "country":"Russia", "region":"Oblast", "city":"Moscow", "added": 123, "deleted": 12, "delta": 111}
{"timestamp": "2013-08-31T11:58:39Z", "page": "Crimson Typhoon", "language" : "zh", "user" : "triplets", "unpatrolled" : "true", "newPage" : "false", "robot": "true", "anonymous": "false", "namespace":"wikipedia", "continent":"Asia", "country":"China", "region":"Shanxi", "city":"Taiyuan", "added": 905, "deleted": 5, "delta": 900}
{"timestamp": "2013-08-31T12:41:27Z", "page": "Coyote Tango", "language" : "ja", "user" : "cancer", "unpatrolled" : "true", "newPage" : "false", "robot": "true", "anonymous": "false", "namespace":"wikipedia", "continent":"Asia", "country":"Japan", "region":"Kanto", "city":"Tokyo", "added": 1, "deleted": 10, "delta": -9}
```

_CSV_

```
2013-08-31T01:02:33Z,"Gypsy Danger","en","nuclear","true","true","false","false","article","North America","United States","Bay Area","San Francisco",57,200,-143
2013-08-31T03:32:45Z,"Striker Eureka","en","speed","false","true","true","false","wikipedia","Australia","Australia","Cantebury","Syndey",459,129,330
2013-08-31T07:11:21Z,"Cherno Alpha","ru","masterYi","false","true","true","false","article","Asia","Russia","Oblast","Moscow",123,12,111
2013-08-31T11:58:39Z,"Crimson Typhoon","zh","triplets","true","false","true","false","wikipedia","Asia","China","Shanxi","Taiyuan",905,5,900
2013-08-31T12:41:27Z,"Coyote Tango","ja","cancer","true","false","true","false","wikipedia","Asia","Japan","Kanto","Tokyo",1,10,-9
```

_TSV (Delimited)_

```
2013-08-31T01:02:33Z  "Gypsy Danger"  "en"  "nuclear" "true"  "true"  "false" "false" "article" "North America" "United States" "Bay Area"  "San Francisco" 57  200 -143
2013-08-31T03:32:45Z  "Striker Eureka"  "en"  "speed" "false" "true"  "true"  "false" "wikipedia" "Australia" "Australia" "Cantebury" "Syndey"  459 129 330
2013-08-31T07:11:21Z  "Cherno Alpha"  "ru"  "masterYi"  "false" "true"  "true"  "false" "article" "Asia"  "Russia"  "Oblast"  "Moscow"  123 12  111
2013-08-31T11:58:39Z  "Crimson Typhoon" "zh"  "triplets"  "true"  "false" "true"  "false" "wikipedia" "Asia"  "China" "Shanxi"  "Taiyuan" 905 5 900
2013-08-31T12:41:27Z  "Coyote Tango"  "ja"  "cancer"  "true"  "false" "true"  "false" "wikipedia" "Asia"  "Japan" "Kanto" "Tokyo" 1 10  -9
```

Note that the CSV and TSV data do not contain column heads. This becomes important when you specify the data for ingesting.

Besides text formats, Druid also supports binary formats such as [Orc](#orc) and [Parquet](#parquet) formats.

## Custom Formats

Druid supports custom data formats and can use the `Regex` parser or the `JavaScript` parsers to parse these formats. Please note that using any of these parsers for
parsing data will not be as efficient as writing a native Java parser or using an external stream processor. We welcome contributions of new Parsers.

## Input Format

> The Input Format is a new way to specify the data format of your input data which was introduced in 0.17.0.
Unfortunately, the Input Format doesn't support all data formats or ingestion methods supported by Druid yet.
Especially if you want to use the Hadoop ingestion, you still need to use the [Parser](#parser).
If your data is formatted in some format not listed in this section, please consider using the Parser instead.

All forms of Druid ingestion require some form of schema object. The format of the data to be ingested is specified using the `inputFormat` entry in your [`ioConfig`](index.md#ioconfig).

### JSON

The `inputFormat` to load data of JSON format. An example is:

```json
"ioConfig": {
  "inputFormat": {
    "type": "json"
  },
  ...
}
```

The JSON `inputFormat` has the following components:

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `json`. | yes |
| flattenSpec | JSON Object | Specifies flattening configuration for nested JSON data. See [`flattenSpec`](#flattenspec) for more info. | no |
| featureSpec | JSON Object | [JSON parser features](https://github.com/FasterXML/jackson-core/wiki/JsonParser-Features) supported by Jackson library. Those features will be applied when parsing the input JSON data. | no |

### CSV

The `inputFormat` to load data of the CSV format. An example is:

```json
"ioConfig": {
  "inputFormat": {
    "type": "csv",
    "columns" : ["timestamp","page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city","added","deleted","delta"]
  },
  ...
}
```

The CSV `inputFormat` has the following components:

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `csv`. | yes |
| listDelimiter | String | A custom delimiter for multi-value dimensions. | no (default = ctrl+A) |
| columns | JSON array | Specifies the columns of the data. The columns should be in the same order with the columns of your data. | yes if `findColumnsFromHeader` is false or missing |
| findColumnsFromHeader | Boolean | If this is set, the task will find the column names from the header row. Note that `skipHeaderRows` will be applied before finding column names from the header. For example, if you set `skipHeaderRows` to 2 and `findColumnsFromHeader` to true, the task will skip the first two lines and then extract column information from the third line. `columns` will be ignored if this is set to true. | no (default = false if `columns` is set; otherwise null) |
| skipHeaderRows | Integer | If this is set, the task will skip the first `skipHeaderRows` rows. | no (default = 0) |

### TSV (Delimited)

```json
"ioConfig": {
  "inputFormat": {
    "type": "tsv",
    "columns" : ["timestamp","page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city","added","deleted","delta"],
    "delimiter":"|"
  },
  ...
}
```

The `inputFormat` to load data of a delimited format. An example is:

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `tsv`. | yes |
| delimiter | String | A custom delimiter for data values. | no (default = `\t`) |
| listDelimiter | String | A custom delimiter for multi-value dimensions. | no (default = ctrl+A) |
| columns | JSON array | Specifies the columns of the data. The columns should be in the same order with the columns of your data. | yes if `findColumnsFromHeader` is false or missing |
| findColumnsFromHeader | Boolean | If this is set, the task will find the column names from the header row. Note that `skipHeaderRows` will be applied before finding column names from the header. For example, if you set `skipHeaderRows` to 2 and `findColumnsFromHeader` to true, the task will skip the first two lines and then extract column information from the third line. `columns` will be ignored if this is set to true. | no (default = false if `columns` is set; otherwise null) |
| skipHeaderRows | Integer | If this is set, the task will skip the first `skipHeaderRows` rows. | no (default = 0) |

Be sure to change the `delimiter` to the appropriate delimiter for your data. Like CSV, you must specify the columns and which subset of the columns you want indexed.

### ORC

> You need to include the [`druid-orc-extensions`](../development/extensions-core/orc.md) as an extension to use the ORC input format.

> If you are considering upgrading from earlier than 0.15.0 to 0.15.0 or a higher version,
> please read [Migration from 'contrib' extension](../development/extensions-core/orc.md#migration-from-contrib-extension) carefully.

The `inputFormat` to load data of ORC format. An example is:

```json
"ioConfig": {
  "inputFormat": {
    "type": "orc",
    "flattenSpec": {
      "useFieldDiscovery": true,
      "fields": [
        {
          "type": "path",
          "name": "nested",
          "expr": "$.path.to.nested"
        }
      ]
    },
    "binaryAsString": false
  },
  ...
}
```

The ORC `inputFormat` has the following components:

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `orc`. | yes |
| flattenSpec | JSON Object | Specifies flattening configuration for nested ORC data. See [`flattenSpec`](#flattenspec) for more info. | no |
| binaryAsString | Boolean | Specifies if the binary orc column which is not logically marked as a string should be treated as a UTF-8 encoded string. | no (default = false) |

### Parquet

> You need to include the [`druid-parquet-extensions`](../development/extensions-core/parquet.md) as an extension to use the Parquet input format.

The `inputFormat` to load data of Parquet format. An example is:

```json
"ioConfig": {
  "inputFormat": {
    "type": "parquet",
    "flattenSpec": {
      "useFieldDiscovery": true,
      "fields": [
        {
          "type": "path",
          "name": "nested",
          "expr": "$.path.to.nested"
        }
      ]
    }
    "binaryAsString": false
  },
  ...
}
```

The Parquet `inputFormat` has the following components:

| Field | Type | Description | Required |
|-------|------|-------------|----------|
|type| String| This should be set to `parquet` to read Parquet file| yes |
|flattenSpec| JSON Object |Define a [`flattenSpec`](#flattenspec) to extract nested values from a Parquet file. Note that only 'path' expression are supported ('jq' is unavailable).| no (default will auto-discover 'root' level properties) |
| binaryAsString | Boolean | Specifies if the bytes parquet column which is not logically marked as a string or enum type should be treated as a UTF-8 encoded string. | no (default = false) |

### Avro OCF

> You need to include the [`druid-avro-extensions`](../development/extensions-core/avro.md) as an extension to use the Avro OCF input format.

The `inputFormat` to load data of Avro OCF format. An example is:
```json
"ioConfig": {
  "inputFormat": {
    "type": "avro_ocf",
    "flattenSpec": {
      "useFieldDiscovery": true,
      "fields": [
        {
          "type": "path",
          "name": "someRecord_subInt",
          "expr": "$.someRecord.subInt"
        }
      ]
    },
    "schema": {
      "namespace": "org.apache.druid.data.input",
      "name": "SomeDatum",
      "type": "record",
      "fields" : [
        { "name": "timestamp", "type": "long" },
        { "name": "eventType", "type": "string" },
        { "name": "id", "type": "long" },
        { "name": "someRecord", "type": {
          "type": "record", "name": "MySubRecord", "fields": [
            { "name": "subInt", "type": "int"},
            { "name": "subLong", "type": "long"}
          ]
        }}]
    },
    "binaryAsString": false
  },
  ...
}
```

| Field | Type | Description | Required |
|-------|------|-------------|----------|
|type| String| This should be set to `avro_ocf` to read Avro OCF file| yes |
|flattenSpec| JSON Object |Define a [`flattenSpec`](#flattenspec) to extract nested values from a Avro records. Note that only 'path' expression are supported ('jq' is unavailable).| no (default will auto-discover 'root' level properties) |
|schema| JSON Object |Define a reader schema to be used when parsing Avro records, this is useful when parsing multiple versions of Avro OCF file data | no (default will decode using the writer schema contained in the OCF file) |
| binaryAsString | Boolean | Specifies if the bytes parquet column which is not logically marked as a string or enum type should be treated as a UTF-8 encoded string. | no (default = false) |

### FlattenSpec

The `flattenSpec` is located in `inputFormat` → `flattenSpec` and is responsible for
bridging the gap between potentially nested input data (such as JSON, Avro, etc) and Druid's flat data model.
An example `flattenSpec` is:

```json
"flattenSpec": {
  "useFieldDiscovery": true,
  "fields": [
    { "name": "baz", "type": "root" },
    { "name": "foo_bar", "type": "path", "expr": "$.foo.bar" },
    { "name": "first_food", "type": "jq", "expr": ".thing.food[1]" }
  ]
}
```
> Conceptually, after input data records are read, the `flattenSpec` is applied first before
> any other specs such as [`timestampSpec`](./index.md#timestampspec), [`transformSpec`](./index.md#transformspec),
> [`dimensionsSpec`](./index.md#dimensionsspec), or [`metricsSpec`](./index.md#metricsspec). Keep this in mind when writing
> your ingestion spec.

Flattening is only supported for [data formats](data-formats.md) that support nesting, including `avro`, `json`, `orc`,
and `parquet`.

A `flattenSpec` can have the following components:

| Field | Description | Default |
|-------|-------------|---------|
| useFieldDiscovery | If true, interpret all root-level fields as available fields for usage by [`timestampSpec`](./index.md#timestampspec), [`transformSpec`](./index.md#transformspec), [`dimensionsSpec`](./index.md#dimensionsspec), and [`metricsSpec`](./index.md#metricsspec).<br><br>If false, only explicitly specified fields (see `fields`) will be available for use. | `true` |
| fields | Specifies the fields of interest and how they are accessed. [See below for details.](#field-flattening-specifications) | `[]` |

#### Field flattening specifications

Each entry in the `fields` list can have the following components:

| Field | Description | Default |
|-------|-------------|---------|
| type | Options are as follows:<br><br><ul><li>`root`, referring to a field at the root level of the record. Only really useful if `useFieldDiscovery` is false.</li><li>`path`, referring to a field using [JsonPath](https://github.com/jayway/JsonPath) notation. Supported by most data formats that offer nesting, including `avro`, `json`, `orc`, and `parquet`.</li><li>`jq`, referring to a field using [jackson-jq](https://github.com/eiiches/jackson-jq) notation. Only supported for the `json` format.</li></ul> | none (required) |
| name | Name of the field after flattening. This name can be referred to by the [`timestampSpec`](./index.md#timestampspec), [`transformSpec`](./index.md#transformspec), [`dimensionsSpec`](./index.md#dimensionsspec), and [`metricsSpec`](./index.md#metricsspec).| none (required) |
| expr | Expression for accessing the field while flattening. For type `path`, this should be [JsonPath](https://github.com/jayway/JsonPath). For type `jq`, this should be [jackson-jq](https://github.com/eiiches/jackson-jq) notation. For other types, this parameter is ignored. | none (required for types `path` and `jq`) |

#### Notes on flattening

* For convenience, when defining a root-level field, it is possible to define only the field name, as a string, instead of a JSON object. For example, `{"name": "baz", "type": "root"}` is equivalent to `"baz"`.
* Enabling `useFieldDiscovery` will only automatically detect "simple" fields at the root level that correspond to data types that Druid supports. This includes strings, numbers, and lists of strings or numbers. Other types will not be automatically detected, and must be specified explicitly in the `fields` list.
* Duplicate field `name`s are not allowed. An exception will be thrown.
* If `useFieldDiscovery` is enabled, any discovered field with the same name as one already defined in the `fields` list will be skipped, rather than added twice.
* [http://jsonpath.herokuapp.com/](http://jsonpath.herokuapp.com/) is useful for testing `path`-type expressions.
* jackson-jq supports a subset of the full [jq](https://stedolan.github.io/jq/) syntax.  Please refer to the [jackson-jq documentation](https://github.com/eiiches/jackson-jq) for details.

## Parser

> The Parser is deprecated for [native batch tasks](./native-batch.md), [Kafka indexing service](../development/extensions-core/kafka-ingestion.md),
and [Kinesis indexing service](../development/extensions-core/kinesis-ingestion.md).
Consider using the [input format](#input-format) instead for these types of ingestion.

This section lists all default and core extension parsers.
For community extension parsers, please see our [community extensions list](../development/extensions.html#community-extensions).

### String Parser

`string` typed parsers operate on text based inputs that can be split into individual records by newlines.
Each line can be further parsed using [`parseSpec`](#parsespec).

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `string` in general, or `hadoopyString` when used in a Hadoop indexing job. | yes |
| parseSpec | JSON Object | Specifies the format, timestamp, and dimensions of the data. | yes |

### Avro Hadoop Parser

> You need to include the [`druid-avro-extensions`](../development/extensions-core/avro.md) as an extension to use the Avro Hadoop Parser.

This parser is for [Hadoop batch ingestion](./hadoop.md).
The `inputFormat` of `inputSpec` in `ioConfig` must be set to `"org.apache.druid.data.input.avro.AvroValueInputFormat"`.
You may want to set Avro reader's schema in `jobProperties` in `tuningConfig`,
e.g.: `"avro.schema.input.value.path": "/path/to/your/schema.avsc"` or
`"avro.schema.input.value": "your_schema_JSON_object"`.
If the Avro reader's schema is not set, the schema in Avro object container file will be used.
See [Avro specification](http://avro.apache.org/docs/1.7.7/spec.html#Schema+Resolution) for more information.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `avro_hadoop`. | yes |
| parseSpec | JSON Object | Specifies the timestamp and dimensions of the data. Should be an "avro" parseSpec. | yes |
| fromPigAvroStorage | Boolean | Specifies whether the data file is stored using AvroStorage. | no(default == false) |

An Avro parseSpec can contain a [`flattenSpec`](#flattenspec) using either the "root" or "path"
field types, which can be used to read nested Avro records. The "jq" field type is not currently supported for Avro.

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
          "format": "avro",
          "timestampSpec": <standard timestampSpec>,
          "dimensionsSpec": <standard dimensionsSpec>,
          "flattenSpec": <optional>
        }
      }
    },
    "ioConfig" : {
      "type" : "hadoop",
      "inputSpec" : {
        "type" : "static",
        "inputFormat": "org.apache.druid.data.input.avro.AvroValueInputFormat",
        "paths" : ""
      }
    },
    "tuningConfig" : {
       "jobProperties" : {
          "avro.schema.input.value.path" : "/path/to/my/schema.avsc"
      }
    }
  }
}
```

### ORC Hadoop Parser

> You need to include the [`druid-orc-extensions`](../development/extensions-core/orc.md) as an extension to use the ORC Hadoop Parser.

> If you are considering upgrading from earlier than 0.15.0 to 0.15.0 or a higher version,
> please read [Migration from 'contrib' extension](../development/extensions-core/orc.md#migration-from-contrib-extension) carefully.

This parser is for [Hadoop batch ingestion](./hadoop.md).
The `inputFormat` of `inputSpec` in `ioConfig` must be set to `"org.apache.orc.mapreduce.OrcInputFormat"`.

|Field     | Type        | Description                                                                            | Required|
|----------|-------------|----------------------------------------------------------------------------------------|---------|
|type      | String      | This should say `orc`                                                                  | yes|
|parseSpec | JSON Object | Specifies the timestamp and dimensions of the data (`timeAndDims` and `orc` format) and a `flattenSpec` (`orc` format) | yes|

The parser supports two `parseSpec` formats: `orc` and `timeAndDims`.

`orc` supports auto field discovery and flattening, if specified with a [`flattenSpec`](#flattenspec).
If no `flattenSpec` is specified, `useFieldDiscovery` will be enabled by default. Specifying a `dimensionSpec` is
optional if `useFieldDiscovery` is enabled: if a `dimensionSpec` is supplied, the list of `dimensions` it defines will be
the set of ingested dimensions, if missing the discovered fields will make up the list.

`timeAndDims` parse spec must specify which fields will be extracted as dimensions through the `dimensionSpec`.

[All column types](https://orc.apache.org/docs/types.html) are supported, with the exception of `union` types. Columns of
 `list` type, if filled with primitives, may be used as a multi-value dimension, or specific elements can be extracted with
`flattenSpec` expressions. Likewise, primitive fields may be extracted from `map` and `struct` types in the same manner.
Auto field discovery will automatically create a string dimension for every (non-timestamp) primitive or `list` of
primitives, as well as any flatten expressions defined in the `flattenSpec`.

#### Hadoop job properties
Like most Hadoop jobs, the best outcomes will add `"mapreduce.job.user.classpath.first": "true"` or
`"mapreduce.job.classloader": "true"` to the `jobProperties` section of `tuningConfig`. Note that it is likely if using
`"mapreduce.job.classloader": "true"` that you will need to set `mapreduce.job.classloader.system.classes` to include
`-org.apache.hadoop.hive.` to instruct Hadoop to load `org.apache.hadoop.hive` classes from the application jars instead
of system jars, e.g.

```json
...
    "mapreduce.job.classloader": "true",
    "mapreduce.job.classloader.system.classes" : "java., javax.accessibility., javax.activation., javax.activity., javax.annotation., javax.annotation.processing., javax.crypto., javax.imageio., javax.jws., javax.lang.model., -javax.management.j2ee., javax.management., javax.naming., javax.net., javax.print., javax.rmi., javax.script., -javax.security.auth.message., javax.security.auth., javax.security.cert., javax.security.sasl., javax.sound., javax.sql., javax.swing., javax.tools., javax.transaction., -javax.xml.registry., -javax.xml.rpc., javax.xml., org.w3c.dom., org.xml.sax., org.apache.commons.logging., org.apache.log4j., -org.apache.hadoop.hbase., -org.apache.hadoop.hive., org.apache.hadoop., core-default.xml, hdfs-default.xml, mapred-default.xml, yarn-default.xml",
...
```

This is due to the `hive-storage-api` dependency of the
`orc-mapreduce` library, which provides some classes under the `org.apache.hadoop.hive` package. If instead using the
setting `"mapreduce.job.user.classpath.first": "true"`, then this will not be an issue.

#### Examples

##### `orc` parser, `orc` parseSpec, auto field discovery, flatten expressions

```json
{
  "type": "index_hadoop",
  "spec": {
    "ioConfig": {
      "type": "hadoop",
      "inputSpec": {
        "type": "static",
        "inputFormat": "org.apache.orc.mapreduce.OrcInputFormat",
        "paths": "path/to/file.orc"
      },
      ...
    },
    "dataSchema": {
      "dataSource": "example",
      "parser": {
        "type": "orc",
        "parseSpec": {
          "format": "orc",
          "flattenSpec": {
            "useFieldDiscovery": true,
            "fields": [
              {
                "type": "path",
                "name": "nestedDim",
                "expr": "$.nestedData.dim1"
              },
              {
                "type": "path",
                "name": "listDimFirstItem",
                "expr": "$.listDim[1]"
              }
            ]
          },
          "timestampSpec": {
            "column": "timestamp",
            "format": "millis"
          }
        }
      },
      ...
    },
    "tuningConfig": <hadoop-tuning-config>
    }
  }
}
```

##### `orc` parser, `orc` parseSpec, field discovery with no flattenSpec or dimensionSpec

```json
{
  "type": "index_hadoop",
  "spec": {
    "ioConfig": {
      "type": "hadoop",
      "inputSpec": {
        "type": "static",
        "inputFormat": "org.apache.orc.mapreduce.OrcInputFormat",
        "paths": "path/to/file.orc"
      },
      ...
    },
    "dataSchema": {
      "dataSource": "example",
      "parser": {
        "type": "orc",
        "parseSpec": {
          "format": "orc",
          "timestampSpec": {
            "column": "timestamp",
            "format": "millis"
          }
        }
      },
      ...
    },
    "tuningConfig": <hadoop-tuning-config>
    }
  }
}
```

##### `orc` parser, `orc` parseSpec, no autodiscovery

```json
{
  "type": "index_hadoop",
  "spec": {
    "ioConfig": {
      "type": "hadoop",
      "inputSpec": {
        "type": "static",
        "inputFormat": "org.apache.orc.mapreduce.OrcInputFormat",
        "paths": "path/to/file.orc"
      },
      ...
    },
    "dataSchema": {
      "dataSource": "example",
      "parser": {
        "type": "orc",
        "parseSpec": {
          "format": "orc",
          "flattenSpec": {
            "useFieldDiscovery": false,
            "fields": [
              {
                "type": "path",
                "name": "nestedDim",
                "expr": "$.nestedData.dim1"
              },
              {
                "type": "path",
                "name": "listDimFirstItem",
                "expr": "$.listDim[1]"
              }
            ]
          },
          "timestampSpec": {
            "column": "timestamp",
            "format": "millis"
          },
          "dimensionsSpec": {
            "dimensions": [
              "dim1",
              "dim3",
              "nestedDim",
              "listDimFirstItem"
            ],
            "dimensionExclusions": [],
            "spatialDimensions": []
          }
        }
      },
      ...
    },
    "tuningConfig": <hadoop-tuning-config>
    }
  }
}
```

##### `orc` parser, `timeAndDims` parseSpec
```json
{
  "type": "index_hadoop",
  "spec": {
    "ioConfig": {
      "type": "hadoop",
      "inputSpec": {
        "type": "static",
        "inputFormat": "org.apache.orc.mapreduce.OrcInputFormat",
        "paths": "path/to/file.orc"
      },
      ...
    },
    "dataSchema": {
      "dataSource": "example",
      "parser": {
        "type": "orc",
        "parseSpec": {
          "format": "timeAndDims",
          "timestampSpec": {
            "column": "timestamp",
            "format": "auto"
          },
          "dimensionsSpec": {
            "dimensions": [
              "dim1",
              "dim2",
              "dim3",
              "listDim"
            ],
            "dimensionExclusions": [],
            "spatialDimensions": []
          }
        }
      },
      ...
    },
    "tuningConfig": <hadoop-tuning-config>
  }
}

```

### Parquet Hadoop Parser

> You need to include the [`druid-parquet-extensions`](../development/extensions-core/parquet.md) as an extension to use the Parquet Hadoop Parser.

The Parquet Hadoop parser is for [Hadoop batch ingestion](./hadoop.md) and parses Parquet files directly.
The `inputFormat` of `inputSpec` in `ioConfig` must be set to `org.apache.druid.data.input.parquet.DruidParquetInputFormat`.

The Parquet Hadoop Parser supports auto field discovery and flattening if provided with a
[`flattenSpec`](#flattenspec) with the `parquet` `parseSpec`. Parquet nested list and map
[logical types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md) _should_ operate correctly with
JSON path expressions for all supported types.

|Field     | Type        | Description                                                                            | Required|
|----------|-------------|----------------------------------------------------------------------------------------|---------|
| type      | String      | This should say `parquet`.| yes |
| parseSpec | JSON Object | Specifies the timestamp and dimensions of the data, and optionally, a flatten spec. Valid parseSpec formats are `timeAndDims` and `parquet` | yes |
| binaryAsString | Boolean | Specifies if the bytes parquet column which is not logically marked as a string or enum type should be treated as a UTF-8 encoded string. | no(default = false) |

When the time dimension is a [DateType column](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md),
a format should not be supplied. When the format is UTF8 (String), either `auto` or a explicitly defined
[format](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html) is required.

#### Parquet Hadoop Parser vs Parquet Avro Hadoop Parser

Both parsers read from Parquet files, but slightly differently. The main
differences are:

* The Parquet Hadoop Parser uses a simple conversion while the Parquet Avro Hadoop Parser
converts Parquet data into avro records first with the `parquet-avro` library and then
parses avro data using the `druid-avro-extensions` module to ingest into Druid.
* The Parquet Hadoop Parser sets a hadoop job property
`parquet.avro.add-list-element-records` to `false` (which normally defaults to `true`), in order to 'unwrap' primitive
list elements into multi-value dimensions.
* The Parquet Hadoop Parser supports `int96` Parquet values, while the Parquet Avro Hadoop Parser does not.
There may also be some subtle differences in the behavior of JSON path expression evaluation of `flattenSpec`.

Based on those differences, we suggest using the Parquet Hadoop Parser over the Parquet Avro Hadoop Parser
to allow ingesting data beyond the schema constraints of Avro conversion.
However, the Parquet Avro Hadoop Parser was the original basis for supporting the Parquet format, and as such it is a bit more mature.

#### Examples

##### `parquet` parser, `parquet` parseSpec
```json
{
  "type": "index_hadoop",
  "spec": {
    "ioConfig": {
      "type": "hadoop",
      "inputSpec": {
        "type": "static",
        "inputFormat": "org.apache.druid.data.input.parquet.DruidParquetInputFormat",
        "paths": "path/to/file.parquet"
      },
      ...
    },
    "dataSchema": {
      "dataSource": "example",
      "parser": {
        "type": "parquet",
        "parseSpec": {
          "format": "parquet",
          "flattenSpec": {
            "useFieldDiscovery": true,
            "fields": [
              {
                "type": "path",
                "name": "nestedDim",
                "expr": "$.nestedData.dim1"
              },
              {
                "type": "path",
                "name": "listDimFirstItem",
                "expr": "$.listDim[1]"
              }
            ]
          },
          "timestampSpec": {
            "column": "timestamp",
            "format": "auto"
          },
          "dimensionsSpec": {
            "dimensions": [],
            "dimensionExclusions": [],
            "spatialDimensions": []
          }
        }
      },
      ...
    },
    "tuningConfig": <hadoop-tuning-config>
    }
  }
}
```

##### `parquet` parser, `timeAndDims` parseSpec
```json
{
  "type": "index_hadoop",
  "spec": {
    "ioConfig": {
      "type": "hadoop",
      "inputSpec": {
        "type": "static",
        "inputFormat": "org.apache.druid.data.input.parquet.DruidParquetInputFormat",
        "paths": "path/to/file.parquet"
      },
      ...
    },
    "dataSchema": {
      "dataSource": "example",
      "parser": {
        "type": "parquet",
        "parseSpec": {
          "format": "timeAndDims",
          "timestampSpec": {
            "column": "timestamp",
            "format": "auto"
          },
          "dimensionsSpec": {
            "dimensions": [
              "dim1",
              "dim2",
              "dim3",
              "listDim"
            ],
            "dimensionExclusions": [],
            "spatialDimensions": []
          }
        }
      },
      ...
    },
    "tuningConfig": <hadoop-tuning-config>
  }
}

```

### Parquet Avro Hadoop Parser

> Consider using the [Parquet Hadoop Parser](#parquet-hadoop-parser) over this parser to ingest
Parquet files. See [Parquet Hadoop Parser vs Parquet Avro Hadoop Parser](#parquet-hadoop-parser-vs-parquet-avro-hadoop-parser)
for the differences between those parsers.

> You need to include both the [`druid-parquet-extensions`](../development/extensions-core/parquet.md)
[`druid-avro-extensions`] as extensions to use the Parquet Avro Hadoop Parser.

The Parquet Avro Hadoop Parser is for [Hadoop batch ingestion](./hadoop.md).
This parser first converts the Parquet data into Avro records, and then parses them to ingest into Druid.
The `inputFormat` of `inputSpec` in `ioConfig` must be set to `org.apache.druid.data.input.parquet.DruidParquetAvroInputFormat`.

The Parquet Avro Hadoop Parser supports auto field discovery and flattening if provided with a
[`flattenSpec`](#flattenspec) with the `avro` `parseSpec`. Parquet nested list and map
[logical types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md) _should_ operate correctly with
JSON path expressions for all supported types. This parser sets a hadoop job property
`parquet.avro.add-list-element-records` to `false` (which normally defaults to `true`), in order to 'unwrap' primitive
list elements into multi-value dimensions.

Note that the `int96` Parquet value type is not supported with this parser.

|Field     | Type        | Description                                                                            | Required|
|----------|-------------|----------------------------------------------------------------------------------------|---------|
| type      | String      | This should say `parquet-avro`. | yes |
| parseSpec | JSON Object | Specifies the timestamp and dimensions of the data, and optionally, a flatten spec. Should be `avro`. | yes |
| binaryAsString | Boolean | Specifies if the bytes parquet column which is not logically marked as a string or enum type should be treated as a UTF-8 encoded string. | no(default = false) |

When the time dimension is a [DateType column](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md),
a format should not be supplied. When the format is UTF8 (String), either `auto` or
an explicitly defined [format](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html) is required.

#### Example

```json
{
  "type": "index_hadoop",
  "spec": {
    "ioConfig": {
      "type": "hadoop",
      "inputSpec": {
        "type": "static",
        "inputFormat": "org.apache.druid.data.input.parquet.DruidParquetAvroInputFormat",
        "paths": "path/to/file.parquet"
      },
      ...
    },
    "dataSchema": {
      "dataSource": "example",
      "parser": {
        "type": "parquet-avro",
        "parseSpec": {
          "format": "avro",
          "flattenSpec": {
            "useFieldDiscovery": true,
            "fields": [
              {
                "type": "path",
                "name": "nestedDim",
                "expr": "$.nestedData.dim1"
              },
              {
                "type": "path",
                "name": "listDimFirstItem",
                "expr": "$.listDim[1]"
              }
            ]
          },
          "timestampSpec": {
            "column": "timestamp",
            "format": "auto"
          },
          "dimensionsSpec": {
            "dimensions": [],
            "dimensionExclusions": [],
            "spatialDimensions": []
          }
        }
      },
      ...
    },
    "tuningConfig": <hadoop-tuning-config>
    }
  }
}
```

### Avro Stream Parser

> You need to include the [`druid-avro-extensions`](../development/extensions-core/avro.md) as an extension to use the Avro Stream Parser.

This parser is for [stream ingestion](./index.md#streaming) and reads Avro data from a stream directly.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `avro_stream`. | no |
| avroBytesDecoder | JSON Object | Specifies how to decode bytes to Avro record. | yes |
| parseSpec | JSON Object | Specifies the timestamp and dimensions of the data. Should be an "avro" parseSpec. | yes |

An Avro parseSpec can contain a [`flattenSpec`](#flattenspec) using either the "root" or "path"
field types, which can be used to read nested Avro records. The "jq" field type is not currently supported for Avro.

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
    "format": "avro",
    "timestampSpec": <standard timestampSpec>,
    "dimensionsSpec": <standard dimensionsSpec>,
    "flattenSpec": <optional>
  }
}
```

#### Avro Bytes Decoder

If `type` is not included, the avroBytesDecoder defaults to `schema_repo`.

##### Inline Schema Based Avro Bytes Decoder

> The "schema_inline" decoder reads Avro records using a fixed schema and does not support schema migration. If you
> may need to migrate schemas in the future, consider one of the other decoders, all of which use a message header that
> allows the parser to identify the proper Avro schema for reading records.

This decoder can be used if all the input events can be read using the same schema. In this case, specify the schema in the input task JSON itself, as described below.

```
...
"avroBytesDecoder": {
  "type": "schema_inline",
  "schema": {
    //your schema goes here, for example
    "namespace": "org.apache.druid.data",
    "name": "User",
    "type": "record",
    "fields": [
      { "name": "FullName", "type": "string" },
      { "name": "Country", "type": "string" }
    ]
  }
}
...
```

##### Multiple Inline Schemas Based Avro Bytes Decoder

Use this decoder if different input events can have different read schemas. In this case, specify the schema in the input task JSON itself, as described below.

```
...
"avroBytesDecoder": {
  "type": "multiple_schemas_inline",
  "schemas": {
    //your id -> schema map goes here, for example
    "1": {
      "namespace": "org.apache.druid.data",
      "name": "User",
      "type": "record",
      "fields": [
        { "name": "FullName", "type": "string" },
        { "name": "Country", "type": "string" }
      ]
    },
    "2": {
      "namespace": "org.apache.druid.otherdata",
      "name": "UserIdentity",
      "type": "record",
      "fields": [
        { "name": "Name", "type": "string" },
        { "name": "Location", "type": "string" }
      ]
    },
    ...
    ...
  }
}
...
```

Note that it is essentially a map of integer schema ID to avro schema object. This parser assumes that record has following format.
  first 1 byte is version and must always be 1.
  next 4 bytes are integer schema ID serialized using big-endian byte order.
  remaining bytes contain serialized avro message.

##### SchemaRepo Based Avro Bytes Decoder

This Avro bytes decoder first extracts `subject` and `id` from the input message bytes, and then uses them to look up the Avro schema used to decode the Avro record from bytes. For details, see the [schema repo](https://github.com/schema-repo/schema-repo) and [AVRO-1124](https://issues.apache.org/jira/browse/AVRO-1124). You will need an http service like schema repo to hold the avro schema. For information on registering a schema on the message producer side, see `org.apache.druid.data.input.AvroStreamInputRowParserTest#testParse()`.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `schema_repo`. | no |
| subjectAndIdConverter | JSON Object | Specifies how to extract the subject and id from message bytes. | yes |
| schemaRepository | JSON Object | Specifies how to look up the Avro schema from subject and id. | yes |

###### Avro-1124 Subject And Id Converter

This section describes the format of the `subjectAndIdConverter` object for the `schema_repo` Avro bytes decoder.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `avro_1124`. | no |
| topic | String | Specifies the topic of your Kafka stream. | yes |


###### Avro-1124 Schema Repository

This section describes the format of the `schemaRepository` object for the `schema_repo` Avro bytes decoder.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `avro_1124_rest_client`. | no |
| url | String | Specifies the endpoint url of your Avro-1124 schema repository. | yes |

##### Confluent Schema Registry-based Avro Bytes Decoder

This Avro bytes decoder first extracts a unique `id` from input message bytes, and then uses it to look up the schema in the Schema Registry used to decode the Avro record from bytes.
For details, see the Schema Registry [documentation](http://docs.confluent.io/current/schema-registry/docs/) and [repository](https://github.com/confluentinc/schema-registry).

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `schema_registry`. | no |
| url | String | Specifies the url endpoint of the Schema Registry. | yes |
| capacity | Integer | Specifies the max size of the cache (default = Integer.MAX_VALUE). | no |

```json
...
"avroBytesDecoder" : {
   "type" : "schema_registry",
   "url" : <schema-registry-url>
}
...
```

### Protobuf Parser

> You need to include the [`druid-protobuf-extensions`](../development/extensions-core/protobuf.md) as an extension to use the Protobuf Parser.

This parser is for [stream ingestion](./index.md#streaming) and reads Protocol buffer data from a stream directly.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `protobuf`. | yes |
| descriptor | String | Protobuf descriptor file name in the classpath or URL. | yes |
| protoMessageType | String | Protobuf message type in the descriptor.  Both short name and fully qualified name are accepted.  The parser uses the first message type found in the descriptor if not specified. | no |
| parseSpec | JSON Object | Specifies the timestamp and dimensions of the data.  The format must be JSON. See [JSON ParseSpec](./index.md) for more configuration options.  Note that timeAndDims parseSpec is no longer supported. | yes |

Sample spec:

```json
"parser": {
  "type": "protobuf",
  "descriptor": "file:///tmp/metrics.desc",
  "protoMessageType": "Metrics",
  "parseSpec": {
    "format": "json",
    "timestampSpec": {
      "column": "timestamp",
      "format": "auto"
    },
    "dimensionsSpec": {
      "dimensions": [
        "unit",
        "http_method",
        "http_code",
        "page",
        "metricType",
        "server"
      ],
      "dimensionExclusions": [
        "timestamp",
        "value"
      ]
    }
  }
}
```

See the [extension description](../development/extensions-core/protobuf.md) for
more details and examples.

## ParseSpec

> The Parser is deprecated for [native batch tasks](./native-batch.md), [Kafka indexing service](../development/extensions-core/kafka-ingestion.md),
and [Kinesis indexing service](../development/extensions-core/kinesis-ingestion.md).
Consider using the [input format](#input-format) instead for these types of ingestion.

ParseSpecs serve two purposes:

- The String Parser use them to determine the format (i.e., JSON, CSV, TSV) of incoming rows.
- All Parsers use them to determine the timestamp and dimensions of incoming rows.

If `format` is not included, the parseSpec defaults to `tsv`.

### JSON ParseSpec

Use this with the String Parser to load JSON.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| format | String | This should say `json`. | no |
| timestampSpec | JSON Object | Specifies the column and format of the timestamp. | yes |
| dimensionsSpec | JSON Object | Specifies the dimensions of the data. | yes |
| flattenSpec | JSON Object | Specifies flattening configuration for nested JSON data. See [`flattenSpec`](#flattenspec) for more info. | no |

Sample spec:

```json
"parseSpec": {
  "format" : "json",
  "timestampSpec" : {
    "column" : "timestamp"
  },
  "dimensionSpec" : {
    "dimensions" : ["page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city"]
  }
}
```

### JSON Lowercase ParseSpec

> The _jsonLowercase_ parser is deprecated and may be removed in a future version of Druid.

This is a special variation of the JSON ParseSpec that lower cases all the column names in the incoming JSON data. This parseSpec is required if you are updating to Druid 0.7.x from Druid 0.6.x, are directly ingesting JSON with mixed case column names, do not have any ETL in place to lower case those column names, and would like to make queries that include the data you created using 0.6.x and 0.7.x.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| format | String | This should say `jsonLowercase`. | yes |
| timestampSpec | JSON Object | Specifies the column and format of the timestamp. | yes |
| dimensionsSpec | JSON Object | Specifies the dimensions of the data. | yes |

### CSV ParseSpec

Use this with the String Parser to load CSV. Strings are parsed using the com.opencsv library.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| format | String | This should say `csv`. | yes |
| timestampSpec | JSON Object | Specifies the column and format of the timestamp. | yes |
| dimensionsSpec | JSON Object | Specifies the dimensions of the data. | yes |
| listDelimiter | String | A custom delimiter for multi-value dimensions. | no (default = ctrl+A) |
| columns | JSON array | Specifies the columns of the data. | yes |

Sample spec:

```json
"parseSpec": {
  "format" : "csv",
  "timestampSpec" : {
    "column" : "timestamp"
  },
  "columns" : ["timestamp","page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city","added","deleted","delta"],
  "dimensionsSpec" : {
    "dimensions" : ["page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city"]
  }
}
```

#### CSV Index Tasks

If your input files contain a header, the `columns` field is optional and you don't need to set.
Instead, you can set the `hasHeaderRow` field to true, which makes Druid automatically extract the column information from the header.
Otherwise, you must set the `columns` field and ensure that field must match the columns of your input data in the same order.

Also, you can skip some header rows by setting `skipHeaderRows` in your parseSpec. If both `skipHeaderRows` and `hasHeaderRow` options are set,
`skipHeaderRows` is first applied. For example, if you set `skipHeaderRows` to 2 and `hasHeaderRow` to true, Druid will
skip the first two lines and then extract column information from the third line.

Note that `hasHeaderRow` and `skipHeaderRows` are effective only for non-Hadoop batch index tasks. Other types of index
tasks will fail with an exception.

#### Other CSV Ingestion Tasks

The `columns` field must be included and and ensure that the order of the fields matches the columns of your input data in the same order.

### TSV / Delimited ParseSpec

Use this with the String Parser to load any delimited text that does not require special escaping. By default,
the delimiter is a tab, so this will load TSV.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| format | String | This should say `tsv`. | yes |
| timestampSpec | JSON Object | Specifies the column and format of the timestamp. | yes |
| dimensionsSpec | JSON Object | Specifies the dimensions of the data. | yes |
| delimiter | String | A custom delimiter for data values. | no (default = \t) |
| listDelimiter | String | A custom delimiter for multi-value dimensions. | no (default = ctrl+A) |
| columns | JSON String array | Specifies the columns of the data. | yes |

Sample spec:

```json
"parseSpec": {
  "format" : "tsv",
  "timestampSpec" : {
    "column" : "timestamp"
  },
  "columns" : ["timestamp","page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city","added","deleted","delta"],
  "delimiter":"|",
  "dimensionsSpec" : {
    "dimensions" : ["page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city"]
  }
}
```

Be sure to change the `delimiter` to the appropriate delimiter for your data. Like CSV, you must specify the columns and which subset of the columns you want indexed.

#### TSV (Delimited) Index Tasks

If your input files contain a header, the `columns` field is optional and doesn't need to be set.
Instead, you can set the `hasHeaderRow` field to true, which makes Druid automatically extract the column information from the header.
Otherwise, you must set the `columns` field and ensure that field must match the columns of your input data in the same order.

Also, you can skip some header rows by setting `skipHeaderRows` in your parseSpec. If both `skipHeaderRows` and `hasHeaderRow` options are set,
`skipHeaderRows` is first applied. For example, if you set `skipHeaderRows` to 2 and `hasHeaderRow` to true, Druid will
skip the first two lines and then extract column information from the third line.

Note that `hasHeaderRow` and `skipHeaderRows` are effective only for non-Hadoop batch index tasks. Other types of index
tasks will fail with an exception.

#### Other TSV (Delimited) Ingestion Tasks

The `columns` field must be included and and ensure that the order of the fields matches the columns of your input data in the same order.

### Multi-value dimensions

Dimensions can have multiple values for TSV and CSV data. To specify the delimiter for a multi-value dimension, set the `listDelimiter` in the `parseSpec`.

JSON data can contain multi-value dimensions as well. The multiple values for a dimension must be formatted as a JSON array in the ingested data. No additional `parseSpec` configuration is needed.

### Regex ParseSpec

```json
"parseSpec":{
  "format" : "regex",
  "timestampSpec" : {
    "column" : "timestamp"
  },
  "dimensionsSpec" : {
    "dimensions" : [<your_list_of_dimensions>]
  },
  "columns" : [<your_columns_here>],
  "pattern" : <regex pattern for partitioning data>
}
```

The `columns` field must match the columns of your regex matching groups in the same order. If columns are not provided, default
columns names ("column_1", "column2", ... "column_n") will be assigned. Ensure that your column names include all your dimensions.

### JavaScript ParseSpec

```json
"parseSpec":{
  "format" : "javascript",
  "timestampSpec" : {
    "column" : "timestamp"
  },
  "dimensionsSpec" : {
    "dimensions" : ["page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city"]
  },
  "function" : "function(str) { var parts = str.split(\"-\"); return { one: parts[0], two: parts[1] } }"
}
```

Note with the JavaScript parser that data must be fully parsed and returned as a `{key:value}` format in the JS logic.
This means any flattening or parsing multi-dimensional values must be done here.

> JavaScript-based functionality is disabled by default. Please refer to the Druid [JavaScript programming guide](../development/javascript.md) for guidelines about using Druid's JavaScript functionality, including instructions on how to enable it.

### TimeAndDims ParseSpec

Use this with non-String Parsers to provide them with timestamp and dimensions information. Non-String Parsers
handle all formatting decisions on their own, without using the ParseSpec.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| format | String | This should say `timeAndDims`. | yes |
| timestampSpec | JSON Object | Specifies the column and format of the timestamp. | yes |
| dimensionsSpec | JSON Object | Specifies the dimensions of the data. | yes |

### Orc ParseSpec

Use this with the Hadoop ORC Parser to load ORC files.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| format | String | This should say `orc`. | no |
| timestampSpec | JSON Object | Specifies the column and format of the timestamp. | yes |
| dimensionsSpec | JSON Object | Specifies the dimensions of the data. | yes |
| flattenSpec | JSON Object | Specifies flattening configuration for nested JSON data. See [`flattenSpec`](#flattenspec) for more info. | no |

### Parquet ParseSpec

Use this with the Hadoop Parquet Parser to load Parquet files.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| format | String | This should say `parquet`. | no |
| timestampSpec | JSON Object | Specifies the column and format of the timestamp. | yes |
| dimensionsSpec | JSON Object | Specifies the dimensions of the data. | yes |
| flattenSpec | JSON Object | Specifies flattening configuration for nested JSON data. See [`flattenSpec`](#flattenspec) for more info. | no |
