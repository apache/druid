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

Apache Druid (incubating) can ingest denormalized data in JSON, CSV, or a delimited form such as TSV, or any custom format. While most examples in the documentation use data in JSON format, it is not difficult to configure Druid to ingest any other delimited data.
We welcome any contributions to new formats.

For additional data formats, please see our [extensions list](../development/extensions.md).

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

## Custom Formats

Druid supports custom data formats and can use the `Regex` parser or the `JavaScript` parsers to parse these formats. Please note that using any of these parsers for
parsing data will not be as efficient as writing a native Java parser or using an external stream processor. We welcome contributions of new Parsers.

## Configuration

All forms of Druid ingestion require some form of schema object. The format of the data to be ingested is specified using the`parseSpec` entry in your `dataSchema`.

### JSON

```json
  "parseSpec":{
    "format" : "json",
    "timestampSpec" : {
      "column" : "timestamp"
    },
    "dimensionSpec" : {
      "dimensions" : ["page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city"]
    }
  }
```

If you have nested JSON, [Druid can automatically flatten it for you](index.md#flattenspec).

### CSV

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

### TSV (Delimited)

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

If your input files contain a header, the `columns` field is optional and you don't need to set.
Instead, you can set the `hasHeaderRow` field to true, which makes Druid automatically extract the column information from the header.
Otherwise, you must set the `columns` field and ensure that field must match the columns of your input data in the same order.

Also, you can skip some header rows by setting `skipHeaderRows` in your parseSpec. If both `skipHeaderRows` and `hasHeaderRow` options are set,
`skipHeaderRows` is first applied. For example, if you set `skipHeaderRows` to 2 and `hasHeaderRow` to true, Druid will
skip the first two lines and then extract column information from the third line.

Note that `hasHeaderRow` and `skipHeaderRows` are effective only for non-Hadoop batch index tasks. Other types of index
tasks will fail with an exception.

#### Other TSV (Delimited) Ingestion Tasks

The `columns` field must be included and and ensure that the order of the fields matches the columns of your input data in the same order.

### Regex

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

### JavaScript

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

### Multi-value dimensions

Dimensions can have multiple values for TSV and CSV data. To specify the delimiter for a multi-value dimension, set the `listDelimiter` in the `parseSpec`.

JSON data can contain multi-value dimensions as well. The multiple values for a dimension must be formatted as a JSON array in the ingested data. No additional `parseSpec` configuration is needed.

## Parser

The default parser type is  `string`, though a handful of extensions provide additional parser types. `string` typed parsers operate on text based inputs that can be split into individual records by newlines. For additional data formats, please see our [extensions list](../development/extensions.html).

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
| flattenSpec | JSON Object | Specifies flattening configuration for nested JSON data. See [`flattenSpec`](index.md#flattenspec) for more info. | no |

#### JSON Lowercase ParseSpec

> The _jsonLowercase_ parser is deprecated and may be removed in a future version of Druid.

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

