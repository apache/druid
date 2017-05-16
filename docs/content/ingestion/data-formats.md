---
layout: doc_page
---
Data Formats for Ingestion
==========================

Druid can ingest denormalized data in JSON, CSV, or a delimited form such as TSV, or any custom format. While most examples in the documentation use data in JSON format, it is not difficult to configure Druid to ingest any other delimited data.
We welcome any contributions to new formats.

For additional data formats, please see our [extensions list](../development/extensions.html).

## Formatting the Data

The following are some samples of the data used in the [Wikipedia example](../tutorials/quickstart.html).

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
2013-08-31T01:02:33Z	"Gypsy Danger"	"en"	"nuclear"	"true"	"true"	"false"	"false"	"article"	"North America"	"United States"	"Bay Area"	"San Francisco"	57	200	-143
2013-08-31T03:32:45Z	"Striker Eureka"	"en"	"speed"	"false"	"true"	"true"	"false"	"wikipedia"	"Australia"	"Australia"	"Cantebury"	"Syndey"	459	129	330
2013-08-31T07:11:21Z	"Cherno Alpha"	"ru"	"masterYi"	"false"	"true"	"true"	"false"	"article"	"Asia"	"Russia"	"Oblast"	"Moscow"	123	12	111
2013-08-31T11:58:39Z	"Crimson Typhoon"	"zh"	"triplets"	"true"	"false"	"true"	"false"	"wikipedia"	"Asia"	"China"	"Shanxi"	"Taiyuan"	905	5	900
2013-08-31T12:41:27Z	"Coyote Tango"	"ja"	"cancer"	"true"	"false"	"true"	"false"	"wikipedia"	"Asia"	"Japan"	"Kanto"	"Tokyo"	1	10	-9
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

If you have nested JSON, [Druid can automatically flatten it for you](flatten-json.html).

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

<div class="note info">
JavaScript-based functionality is disabled by default. Please refer to the Druid <a href="../development/javascript.html">JavaScript programming guide</a> for guidelines about using Druid's JavaScript functionality, including instructions on how to enable it.
</div>

### Multi-value dimensions

Dimensions can have multiple values for TSV and CSV data. To specify the delimiter for a multi-value dimension, set the `listDelimiter` in the `parseSpec`.

JSON data can contain multi-value dimensions as well. The multiple values for a dimension must be formatted as a JSON array in the ingested data. No additional `parseSpec` configuration is needed.
