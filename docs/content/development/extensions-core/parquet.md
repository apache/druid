---
layout: doc_page
title: "Apache Parquet Extension"
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
  
# Apache Parquet Extension

This Apache Druid (incubating) module extends [Druid Hadoop based indexing](../../ingestion/hadoop.html) to ingest data directly from offline 
Apache Parquet files. 

Note: `druid-parquet-extensions` depends on the `druid-avro-extensions` module, so be sure to
 [include  both](../../operations/including-extensions.html).

## Parquet Hadoop Parser

This extension provides two ways to parse Parquet files:
* `parquet` - using a simple conversion contained within this extension 
* `parquet-avro` - conversion to avro records with the `parquet-avro` library and using the `druid-avro-extensions`
 module to parse the avro data

Selection of conversion method is controlled by parser type, and the correct hadoop input format must also be set in 
the `ioConfig`,  `org.apache.druid.data.input.parquet.DruidParquetInputFormat` for `parquet` and 
`org.apache.druid.data.input.parquet.DruidParquetAvroInputFormat` for `parquet-avro`.
 

Both parse options support auto field discovery and flattening if provided with a 
[flattenSpec](../../ingestion/flatten-json.html) with `parquet` or `avro` as the `format`. Parquet nested list and map 
[logical types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md) _should_ operate correctly with 
json path expressions for all supported types. `parquet-avro` sets a hadoop job property 
`parquet.avro.add-list-element-records` to `false` (which normally defaults to `true`), in order to 'unwrap' primitive 
list elements into multi-value dimensions.

The `parquet` parser supports `int96` Parquet values, while `parquet-avro` does not. There may also be some subtle 
differences in the behavior of json path expression evaluation of `flattenSpec`.

We suggest using `parquet` over `parquet-avro` to allow ingesting data beyond the schema constraints of Avro conversion. 
However, `parquet-avro` was the original basis for this extension, and as such it is a bit more mature.


|Field     | Type        | Description                                                                            | Required|
|----------|-------------|----------------------------------------------------------------------------------------|---------|
| type      | String      | Choose `parquet` or `parquet-avro` to determine how Parquet files are parsed | yes |
| parseSpec | JSON Object | Specifies the timestamp and dimensions of the data, and optionally, a flatten spec. Valid parseSpec formats are `timeAndDims`, `parquet`, `avro` (if used with avro conversion). | yes |
| binaryAsString | Boolean | Specifies if the bytes parquet column which is not logically marked as a string or enum type should be converted to strings anyway. | no(default == false) |

When the time dimension is a [DateType column](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md), a format should not be supplied. When the format is UTF8 (String), either `auto` or a explicitly defined [format](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html) is required.

### Examples

#### `parquet` parser, `parquet` parseSpec
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

#### `parquet` parser, `timeAndDims` parseSpec
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
#### `parquet-avro` parser, `avro` parseSpec
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

For additional details see [hadoop ingestion](../../ingestion/hadoop.html) and [general ingestion spec](../../ingestion/ingestion-spec.html) documentation.