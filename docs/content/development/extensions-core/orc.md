---
layout: doc_page
title: "Druid ORC Extension"
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
  
# Druid ORC Extension

This module extends [Druid Hadoop based indexing](../../ingestion/hadoop.html) to ingest data directly from offline 
Apache ORC files. 

To use this extension, make sure to [include](../../operations/including-extensions.html) `druid-orc-extensions`.

## ORC Hadoop Parser

The `inputFormat` of `inputSpec` in `ioConfig` must be set to `"org.apache.orc.mapreduce.OrcInputFormat"`.


|Field     | Type        | Description                                                                            | Required|
|----------|-------------|----------------------------------------------------------------------------------------|---------|
|type      | String      | This should say `orc`                                                                  | yes|
|parseSpec | JSON Object | Specifies the timestamp and dimensions of the data. Any parse spec that extends ParseSpec is possible but only their TimestampSpec and DimensionsSpec are used. | yes|

The parser support auto field discovery and flattening if provided with a 
[flattenSpec](../../ingestion/flatten-json.html) with `orc` as the `format`. 
[All column types](https://orc.apache.org/docs/types.html) with the exception of `union` types are supported. Columns of
 `list` type if filled with primitives may be used as a multi-value dimension, or specific elements can be extracted with 
`flattenSpec` expressions. Likewise, primitive fields may be extracted from `map` and `struct` types in the same manner.

### Examples

#### `orc` parser, `orc` parseSpec

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

#### `orc` parser, `timeAndDims` parseSpec
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
