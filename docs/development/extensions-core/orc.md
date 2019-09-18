---
id: orc
title: "ORC Extension"
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


This Apache Druid (incubating) module extends [Druid Hadoop based indexing](../../ingestion/hadoop.md) to ingest data directly from offline
Apache ORC files.

To use this extension, make sure to [include](../../development/extensions.md#loading-extensions) `druid-orc-extensions`.

## ORC Hadoop Parser

The `inputFormat` of `inputSpec` in `ioConfig` must be set to `"org.apache.orc.mapreduce.OrcInputFormat"`.


|Field     | Type        | Description                                                                            | Required|
|----------|-------------|----------------------------------------------------------------------------------------|---------|
|type      | String      | This should say `orc`                                                                  | yes|
|parseSpec | JSON Object | Specifies the timestamp and dimensions of the data (`timeAndDims` and `orc` format) and a `flattenSpec` (`orc` format) | yes|

The parser supports two `parseSpec` formats: `orc` and `timeAndDims`.

`orc` supports auto field discovery and flattening, if specified with a [`flattenSpec`](../../ingestion/index.md#flattenspec).
If no `flattenSpec` is specified, `useFieldDiscovery` will be enabled by default. Specifying a `dimensionSpec` is
optional if `useFieldDiscovery` is enabled: if a `dimensionSpec` is supplied, the list of `dimensions` it defines will be
the set of ingested dimensions, if missing the discovered fields will make up the list.

`timeAndDims` parse spec must specify which fields will be extracted as dimensions through the `dimensionSpec`.

[All column types](https://orc.apache.org/docs/types.html) are supported, with the exception of `union` types. Columns of
 `list` type, if filled with primitives, may be used as a multi-value dimension, or specific elements can be extracted with
`flattenSpec` expressions. Likewise, primitive fields may be extracted from `map` and `struct` types in the same manner.
Auto field discovery will automatically create a string dimension for every (non-timestamp) primitive or `list` of
primitives, as well as any flatten expressions defined in the `flattenSpec`.

### Hadoop job properties
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

### Examples

#### `orc` parser, `orc` parseSpec, auto field discovery, flatten expressions

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

#### `orc` parser, `orc` parseSpec, field discovery with no flattenSpec or dimensionSpec

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

#### `orc` parser, `orc` parseSpec, no autodiscovery

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

### Migration from 'contrib' extension
This extension, first available in version 0.15.0, replaces the previous 'contrib' extension which was available until
0.14.0-incubating. While this extension can index any data the 'contrib' extension could, the JSON spec for the
ingestion task is *incompatible*, and will need modified to work with the newer 'core' extension.

To migrate to 0.15.0+:

* In `inputSpec` of `ioConfig`, `inputFormat` must be changed from `"org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat"` to
`"org.apache.orc.mapreduce.OrcInputFormat"`
* The 'contrib' extension supported a `typeString` property, which provided the schema of the
ORC file, of which was essentially required to have the types correct, but notably _not_ the column names, which
facilitated column renaming. In the 'core' extension, column renaming can be achieved with
[`flattenSpec`](../../ingestion/index.md#flattenspec). For example, `"typeString":"struct<time:string,name:string>"`
with the actual schema `struct<_col0:string,_col1:string>`, to preserve Druid schema would need replaced with:

```json
"flattenSpec": {
  "fields": [
    {
      "type": "path",
      "name": "time",
      "expr": "$._col0"
    },
    {
      "type": "path",
      "name": "name",
      "expr": "$._col1"
    }
  ]
  ...
}
```

* The 'contrib' extension supported a `mapFieldNameFormat` property, which provided a way to specify a dimension to
 flatten `OrcMap` columns with primitive types. This functionality has also been replaced with
 [`flattenSpec`](../../ingestion/index.md#flattenspec). For example: `"mapFieldNameFormat": "<PARENT>_<CHILD>"`
 for a dimension `nestedData_dim1`, to preserve Druid schema could be replaced with

 ```json
"flattenSpec": {
  "fields": [
    {
      "type": "path",
      "name": "nestedData_dim1",
      "expr": "$.nestedData.dim1"
    }
  ]
  ...
}
```
