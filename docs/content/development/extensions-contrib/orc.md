---
layout: doc_page
---

# Orc

To use this extension, make sure to [include](../../operations/including-extensions.html) `druid-orc-extensions`.

This extension enables Druid to ingest and understand the Apache Orc data format offline.

## Orc Hadoop Parser

This is for batch ingestion using the HadoopDruidIndexer. The inputFormat of inputSpec in ioConfig must be set to `"org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat"`.

|Field     | Type        | Description                                                                            | Required|
|----------|-------------|----------------------------------------------------------------------------------------|---------|
|type      | String      | This should say `orc`                                                                  | yes|
|parseSpec | JSON Object | Specifies the timestamp and dimensions of the data. Any parse spec that extends ParseSpec is possible but only their TimestampSpec and DimensionsSpec are used. | yes|
|typeString| String      | String representation of Orc struct type info. If not specified, auto constructed from parseSpec but all metric columns are dropped | no|
|mapFieldNameFormat| String | String format for resolving the flatten map fields. Default is `<PARENT>_<CHILD>`. | no |

For example of `typeString`, string column col1 and array of string column col2 is represented by `"struct<col1:string,col2:array<string>>"`.

Currently, it only supports java primitive types, array of java primitive types and map of java primitive types. Thus, compound types 'list' and 'map' in [ORC types](https://orc.apache.org/docs/types.html) are supported. Note that, list of list is not supported, nor map of compound types. For map types, values will be exploded to several columns where column names will be resolved via `mapFieldNameFormat`.

For example of hadoop indexing:

```json
{
  "type": "index_hadoop",
  "spec": {
    "ioConfig": {
      "type": "hadoop",
      "inputSpec": {
        "type": "static",
        "inputFormat": "org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat",
        "paths": "/data/path/in/HDFS/"
      },
      "metadataUpdateSpec": {
        "type": "postgresql",
        "connectURI": "jdbc:postgresql://localhost/druid",
        "user" : "druid",
        "password" : "asdf",
        "segmentTable": "druid_segments"
      },
      "segmentOutputPath": "tmp/segments"
    },
    "dataSchema": {
      "dataSource": "no_metrics",
      "parser": {
        "type": "orc",
        "parseSpec": {
          "format": "timeAndDims",
          "timestampSpec": {
            "column": "time",
            "format": "auto"
          },
          "dimensionsSpec": {
            "dimensions": [
              "name"
            ],
            "dimensionExclusions": [],
            "spatialDimensions": []
          }
        },
        "typeString": "struct<time:string,name:string>",
        "mapFieldNameFormat": "<PARENT>_<CHILD>"
      },
      "metricsSpec": [{
        "type": "count",
        "name": "count"
      }],
      "granularitySpec": {
        "type": "uniform",
        "segmentGranularity": "DAY",
        "queryGranularity": "ALL",
        "intervals": ["2015-12-31/2016-01-02"]
      }
    },
    "tuningConfig": {
      "type": "hadoop",
      "workingPath": "tmp/working_path",
      "partitionsSpec": {
        "targetPartitionSize": 5000000
      },
      "jobProperties" : {},
      "leaveIntermediate": true
    }
  }
}
```

Almost all the fields listed above are required, including `inputFormat`, `metadataUpdateSpec`(`type`, `connectURI`, `user`, `password`, `segmentTable`). Set `jobProperties` to make hdfs path timezone unrelated.
