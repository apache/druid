---
layout: doc_page
---

# Ingestion using Parquet format

To use this extension, make sure to [include](../../operations/including-extensions.html) both `druid-avro-extensions` and `druid-parquet-extensions`.

This extension enables Druid to ingest and understand the Apache Parquet data format offline.

## Parquet Hadoop Parser

This is for batch ingestion using the HadoopDruidIndexer. The inputFormat of `inputSpec` in `ioConfig` must be set to `"io.druid.data.input.parquet.DruidParquetInputFormat"`.

|Field     | Type        | Description                                                                            | Required|
|----------|-------------|----------------------------------------------------------------------------------------|---------|
| type      | String      | This should say `parquet`                                                              | yes |
| parseSpec | JSON Object | Specifies the timestamp and dimensions of the data. Should be a timeAndDims parseSpec. | yes |
| binaryAsString | Boolean | Specifies if the bytes parquet column should be converted to strings. | no(default == false) |

When the time dimension is a [DateType column](https://github.com/Parquet/parquet-format/blob/master/LogicalTypes.md), a format should not be supplied. When the format is UTF8 (String), either `auto` or a explicitly defined [format](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html) is required.

### Example json for overlord

When posting the index job to the overlord, setting the correct `inputFormat` is required to switch to parquet ingestion. Make sure to set `jobProperties` to make hdfs path timezone unrelated:

```json
{
  "type": "index_hadoop",
  "spec": {
    "ioConfig": {
      "type": "hadoop",
      "inputSpec": {
        "type": "static",
        "inputFormat": "io.druid.data.input.parquet.DruidParquetInputFormat",
        "paths": "no_metrics"
      }
    },
    "dataSchema": {
      "dataSource": "no_metrics",
      "parser": {
        "type": "parquet",
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
        }
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
      "partitionsSpec": {
        "targetPartitionSize": 5000000
      },
      "jobProperties" : {},
      "leaveIntermediate": true
    }
  }
}
```

### Example json for standalone jvm
When using a standalone JVM instead, additional configuration fields are required. You can just fire a hadoop job with your local compiled jars like:

```bash
HADOOP_CLASS_PATH=`hadoop classpath | sed s/*.jar/*/g`

java -Xmx32m -Duser.timezone=UTC -Dfile.encoding=UTF-8 \
  -classpath config/overlord:config/_common:lib/*:$HADOOP_CLASS_PATH:extensions/druid-avro-extensions/*  \
  io.druid.cli.Main index hadoop \
  wikipedia_hadoop_parquet_job.json
```

An example index json when using the standalone JVM:

```json
{
  "type": "index_hadoop",
  "spec": {
    "ioConfig": {
      "type": "hadoop",
      "inputSpec": {
        "type": "static",
        "inputFormat": "io.druid.data.input.parquet.DruidParquetInputFormat",
        "paths": "no_metrics"
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
        "type": "parquet",
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
        }
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

Almost all the fields listed above are required, including `inputFormat`, `metadataUpdateSpec`(`type`, `connectURI`, `user`, `password`, `segmentTable`).
