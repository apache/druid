# Parquet

This extension enables Druid to ingest and understand the Apache Parquet data format offline.

## Parquet Hadoop Parser

This is for batch ingestion using the HadoopDruidIndexer. The inputFormat of inputSpec in ioConfig must be set to `"io.druid.data.input.parquet.DruidParquetInputFormat"`. Make sure also to include "io.druid.extensions:druid-avro-extensions" as an extension.

Field     | Type        | Description                                                                            | Required
----------|-------------|----------------------------------------------------------------------------------------|---------
type      | String      | This should say `parquet`                                                              | yes
parseSpec | JSON Object | Specifies the timestamp and dimensions of the data. Should be a timeAndDims parseSpec. | yes

For example:
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

Almost all the fields listed above are required, including `inputFormat`, `metadataUpdateSpec`(`type`, `connectURI`, `user`, `password`, `segmentTable`). Set `jobProperties` to make hdfs path timezone unrelated.

It is no need to make your cluster to update to SNAPSHOT, you can just fire a hadoop job with your local compiled jars like:

```bash
HADOOP_CLASS_PATH=`hadoop classpath | sed s/*.jar/*/g`

java -Xmx32m -Duser.timezone=UTC -Dfile.encoding=UTF-8 \
  -classpath config/overlord:config/_common:lib/*:$HADOOP_CLASS_PATH:extensions/druid-avro-extensions/*  \
  io.druid.cli.Main index hadoop \
  wikipedia_hadoop_parquet_job.json
```
