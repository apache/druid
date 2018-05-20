[![Build Status](https://travis-ci.org/druid-io/druid.svg?branch=master)](https://travis-ci.org/druid-io/druid) [![Inspections Status](https://img.shields.io/teamcity/http/teamcity.jetbrains.com/s/OpenSourceProjects_Druid_Inspections.svg?label=TeamCity%20inspections)](https://teamcity.jetbrains.com/viewType.html?buildTypeId=OpenSourceProjects_Druid_Inspections) [![Coverage Status](https://coveralls.io/repos/druid-io/druid/badge.svg?branch=master)](https://coveralls.io/r/druid-io/druid?branch=master)

## Druid - Parquet extension

#### Ingestion using Parquet format
To use this extension, make sure to include both `druid-avro-extensions` and `druid-parquet-extensions`.

This extension enables Druid to ingest and understand the Apache Parquet data format offline.

#### Parquet Hadoop Parser
This is for batch ingestion using the HadoopDruidIndexer. The inputFormat of inputSpec in ioConfig must be set to `"io.druid.data.input.parquet.DruidParquetInputFormat"`.

| Field	| Type       | Description           | Required  |
| ------|:-------    |:-------------:        | :-----|
| type |String          | This should say `parquet`        | yes |
| parseSpec |JSON Object          | Specifies the timestamp and dimensions of the data. Should be a timeAndDims parseSpec.              |   yes |
| binaryAsString |Boolean     |  Specifies if the bytes parquet column should be converted to strings.  |    no (default == false) |
| parquetParser| JSON Object | Specifies field definitions on how a specific parquet column has to be parsed, Should be a `ParquetParser` spec | no (default == false) |

When the time dimension is a DateType column, a format should not be supplied. When the format is UTF8 (String), either auto or a explicitly defined format is required.

#### Parquet parser
A sample `parquetParser` spec looks line the one below

*Spec with parquet parser*
```json
{
  "type": "index_hadoop",
  "spec": {
    "dataSchema": {
      "dataSource": "test_datasource",
      "parser": {
        "type": "parquet",
        "binaryAsString": true,
        "parquetParser": {
          "depth": 5,
          "fields": [
            {
              "key": "eventId",
              "fieldType": "STRING"
            },
            {
              "key": "eventTimestamp",
              "fieldType": "LONG"
            },
            {
              "rootFieldName": "attributes",
              "key": "geo",
              "fieldType": "MAP",
              "dimensionName": "valid_map_access"
            },
            {
              "key": "referenceIds",
              "fieldType": "LIST",
              "index": 1,
              "dimensionName": "list_access_with_index"
            },
            {
              "key": "referenceIds",
              "fieldType": "LIST",
              "dimensionName": "multi_value_dimension"
            },
            {
              "key": "field1",
              "fieldType": "GENERIC_RECORD",
              "field": {
                "key": "field2",
                "fieldType": "GENERIC_RECORD"
              },
              "dimensionName": "generic_record_nested"
            },
            {
              "key": "union_with_map",
              "fieldType": "UNION",
              "field": {
                "key": "map_key",
                "rootFieldName": "root_map_key",
                "fieldType": "MAP"
              },
              "dimensionName": "union_with_map_dimension"
            },
            {
              "key": "e2",
              "fieldType": "STRUCT",
              "field": {
                "key": "e3",
                "fieldType": "STRUCT",
                "index": 1,
                "field": {
                  "key": "e4",
                  "fieldType": "STRUCT",
                  "field": {
                    "key": "e5",
                    "fieldType": "STRUCT",
                    "field": {
                      "key": "id",
                      "fieldType": "STRUCT"
                    }
                  }
                }
              },
              "dimensionName": "nested_struct"
            },
            {
              "key": "union_field",
              "fieldType": "UNION"
            },
            {
              "key": "tags",
              "fieldType": "LIST",
              "field": {
                "key": "tagId",
                "fieldType": "STRUCT"
              },
              "dimensionName": "tag_ids"
            },
            {
              "key": "tags",
              "fieldType": "LIST",
              "field": {
                "key": "tagCostAttributes",
                "fieldType": "STRUCT",
                "field": {
                  "key": "CostA",
                  "fieldType": "MAP",
                  "rootFieldName": "tagCostAttributes"
                }
              },
              "dimensionName": "cost_a"
            },
            {
              "key": "tags",
              "fieldType": "LIST",
              "index": 0,
              "field": {
                "key": "CostB",
                "fieldType": "MAP",
                "rootFieldName": "tagCostAttributes"
              },
              "dimensionName": "cost_b_1st_tag"
            },
            {
              "key": "tags",
              "fieldType": "LIST",
              "index": 1,
              "field": {
                "key": "CostB",
                "fieldType": "MAP",
                "rootFieldName": "tagCostAttributes"
              },
              "dimensionName": "cost_b_2nd_tag"
            }
          ]
        },
        "parseSpec": {
          "format": "timeAndDims",
          "timestampSpec": {
            "column": "eventTimestamp",
            "format": "millis"
          },
          "dimensionsSpec": {
            "dimensions": [
              "eventId",
              "valid_map_access",
              "list_access_with_index",
              "multi_value_dimension",
              "generic_record_nested",
              "union_with_map_dimension",
              "nested_struct",
              "union_field",
              "tag_ids",
              "cost_a",
              "cost_b_1st_tag"
            ],
            "dimensionExclusions": [
            ],
            "spatialDimensions": [
            ]
          }
        }
      },
      "metricsSpec": [
        {
          "type": "count",
          "name": "count"
        },
        {
          "type": "doubleSum",
          "name": "cost_b_2nd_tag_sum",
          "fieldName": "cost_b_2nd_tag"
        }
      ],
      "granularitySpec": {
        "type": "uniform",
        "segmentGranularity": "DAY",
        "queryGranularity": "DAY",
        "rollup": true,
        "intervals": [
          "2017-01-26T00:00:00.000Z/2017-01-27T00:59:59.000Z"
        ]
      }
    },
    "ioConfig": {
      "type": "hadoop",
      "inputSpec": {
        "type": "static",
        "inputFormat": "io.druid.data.input.parquet.DruidParquetInputFormat",
        "paths": "example/events/events_with_all_datatypes.parquet"
      },
      "segmentOutputPath": "/tmp/segments"
    },
    "tuningConfig": {
      "type": "hadoop",
      "workingPath": "tmp/working_path",
      "version": "2017-02-09T07:36:09.249Z",
      "partitionsSpec": {
        "type": "hashed",
        "targetPartitionSize": 750000,
        "maxPartitionSize": 750000,
        "assumeGrouped": false,
        "numShards": -1,
        "partitionDimensions": [
        ]
      },
      "shardSpecs": {
      },
      "indexSpec": {
        "bitmap": {
          "type": "concise"
        },
        "dimensionCompression": "lz4",
        "metricCompression": "lz4",
        "longEncoding": "longs"
      },
      "maxRowsInMemory": 75000,
      "leaveIntermediate": true,
      "cleanupOnFailure": true,
      "overwriteFiles": false,
      "ignoreInvalidRows": false,
      "jobProperties": {
        "hdp.version": "2.5.3.58-3",
        "mapreduce.job.user.classpath.first": "true",
        "mapreduce.task.timeout": "1800000",
        "mapreduce.map.memory.mb": "2048",
        "mapreduce.map.java.opts": "-server -Xmx3072m -Duser.timezone=UTC -Dfile.encoding=UTF-8",
        "mapreduce.reduce.memory.mb": "2048",
        "mapreduce.reduce.java.opts": "-server -Xmx8192m -Duser.timezone=UTC -Dfile.encoding=UTF-8",
        "mapreduce.job.queuename": "default",
        "mapred.child.ulimit": "16777216",
        "mapreduce.map.output.compress": "true",
        "mapred.map.output.compress.codec": "org.apache.hadoop.io.compress.GzipCodec"
      },
      "combineText": false,
      "useCombiner": false,
      "buildV9Directly": false,
      "numBackgroundPersistThreads": 0,
      "forceExtendableShardSpecs": false
    },
    "uniqueId": "715f31321096480b869aca2cc9c5d252"
  },
  "hadoopDependencyCoordinates": [
    "org.apache.hadoop:hadoop-client:2.6.0"
  ]
}
```

 ##### ***Field***

| Field	| Type       | Description           | Required  |
| ------|:-------    |:-------------:        | :-----|
| **key**|String | Column name of the field to be extracted | true |
| **fieldType** | String [mapping is mentioned below] | Type of the field | true |
|rootFieldName|String|This attribute is used while extracting a dimension or metric from a map|optional (`true` when the field is a map else none )|
|index|Integer|Used while referencing a list by its index position|no|
|dimensionName|String|For overriding the default name else the value of  `key` is used as dimension or metric name|no|
|depth|Integer| Maximum depth till which the nested fields will be traversed, default depth is 3|no (default == 3)|
|field|Field|Used while extracting nested datatypes|no|

 ##### ***FieldType***
 
> Field type is mapped to the corresponding ENUM values ***``` STRING, MAP, INT, LONG, UNION, STRUCT,UTF8, LIST, GENERIC_RECORD, BYTE_BUFFER, GENERIC_DATA_RECORD```*** 

```text
Dimensions, Metrics can be used in conjunction with both the parser fields and the columns of parquet file
```
  
*Example json for overlord*
When posting the index job to the overlord, setting the correct inputFormat is required to switch to parquet ingestion. Make sure to set jobProperties to make hdfs path timezone unrelated:

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

#### Example json for standalone jvm
When using a standalone JVM instead, additional configuration fields are required. You can just fire a hadoop job with your local compiled jars like:

HADOOP_CLASS_PATH=`hadoop classpath | sed s/*.jar/*/g`

```bash
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
Almost all the fields listed above are required, including inputFormat, metadataUpdateSpec(type, connectURI, user, password, segmentTable).