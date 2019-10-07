---
id: thrift
title: "Thrift"
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


To use this Apache Druid (incubating) extension, make sure to [include](../../development/extensions.md#loading-extensions) `druid-thrift-extensions`.

This extension enables Druid to ingest thrift compact data online (`ByteBuffer`) and offline (SequenceFile of type `<Writable, BytesWritable>` or LzoThriftBlock File).

You may want to use another version of thrift, change the dependency in pom and compile yourself.

## LZO Support

If you plan to read LZO-compressed Thrift files, you will need to download version 0.4.19 of the [hadoop-lzo JAR](https://mvnrepository.com/artifact/com.hadoop.gplcompression/hadoop-lzo/0.4.19) and place it in your `extensions/druid-thrift-extensions` directory.

## Thrift Parser


| Field       | Type        | Description                              | Required |
| ----------- | ----------- | ---------------------------------------- | -------- |
| type        | String      | This should say `thrift`                 | yes      |
| parseSpec   | JSON Object | Specifies the timestamp and dimensions of the data. Should be a JSON parseSpec. | yes      |
| thriftJar   | String      | path of thrift jar, if not provided, it will try to find the thrift class in classpath. Thrift jar in batch ingestion should be uploaded to HDFS first and configure `jobProperties` with `"tmpjars":"/path/to/your/thrift.jar"` | no       |
| thriftClass | String      | classname of thrift                      | yes      |

- Realtime Ingestion (tranquility example)

```json
{
  "dataSources": [{
    "spec": {
      "dataSchema": {
        "dataSource": "book",
        "granularitySpec": {          },
        "parser": {
          "type": "thrift",
          "thriftClass": "org.apache.druid.data.input.thrift.Book",
          "protocol": "compact",
          "parseSpec": {
            "format": "json",
            ...
          }
        },
        "metricsSpec": [...]
      },
      "tuningConfig": {...}
    },
    "properties": {...}
  }],
  "properties": {...}
}
```

To use it with tranquility,

```bash
bin/tranquility kafka \
  -configFile $jsonConfig \
  -Ddruid.extensions.directory=/path/to/extensions \
  -Ddruid.extensions.loadList='["druid-thrift-extensions"]'
```

Hadoop-client is also needed, you may copy all the hadoop-client dependency jars into directory `druid-thrift-extensions` to make is simple.


- Batch Ingestion - `inputFormat` and `tmpjars` should be set.

This is for batch ingestion using the HadoopDruidIndexer. The inputFormat of inputSpec in ioConfig could be one of `"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat"` and `com.twitter.elephantbird.mapreduce.input.LzoThriftBlockInputFormat`. Be careful, when `LzoThriftBlockInputFormat` is used, thrift class must be provided twice.

```json
{
  "type": "index_hadoop",
  "spec": {
    "dataSchema": {
      "dataSource": "book",
      "parser": {
        "type": "thrift",
        "jarPath": "book.jar",
        "thriftClass": "org.apache.druid.data.input.thrift.Book",
        "protocol": "compact",
        "parseSpec": {
          "format": "json",
          ...
        }
      },
      "metricsSpec": [],
      "granularitySpec": {}
    },
    "ioConfig": {
      "type": "hadoop",
      "inputSpec": {
        "type": "static",
        "inputFormat": "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
        // "inputFormat": "com.twitter.elephantbird.mapreduce.input.LzoThriftBlockInputFormat",
        "paths": "/user/to/some/book.seq"
      }
    },
    "tuningConfig": {
      "type": "hadoop",
      "jobProperties": {
        "tmpjars":"/user/h_user_profile/du00/druid/test/book.jar",
        // "elephantbird.class.for.MultiInputFormat" : "${YOUR_THRIFT_CLASS_NAME}"
      }
    }
  }
}
```
