---
layout: doc_page
---

# Thrift

This extension enables Druid to ingest and understand the Apache Thrift data format. Make sure to [include](../../operations/including-extensions.html) `druid-thrift-extensions` as an extension.

Notice that for both stream and hadoop parser explained below, the thrift data class file should be included in classpath.

### Thrift Stream Parser

This is for streaming/realtime ingestion.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `thrift_stream`. | yes |
| parseSpec | JSON Object | Specifies the timestamp and dimensions of the data. Should be a timeAndDims parseSpec. | yes |
| tClassName | String | Specifies the class name of thrift object type. | yes |

For example, using Thrift stream parser with schema repo Thrift bytes decoder:

```json
"parser" : {
  "type" : "thrift_stream",
  "tClassName" : "${YOUR_THRIFT_CLASS_NAME}",
  "parseSpec" : {
    "format": "timeAndDims",
    "timestampSpec": <standard timestampSpec>,
    "dimensionsSpec": <standard dimensionsSpec>
  }
}
```

### Thrift Hadoop Parser

This is for batch ingestion using the HadoopDruidIndexer. The `inputFormat` of `inputSpec` in `ioConfig` can be set to `"com.twitter.elephantbird.mapreduce.input.LzoThriftBlockInputFormat"` or other inputFormat depending on your demand. If using InputFormat from elephant-bird, the `elephantbird.class.for.MultiInputFormat` of `jobProperties` in `tuningConfig` should be set to your thrift class name. Make sure to include "io.druid.extensions:druid-thrift-extensions" as an extension.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `thrift_hadoop`. | yes |
| parseSpec | JSON Object | Specifies the timestamp and dimensions of the data. Should be a timeAndDims parseSpec. | yes |

For example, using Thrift Hadoop parser with custom reader's schema file:

```json
{
  "type" : "index_hadoop",
  "spec" : {
    "dataSchema" : {
      "dataSource" : "",
      "parser" : {
        "type" : "thrift_hadoop",
        "parseSpec" : {
          "format": "timeAndDims",
          "timestampSpec": <standard timestampSpec>,
          "dimensionsSpec": <standard dimensionsSpec>
        }
      }
    },
    "ioConfig" : {
      "type" : "hadoop",
      "inputSpec" : {
        "type" : "static",
        "inputFormat": "com.twitter.elephantbird.mapreduce.input.LzoThriftBlockInputFormat",
        "paths" : ""
      }
    },
    "tuningConfig" : {
       "jobProperties" : {
          "elephantbird.class.for.MultiInputFormat" : "${YOUR_THRIFT_CLASS_NAME}"
      }
    }
  }
}
```
