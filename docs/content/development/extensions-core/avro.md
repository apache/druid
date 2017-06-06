---
layout: doc_page
---

# Avro

This extension enables Druid to ingest and understand the Apache Avro data format. Make sure to [include](../../operations/including-extensions.html) `druid-avro-extensions` as an extension.

### Avro Stream Parser

This is for streaming/realtime ingestion.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `avro_stream`. | no |
| avroBytesDecoder | JSON Object | Specifies how to decode bytes to Avro record. | yes |
| parseSpec | JSON Object | Specifies the timestamp and dimensions of the data. Should be a timeAndDims parseSpec. | yes |

For example, using Avro stream parser with schema repo Avro bytes decoder:

```json
"parser" : {
  "type" : "avro_stream",
  "avroBytesDecoder" : {
    "type" : "schema_repo",
    "subjectAndIdConverter" : {
      "type" : "avro_1124",
      "topic" : "${YOUR_TOPIC}"
    },
    "schemaRepository" : {
      "type" : "avro_1124_rest_client",
      "url" : "${YOUR_SCHEMA_REPO_END_POINT}",
    }
  },
  "parseSpec" : {
    "format": "timeAndDims",
    "timestampSpec": <standard timestampSpec>,
    "dimensionsSpec": <standard dimensionsSpec>
  }
}
```

#### Avro Bytes Decoder

If `type` is not included, the avroBytesDecoder defaults to `schema_repo`.

##### Inline Schema Based Avro Bytes Decoder

This decoder can be used if all the input events can be read using the same schema. In that case schema can be specified in the input task json itself as described below.

```
...
"avroBytesDecoder": {
  "type": "schema_inline",
  "schema": {
    //your schema goes here, for example
    "namespace": "io.druid.data",
    "name": "User",
    "type": "record",
    "fields": [
      { "name": "FullName", "type": "string" },
      { "name": "Country", "type": "string" }
    ]
  }
}
...
```

##### Multiple Inline Schemas Based Avro Bytes Decoder

This decoder can be used if different input events can have different read schema. In that case schema can be specified in the input task json itself as described below.

```
...
"avroBytesDecoder": {
  "type": "multiple_schemas_inline",
  "schemas": {
    //your id -> schema map goes here, for example
    "1": {
      "namespace": "io.druid.data",
      "name": "User",
      "type": "record",
      "fields": [
        { "name": "FullName", "type": "string" },
        { "name": "Country", "type": "string" }
      ]
    },
    "2": {
      "namespace": "io.druid.otherdata",
      "name": "UserIdentity",
      "type": "record",
      "fields": [
        { "name": "Name", "type": "string" },
        { "name": "Location", "type": "string" }
      ]
    },
    ...
    ...
  }
}
...
```

Note that it is essentially a map of integer schema ID to avro schema object. This parser assumes that record has following format.
  first 1 byte is version and must always be 1.
  next 4 bytes are integer schema ID serialized using big-endian byte order.
  remaining bytes contain serialized avro message.

##### SchemaRepo Based Avro Bytes Decoder

This Avro bytes decoder first extract `subject` and `id` from input message bytes, then use them to lookup the Avro schema with which to decode Avro record from bytes. Details can be found in [schema repo](https://github.com/schema-repo/schema-repo) and [AVRO-1124](https://issues.apache.org/jira/browse/AVRO-1124). You will need an http service like schema repo to hold the avro schema. Towards schema registration on the message producer side, you can refer to `io.druid.data.input.AvroStreamInputRowParserTest#testParse()`.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `schema_repo`. | no |
| subjectAndIdConverter | JSON Object | Specifies the how to extract subject and id from message bytes. | yes |
| schemaRepository | JSON Object | Specifies the how to lookup Avro schema from subject and id. | yes |

##### Avro-1124 Subject And Id Converter

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `avro_1124`. | no |
| topic | String | Specifies the topic of your kafka stream. | yes |


##### Avro-1124 Schema Repository

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `avro_1124_rest_client`. | no |
| url | String | Specifies the endpoint url of your Avro-1124 schema repository. | yes |

##### Confluent's Schema Registry

This Avro bytes decoder first extract unique `id` from input message bytes, then use them it lookup in the Schema Registry for the related schema, with which to decode Avro record from bytes.
Details can be found in Schema Registry [documentation](http://docs.confluent.io/current/schema-registry/docs/) and [repository](https://github.com/confluentinc/schema-registry).

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `schema_registry`. | no |
| url | String | Specifies the url endpoint of the Schema Registry. | yes |
| capacity | Integer | Specifies the max size of the cache (default == Integer.MAX_VALUE). | no |


### Avro Hadoop Parser

This is for batch ingestion using the HadoopDruidIndexer. The `inputFormat` of `inputSpec` in `ioConfig` must be set to `"io.druid.data.input.avro.AvroValueInputFormat"`. You may want to set Avro reader's schema in `jobProperties` in `tuningConfig`, eg: `"avro.schema.input.value.path": "/path/to/your/schema.avsc"` or `"avro.schema.input.value": "your_schema_JSON_object"`, if reader's schema is not set, the schema in Avro object container file will be used, see [Avro specification](http://avro.apache.org/docs/1.7.7/spec.html#Schema+Resolution). Make sure to include "io.druid.extensions:druid-avro-extensions" as an extension.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | This should say `avro_hadoop`. | no |
| parseSpec | JSON Object | Specifies the timestamp and dimensions of the data. Should be a timeAndDims parseSpec. | yes |
| fromPigAvroStorage | Boolean | Specifies whether the data file is stored using AvroStorage. | no(default == false) |

For example, using Avro Hadoop parser with custom reader's schema file:

```json
{
  "type" : "index_hadoop",  
  "spec" : {
    "dataSchema" : {
      "dataSource" : "",
      "parser" : {
        "type" : "avro_hadoop",
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
        "inputFormat": "io.druid.data.input.avro.AvroValueInputFormat",
        "paths" : ""
      }
    },
    "tuningConfig" : {
       "jobProperties" : {
          "avro.schema.input.value.path" : "/path/to/my/schema.avsc"
      }
    }
  }
}
```
