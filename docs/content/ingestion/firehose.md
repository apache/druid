---
layout: doc_page
---

# Druid Firehoses

Firehoses describe the data stream source. They are pluggable and thus the configuration schema can and will vary based on the `type` of the firehose.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | Specifies the type of firehose. Each value will have its own configuration schema, firehoses packaged with Druid are described below. | yes |

We describe the configuration of the [Kafka firehose example](../ingestion/stream-pull.html#realtime-specfile), but there are other types available in Druid (see below).

-   `consumerProps` is a map of properties for the Kafka consumer. The JSON object is converted into a Properties object and passed along to the Kafka consumer.
-   `feed` is the feed that the Kafka consumer should read from.

## Available Firehoses

There are several firehoses readily available in Druid, some are meant for examples, others can be used directly in a production environment.

#### KafkaEightFirehose

Please note that the [druid-kafka-eight module](../operations/including-extensions.html) is required for this firehose. This firehose acts as a Kafka 0.8.x consumer and ingests data from Kafka.

Sample spec:

```json
"firehose": {
  "type": "kafka-0.8",
  "consumerProps": {
    "zookeeper.connect": "localhost:2181",
    "zookeeper.connection.timeout.ms" : "15000",
    "zookeeper.session.timeout.ms" : "15000",
    "zookeeper.sync.time.ms" : "5000",
    "group.id": "druid-example",
    "fetch.message.max.bytes" : "1048586",
    "auto.offset.reset": "largest",
    "auto.commit.enable": "false"
  },
  "feed": "wikipedia"
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This should be "kafka-0.8"|yes|
|consumerProps|The full list of consumer configs can be [here](https://kafka.apache.org/08/configuration.html).|yes|
|feed|Kafka maintains feeds of messages in categories called topics. This is the topic name.|yes|

#### StaticS3Firehose

This firehose ingests events from a predefined list of S3 objects.

Sample spec:

```json
"firehose" : {
    "type" : "static-s3",
    "uris": ["s3://foo/bar/file.gz", "s3://bar/foo/file2.gz"]
}
```

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be "static-s3"|N/A|yes|
|uris|JSON array of URIs where s3 files to be ingested are located.|N/A|yes|

#### LocalFirehose

This Firehose can be used to read the data from files on local disk.
It can be used for POCs to ingest data on disk.
A sample local firehose spec is shown below:

```json
{
    "type"    : "local",
    "filter"   : "*.csv",
    "baseDir"  : "/data/directory"
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This should be "local".|yes|
|filter|A wildcard filter for files. See [here](http://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/filefilter/WildcardFileFilter.html) for more information.|yes|
|baseDir|directory to search recursively for files to be ingested. |yes|

#### IngestSegmentFirehose

This Firehose can be used to read the data from existing druid segments.
It can be used ingest existing druid segments using a new schema and change the name, dimensions, metrics, rollup, etc. of the segment.
A sample ingest firehose spec is shown below -

```json
{
    "type"    : "ingestSegment",
    "dataSource"   : "wikipedia",
    "interval" : "2013-01-01/2013-01-02"
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This should be "ingestSegment".|yes|
|dataSource|A String defining the data source to fetch rows from, very similar to a table in a relational database|yes|
|interval|A String representing ISO-8601 Interval. This defines the time range to fetch the data over.|yes|
|dimensions|The list of dimensions to select. If left empty, no dimensions are returned. If left null or not defined, all dimensions are returned. |no|
|metrics|The list of metrics to select. If left empty, no metrics are returned. If left null or not defined, all metrics are selected.|no|
|filter| See [Filters](../querying/filters.html)|yes|

#### CombiningFirehose

This firehose can be used to combine and merge data from a list of different firehoses.
This can be used to merge data from more than one firehose.

```json
{
    "type"  :   "combining",
    "delegates" : [ { firehose1 }, { firehose2 }, ..... ]
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This should be "combining"|yes|
|delegates|list of firehoses to combine data from|yes|

#### EventReceiverFirehose

EventReceiverFirehoseFactory can be used to ingest events using an http endpoint.

```json
{
  "type": "receiver",
  "serviceName": "eventReceiverServiceName",
  "bufferSize": 10000
}
```
When using this firehose, events can be sent by submitting a POST request to the http endpoint:

`http://<peonHost>:<port>/druid/worker/v1/chat/<eventReceiverServiceName>/push-events/`

|property|description|required?|
|--------|-----------|---------|
|type|This should be "receiver"|yes|
|serviceName|name used to announce the event receiver service endpoint|yes|
|bufferSize| size of buffer used by firehose to store events|no default(100000)|

#### TimedShutoffFirehose

This can be used to start a firehose that will shut down at a specified time.
An example is shown below:

```json
{
    "type"  :   "timed",
    "shutoffTime": "2015-08-25T01:26:05.119Z",
    "delegate": {
          "type": "receiver",
          "serviceName": "eventReceiverServiceName",
          "bufferSize": 100000
     }
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This should be "timed"|yes|
|shutoffTime|time at which the firehose should shut down, in ISO8601 format|yes|
|delegate|firehose to use|yes|

#### TwitterSpritzerFirehose

This firehose connects directly to the twitter spritzer data stream.

Sample spec:

```json
"firehose" : {
    "type" : "twitzer",
    "maxEventCount": -1,
    "maxRunMinutes": 0
}
```

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be "twitzer"|N/A|yes|
|maxEventCount|max events to receive, -1 is infinite, 0 means nothing is delivered; use this to prevent infinite space consumption or to prevent getting throttled at an inconvenient time.|N/A|yes|
|maxRunMinutes|maximum number of minutes to fetch Twitter events.  Use this to prevent getting throttled at an inconvenient time. If zero or less, no time limit for run.|N/A|yes|
