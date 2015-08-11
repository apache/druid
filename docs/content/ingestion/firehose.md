---
layout: doc_page
---

# Druid Firehoses
Firehoses describe the data stream source. They are pluggable and thus the configuration schema can and will vary based on the `type` of the firehose.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | Specifies the type of firehose. Each value will have its own configuration schema, firehoses packaged with Druid are described below. | yes |

We describe the configuration of the [Kafka firehose example](realtime-ingestion.html#realtime-specfile), but there are other types available in Druid (see below).

-   `consumerProps` is a map of properties for the Kafka consumer. The JSON object is converted into a Properties object and passed along to the Kafka consumer.
-   `feed` is the feed that the Kafka consumer should read from.

Available Firehoses
-------------------

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

#### StaticAzureBlobStoreFirehose

This firehose ingests events, similar to the StaticS3Firehose, but from an Azure Blob Store.

Data is newline delimited, with one JSON object per line and parsed as per the `InputRowParser` configuration.

The storage account is shared with the one used for Azure deep storage functionality, but blobs can be in a different container.

As with the S3 blobstore, it is assumed to be gzipped if the extension ends in .gz

Sample spec:

```json
"firehose" : {
    "type" : "static-azure-blobstore",
    "blobs": [
        {
          "container": "container",
          "path": "/path/to/your/file.json"
        },
        {
          "container": "anothercontainer",
          "path": "/another/path.json"
        }
    ]
}
```

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be "static-azure-blobstore".|N/A|yes|
|blobs|JSON array of [Azure blobs](https://msdn.microsoft.com/en-us/library/azure/ee691964.aspx).|N/A|yes|

Azure Blobs:

|property|description|default|required?|
|--------|-----------|-------|---------|
|container|Name of the azure [container](https://azure.microsoft.com/en-us/documentation/articles/storage-dotnet-how-to-use-blobs/#create-a-container)|N/A|yes|
|path|The path where data is located.|N/A|yes|

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

#### RabbitMQFirehose

This firehose ingests events from a define rabbit-mq queue.

**Note:** Add **amqp-client-3.2.1.jar** to lib directory of druid to use this firehose.

A sample spec for rabbitmq firehose:

```json
"firehose" : {
   "type" : "rabbitmq",
   "connection" : {
     "host": "localhost",
     "port": "5672",
     "username": "test-dude",
     "password": "test-word",
     "virtualHost": "test-vhost",
     "uri": "amqp://mqserver:1234/vhost"
   },
   "config" : {
     "exchange": "test-exchange",
     "queue" : "druidtest",
     "routingKey": "#",
     "durable": "true",
     "exclusive": "false",
     "autoDelete": "false",
     "maxRetries": "10",
     "retryIntervalSeconds": "1",
     "maxDurationSeconds": "300" 
   }
}
```

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be "rabbitmq"|N/A|yes|
|host|The hostname of the RabbitMQ broker to connect to|localhost|no|
|port|The port number to connect to on the RabbitMQ broker|5672|no|
|username|The username to use to connect to RabbitMQ|guest|no|
|password|The password to use to connect to RabbitMQ|guest|no|
|virtualHost|The virtual host to connect to|/|no|
|uri|The URI string to use to connect to RabbitMQ| |no|
|exchange|The exchange to connect to| |yes|
|queue|The queue to connect to or create| |yes|
|routingKey|The routing key to use to bind the queue to the exchange| |yes|
|durable|Whether the queue should be durable|false|no|
|exclusive|Whether the queue should be exclusive|false|no|
|autoDelete|Whether the queue should auto-delete on disconnect|false|no|
|maxRetries|The max number of reconnection retry attempts| |yes|
|retryIntervalSeconds|The reconnection interval| |yes|
|maxDurationSeconds|The max duration of trying to reconnect| |yes|

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
This can be used to merge data from more than one firehoses.

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
