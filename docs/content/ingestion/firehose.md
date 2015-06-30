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

#### KafkaFirehose

This firehose acts as a Kafka consumer and ingests data from Kafka.

#### StaticS3Firehose

This firehose ingests events from a predefined list of S3 objects.

#### TwitterSpritzerFirehose

See [Examples](../tutorials/examples.html). This firehose connects directly to the twitter spritzer data stream.

#### RandomFirehose

See [Examples](../tutorials/examples.html). This firehose creates a stream of random numbers.

#### RabbitMqFirehose

This firehose ingests events from a define rabbit-mq queue.
<br>
**Note:** Add **amqp-client-3.2.1.jar** to lib directory of druid to use this firehose.
<br>
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
       "uri": "amqp://mqserver:1234/vhost",
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
     },
     "parser" : {
       "timestampSpec" : { "column" : "utcdt", "format" : "iso" },
       "data" : { "format" : "json" },
       "dimensionExclusions" : ["wp"]
     }
   }
```
|property|description|Default|required?|
|--------|-----------|---------|
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
|baseDir|location of baseDirectory containing files to be ingested. |yes|

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
|type|ingestSegment. Type of firehose|yes|
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
|type|combining|yes|
|delegates|list of firehoses to combine data from|yes|


#### EventReceiverFirehose
EventReceiverFirehoseFactory can be used to ingest events using http endpoint.
when using this firehose `druid.realtime.chathandler.type` needs to be set to `announce` in runtime.properties.

```json
{
  "type": "receiver",
  "serviceName": "eventReceiverServiceName",
  "bufferSize": 10000
}
```
when using above firehose the events can be sent via submitting a POST request to the http endpoint -
`http://<peonHost>:<port>/druid/worker/v1/chat/<eventReceiverServiceName>/push-events/`

|property|description|required?|
|--------|-----------|---------|
|type|receiver|yes|
|serviceName|name used to announce the event receiver service endpoint|yes|
|bufferSize| size of buffer used by firehose to store events|no default(100000)|
