---
layout: doc_page
---
Firehoses describe the data stream source. They are pluggable and thus the configuration schema can and will vary based on the `type` of the firehose.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|Specifies the type of firehose. Each value will have its own configuration schema, firehoses packaged with Druid are described [here](https://github.com/metamx/druid/wiki/Firehose#available-firehoses)|yes|

We describe the configuration of the Kafka firehose from the example below, but check [here](https://github.com/metamx/druid/wiki/Firehose#available-firehoses) for more information about the various firehoses that are available in Druid.

-   `consumerProps` is a map of properties for the Kafka consumer. The JSON object is converted into a Properties object and passed along to the Kafka consumer.
-   `feed` is the feed that the Kafka consumer should read from.
-   `parser` represents a parser that knows how to convert from String representations into the required `InputRow` representation that Druid uses. This is a potentially reusable piece that can be found in many of the firehoses that are based on text streams. The spec in the example describes a JSON feed (new-line delimited objects), with a timestamp column called "timestamp" in ISO8601 format and that it should not include the dimension "value" when processing. More information about the options available for the parser are available [here](https://github.com/metamx/druid/wiki/Firehose#parsing-data).

Available Firehoses
-------------------

There are several firehoses readily available in Druid, some are meant for examples, others can be used directly in a production environment.

#### KafkaFirehose

This firehose acts as a Kafka consumer and ingests data from Kafka.

#### StaticS3Firehose

This firehose ingests events from a predefined list of S3 objects.

#### TwitterSpritzerFirehose

See [Examples](Examples.html). This firehose connects directly to the twitter spritzer data stream.

#### RandomFirehose

See [Examples](Examples.html). This firehose creates a stream of random numbers.

#### RabbitMqFirehouse

This firehose ingests events from a define rabbit-mq queue.

Parsing Data
------------

There are several ways to parse data.

#### StringInputRowParser

This parser converts Strings.

#### MapInputRowParser

This parser converts flat, key/value pair maps.
