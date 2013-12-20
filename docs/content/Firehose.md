---
layout: doc_page
---

# Druid Firehoses
Firehoses describe the data stream source. They are pluggable and thus the configuration schema can and will vary based on the `type` of the firehose.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | Specifies the type of firehose. Each value will have its own configuration schema, firehoses packaged with Druid are described below. | yes |

We describe the configuration of the [Kafka firehose example](Realtime-ingestion.html#realtime-specfile), but there are other types available in Druid (see below).

-   `consumerProps` is a map of properties for the Kafka consumer. The JSON object is converted into a Properties object and passed along to the Kafka consumer.
-   `feed` is the feed that the Kafka consumer should read from.
-   `parser` represents a parser that knows how to convert from String representations into the required `InputRow` representation that Druid uses. This is a potentially reusable piece that can be found in many of the firehoses that are based on text streams. The spec in the example describes a JSON feed (new-line delimited objects), with a timestamp column called "timestamp" in ISO8601 format and that it should not include the dimension "value" when processing. More information about the options available for the parser are available below.

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
