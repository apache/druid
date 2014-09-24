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

#### RabbitMqFirehose

This firehose ingests events from a define rabbit-mq queue.

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
|filter| See [Filters](Filters.html)|yes|






Parsing Data
------------

There are several ways to parse data.

#### StringInputRowParser

This parser converts Strings.

#### MapInputRowParser

This parser converts flat, key/value pair maps.
