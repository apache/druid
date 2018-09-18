# Kinesis Indexing Service

Similar to the [Kafka indexing service](http://druid.io/docs/0.10.0/development/extensions-core/kafka-ingestion.html),
the Kinesis indexing service uses supervisors which run on the overlord and manage the creation and lifetime of Kinesis
indexing tasks. This indexing service can handle non-recent events and provides exactly-once ingestion semantics.

The Kinesis indexing service is provided as the `druid-kinesis-indexing-service` core extension (see
[Including Extensions](http://druid.io/docs/0.10.0/operations/including-extensions.html)). Please note that this is
currently designated as an *experimental feature* and is subject to the usual
[experimental caveats](http://druid.io/docs/0.10.0/development/experimental.html).

## Submitting a Supervisor Spec

The Kinesis indexing service requires that the `druid-kinesis-indexing-service` extension be loaded on both the overlord
and the middle managers. A supervisor for a dataSource is started by submitting a supervisor spec via HTTP POST to
`http://<OVERLORD_IP>:<OVERLORD_PORT>/druid/indexer/v1/supervisor`, for example:

```
curl -X POST -H 'Content-Type: application/json' -d @supervisor-spec.json http://localhost:8090/druid/indexer/v1/supervisor
```

A sample supervisor spec is shown below:

```json
{
  "type": "kinesis",
  "dataSchema": {
    "dataSource": "metrics-kinesis",
    "parser": {
      "type": "string",
      "parseSpec": {
        "format": "json",
        "timestampSpec": {
          "column": "timestamp",
          "format": "auto"
        },
        "dimensionsSpec": {
          "dimensions": [],
          "dimensionExclusions": [
            "timestamp",
            "value"
          ]
        }
      }
    },
    "metricsSpec": [
      {
        "name": "count",
        "type": "count"
      },
      {
        "name": "value_sum",
        "fieldName": "value",
        "type": "doubleSum"
      },
      {
        "name": "value_min",
        "fieldName": "value",
        "type": "doubleMin"
      },
      {
        "name": "value_max",
        "fieldName": "value",
        "type": "doubleMax"
      }
    ],
    "granularitySpec": {
      "type": "uniform",
      "segmentGranularity": "HOUR",
      "queryGranularity": "NONE"
    }
  },
  "tuningConfig": {
    "type": "kinesis",
    "maxRowsPerSegment": 5000000
  },
  "ioConfig": {
    "stream": "metrics",
    "endpoint": "kinesis.us-east-1.amazonaws.com",
    "taskCount": 1,
    "replicas": 1,
    "taskDuration": "PT1H",
    "recordsPerFetch": 2000,
    "fetchDelayMillis": 1000
  }
}
```

## Supervisor Configuration

|Field|Description|Required|
|--------|-----------|---------|
|`type`|The supervisor type, this should always be `kinesis`.|yes|
|`dataSchema`|The schema that will be used by the Kinesis indexing task during ingestion, see [Ingestion Spec](http://druid.io/docs/0.10.0/ingestion/index.html).|yes|
|`tuningConfig`|A KinesisSupervisorTuningConfig to configure the supervisor and indexing tasks, see below.|no|
|`ioConfig`|A KinesisSupervisorIOConfig to configure the supervisor and indexing tasks, see below.|yes|

### KinesisSupervisorTuningConfig

The tuningConfig is optional and default parameters will be used if no tuningConfig is specified.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`|String|The indexing task type, this should always be `kinesis`.|yes|
|`maxRowsInMemory`|Integer|The number of rows to aggregate before persisting. This number is the post-aggregation rows, so it is not equivalent to the number of input events, but the number of aggregated rows that those events result in. This is used to manage the required JVM heap size. Maximum heap memory usage for indexing scales with maxRowsInMemory * (2 + maxPendingPersists).|no (default == 75000)|
|`maxRowsPerSegment`|Integer|The number of rows to aggregate into a segment; this number is post-aggregation rows.|no (default == 5000000)|
|`intermediatePersistPeriod`|ISO8601 Period|The period that determines the rate at which intermediate persists occur.|no (default == PT10M)|
|`maxPendingPersists`|Integer|Maximum number of persists that can be pending but not started. If this limit would be exceeded by a new intermediate persist, ingestion will block until the currently-running persist finishes. Maximum heap memory usage for indexing scales with maxRowsInMemory * (2 + maxPendingPersists).|no (default == 0, meaning one persist can be running concurrently with ingestion, and none can be queued up)|
|`indexSpec`|Object|Tune how data is indexed, see 'IndexSpec' [here](http://druid.io/docs/0.10.0/development/extensions-core/kafka-ingestion.html#indexspec).|no|
|`buildV9Directly`|Boolean|Whether to build a v9 index directly instead of first building a v8 index and then converting it to v9 format.|no (default == true)|
|`reportParseExceptions`|Boolean|If true, exceptions encountered during parsing will be thrown and will halt ingestion; if false, unparseable rows and fields will be skipped.|no (default == false)|
|`workerThreads`|Integer|The number of threads that will be used by the supervisor for asynchronous operations.|no (default == min(10, taskCount))|
|`chatThreads`|Integer|The number of threads that will be used for communicating with indexing tasks.|no (default == min(10, taskCount * replicas))|
|`chatRetries`|Integer|The number of times HTTP requests to indexing tasks will be retried before considering tasks unresponsive.|no (default == 8)|
|`httpTimeout`|ISO8601 Period|How long to wait for a HTTP response from an indexing task.|no (default == PT10S)|
|`shutdownTimeout`|ISO8601 Period|How long to wait for the supervisor to attempt a graceful shutdown of tasks before exiting.|no (default == PT80S)|
|`recordBufferSize`|Integer|Size of the buffer (number of events) used between the Kinesis fetch threads and the main ingestion thread.|no (default == 10000)|
|`recordBufferOfferTimeout`|Integer|Length of time in milliseconds to wait for space to become available in the buffer before timing out.|no (default == 10000)|
|`recordBufferFullWait`|Integer|Length of time in milliseconds to wait for the buffer to drain before attempting to fetch records from Kinesis again.|no (default == 10000)|
|`fetchSequenceNumberTimeout`|Integer|Length of time in milliseconds to wait for Kinesis to return the earliest or latest sequence number for a partition. Kinesis will not return the latest sequence number if no data is actively being written to that partition. In this case, this fetch call will repeatedly timeout and retry until fresh data is written to the stream.|no (default == 60000)|
|`fetchThreads`|Integer|Size of the pool of threads fetching data from Kinesis. There is no benefit in having more threads than Kinesis partitions.|no (default == max(1, {numProcessors} - 1))|

### KinesisSupervisorIOConfig

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`stream`|String|The Kinesis stream to read.|yes|
|`endpoint`|String|The AWS Kinesis stream endpoint for a region. You can find a list of endpoints [here](http://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region).|no (default == kinesis.us-east-1.amazonaws.com)|
|`replicas`|Integer|The number of replica sets, where 1 means a single set of tasks (no replication). Replica tasks will always be assigned to different workers to provide resiliency against node failure.|no (default == 1)|
|`taskCount`|Integer|The maximum number of *reading* tasks in a *replica set*. This means that the maximum number of reading tasks will be `taskCount * replicas` and the total number of tasks (*reading* + *publishing*) will be higher than this. See 'Capacity Planning' below for more details. The number of reading tasks will be less than `taskCount` if `taskCount > {numKinesisPartitions}`.|no (default == 1)|
|`taskDuration`|ISO8601 Period|The length of time before tasks stop reading and begin publishing their segment. Note that segments are only pushed to deep storage and loadable by historical nodes when the indexing task completes.|no (default == PT1H)|
|`startDelay`|ISO8601 Period|The period to wait before the supervisor starts managing tasks.|no (default == PT5S)|
|`period`|ISO8601 Period|How often the supervisor will execute its management logic. Note that the supervisor will also run in response to certain events (such as tasks succeeding, failing, and reaching their taskDuration) so this value specifies the maximum time between iterations.|no (default == PT30S)|
|`useEarliestSequenceNumber`|Boolean|If a supervisor is managing a dataSource for the first time, it will obtain a set of starting sequence numbers from Kinesis. This flag determines whether it retrieves the earliest or latest offsets in Kinesis. Under normal circumstances, subsequent tasks will start from where the previous segments ended so this flag will only be used on first run.|no (default == false)|
|`completionTimeout`|ISO8601 Period|The length of time to wait before declaring a publishing task as failed and terminating it. If this is set too low, your tasks may never publish. The publishing clock for a task begins roughly after `taskDuration` elapses.|no (default == PT6H)|
|`lateMessageRejectionPeriod`|ISO8601 Period|Configure tasks to reject messages with timestamps earlier than this period before the task was created; for example if this is set to `PT1H` and the supervisor creates a task at *2016-01-01T12:00Z*, messages with timestamps earlier than *2016-01-01T11:00Z* will be dropped. This may help prevent concurrency issues if your data stream has late messages and you have multiple pipelines that need to operate on the same segments (e.g. a realtime and a nightly batch ingestion pipeline).|no (default == none)|
|`recordsPerFetch`|Integer|The number of records to request per GetRecords call to Kinesis. See 'Determining Fetch Settings' below.|no (default == 2000)|
|`fetchDelayMillis`|Integer|Time in milliseconds to wait between subsequent GetRecords calls to Kinesis. See 'Determining Fetch Settings' below.|no (default == 1000)|
|`awsAccessKeyId`|String|The AWS access key ID to use for Kinesis API requests. If this is not provided, the service will look for credentials set in the environment variables, system properties, in the default profile configuration file, and from the EC2 instance profile provider (in this order).|no|
|`awsSecretAccessKey`|String|The AWS secret access key to use for Kinesis API requests. Only used if `awsAccessKeyId` is also provided.|no|

## Determining Fetch Settings

Kinesis places the following restrictions on calls to fetch records:

- Each data record can be up to 1 MB in size.
- Each shard can support up to 5 transactions per second for reads.
- Each shard can read up to 2 MB per second.
- The maximum size of data that GetRecords can return is 10 MB.

Values for `recordsPerFetch` and `fetchDelayMillis` should be chosen to maximize throughput under the above constraints.
The values that you choose will depend on the average size of a record and the number of consumers you have reading from
a given shard (which will be `replicas` unless you have other consumers also reading from this Kinesis stream).

If the above limits are violated, AWS will throw ProvisionedThroughputExceededException errors on subsequent calls to
read data. When this happens, the Kinesis indexing service will pause by `fetchDelayMillis` and then attempt the call
again.

## Supervisor API, Capacity Planning, Persistence, and Schema Changes

The Kinesis indexing service uses the same supervisor API and has the same considerations for capacity planning,
persistence, and schema changes as the Kafka indexing service. For documentation on these topics, see the relevant
sections of the Kafka indexing service [documentation](http://druid.io/docs/0.10.0/development/extensions-core/kafka-ingestion.html#supervisor-api).
