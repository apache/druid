---
layout: doc_page
title: "Amazon Kinesis Indexing Service"
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

# Kinesis Indexing Service

Similar to the [Kafka indexing service](./kafka-ingestion.html), the Kinesis indexing service enables the configuration of *supervisors* on the Overlord, which facilitate ingestion from
Kinesis by managing the creation and lifetime of Kinesis indexing tasks. These indexing tasks read events using Kinesis's own
Shards and Sequence Number mechanism and are therefore able to provide guarantees of exactly-once ingestion. They are also
able to read non-recent events from Kinesis and are not subject to the window period considerations imposed on other
ingestion mechanisms using Tranquility. The supervisor oversees the state of the indexing tasks to coordinate handoffs, manage failures,
and ensure that the scalability and replication requirements are maintained.

The Kinesis indexing service is provided as the `druid-kinesis-indexing-service` core Apache Druid (incubating) extension (see
[Including Extensions](../../operations/including-extensions.html)). Please note that this is
currently designated as an *experimental feature* and is subject to the usual
[experimental caveats](../experimental.html).

## Submitting a Supervisor Spec

The Kinesis indexing service requires that the `druid-kinesis-indexing-service` extension be loaded on both the Overlord
and the MiddleManagers. A supervisor for a dataSource is started by submitting a supervisor spec via HTTP POST to
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
|`dataSchema`|The schema that will be used by the Kinesis indexing task during ingestion, see [Ingestion Spec DataSchema](../../ingestion/ingestion-spec.html#dataschema).|yes|
|`ioConfig`|A KinesisSupervisorIOConfig to configure the supervisor and indexing tasks, see below.|yes|
|`tuningConfig`|A KinesisSupervisorTuningConfig to configure the supervisor and indexing tasks, see below.|no|

### KinesisSupervisorTuningConfig

The tuningConfig is optional and default parameters will be used if no tuningConfig is specified.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`|String|The indexing task type, this should always be `kinesis`.|yes|
|`maxRowsInMemory`|Integer|The number of rows to aggregate before persisting. This number is the post-aggregation rows, so it is not equivalent to the number of input events, but the number of aggregated rows that those events result in. This is used to manage the required JVM heap size. Maximum heap memory usage for indexing scales with maxRowsInMemory * (2 + maxPendingPersists).|no (default == 100000)|
|`maxBytesInMemory`|Long|The number of bytes to aggregate in heap memory before persisting. This is based on a rough estimate of memory usage and not actual usage. Normally this is computed internally and user does not need to set it. The maximum heap memory usage for indexing is maxBytesInMemory * (2 + maxPendingPersists).  |no (default == One-sixth of max JVM memory)|
|`maxRowsPerSegment`|Integer|The number of rows to aggregate into a segment; this number is post-aggregation rows. Handoff will happen either if `maxRowsPerSegment` or `maxTotalRows` is hit or every `intermediateHandoffPeriod`, whichever happens earlier.|no (default == 5000000)|
|`maxTotalRows`|Long|The number of rows to aggregate across all segments; this number is post-aggregation rows. Handoff will happen either if `maxRowsPerSegment` or `maxTotalRows` is hit or every `intermediateHandoffPeriod`, whichever happens earlier.|no (default == unlimited)|
|`intermediatePersistPeriod`|ISO8601 Period|The period that determines the rate at which intermediate persists occur.|no (default == PT10M)|
|`maxPendingPersists`|Integer|Maximum number of persists that can be pending but not started. If this limit would be exceeded by a new intermediate persist, ingestion will block until the currently-running persist finishes. Maximum heap memory usage for indexing scales with maxRowsInMemory * (2 + maxPendingPersists).|no (default == 0, meaning one persist can be running concurrently with ingestion, and none can be queued up)|
|`indexSpec`|Object|Tune how data is indexed, see 'IndexSpec' below for more details.|no|
|`reportParseExceptions`|Boolean|If true, exceptions encountered during parsing will be thrown and will halt ingestion; if false, unparseable rows and fields will be skipped.|no (default == false)|
|`handoffConditionTimeout`|Long|Milliseconds to wait for segment handoff. It must be >= 0, where 0 means to wait forever.|no (default == 0)|
|`resetOffsetAutomatically`|Boolean|Whether to reset the consumer sequence numbers if the next sequence number that it is trying to fetch is less than the earliest available sequence number for that particular shard. The sequence number will be reset to either the earliest or latest sequence number depending on `useEarliestOffset` property of `KinesisSupervisorIOConfig` (see below). This situation typically occurs when messages in Kinesis are no longer available for consumption and therefore won't be ingested into Druid. If set to false then ingestion for that particular shard will halt and manual intervention is required to correct the situation, please see `Reset Supervisor` API below.|no (default == false)|
|`skipSequenceNumberAvailabilityCheck`|Boolean|Whether to enable checking if the current sequence number is still available in a particular Kinesis shard. If set to false, the indexing task will attempt to reset the current sequence number (or not), depending on the value of `resetOffsetAutomatically`. |no (default == false)|
|`workerThreads`|Integer|The number of threads that will be used by the supervisor for asynchronous operations.|no (default == min(10, taskCount))|
|`chatThreads`|Integer|The number of threads that will be used for communicating with indexing tasks.|no (default == min(10, taskCount * replicas))|
|`chatRetries`|Integer|The number of times HTTP requests to indexing tasks will be retried before considering tasks unresponsive.|no (default == 8)|
|`httpTimeout`|ISO8601 Period|How long to wait for a HTTP response from an indexing task.|no (default == PT10S)|
|`shutdownTimeout`|ISO8601 Period|How long to wait for the supervisor to attempt a graceful shutdown of tasks before exiting.|no (default == PT80S)|
|`recordBufferSize`|Integer|Size of the buffer (number of events) used between the Kinesis fetch threads and the main ingestion thread.|no (default == 10000)|
|`recordBufferOfferTimeout`|Integer|Length of time in milliseconds to wait for space to become available in the buffer before timing out.|no (default == 5000)|
|`recordBufferFullWait`|Integer|Length of time in milliseconds to wait for the buffer to drain before attempting to fetch records from Kinesis again.|no (default == 5000)|
|`fetchSequenceNumberTimeout`|Integer|Length of time in milliseconds to wait for Kinesis to return the earliest or latest sequence number for a shard. Kinesis will not return the latest sequence number if no data is actively being written to that shard. In this case, this fetch call will repeatedly timeout and retry until fresh data is written to the stream.|no (default == 60000)|
|`fetchThreads`|Integer|Size of the pool of threads fetching data from Kinesis. There is no benefit in having more threads than Kinesis shards.|no (default == max(1, {numProcessors} - 1))|
|`segmentWriteOutMediumFactory`|Object|Segment write-out medium to use when creating segments. See below for more information.|no (not specified by default, the value from `druid.peon.defaultSegmentWriteOutMediumFactory.type` is used)|
|`intermediateHandoffPeriod`|ISO8601 Period|How often the tasks should hand off segments. Handoff will happen either if `maxRowsPerSegment` or `maxTotalRows` is hit or every `intermediateHandoffPeriod`, whichever happens earlier.|no (default == P2147483647D)|
|`logParseExceptions`|Boolean|If true, log an error message when a parsing exception occurs, containing information about the row where the error occurred.|no, default == false|
|`maxParseExceptions`|Integer|The maximum number of parse exceptions that can occur before the task halts ingestion and fails. Overridden if `reportParseExceptions` is set.|no, unlimited default|
|`maxSavedParseExceptions`|Integer|When a parse exception occurs, Druid can keep track of the most recent parse exceptions. "maxSavedParseExceptions" limits how many exception instances will be saved. These saved exceptions will be made available after the task finishes in the [task completion report](../../ingestion/reports.html). Overridden if `reportParseExceptions` is set.|no, default == 0|
|`maxRecordsPerPoll`|Integer| The maximum number of records/events to be fetched from buffer per poll. The actual maximum will be `Max(maxRecordsPerPoll, Max(bufferSize, 1)) |no, default == 100|

#### IndexSpec

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|bitmap|Object|Compression format for bitmap indexes. Should be a JSON object; see below for options.|no (defaults to Concise)|
|dimensionCompression|String|Compression format for dimension columns. Choose from `LZ4`, `LZF`, or `uncompressed`.|no (default == `LZ4`)|
|metricCompression|String|Compression format for metric columns. Choose from `LZ4`, `LZF`, `uncompressed`, or `none`.|no (default == `LZ4`)|
|longEncoding|String|Encoding format for metric and dimension columns with type long. Choose from `auto` or `longs`. `auto` encodes the values using sequence number or lookup table depending on column cardinality, and store them with variable size. `longs` stores the value as is with 8 bytes each.|no (default == `longs`)|

##### Bitmap types

For Concise bitmaps:

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`|String|Must be `concise`.|yes|

For Roaring bitmaps:

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`|String|Must be `roaring`.|yes|
|`compressRunOnSerialization`|Boolean|Use a run-length encoding where it is estimated as more space efficient.|no (default == `true`)|

#### SegmentWriteOutMediumFactory

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`|String|See [Additional Peon Configuration: SegmentWriteOutMediumFactory](../../configuration/index.html#segmentwriteoutmediumfactory) for explanation and available options.|yes|

### KinesisSupervisorIOConfig

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`stream`|String|The Kinesis stream to read.|yes|
|`endpoint`|String|The AWS Kinesis stream endpoint for a region. You can find a list of endpoints [here](http://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region).|no (default == kinesis.us-east-1.amazonaws.com)|
|`replicas`|Integer|The number of replica sets, where 1 means a single set of tasks (no replication). Replica tasks will always be assigned to different workers to provide resiliency against process failure.|no (default == 1)|
|`taskCount`|Integer|The maximum number of *reading* tasks in a *replica set*. This means that the maximum number of reading tasks will be `taskCount * replicas` and the total number of tasks (*reading* + *publishing*) will be higher than this. See 'Capacity Planning' below for more details. The number of reading tasks will be less than `taskCount` if `taskCount > {numKinesisshards}`.|no (default == 1)|
|`taskDuration`|ISO8601 Period|The length of time before tasks stop reading and begin publishing their segment.|no (default == PT1H)|
|`startDelay`|ISO8601 Period|The period to wait before the supervisor starts managing tasks.|no (default == PT5S)|
|`period`|ISO8601 Period|How often the supervisor will execute its management logic. Note that the supervisor will also run in response to certain events (such as tasks succeeding, failing, and reaching their taskDuration) so this value specifies the maximum time between iterations.|no (default == PT30S)|
|`useEarliestSequenceNumber`|Boolean|If a supervisor is managing a dataSource for the first time, it will obtain a set of starting sequence numbers from Kinesis. This flag determines whether it retrieves the earliest or latest sequence numbers in Kinesis. Under normal circumstances, subsequent tasks will start from where the previous segments ended so this flag will only be used on first run.|no (default == false)|
|`completionTimeout`|ISO8601 Period|The length of time to wait before declaring a publishing task as failed and terminating it. If this is set too low, your tasks may never publish. The publishing clock for a task begins roughly after `taskDuration` elapses.|no (default == PT6H)|
|`lateMessageRejectionPeriod`|ISO8601 Period|Configure tasks to reject messages with timestamps earlier than this period before the task was created; for example if this is set to `PT1H` and the supervisor creates a task at *2016-01-01T12:00Z*, messages with timestamps earlier than *2016-01-01T11:00Z* will be dropped. This may help prevent concurrency issues if your data stream has late messages and you have multiple pipelines that need to operate on the same segments (e.g. a realtime and a nightly batch ingestion pipeline).|no (default == none)|
|`earlyMessageRejectionPeriod`|ISO8601 Period|Configure tasks to reject messages with timestamps later than this period after the task reached its taskDuration; for example if this is set to `PT1H`, the taskDuration is set to `PT1H` and the supervisor creates a task at *2016-01-01T12:00Z*, messages with timestamps later than *2016-01-01T14:00Z* will be dropped. **Note:** Tasks sometimes run past their task duration, for example, in cases of supervisor failover. Setting earlyMessageRejectionPeriod too low may cause messages to be dropped unexpectedly whenever a task runs past its originally configured task duration.|no (default == none)|
|`recordsPerFetch`|Integer|The number of records to request per GetRecords call to Kinesis. See 'Determining Fetch Settings' below.|no (default == 2000)|
|`fetchDelayMillis`|Integer|Time in milliseconds to wait between subsequent GetRecords calls to Kinesis. See 'Determining Fetch Settings' below.|no (default == 1000)|
|`awsAssumedRoleArn`|String|The AWS assumed role to use for additional permissions.|no|
|`awsExternalId`|String|The AWS external id to use for additional permissions.|no|
|`deaggregate`|Boolean|Whether to use the de-aggregate function of the KCL. See below for details.|no|

## Operations

This section gives descriptions of how some supervisor APIs work specifically in Kinesis Indexing Service.
For all supervisor APIs, please check [Supervisor APIs](../../operations/api-reference.html#supervisors).

### AWS Authentication
To authenticate with AWS, you must provide your AWS access key and AWS secret key via runtime.properties, for example:
```
-Ddruid.kinesis.accessKey=123 -Ddruid.kinesis.secretKey=456
```
The AWS access key ID and secret access key are used for Kinesis API requests. If this is not provided, the service will look for credentials set in environment variables, in the default profile configuration file, and from the EC2 instance profile provider (in this order).

### Getting Supervisor Status Report

`GET /druid/indexer/v1/supervisor/<supervisorId>/status` returns a snapshot report of the current state of the tasks managed by the given supervisor. This includes the latest
sequence numbers as reported by Kinesis. Unlike the Kafka Indexing Service, stats about lag is not yet supported.

### Updating Existing Supervisors

`POST /druid/indexer/v1/supervisor` can be used to update existing supervisor spec.
Calling this endpoint when there is already an existing supervisor for the same dataSource will cause:

- The running supervisor to signal its managed tasks to stop reading and begin publishing.
- The running supervisor to exit.
- A new supervisor to be created using the configuration provided in the request body. This supervisor will retain the
existing publishing tasks and will create new tasks starting at the sequence numbers the publishing tasks ended on.

Seamless schema migrations can thus be achieved by simply submitting the new schema using this endpoint.

### Suspending and Resuming Supervisors

You can suspend and resume a supervisor using `POST /druid/indexer/v1/supervisor/<supervisorId>/suspend` and `POST /druid/indexer/v1/supervisor/<supervisorId>/resume`, respectively.

Note that the supervisor itself will still be operating and emitting logs and metrics,
it will just ensure that no indexing tasks are running until the supervisor is resumed.

### Resetting Supervisors

To reset a running supervisor, you can use `POST /druid/indexer/v1/supervisor/<supervisorId>/reset`.

The indexing service keeps track of the latest persisted Kinesis sequence number in order to provide exactly-once ingestion
guarantees across tasks. Subsequent tasks must start reading from where the previous task completed in order for the
generated segments to be accepted. If the messages at the expected starting sequence numbers are no longer available in Kinesis
(typically because the message retention period has elapsed or the topic was removed and re-created) the supervisor will
refuse to start and in-flight tasks will fail.

This endpoint can be used to clear the stored sequence numbers which will cause the supervisor to start reading from
either the earliest or latest sequence numbers in Kinesis (depending on the value of `useEarliestSequenceNumber`). The supervisor must be
running for this endpoint to be available. After the stored sequence numbers are cleared, the supervisor will automatically kill
and re-create any active tasks so that tasks begin reading from valid sequence numbers.

Note that since the stored sequence numbers are necessary to guarantee exactly-once ingestion, resetting them with this endpoint
may cause some Kinesis messages to be skipped or to be read twice.

### Terminating Supervisors

`POST /druid/indexer/v1/supervisor/<supervisorId>/terminate` terminates a supervisor and causes all associated indexing
tasks managed by this supervisor to immediately stop and begin
publishing their segments. This supervisor will still exist in the metadata store and it's history may be retrieved
with the supervisor history api, but will not be listed in the 'get supervisors' api response nor can it's configuration
or status report be retrieved. The only way this supervisor can start again is by submitting a functioning supervisor
spec to the create api.

### Capacity Planning

Kinesis indexing tasks run on MiddleManagers and are thus limited by the resources available in the MiddleManager
cluster. In particular, you should make sure that you have sufficient worker capacity (configured using the
`druid.worker.capacity` property) to handle the configuration in the supervisor spec. Note that worker capacity is
shared across all types of indexing tasks, so you should plan your worker capacity to handle your total indexing load
(e.g. batch processing, realtime tasks, merging tasks, etc.). If your workers run out of capacity, Kinesis indexing tasks
will queue and wait for the next available worker. This may cause queries to return partial results but will not result
in data loss (assuming the tasks run before Kinesis purges those sequence numbers).

A running task will normally be in one of two states: *reading* or *publishing*. A task will remain in reading state for
`taskDuration`, at which point it will transition to publishing state. A task will remain in publishing state for as long
as it takes to generate segments, push segments to deep storage, and have them be loaded and served by a Historical process
(or until `completionTimeout` elapses).

The number of reading tasks is controlled by `replicas` and `taskCount`. In general, there will be `replicas * taskCount`
reading tasks, the exception being if taskCount > {numKinesisShards} in which case {numKinesisShards} tasks will
be used instead. When `taskDuration` elapses, these tasks will transition to publishing state and `replicas * taskCount`
new reading tasks will be created. Therefore to allow for reading tasks and publishing tasks to run concurrently, there
should be a minimum capacity of:

```
workerCapacity = 2 * replicas * taskCount
```

This value is for the ideal situation in which there is at most one set of tasks publishing while another set is reading.
In some circumstances, it is possible to have multiple sets of tasks publishing simultaneously. This would happen if the
time-to-publish (generate segment, push to deep storage, loaded on Historical) > `taskDuration`. This is a valid
scenario (correctness-wise) but requires additional worker capacity to support. In general, it is a good idea to have
`taskDuration` be large enough that the previous set of tasks finishes publishing before the current set begins.

### Supervisor Persistence

When a supervisor spec is submitted via the `POST /druid/indexer/v1/supervisor` endpoint, it is persisted in the
configured metadata database. There can only be a single supervisor per dataSource, and submitting a second spec for
the same dataSource will overwrite the previous one.

When an Overlord gains leadership, either by being started or as a result of another Overlord failing, it will spawn
a supervisor for each supervisor spec in the metadata database. The supervisor will then discover running Kinesis indexing
tasks and will attempt to adopt them if they are compatible with the supervisor's configuration. If they are not
compatible because they have a different ingestion spec or shard allocation, the tasks will be killed and the
supervisor will create a new set of tasks. In this way, the supervisors are persistent across Overlord restarts and
fail-overs.

A supervisor is stopped via the `POST /druid/indexer/v1/supervisor/<supervisorId>/terminate` endpoint. This places a
tombstone marker in the database (to prevent the supervisor from being reloaded on a restart) and then gracefully
shuts down the currently running supervisor. When a supervisor is shut down in this way, it will instruct its
managed tasks to stop reading and begin publishing their segments immediately. The call to the shutdown endpoint will
return after all tasks have been signalled to stop but before the tasks finish publishing their segments.

### Schema/Configuration Changes

Schema and configuration changes are handled by submitting the new supervisor spec via the same
`POST /druid/indexer/v1/supervisor` endpoint used to initially create the supervisor. The Overlord will initiate a
graceful shutdown of the existing supervisor which will cause the tasks being managed by that supervisor to stop reading
and begin publishing their segments. A new supervisor will then be started which will create a new set of tasks that
will start reading from the sequence numbers where the previous now-publishing tasks left off, but using the updated schema.
In this way, configuration changes can be applied without requiring any pause in ingestion.

### Deployment Notes

#### On the Subject of Segments

Each Kinesis Indexing Task puts events consumed from Kinesis Shards assigned to it in a single segment for each segment
granular interval until maxRowsPerSegment, maxTotalRows or intermediateHandoffPeriod limit is reached, at this point a new shard
for this segment granularity is created for further events. Kinesis Indexing Task also does incremental hand-offs which
means that all the segments created by a task will not be held up till the task duration is over. As soon as maxRowsPerSegment,
maxTotalRows or intermediateHandoffPeriod limit is hit, all the segments held by the task at that point in time will be handed-off
and new set of segments will be created for further events. This means that the task can run for longer durations of time
without accumulating old segments locally on Middle Manager processes and it is encouraged to do so.

Kinesis Indexing Service may still produce some small segments. Lets say the task duration is 4 hours, segment granularity
is set to an HOUR and Supervisor was started at 9:10 then after 4 hours at 13:10, new set of tasks will be started and
events for the interval 13:00 - 14:00 may be split across previous and new set of tasks. If you see it becoming a problem then
one can schedule re-indexing tasks be run to merge segments together into new segments of an ideal size (in the range of ~500-700 MB per segment).
Details on how to optimize the segment size can be found on [Segment size optimization](../../operations/segment-optimization.html).
There is also ongoing work to support automatic segment compaction of sharded segments as well as compaction not requiring
Hadoop (see [here](https://github.com/apache/incubator-druid/pull/5102)).

### Determining Fetch Settings
Internally, the Kinesis Indexing Service uses the Kinesis Record Supplier abstraction for fetching Kinesis data records and storing the records
locally. The way the Kinesis Record Supplier fetches records is to have a separate thread run the fetching operation per each Kinesis Shard, the
max number of threads is determined by `fetchThreads`. For example, a Kinesis stream with 3 shards will have 3 threads, each fetching from a shard separately.
There is a delay between each fetching operation, which is controlled by `fetchDelayMillis`. The maximum number of records to be fetched per thread per
operation is controlled by `recordsPerFetch`. Note that this is not the same as `maxRecordsPerPoll`.

The records fetched by each thread will be pushed to a queue in the order that they are fetched. The records are stored in this queue until `poll()` is called
by either the supervisor or the indexing task. `poll()` will attempt to drain the internal buffer queue up to a limit of `max(maxRecordsPerPoll, q.size())`.
Here `maxRecordsPerPoll` controls the theoretical maximum records to drain out of the buffer queue, so setting this parameter to a reasonable value is essential
in preventing the queue from overflowing or memory exceeding heap size.

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

Internally, each indexing task maintains a buffer that stores the fetched but not yet processed record. `recordsPerFetch` and `fetchDelayMillis`
control this behavior. The number of records that the indexing task fetch from the buffer is controlled by `maxRecordsPerPoll`, which
determines the number of records to be processed per each ingestion loop in the task.

## Deaggregation
See [issue](https://github.com/apache/incubator-druid/issues/6714)

The Kinesis indexing service supports de-aggregation of multiple rows packed into a single record by the Kinesis
Producer Library's aggregate method for more efficient data transfer. Currently, enabling the de-aggregate functionality
requires the user to manually provide the Kinesis Client Library on the classpath, since this library has a license not
compatible with Apache projects.

To enable this feature, add the `amazon-kinesis-client` (tested on version `1.9.2`) jar file ([link](https://mvnrepository.com/artifact/com.amazonaws/amazon-kinesis-client/1.9.2)) under `dist/druid/extensions/druid-kinesis-indexing-service/`.
Then when submitting a supervisor-spec, set `deaggregate` to true.