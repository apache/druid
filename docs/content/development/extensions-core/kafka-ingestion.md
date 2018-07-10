---
layout: doc_page
---

# Kafka Indexing Service

The Kafka indexing service enables the configuration of *supervisors* on the Overlord, which facilitate ingestion from
Kafka by managing the creation and lifetime of Kafka indexing tasks. These indexing tasks read events using Kafka's own
partition and offset mechanism and are therefore able to provide guarantees of exactly-once ingestion. They are also
able to read non-recent events from Kafka and are not subject to the window period considerations imposed on other
ingestion mechanisms. The supervisor oversees the state of the indexing tasks to coordinate handoffs, manage failures,
and ensure that the scalability and replication requirements are maintained.

This service is provided in the `kafka-indexing-service` core extension (see
[Including Extensions](../../operations/including-extensions.html)). Please note that the Kafka indexing service is
currently designated as an *experimental feature* and is subject to the usual
[experimental caveats](../experimental.html).

## Submitting a Supervisor Spec

The Kafka indexing service requires that the `kafka-indexing-service` extension be loaded on both the overlord and the
middle managers. A supervisor for a dataSource is started by submitting a supervisor spec via HTTP POST to
`http://<OVERLORD_IP>:<OVERLORD_PORT>/druid/indexer/v1/supervisor`, for example:

```
curl -X POST -H 'Content-Type: application/json' -d @supervisor-spec.json http://localhost:8090/druid/indexer/v1/supervisor
```

A sample supervisor spec is shown below:

```json
{
  "type": "kafka",
  "dataSchema": {
    "dataSource": "metrics-kafka",
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
    "type": "kafka",
    "maxRowsPerSegment": 5000000
  },
  "ioConfig": {
    "topic": "metrics",
    "consumerProperties": {
      "bootstrap.servers": "localhost:9092"
    },
    "taskCount": 1,
    "replicas": 1,
    "taskDuration": "PT1H"
  }
}
```

## Supervisor Configuration

|Field|Description|Required|
|--------|-----------|---------|
|`type`|The supervisor type, this should always be `kafka`.|yes|
|`dataSchema`|The schema that will be used by the Kafka indexing task during ingestion, see [Ingestion Spec](../../ingestion/index.html).|yes|
|`tuningConfig`|A KafkaTuningConfig that will be provided to indexing tasks, see below.|no|
|`ioConfig`|A KafkaSupervisorIOConfig to configure the supervisor, see below.|yes|

### KafkaTuningConfig

The tuningConfig is optional and default parameters will be used if no tuningConfig is specified.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`|String|The indexing task type, this should always be `kafka`.|yes|
|`maxRowsInMemory`|Integer|The number of rows to aggregate before persisting. This number is the post-aggregation rows, so it is not equivalent to the number of input events, but the number of aggregated rows that those events result in. This is used to manage the required JVM heap size. Maximum heap memory usage for indexing scales with maxRowsInMemory * (2 + maxPendingPersists).|no (default == 75000)|
|`maxRowsPerSegment`|Integer|The number of rows to aggregate into a segment; this number is post-aggregation rows.|no (default == 5000000)|
|`intermediatePersistPeriod`|ISO8601 Period|The period that determines the rate at which intermediate persists occur.|no (default == PT10M)|
|`maxPendingPersists`|Integer|Maximum number of persists that can be pending but not started. If this limit would be exceeded by a new intermediate persist, ingestion will block until the currently-running persist finishes. Maximum heap memory usage for indexing scales with maxRowsInMemory * (2 + maxPendingPersists).|no (default == 0, meaning one persist can be running concurrently with ingestion, and none can be queued up)|
|`indexSpec`|Object|Tune how data is indexed, see 'IndexSpec' below for more details.|no|
|`buildV9Directly`|Boolean|Whether to build a v9 index directly instead of first building a v8 index and then converting it to v9 format.|no (default == false)|
|`reportParseExceptions`|Boolean|If true, exceptions encountered during parsing will be thrown and will halt ingestion; if false, unparseable rows and fields will be skipped.|no (default == false)|
|`handoffConditionTimeout`|Long|Milliseconds to wait for segment handoff. It must be >= 0, where 0 means to wait forever.|no (default == 0)|

#### IndexSpec

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`bitmap`|String|The type of bitmap index to create. Choose from `roaring` or `concise`.|no (default == `concise`)|
|`dimensionCompression`|String|Compression format for dimension columns. Choose from `LZ4`, `LZF`, or `uncompressed`.|no (default == `LZ4`)|
|`metricCompression`|String|Compression format for metric columns. Choose from `LZ4`, `LZF`, or `uncompressed`.|no (default == `LZ4`)|

### KafkaSupervisorIOConfig

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`topic`|String|The Kafka topic to read from. This must be a specific topic as topic patterns are not supported.|yes|
|`consumerProperties`|Map<String, String>|A map of properties to be passed to the Kafka consumer. This must contain a property `bootstrap.servers` with a list of Kafka brokers in the form: `<BROKER_1>:<PORT_1>,<BROKER_2>:<PORT_2>,...`.|yes|
|`replicas`|Integer|The number of replica sets, where 1 means a single set of tasks (no replication). Replica tasks will always be assigned to different workers to provide resiliency against node failure.|no (default == 1)|
|`taskCount`|Integer|The maximum number of *reading* tasks in a *replica set*. This means that the maximum number of reading tasks will be `taskCount * replicas` and the total number of tasks (*reading* + *publishing*) will be higher than this. See 'Capacity Planning' below for more details. The number of reading tasks will be less than `taskCount` if `taskCount > {numKafkaPartitions}`.|no (default == 1)|
|`taskDuration`|ISO8601 Period|The length of time before tasks stop reading and begin publishing their segment. Note that segments are only pushed to deep storage and loadable by historical nodes when the indexing task completes.|no (default == PT1H)|
|`startDelay`|ISO8601 Period|The period to wait before the supervisor starts managing tasks.|no (default == PT5S)|
|`period`|ISO8601 Period|How often the supervisor will execute its management logic. Note that the supervisor will also run in response to certain events (such as tasks succeeding, failing, and reaching their taskDuration) so this value specifies the maximum time between iterations.|no (default == PT30S)|
|`useEarliestOffset`|Boolean|If a supervisor is managing a dataSource for the first time, it will obtain a set of starting offsets from Kafka. This flag determines whether it retrieves the earliest or latest offsets in Kafka. Under normal circumstances, subsequent tasks will start from where the previous segments ended so this flag will only be used on first run.|no (default == false)|
|`completionTimeout`|ISO8601 Period|The length of time to wait before declaring a publishing task as failed and terminating it. If this is set too low, your tasks may never publish. The publishing clock for a task begins roughly after `taskDuration` elapses.|no (default == PT30M)|

## Supervisor API

The following endpoints are available on the Overlord:

#### Create Supervisor
```
POST /druid/indexer/v1/supervisor
```
Use `Content-Type: application/json` and provide a supervisor spec in the request body.

#### Shutdown Supervisor
```
POST /druid/indexer/v1/supervisor/<supervisorId>/shutdown
```
Note that this will cause all indexing tasks managed by this supervisor to immediately stop and begin publishing their segments.

#### Get Supervisor IDs
```
GET /druid/indexer/v1/supervisor
```
Returns a list of the currently active supervisors.

#### Get Supervisor Spec
```
GET /druid/indexer/v1/supervisor/<supervisorId>
```
Returns the current spec for the supervisor with the provided ID.

#### Get Supervisor Status Report
```
GET /druid/indexer/v1/supervisor/<supervisorId>/status
```
Returns a snapshot report of the current state of the tasks managed by the given supervisor.

#### Get All Supervisor History
```
GET /druid/indexer/v1/supervisor/history
```
Returns an audit history of specs for all supervisors (current and past).

#### Get Supervisor History
```
GET /druid/indexer/v1/supervisor/<supervisorId>/history
```
Returns an audit history of specs for the supervisor with the provided ID.

## Capacity Planning

Kafka indexing tasks run on middle managers and are thus limited by the resources available in the middle manager
cluster. In particular, you should make sure that you have sufficient worker capacity (configured using the
`druid.worker.capacity` property) to handle the configuration in the supervisor spec. Note that worker capacity is
shared across all types of indexing tasks, so you should plan your worker capacity to handle your total indexing load
(e.g. batch processing, realtime tasks, merging tasks, etc.). If your workers run out of capacity, Kafka indexing tasks
will queue and wait for the next available worker. This may cause queries to return partial results but will not result
in data loss (assuming the tasks run before Kafka purges those offsets).

A running task will normally be in one of two states: *reading* or *publishing*. A task will remain in reading state for
`taskDuration`, at which point it will transition to publishing state. A task will remain in publishing state for as long
as it takes to generate segments, push segments to deep storage, and have them be loaded and served by a historical node
(or until `completionTimeout` elapses).

The number of reading tasks is controlled by `replicas` and `taskCount`. In general, there will be `replicas * taskCount`
reading tasks, the exception being if taskCount > {numKafkaPartitions} in which case {numKafkaPartitions} tasks will
be used instead. When `taskDuration` elapses, these tasks will transition to publishing state and `replicas * taskCount`
new reading tasks will be created. Therefore to allow for reading tasks and publishing tasks to run concurrently, there
should be a minimum capacity of:

```
workerCapacity = 2 * replicas * taskCount
```

This value is for the ideal situation in which there is at most one set of tasks publishing while another set is reading.
In some circumstances, it is possible to have multiple sets of tasks publishing simultaneously. This would happen if the
time-to-publish (generate segment, push to deep storage, loaded on historical) > `taskDuration`. This is a valid
scenario (correctness-wise) but requires additional worker capacity to support. In general, it is a good idea to have
`taskDuration` be large enough that the previous set of tasks finishes publishing before the current set begins.

## Supervisor Persistence

When a supervisor spec is submitted via the `POST /druid/indexer/v1/supervisor` endpoint, it is persisted in the
configured metadata database. There can only be a single supervisor per dataSource, and submitting a second spec for
the same dataSource will fail with a `409 Conflict` if one already exists.

When an overlord gains leadership, either by being started or as a result of another overlord failing, it will spawn
a supervisor for each supervisor spec in the metadata database. The supervisor will then discover running Kafka indexing
tasks and will attempt to adopt them if they are compatible with the supervisor's configuration. If they are not
compatible because they have a different ingestion spec or partition allocation, the tasks will be killed and the
supervisor will create a new set of tasks. In this way, the supervisors are persistent across overlord restarts and
fail-overs.

A supervisor is stopped via the `POST /druid/indexer/v1/supervisor/<supervisorId>/shutdown` endpoint. This places a
tombstone marker in the database (to prevent the supervisor from being reloaded on a restart) and then gracefully
shuts down the currently running supervisor. When a supervisor is shut down in this way, it will instruct its
managed tasks to stop reading and begin publishing their segments immediately. The call to the shutdown endpoint will
return after all tasks have been signalled to stop but before the tasks finish publishing their segments.

### Schema/Configuration Changes

Following from the previous section, schema and configuration changes are managed by first shutting down the supervisor
with a call to the `POST /druid/indexer/v1/supervisor/<supervisorId>/shutdown` endpoint, waiting for the running tasks
to complete, and then submitting the updated schema via the `POST /druid/indexer/v1/supervisor` create supervisor
endpoint. The updated supervisor will begin reading from the offsets where the previous supervisor ended and no data
will be lost. If the updated schema is posted before the previously running tasks have completed, the supervisor will
detect that the tasks are no longer compatible with the new schema and will issue a shutdown command to the tasks which
may result in the current segments not being published. If this happens, the tasks based on the updated schema will
begin reading from the same starting offsets as the previous aborted tasks and no data will be lost.
