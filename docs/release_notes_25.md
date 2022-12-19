Apache Druid 25.0.0 contains over 300 new features, bug fixes, performance enhancements, documentation improvements, and additional test coverage from 51 contributors. [See the complete set of changes for additional details](https://github.com/apache/druid/milestone/48).

## <a name="25.0.0-highlights" href="#25.0.0-highlights">#</a> Highlights

### <a name="25.0.0-highlights-multi-stage-query-" href="#25.0.0-highlights-multi-stage-query-">#</a> Multi-stage query 

The multi-stage query (MSQ) task engine used for SQL-based ingestion is now production ready. Use it for any supported workloads. For more information, see the following pages:

- [Ingestion](https://druid.apache.org/docs/latest/ingestion/index.html)
- [SQL-based ingestion](https://druid.apache.org/docs/latest/multi-stage-query/index.html)

### <a name="25.0.0-highlights-deploying-druid" href="#25.0.0-highlights-deploying-druid">#</a> Deploying Druid

The new `start-druid` script greatly simplifies deploying any combination of Druid services on a single-server. It comes pre-packaged with the required configs and can be used to launch a fully functional Druid cluster simply by invoking `./start-druid`. For experienced Druids, it also gives complete control over the runtime properties and JVM arguments to have a cluster that exactly fits your needs.

The `start-druid` script deprecates the existing profiles such as `start-micro-quickstart` and `start-nano-quickstart`. These profiles may be removed in future releases. For more information, see [Single server deployment](https://druid.apache.org/docs/latest/operations/single-server.html).

### <a name="25.0.0-highlights-string-dictionary-compression-%28experimental%29" href="#25.0.0-highlights-string-dictionary-compression-%28experimental%29">#</a> String dictionary compression (experimental)

Added support for front coded string dictionaries for smaller string columns, leading to reduced segment sizes with only minor performance penalties for most Druid queries.

This functionality can be utilized by a new property to `IndexSpec.stringDictionaryEncoding`, which can be set to `{"type":"frontCoded", "bucketSize": 4}`, `{"type":"frontCoded", "bucketSize": 16}`, or any power of 2 that is 128 or lower. This property instructs indexing tasks to write segments with the compressed dictionaries with the specific bucket size specified.  (`{"type":"utf8"}` is the default).

> Any segment written using string dictionary compression is not readable by older versions of Druid.

For more information, see [Front coding](https://druid.apache.org/docs/latest/ingestion/ingestion-spec.html#front-coding).

https://github.com/apache/druid/pull/12277 

### <a name="25.0.0-highlights-kubernetes-native-tasks" href="#25.0.0-highlights-kubernetes-native-tasks">#</a> Kubernetes native tasks

Druid can now use Kubernetes to launch and manage tasks, eliminating the need for MiddleManagers.

To use this feature, enable the [`druid-kubernetes-overlord-extensions`]((../extensions.md#loading-extensions) in the extensions load list for your Overlord process.

https://github.com/apache/druid/pull/13156

## <a name="25.0.0-behavior-changes" href="#25.0.0-behavior-changes">#</a> Behavior changes

### <a name="25.0.0-behavior-changes-hll-and-quantiles-sketches" href="#25.0.0-behavior-changes-hll-and-quantiles-sketches">#</a> HLL and quantiles sketches

The aggregation functions for HLL and quantiles sketches returned sketches or numbers when they are finalized depending on where they were in the native query plan. 

Druid no longer finalizes aggregators in the following two cases:

  - aggregators appear in the outer level of a query
  - aggregators are used as input to an expression or finalizing-field-access post-aggregator

This change aligns the behavior of HLL and quantiles sketches with theta sketches.

To provide backwards compatibility, you can use the `sqlFinalizeOuterSketches` query context parameter that restores the old behavior. 
    
https://github.com/apache/druid/pull/13247

### <a name="25.0.0-behavior-changes-segment-discovery" href="#25.0.0-behavior-changes-segment-discovery">#</a> Segment discovery

The default segment discovery method now uses HTTP instead of ZooKeeper.

This update changes the defaults for the following properties:

| Property | New default | Previous default | 
| - | - | - |
| `druid.serverview.type` for segment management | http | batch |
| `druid.coordinator.loadqueuepeon.type` for segment management | http | curator |
| `druid.indexer.runner.type` for the Overlord | httpRemote | local |

To use ZooKeeper instead of HTTP, change the values for the properties back to the previous defaults.
ZooKeeper-based implementations for these properties are deprecated and will be removed in a subsequent release.

https://github.com/apache/druid/pull/13092

### <a name="25.0.0-behavior-changes-kill-tasks-do-not-include-markasunuseddone" href="#25.0.0-behavior-changes-kill-tasks-do-not-include-markasunuseddone">#</a> Kill tasks do not include markAsUnusedDone

When you kill a task, Druid no longer automatically marks segments as unused. You must explicitly mark them as unused with `POST /druid/coordinator/v1/datasources/{dataSourceName}/markUnused`. 
For more information, see the [API reference](https://druid.apache.org/docs/latest/operations/api-reference.html#coordinator)

https://github.com/apache/druid/pull/13104

### <a name="25.0.0-behavior-changes-memory-estimates" href="#25.0.0-behavior-changes-memory-estimates">#</a> Memory estimates

The task context flag `useMaxMemoryEstimates` is now set to false by default to improve memory usage estimation while building an on-heap incremental index.

https://github.com/apache/druid/pull/13178

## <a name="25.0.0-multi-stage-query-task-engine" href="#25.0.0-multi-stage-query-task-engine">#</a> Multi-stage query task engine

### <a name="25.0.0-multi-stage-query-task-engine-clustered-by-limit" href="#25.0.0-multi-stage-query-task-engine-clustered-by-limit">#</a> CLUSTERED BY limit

When using the MSQ task engine to ingest data, there is now a 1,500 column limit to the number of columns that can be passed in the CLUSTERED BY clause.

https://github.com/apache/druid/pull/13352

### <a name="25.0.0-multi-stage-query-task-engine-metrics-used-to-downsample-bucket" href="#25.0.0-multi-stage-query-task-engine-metrics-used-to-downsample-bucket">#</a> Metrics used to downsample bucket

Changed the way the MSQ task engine determines whether or not to downsample data, to improve accuracy. The task engine now uses the number of bytes instead of number of keys.

https://github.com/apache/druid/pull/12998

### <a name="25.0.0-multi-stage-query-task-engine-msq-heap-footprint" href="#25.0.0-multi-stage-query-task-engine-msq-heap-footprint">#</a> MSQ heap footprint

When determining partition boundaries, the heap footprint of the sketches that MSQ uses is capped at 10% of available memory or 300 MB, whichever is lower. Previously, the cap was strictly 300 MB.

https://github.com/apache/druid/pull/13274

### <a name="25.0.0-multi-stage-query-task-engine-msq-docker-improvement" href="#25.0.0-multi-stage-query-task-engine-msq-docker-improvement">#</a> MSQ Docker improvement

Enabled MSQ task query engine for Docker by default.

https://github.com/apache/druid/pull/13069


### <a name="25.0.0-multi-stage-query-task-engine-improved-msq-warnings" href="#25.0.0-multi-stage-query-task-engine-improved-msq-warnings">#</a> Improved MSQ warnings

For disallowed MSQ warnings of certain types, the warning is now surfaced as the error.

https://github.com/apache/druid/pull/13198

### <a name="25.0.0-multi-stage-query-task-engine-added-support-for-indexspec" href="#25.0.0-multi-stage-query-task-engine-added-support-for-indexspec">#</a> Added support for indexSpec

The MSQ task engine now supports the `indexSpec` context parameter. This context parameter can also be configured through the web console.

https://github.com/apache/druid/pull/13275

### <a name="25.0.0-multi-stage-query-task-engine-added-task-start-status-to-the-worker-report" href="#25.0.0-multi-stage-query-task-engine-added-task-start-status-to-the-worker-report">#</a> Added task start status to the worker report

Added `pendingTasks` and `runningTasks` fields to the worker report for the MSQ task engine.
See [Query task status information](#query-task-status-information) for related web console changes.

https://github.com/apache/druid/pull/13263

### <a name="25.0.0-multi-stage-query-task-engine-improved-handling-of-secrets" href="#25.0.0-multi-stage-query-task-engine-improved-handling-of-secrets">#</a> Improved handling of secrets

When MSQ submits tasks containing SQL with sensitive keys, the keys can get logged in the file.
Druid now masks the sensitive keys in the log files using regular expressions.

https://github.com/apache/druid/pull/13231

### <a name="25.0.0-multi-stage-query-task-engine-query-history" href="#25.0.0-multi-stage-query-task-engine-query-history">#</a> Query history

Multi-stage queries no longer show up in the Query history dialog. They are still available in the **Recent query tasks** panel.


### <a name="25.0.0-query-engine-use-worker-number-to-communicate-between-tasks" href="#25.0.0-query-engine-use-worker-number-to-communicate-between-tasks">#</a> Use worker number to communicate between tasks

Changed the way WorkerClient communicates between the worker tasks, to abstract away the complexity of resolving the `workerNumber` to the `taskId` from the callers.
Once the WorkerClient writes it's outputs to the durable storage, it adds a file with `__success` in the `workerNumber` output directory for that stage and with its `taskId`. This allows you to determine the worker, which has successfully written its outputs to the durable storage, and differentiate from the partial outputs by orphan or failed worker tasks.

https://github.com/apache/druid/pull/13062

### <a name="25.0.0-query-engine-sketch-merging-mode" href="#25.0.0-query-engine-sketch-merging-mode">#</a> Sketch merging mode

When a query requires key statistics to generate partition boundaries, key statistics are gathered by the workers while reading rows from the datasource.You can now configure whether the MSQ task engine does this task in parallel or sequentially. Configure the behavior using `clusterStatisticsMergeMode` context parameter. For more information, see [Sketch merging mode](https://druid.apache.org/docs/latest/multi-stage-query/reference.html#sketch-merging-mode).

https://github.com/apache/druid/pull/13205 

## <a name="25.0.0-querying" href="#25.0.0-querying">#</a> Querying

### <a name="25.0.0-querying-http-response-headers" href="#25.0.0-querying-http-response-headers">#</a> HTTP response headers

Exposed HTTP response headers for SQL queries. 

https://github.com/apache/druid/pull/13052

### <a name="25.0.0-querying-enabled-async-reads-for-jdbc" href="#25.0.0-querying-enabled-async-reads-for-jdbc">#</a> Enabled async reads for JDBC

Prevented JDBC timeouts on long queries by returning empty batches when a batch fetch takes too long. Uses an async model to run the result fetch concurrently with JDBC requests.

https://github.com/apache/druid/pull/13196

### <a name="25.0.0-querying-enabled-composite-approach-for-checking-in-filter-values-set-in-column-dictionary" href="#25.0.0-querying-enabled-composite-approach-for-checking-in-filter-values-set-in-column-dictionary">#</a> Enabled composite approach for checking in-filter values set in column dictionary

To accommodate large value sets arising from large in-filters or from joins pushed down as in-filters, Druid now uses sorted merge algorithm for merging the set and dictionary for larger values.

https://github.com/apache/druid/pull/13133

### <a name="25.0.0-querying-added-new-configuration-keys-to-query-context-security-model" href="#25.0.0-querying-added-new-configuration-keys-to-query-context-security-model">#</a> Added new configuration keys to query context security model

Added the following configuration properties that refine the query context security model controlled by `druid.auth.authorizeQueryContextParams`:

* `druid.auth.unsecuredContextKeys`: A JSON list of query context keys that do not require a security check.
* `druid.auth.securedContextKeys`: A JSON list of query context keys that do require a security check.

If both are set, `unsecuredContextKeys` acts as exceptions to `securedContextKeys`..

https://github.com/apache/druid/pull/13071

## <a name="25.0.0-metrics" href="#25.0.0-metrics">#</a> Metrics

### <a name="25.0.0-metrics-improved-metric-reporting" href="#25.0.0-metrics-improved-metric-reporting">#</a> Improved metric reporting

Improved global-cached-lookups metric reporting.

https://github.com/apache/druid/pull/13219


### <a name="25.0.0-metrics-new-metric-for-segment-handoff" href="#25.0.0-metrics-new-metric-for-segment-handoff">#</a> New metric for segment handoff

`segment/handoff/time` captures the total time taken for handoff for a given set of published segments.

https://github.com/apache/druid/pull/13238 

### <a name="25.0.0-metrics-new-metrics-for-segment-allocation-" href="#25.0.0-metrics-new-metrics-for-segment-allocation-">#</a> New metrics for segment allocation 

Segment allocation can now be performed in batches, which can improve performance and decrease ingestion lag. For more information about this change, see [Segment batch allocation](#segment-batch-allocation). The following metrics correspond to those changes.

Metrics emitted for a batch segment allocate request (dims: dataSource, taskActionType=segmentAllocate):

- `task/action/batch/runTime`: Milliseconds taken to execute a batch of task actions. Currently only being emitted for [batched `segmentAllocate` actions
- `task/action/batch/queueTime`: Milliseconds spent by a batch of task actions in queue. Currently only being emitted for [batched `segmentAllocate` actions](#segment-batch-allocation).
- `task/action/batch/size`: Number of task actions in a batch that was executed during the emission period. Currently only being emitted for [batched `segmentAllocate` actions](#segment-batch-allocation).
- `task/action/batch/attempts`: Number of execution attempts for a single batch of task actions. Currently only being emitted for [batched `segmentAllocate` actions](#segment-batch-allocation). 

Metrics emitted for a single segment allocate request (dims: taskId, taskType, dataSource, taskActionType=segmentAllocate):

- `task/action/failed/count`: Number of task actions that failed during the emission period. Currently only being emitted for [batched `segmentAllocate` actions](#segment-batch-allocation).
- `task/action/success/count`: Number of task actions that were executed successfully during the emission period. Currently only being emitted for [batched `segmentAllocate` actions](#segment-batch-allocation).

https://github.com/apache/druid/pull/13369
https://github.com/apache/druid/pull/13503

### <a name="25.0.0-metrics-new-metrics-for-streaming-ingestion" href="#25.0.0-metrics-new-metrics-for-streaming-ingestion">#</a> New metrics for streaming ingestion

The following metrics related to streaming ingestion have been added:

- `ingest/kafka/partitionLag`: Partition-wise lag between the offsets consumed by the Kafka indexing tasks and latest offsets in Kafka brokers. 
- `ingest/kinesis/partitionLag/time`: Partition-wise lag time in milliseconds between the current message sequence number consumed by the Kinesis indexing tasks and latest sequence number in Kinesis.
- `ingest/pause/time`: Milliseconds spent by a task in a paused state without ingesting.|dataSource, taskId| < 10 seconds.|

https://github.com/apache/druid/pull/13331
https://github.com/apache/druid/pull/13313

### <a name="25.0.0-metrics-taskactiontype-dimension-for-task%2Faction%2Frun%2Ftime-metric" href="#25.0.0-metrics-taskactiontype-dimension-for-task%2Faction%2Frun%2Ftime-metric">#</a> taskActionType dimension for task/action/run/time metric

The `task/action/run/time` metric for the Indexing service now includes the `taskActionType` dimension.

https://github.com/apache/druid/pull/13333


## <a name="25.0.0-nested-columns" href="#25.0.0-nested-columns">#</a> Nested columns

### <a name="25.0.0-nested-columns-nested-columns-performance-improvement" href="#25.0.0-nested-columns-nested-columns-performance-improvement">#</a> Nested columns performance improvement

Improved `NestedDataColumnSerializer` to no longer explicitly write null values to the field writers for the missing values of every row. Instead, passing the row counter is moved to the field writers so that they can backfill null values in bulk.

https://github.com/apache/druid/pull/13101

### <a name="25.0.0-nested-columns-support-for-more-formats" href="#25.0.0-nested-columns-support-for-more-formats">#</a> Support for more formats

Druid nested columns and the associated JSON transform functions now support Avro, ORC, and Parquet.

https://github.com/apache/druid/pull/13325 

https://github.com/apache/druid/pull/13375 

### <a name="25.0.0-nested-columns-refactored-a-datasource-before-unnest-" href="#25.0.0-nested-columns-refactored-a-datasource-before-unnest-">#</a> Refactored a datasource before unnest 

When data requires "flattening" during processing, the operator now takes in an array and then flattens the array into N (N=number of elements in the array) rows where each row has one of the values from the array.

https://github.com/apache/druid/pull/13085

## <a name="25.0.0-ingestion" href="#25.0.0-ingestion">#</a> Ingestion

### <a name="25.0.0-ingestion-improved-filtering-for-cloud-objects" href="#25.0.0-ingestion-improved-filtering-for-cloud-objects">#</a> Improved filtering for cloud objects

You can now stop at arbitrary subfolders using glob syntax in the `ioConfig.inputSource.filter` field for native batch ingestion from cloud storage, such as S3. 

https://github.com/apache/druid/pull/13027


### <a name="25.0.0-ingestion-async-task-client-for-streaming-ingestion" href="#25.0.0-ingestion-async-task-client-for-streaming-ingestion">#</a> Async task client for streaming ingestion

You can now use asynchronous communication with indexing tasks by setting `chatAsync` to true in the `tuningConfig`. Enabling asynchronous communication means that the `chatThreads` property is ignored.

https://github.com/apache/druid/pull/13354 

### <a name="25.0.0-ingestion-improved-control-for-how-druid-reads-json-data-for-streaming-ingestion" href="#25.0.0-ingestion-improved-control-for-how-druid-reads-json-data-for-streaming-ingestion">#</a> Improved control for how Druid reads JSON data for streaming ingestion

You can now better control how Druid reads JSON data for streaming ingestion by setting the following fields in the input format specification:

* `assumedNewlineDelimited` to parse lines of JSON independently.
* `useJsonNodeReader` to retain valid JSON events when parsing multi-line JSON events when a parsing exception occurs.

The web console has been updated to include these options.

https://github.com/apache/druid/pull/13089

### <a name="25.0.0-ingestion-when-a-kafka-stream-becomes-inactive%2C-prevent-supervisor-from-creating-new-indexing-tasks" href="#25.0.0-ingestion-when-a-kafka-stream-becomes-inactive%2C-prevent-supervisor-from-creating-new-indexing-tasks">#</a> When a Kafka stream becomes inactive, prevent Supervisor from creating new indexing tasks

Added Idle feature to `SeekableStreamSupervisor` for inactive stream.

https://github.com/apache/druid/pull/13144

### <a name="25.0.0-ingestion-kafka-consumer-improvement" href="#25.0.0-ingestion-kafka-consumer-improvement">#</a> Kafka Consumer improvement

You can now configure the Kafka Consumer's custom deserializer after its instantiation.

https://github.com/apache/druid/pull/13097

### <a name="25.0.0-ingestion-kafka-supervisor-logging" href="#25.0.0-ingestion-kafka-supervisor-logging">#</a> Kafka supervisor logging

Kafka supervisor logs are now less noisy. The supervisors now log events at the DEBUG level instead of INFO. 

https://github.com/apache/druid/pull/13392

### <a name="25.0.0-ingestion-fixed-overlord-leader-election" href="#25.0.0-ingestion-fixed-overlord-leader-election">#</a> Fixed Overlord leader election

Fixed a problem where Overlord leader election failed due to lock reacquisition issues. Druid now fails these tasks and clears all locks so that the Overlord leader election isn't blocked.

https://github.com/apache/druid/pull/13172

### <a name="25.0.0-ingestion-support-for-inline-protobuf-descriptor" href="#25.0.0-ingestion-support-for-inline-protobuf-descriptor">#</a> Support for inline protobuf descriptor

Added a new `inline` type `protoBytesDecoder` that allows a user to pass inline the contents of a Protobuf descriptor file, encoded as a Base64 string.

https://github.com/apache/druid/pull/13192

### <a name="25.0.0-ingestion-duplicate-notices" href="#25.0.0-ingestion-duplicate-notices">#</a> Duplicate notices

For streaming ingestion, notices that are the same as one already in queue won't be enqueued. This will help reduce notice queue size. 

https://github.com/apache/druid/pull/13334


### <a name="25.0.0-ingestion-sampling-from-stream-input-now-respects-the-configured-timeout" href="#25.0.0-ingestion-sampling-from-stream-input-now-respects-the-configured-timeout">#</a> Sampling from stream input now respects the configured timeout

Fixed a problem where sampling from a stream input, such as Kafka or Kinesis, failed to respect the configured timeout when the stream had no records available. You can now set the maximum amount of time in which the entry iterator will return results.

https://github.com/apache/druid/pull/13296

### <a name="25.0.0-ingestion-streaming-tasks-resume-on-overlord-switch" href="#25.0.0-ingestion-streaming-tasks-resume-on-overlord-switch">#</a> Streaming tasks resume on Overlord switch

Fixed a problem where streaming ingestion tasks continued to run until their duration elapsed after the Overlord leader had issued a pause to the tasks. Now, when the Overlord switch occurs right after it has issued a pause to the task, the task remains in a paused state even after the Overlord re-election.

https://github.com/apache/druid/pull/13223

### <a name="25.0.0-ingestion-fixed-parquet-list-conversion" href="#25.0.0-ingestion-fixed-parquet-list-conversion">#</a> Fixed Parquet list conversion

Fixes an issue with Parquet list conversion, where lists of complex objects could unexpectedly be wrapped in an extra object, appearing as `[{"element":<actual_list_element>},{"element":<another_one>}...]` instead of the direct list. This changes the behavior of the parquet reader for lists of structured objects to be consistent with other parquet logical list conversions. The data is now fetched directly, more closely matching its expected structure.

https://github.com/apache/druid/pull/13294

### <a name="25.0.0-ingestion-introduced-a-tree-type-to-flattenspec" href="#25.0.0-ingestion-introduced-a-tree-type-to-flattenspec">#</a> Introduced a tree type to flattenSpec

Introduced a `tree` type to `flattenSpec`. In the event that a simple hierarchical lookup is required, the `tree` type allows for faster JSON parsing than `jq` and `path` parsing types.

https://github.com/apache/druid/pull/12177

## <a name="25.0.0-operations" href="#25.0.0-operations">#</a> Operations

### <a name="25.0.0-operations-compaction" href="#25.0.0-operations-compaction">#</a> Compaction

Compaction behavior has changed to improve the amount of time it takes and disk space it takes:

- When segments need to be fetched, download them one at a time and delete them when Druid is done with them. This still takes time but minimizes the required disk space.
- Don't fetch segments on the main compact task when they aren't needed. If the user provides a full `granularitySpec`, `dimensionsSpec`, and `metricsSpec`, Druid skips fetching segments.

For more information, see the documentation on [Compaction](https://druid.apache.org/docs/latest/data-management/compaction.html) and [Automatic compaction](https://druid.apache.org/docs/latest/data-management/automatic-compaction.html).

https://github.com/apache/druid/pull/13280

### <a name="25.0.0-operations-idle-configs-for-the-supervisor" href="#25.0.0-operations-idle-configs-for-the-supervisor">#</a> Idle configs for the Supervisor

You can now set the Supervisor to idle, which is useful in cases where freeing up slots so that autoscaling can be more effective.

To configure the idle behavior, use the following properties:

| Property | Description | Default |
| - | - | -|
|`druid.supervisor.idleConfig.enabled`| (Cluster wide) If `true`, supervisor can become idle if there is no data on input stream/topic for some time.|false|
|`druid.supervisor.idleConfig.inactiveAfterMillis`| (Cluster wide) Supervisor is marked as idle if all existing data has been read from input topic and no new data has been published for `inactiveAfterMillis` milliseconds.|`600_000`|
| `inactiveAfterMillis` | (Individual Supervisor) Supervisor is marked as idle if all existing data has been read from input topic and no new data has been published for `inactiveAfterMillis` milliseconds. | no (default == `600_000`) |

https://github.com/apache/druid/pull/13311

### <a name="25.0.0-operations-improved-supervisor-termination" href="#25.0.0-operations-improved-supervisor-termination">#</a> Improved supervisor termination

Fixed issues with delayed supervisor termination during certain transient states.

https://github.com/apache/druid/pull/13072


### <a name="25.0.0-operations-backoff-for-httppostemitter" href="#25.0.0-operations-backoff-for-httppostemitter">#</a> Backoff for HttpPostEmitter

The `HttpPostEmitter` option now has a backoff. This means that there should be less noise in the logs and lower CPU usage if you use this option for logging. 

https://github.com/apache/druid/pull/12102

### <a name="25.0.0-operations-dumpsegment-tool-for-nested-columns" href="#25.0.0-operations-dumpsegment-tool-for-nested-columns">#</a> DumpSegment tool for nested columns

The DumpSegment tool can now be used on nested columns with the `--dump nested` option. 

For more information, see [dump-segment tool](https://druid.apache.org/docs/latest/operations/dump-segment).

https://github.com/apache/druid/pull/13356

### <a name="25.0.0-operations-segment-loading-and-balancing" href="#25.0.0-operations-segment-loading-and-balancing">#</a> Segment loading and balancing

#### <a name="25.0.0-operations-segment-loading-and-balancing-segment-batch-allocation" href="#25.0.0-operations-segment-loading-and-balancing-segment-batch-allocation">#</a> Segment batch allocation

Segment allocation on the Overlord can take some time to finish, which can cause ingestion lag while a task waits for segments to be allocated.  Performing segment allocation in batches can help improve performance.

There are two new properties that affect how Druid performs segment allocation:

| Property | Description | Default | 
| - | - | - |
|`druid.indexer.tasklock.batchSegmentAllocation`| If set to true, Druid performs segment allocate actions in batches to improve throughput and reduce the average `task/action/run/time`. See [batching `segmentAllocate` actions](https://druid.apache.org/docs/latest/ingestion/tasks.html#batching-segmentallocate-actions) for details.|false|
|`druid.indexer.tasklock.batchAllocationWaitTime`|Number of milliseconds after Druid adds the first segment allocate action to a batch, until it executes the batch. Allows the batch to add more requests and improve the average segment allocation run time. This configuration takes effect only if `batchSegmentAllocation` is enabled.|500|

In addition to these properties, there are new metrics to track batch segment allocation. For more information, see [New metrics for segment  allocation](#new-metrics-for-segment-allocation).

For more information, see the following:
- [Overlord operations](https://druid.apache.org/docs/latest/configuration/index.html#overlord-operations)
- [Task actions and Batching `segmentAllocate` actions](https://druid.apache.org/docs/latest/ingestion/tasks.html#task-actions)

https://github.com/apache/druid/pull/13369
https://github.com/apache/druid/pull/13503

#### <a name="25.0.0-operations-segment-loading-and-balancing-cachingcost-balancer-strategy" href="#25.0.0-operations-segment-loading-and-balancing-cachingcost-balancer-strategy">#</a> cachingCost balancer strategy

The `cachingCost` balancer strategy now behaves more similarly to cost strategy. When computing the cost of moving a segment to a server, the following calculations are performed:

- Subtract the self cost of a segment if it is being served by the target server
- Subtract the cost of segments that are marked to be dropped

https://github.com/apache/druid/pull/13321

#### <a name="25.0.0-operations-segment-loading-and-balancing-segment-assignment" href="#25.0.0-operations-segment-loading-and-balancing-segment-assignment">#</a> Segment assignment

You can now use a round-robin segment strategy to speed up initial segment assignments.

Set `useRoundRobinSegmentAssigment` to `true` in the Coordinator dynamic config to enable this feature.

https://github.com/apache/druid/pull/13367

#### <a name="25.0.0-operations-segment-loading-and-balancing-segment-load-queue-peon" href="#25.0.0-operations-segment-loading-and-balancing-segment-load-queue-peon">#</a> Segment load queue peon

Batch sampling is now the default method for sampling segments during balancing as it performs significantly better than the alternative when there is a large number of used segments in the cluster.

As part of this change, the following have been deprecated and will be removed in future releases:

- coordinator dynamic config `useBatchedSegmentSampler`
- coordinator dynamic config `percentOfSegmentsToConsiderPerMove`
- non-batch method of sampling segments used by coordinator duty `BalanceSegments`

The unused coordinator property `druid.coordinator.loadqueuepeon.repeatDelay` has been removed.

Use only `druid.coordinator.loadqueuepeon.http.repeatDelay` to configure repeat delay for the HTTP-based segment loading queue.

https://github.com/apache/druid/pull/13391

#### <a name="25.0.0-operations-segment-loading-and-balancing-segment-replication" href="#25.0.0-operations-segment-loading-and-balancing-segment-replication">#</a> Segment replication

Improved the process of checking server inventory to prevent over-replication of segments during segment balancing.

https://github.com/apache/druid/pull/13114

### <a name="25.0.0-operations-provided-service-specific-log4j-overrides-in-containerized-deployments" href="#25.0.0-operations-provided-service-specific-log4j-overrides-in-containerized-deployments">#</a> Provided service specific log4j overrides in containerized deployments

Provided an option to override log4j configs setup at the service level directories so that it works with Druid-operator based deployments.

https://github.com/apache/druid/pull/13020

### <a name="25.0.0-operations-various-docker-improvements" href="#25.0.0-operations-various-docker-improvements">#</a> Various Docker improvements

* Updated Docker to run with JRE 11 by default.
* Updated Docker to use [`gcr.io/distroless/java11-debian11`](https://github.com/GoogleContainerTools/distroless) image as base by default.
* Enabled Docker buildkit cache to speed up building.
* Downloaded [`bash-static`](https://github.com/robxu9/bash-static) to the Docker image so that scripts that require bash can be executed.
* Bumped builder image from `3.8.4-jdk-11-slim` to `3.8.6-jdk-11-slim`.
* Switched busybox from `amd64/busybox:1.30.0-glibc` to `busybox:1.35.0-glibc`.
* Added support to build arm64-based image.

https://github.com/apache/druid/pull/13059



### <a name="25.0.0-operations-enabled-cleaner-json-for-various-input-sources-and-formats" href="#25.0.0-operations-enabled-cleaner-json-for-various-input-sources-and-formats">#</a> Enabled cleaner JSON for various input sources and formats

Added `JsonInclude` to various properties, to avoid population of default values in serialized JSON.

https://github.com/apache/druid/pull/13064

### <a name="25.0.0-operations-improved-direct-memory-check-on-startup" href="#25.0.0-operations-improved-direct-memory-check-on-startup">#</a> Improved direct memory check on startup

Improved direct memory check on startup by providing better support for Java 9+ in `RuntimeInfo`, and clearer log messages where validation fails.

https://github.com/apache/druid/pull/13207


### <a name="25.0.0-operations-improved-the-run-time-of-the-markasunusedovershadowedsegments-duty" href="#25.0.0-operations-improved-the-run-time-of-the-markasunusedovershadowedsegments-duty">#</a> Improved the run time of the MarkAsUnusedOvershadowedSegments duty

Improved the run time of the `MarkAsUnusedOvershadowedSegments` duty by iterating over all overshadowed segments and marking segments as unused in batches.

https://github.com/apache/druid/pull/13287


## <a name="25.0.0-web-console" href="#25.0.0-web-console">#</a> Web console

### <a name="25.0.0-web-console-delete-an-interval" href="#25.0.0-web-console-delete-an-interval">#</a> Delete an interval

You can now pick an interval to delete from a dropdown in the kill task dialog.  

https://github.com/apache/druid/pull/13431

### <a name="25.0.0-web-console-removed-the-old-query-view" href="#25.0.0-web-console-removed-the-old-query-view">#</a> Removed the old query view

The old query view is removed. Use the new query view with tabs.
For more information, see [Web console](https://druid.apache.org/docs/latest/operations/web-console.html#query).

https://github.com/apache/druid/pull/13169

### <a name="25.0.0-web-console-filter-column-values-in-query-results" href="#25.0.0-web-console-filter-column-values-in-query-results">#</a> Filter column values in query results

The web console now allows you to add to existing filters for a selected column.

https://github.com/apache/druid/pull/13169

### <a name="25.0.0-web-console-ability-to-add-issue-comments" href="#25.0.0-web-console-ability-to-add-issue-comments">#</a> Ability to add issue comments

You can now add an issue comment in SQL, for example `--:ISSUE: this is an issue` that is rendered in red and prevents the SQL from running. The comments are used by the spec-to-SQL converter to indicate that something could not be converted.

https://github.com/apache/druid/pull/13136

### <a name="25.0.0-web-console-support-for-kafka-lookups" href="#25.0.0-web-console-support-for-kafka-lookups">#</a> Support for Kafka lookups

Added support for Kafka-based lookups rendering and input in the web console.

https://github.com/apache/druid/pull/13098

### <a name="25.0.0-web-console-improved-array-detection" href="#25.0.0-web-console-improved-array-detection">#</a> Improved array detection

Added better detection for arrays containing objects.

https://github.com/apache/druid/pull/13077

### <a name="25.0.0-web-console-updated-druid-query-toolkit-version" href="#25.0.0-web-console-updated-druid-query-toolkit-version">#</a> Updated Druid Query Toolkit version

[Druid Query Toolkit](https://www.npmjs.com/package/druid-query-toolkit) version 0.16.1 adds quotes to references in auto generated queries by default.

https://github.com/apache/druid/pull/13243

### <a name="25.0.0-web-console-query-task-status-information" href="#25.0.0-web-console-query-task-status-information">#</a> Query task status information

The web console now exposes a textual indication about running and pending tasks when a query is stuck due to lack of task slots.

https://github.com/apache/druid/pull/13291

## <a name="25.0.0-extensions" href="#25.0.0-extensions">#</a> Extensions

### <a name="25.0.0-extensions-big_sum-sql" href="#25.0.0-extensions-big_sum-sql">#</a> BIG_SUM SQL

#### <a name="25.0.0-extensions-big_sum-sql-big_sum-sql-function" href="#25.0.0-extensions-big_sum-sql-big_sum-sql-function">#</a> BIG_SUM SQL function

Added SQL function `BIG_SUM` that uses the [Compressed Big Decimal](https://github.com/apache/druid/pull/10705) Druid extension.

https://github.com/apache/druid/pull/13102

#### <a name="25.0.0-extensions-big_sum-sql-added-compressed-big-decimal-min-and-max-functions" href="#25.0.0-extensions-big_sum-sql-added-compressed-big-decimal-min-and-max-functions">#</a> Added Compressed Big Decimal min and max functions

Added min and max functions for Compressed Big Decimal and exposed these functions via SQL: BIG_MIN and BIG_MAX.

https://github.com/apache/druid/pull/13141


### <a name="25.0.0-extensions-extension-optimization" href="#25.0.0-extensions-extension-optimization">#</a> Extension optimization

Optimized the `compareTo` function in `CompressedBigDecimal`.

https://github.com/apache/druid/pull/13086

### <a name="25.0.0-extensions-compressedbigdecimal-cleanup-and-extension" href="#25.0.0-extensions-compressedbigdecimal-cleanup-and-extension">#</a> CompressedBigDecimal cleanup and extension

Removed unnecessary generic type from CompressedBigDecimal, added support for number input types, added support for reading aggregator input types directly (uningested data), and fixed scaling bug in buffer aggregator.

https://github.com/apache/druid/pull/13048


### <a name="25.0.0-extensions-support-for-kubernetes-discovery" href="#25.0.0-extensions-support-for-kubernetes-discovery">#</a> Support for Kubernetes discovery

Added `POD_NAME` and `POD_NAMESPACE` env variables to all Kubernetes Deployments and StatefulSets.
Helm deployment is now compatible with `druid-kubernetes-extension`.

https://github.com/apache/druid/pull/13262

## <a name="25.0.0-docs" href="#25.0.0-docs">#</a> Docs

### <a name="25.0.0-docs-jupyter-notebook-tutorials" href="#25.0.0-docs-jupyter-notebook-tutorials">#</a> Jupyter Notebook tutorials

We released our first Jupyter Notebook-based tutorial to learn the basics of the Druid API. Download the notebook and follow along with the tutorial to learn how to get basic cluster information, ingest data, and query data.
For more information, see [Jupyter Notebook tutorials](https://druid.apache.org/docs/latest/tutorials/tutorial-jupyter-index.html).

https://github.com/apache/druid/pull/13342  
https://github.com/apache/druid/pull/13345

## <a name="25.0.0-dependency-updates" href="#25.0.0-dependency-updates">#</a> Dependency updates

### <a name="25.0.0-dependency-updates-updated-kafka-version" href="#25.0.0-dependency-updates-updated-kafka-version">#</a> Updated Kafka version

Updated the Apache Kafka core dependency to version 3.3.1.

https://github.com/apache/druid/pull/13176

### <a name="25.0.0-dependency-updates-docker-improvements" href="#25.0.0-dependency-updates-docker-improvements">#</a> Docker improvements

Updated dependencies for the Druid image for Docker, including JRE 11. Docker BuildKit cache is enabled to speed up building.

https://github.com/apache/druid/pull/13059

## <a name="24-1-credits" href="#24-1-credits">#</a> Credits
Thanks to everyone who contributed to this release!

@317brian
@599166320
@a2l007
@abhagraw
@abhishekagarwal87
@adarshsanjeev
@adelcast
@AlexanderSaydakov
@amaechler
@AmatyaAvadhanula
@ApoorvGuptaAi
@arvindanugula
@asdf2014
@churromorales
@clintropolis
@cloventt
@cristian-popa
@cryptoe
@dampcake
@dependabot[bot]
@didip
@ektravel
@eshengit
@findingrish
@FrankChen021
@gianm
@hnakamor
@hosswald
@imply-cheddar
@jasonk000
@jon-wei
@Junge-401
@kfaraz
@LakshSingla
@mcbrewster
@paul-rogers
@petermarshallio
@rash67
@rohangarg
@sachidananda007
@santosh-d3vpl3x
@senthilkv
@somu-imply
@techdocsmith
@tejaswini-imply
@vogievetsky
@vtlim
@wcc526
@writer-jill
@xvrl
@zachjsh

