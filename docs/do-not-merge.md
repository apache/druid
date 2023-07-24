<!--Intentionally, there's no Apache license so that the GHA fails it. This file is not meant to be merged.

- https://github.com/apache/druid/pull/14266 - we removed input source security from 26 (https://github.com/apache/druid/pull/14003). Should we not include this in 27 release notes?

-->

Apache Druid 27.0.0 contains over $NUMBER_FEATURES new features, bug fixes, performance enhancements, documentation improvements, and additional test coverage from $NUMBER_OF_CONTRIBUTORS contributors.

[See the complete set of changes for additional details]($LINK_TO_RELEASE_MILESTONE).

Review the upgrade notes and incompatible changes before you upgrade to Druid 27.0.0.

# Highlights

<!-- HIGHLIGHTS H2. FOR EACH MAJOR FEATURE FOR THE RELEASE -->

## Query from deep storage

TBD

### New statements API

Added a new API /druid/v2/sql/statements/ which allows users to fetch results in an asynchronous manner.

[14416](https://github.com/apache/druid/pull/14416)

### New and updated fields in API to get results

The API response to get results now returns a `pages` field containing information on each page of the results status.
The `numRows` and `sizeInBytes` fields are renamed to `numTotalRows` and `totalSizeInBytes`, respectively.

[14512](https://github.com/apache/druid/pull/14512)


### Durable storage for results

MSQ can now write SELECT query results to durable storage. To do so, set the context flag `selectDestination:DURABLE_STORAGE` while issuing SELECT queries to MSQ.

[14527](https://github.com/apache/druid/pull/14527)

## Java 17 support

Druid now fully supports Java 17.
[14384](https://github.com/apache/druid/pull/14384)

## Array column types

GA + TBD

## Schema auto-discovery

GA + TBD

## Smart segment loading

[13197](https://github.com/apache/druid/pull/13197)

## Hadoop 2 support dropped

# Additional features and improvements

## MSQ task engine

### `maxInputBytesPerWorker` context parameter

The context parameter now denotes the estimated weighted size (in bytes) of the input to split on. The MSQ task engine now takes into account the input format and compression format instead of the actual file size reported by the file system.

The default value for the context parameter has also been changed. It is now 512 MiB (previously 10 GiB).

[14307](https://github.com/apache/druid/pull/14307)

### Improved query planning behavior

Druid now fails query planning if a CLUSTERED BY column contains descending order.
Previously, queries would successfully plan if any CLUSTERED BY columns contained descending order.

The MSQ fault, `InsertCannotOrderByDescending`, is deprecated. An INSERT or REPLACE query containing a CLUSTERED BY expression cannot be in descending order. Druid's segment generation code only supports ascending order. Instead of the fault, Druid now throws a query `ValidationException`.

[14436](https://github.com/apache/druid/pull/14436)

### SELECT query results

SELECT queries executed using MSQ now generate only a subset of the results in the query reports.
To fetch the complete result set, run the query using the native engine.

[14370](https://github.com/apache/druid/pull/14370)


### Other MSQ improvements

- The same aggregator can now have two output names [14367](https://github.com/apache/druid/pull/14367)
- Changed the default `clusterStatisticsMergeMode` to SEQUENTIAL (#14310)[https://github.com/apache/druid/pull/14310]
- Added a query context parameter `MultiStageQueryContext` to determine whether the result of an MSQ select query is limited (#14476)[https://github.com/apache/druid/pull/14476]
- Enabled using functions as inputs for `index` and `length` parameters (#14480)[https://github.com/apache/druid/pull/14480]

## Ingestion

### New property for task completion updates

The new property `druid.indexer.queue.taskCompleteHandlerNumThreads` controls the number of threads used by the Overlord `TaskQueue` to handle task completion updates received from the workers.

The following metrics have been added:
* `task/status/queue/count`: Monitors the number of queued items
* `task/status/updated/count`: Monitors the number of processed items

[14533](https://github.com/apache/druid/pull/14533)

### Improved response to max_allowed_packet limit

If the Overlord fails to insert a task into the metadata because of a payload that exceeds the `max_allowed_packet` limit, the response now returns `400 Bad request` instead of `500 Internal server error`. This prevents an `index_parallel` task from retrying the insertion of a bad sub-task indefinitely and causes it to fail immediately.

[14271](https://github.com/apache/druid/pull/14271)

### Improved handling of mixed type arrays

Druid now handles mixed type arrays such as `[["a", "b", "c"], {"x": 123}]` as `ARRAY<COMPLEX<json>` rather than throwing an incompatible type exception.

[14438](https://github.com/apache/druid/pull/14438)

### Other ingestion improvements

* A negative streaming ingestion lag is no longer emitted as a result of stale offsets. [14292](https://github.com/apache/druid/pull/14292)
* Removed double synchronization on simple map operations in Kubernetes task runner. [14435](https://github.com/apache/druid/pull/14435)
* Kubernetes overlord extension now cleans up the job if the task pod fails to come up in time. [14425](https://github.com/apache/druid/pull/14425)

## Querying

### New function for regular expression replacement

The new function `REGEXP_REPLACE` allows you to replace all instances of a pattern with a replacement string.

[14460](https://github.com/apache/druid/pull/14460)

### Query results directory

Druid now supports a `query-results` directory in durable storage to store query results after the task finishes. The auto cleaner does not remove this directory unless the task ID is not known to the Overlord.

[14446](https://github.com/apache/druid/pull/14446)

### Limit for subquery results by memory usage

Users can now add a guardrail to prevent subquery’s results from exceeding the set number of bytes by setting `druid.server.http.maxSubqueryRows` in the Broker's config or `maxSubqueryRows` in the query context. This feature is experimental for now and would default back to row-based limiting in case it fails to get the accurate size of the results consumed by the query.

[13952](https://github.com/apache/druid/pull/13952)

### HLL and Theta sketch estimates

You can now use `HLL_SKETCH_ESTIMATE` and `THETA_SKETCH_ESTIMATE` as expressions. These estimates work on sketch columns and have the same behavior as `postAggs`.

[14312](https://github.com/apache/druid/pull/14312)

### String encoding parameter for HLL sketches

HLL sketches now accept a new `stringEncoding` parameter to build sketches with UTF-8 bytes.

[11201](https://github.com/apache/druid/pull/11201)

### EARLIEST_BY and LATEST_BY signatures

Updated EARLIEST_BY and LATEST_BY function signatures as follows:

* Changed `EARLIEST(expr, timeColumn)` to `EARLIEST_BY(expr, timeColumn)`  
* Changed `LATEST(expr, timeColumn)` to `LATEST_BY(expr, timeColumn)`

[14352](https://github.com/apache/druid/pull/14352)


### Exposed Druid functions

Exposed information about Druid functions in the `INFORMATION_SCHEMA.ROUTINES` table. The table contains the following columns:

* ROUTINE_CATALOG: Name of the database. Always set to druid.
* ROUTINE_SCHEMA: Name of the schema. Always set to INFORMATION_SCHEMA.
* ROUTINE_NAME: Name of the function or operator. For example: APPROX_COUNT_DISTINCT_DS_THETA.
* ROUTINE_TYPE: Type of routine. Always set to FUNCTION.
* IS_AGGREGATOR: Determines if the routine is an aggregator. Returns YES for aggregator functions; NO for scalar functions.
* SIGNATURES: Possible signatures for the routine as a string.

[14378](https://github.com/apache/druid/pull/14378)

### New Broker configuration for SQL schema migrations

A new broker configuration, `druid.sql.planner.metadataColumnTypeMergePolicy`, adds configurable modes to how column types are computed for the SQL table schema when faced with differences between segments.

The new `leastRestrictive` mode chooses the most appropriate type that data across all segments can best be coerced into and is the default behavior. This is a subtle behavior change around when segment driven schema migrations will take effect for the SQL schema:

- With `leastRestrictive` mode, the schema only updates once all segments are reindexed to the new type.
- With `latestInterval` mode, the SQL schema gets updated as soon as the first job with the new schema publishes segments in the latest time interval of the data.

`leastRestrictive` can have better query time behavior and eliminates some query time errors that can occur when using `latestInterval`.

[14319](https://github.com/apache/druid/pull/14319)

### EXPLAIN PLAN improvements

The EXPLAIN PLAN result includes a new column `ATTRIBUTES` that describes the attributes of a query.
See [SQL query translation](docs/querying/sql-translation.md) for more information and examples.

[14391](https://github.com/apache/druid/pull/14391)


## Metrics and monitoring

### Added a new SysMonitorOshi monitor

Added a new monitor `SysMonitorOshi` to replace `SysMonitor`. The new monitor has a wider support for different machine architectures including ARM instances. We recommend switching to `SysMonitorOshi` as `SysMonitor` is now deprecated and will be removed in future releases.

[14359](https://github.com/apache/druid/pull/14359)

### Monitor for Overlord and Coordinator service health

Added a new monitor `ServiceStatusMonitor` to monitor the service health of the Overlord and Coordinator.

[14443](https://github.com/apache/druid/pull/14443)

### New segment metrics

The following metrics are now available for Brokers:

|Metric|Description|Normal value|
|------|-----------|------------|
|`segment/metadatacache/refresh/count`|Number of segments to refresh in broker segment metadata cache. Emitted once per refresh per datasource.|`dataSource`|
|`segment/metadatacache/refresh/time`|Time taken to refresh segments in broker segment metadata cache. Emitted once per refresh per datasource.|`dataSource`|

[14453](https://github.com/apache/druid/pull/14453)

### New metrics for task completion updates

The following metrics have been added:
* `task/status/queue/count`: Monitors the number of queued items
* `task/status/updated/count`: Monitors the number of processed items

[14533](https://github.com/apache/druid/pull/14533)

### Added `groupId` to Overlord task metrics

Added `groupId` to task metrics emitted by the Overlord. This is helpful for grouping metrics like task/run/time by a single task group, such as a single compaction task or a single MSQ query.

[14402](https://github.com/apache/druid/pull/14402)

## Cluster management

### Enabled cancellation and prioritization of load queue items

Revamped Coordinator to make it more stable and user-friendly. This is accompanied by several bug fixes, logging and metric improvements, and a whole new range of capabilities.

Coordinator now supports a `smartSegmentLoading` mode, which is enabled by default. When enabled, users don't need to specify any of the following dynamic configs as they would be ignored by the coordinator. Instead, the coordinator computes the optimal values of the following configs at run time to best utilize coordinator runs:

* `maxSegmentsInNodeLoadingQueue`
* `maxSegmentsToMove`
* `replicationThrottleLimit`
* `useRoundRobinSegmentAssignment`
* `useBatchedSegmentSampler`
* `emitBalancingStats`

These configs are now deprecated and will be removed in subsequent releases.

Coordinator is now capable of prioritization and cancellation of items in segment load queues. This means that the coordinator now reacts faster to changes in the cluster and makes better segment assignment decisions.

[13197](https://github.com/apache/druid/pull/13197)

### Removed unused Coordinator dynamic configuration properties

The following Coordinator dynamic configs have been removed:

* `emitBalancingStats`: Stats for errors encountered while balancing will always be emitted. Other debugging stats will not be emitted but can be logged by setting the appropriate `debugDimensions`.
* `useBatchedSegmentSampler` and `percentOfSegmentsToConsiderPerMove`: Batched segment sampling is now the standard and will always be on.

[14524](https://github.com/apache/druid/pull/14524)

### Improved segment balancing strategy

The `cost` balancer strategy performs much better now and is capable of moving more segments in a single Coordinator run. These improvements were made by borrowing ideas from the `cachingCost` strategy.

The `cachingCost` strategy itself has been known to cause issues and is now deprecated. It is likely to be removed in future Druid releases and must not be used on any cluster.

[14484](https://github.com/apache/druid/pull/14484)

### Enabled empty tiered replicants for load rules

Druid now allows empty tiered replicants in load rules.

[14432](https://github.com/apache/druid/pull/14432)

### Changed Coordinator configs values

The following Coordinator dynamic configs have new default values:

* `maxsegmentsInNodeLoadingQueue` : 500, previously 100
* `maxSegmentsToMove`: 100, previously 5
* `replicationThrottleLimit`: 500, previously 10

These new defaults can improve performance for most use cases.

[14269](https://github.com/apache/druid/pull/14269)

### Improved error messages

Introduced a new unified exception, `DruidException`, for surfacing errors. It is partially compatible with the old way of reporting error messages. Response codes remain the same, all fields that previously existed on the response will continue to exist and be populated, including `errorMessage`. Some error messages have changed to be more consumable by humans and some cases have the message restructured. There should be no impact to the response codes.

[14004](https://github.com/apache/druid/pull/14004)

## Web console

### Supervisors and Tasks

Replaced the **Ingestion** view with two views: **Supervisors** and **Tasks**.

[14395](https://github.com/apache/druid/pull/14395)

### Added replication factor column

Added a new virtual column `replication_factor` to the `sys.segments` table. This returns the total number of replicants of the segment across all tiers. The column is set to -1 if the information is not available.

[14403](https://github.com/apache/druid/pull/14403)

### Other web console improvements

## Extensions

### Improved segment metadata for Kafka emitter extension

The Kafka emitter extension has been improved. You can now publish events related to segments and their metadata to Kafka.
You can set the new properties such as in the following example:

```properties
druid.emitter.kafka.event.types=["metrics", "alerts", "segment_metadata"]
druid.emitter.kafka.segmentMetadata.topic=foo
```

[14281](https://github.com/apache/druid/pull/14281)

## Dependency updates

The following dependencies have had their versions bumped:

* Apache DataSketches has been upgraded to 4.1.0. Additionally, the datasketches-memory component has been upgraded to version 2.2.0. [14430](https://github.com/apache/druid/pull/14430)
* Hadoop has been upgraded to version 3.3.6 [#14489](https://github.com/apache/druid/pull/14489)
* Avro has been upgraded to version 1.11.1 [#14440](https://github.com/apache/druid/pull/14440)

## Developer notes

[`org.apache.druid.common.exception.DruidException`](https://github.com/apache/druid/blob/27.0.0/processing/src/main/java/org/apache/druid/common/exception/DruidException.java#L28) is deprecated in favor of the more comprehensive [`org.apache.druid.error.DruidException`](https://github.com/apache/druid/blob/master/processing/src/main/java/org/apache/druid/error/DruidException.java).

[`org.apache.druid.metadata.EntryExistsException`](https://github.com/apache/druid/blob/27.0.0/processing/src/main/java/org/apache/druid/metadata/EntryExistsException.java) is deprecated and will be removed in a future release.

[14554](https://github.com/apache/druid/pull/14554)

# Upgrade notes and incompatible changes

## Upgrade notes

## Incompatible changes

### Removed property for setting max bytes for dimension lookup cache

`druid.processing.columnCache.sizeBytes` has been removed since it provided limited utility after a number of internal changes. Leaving this config is harmless, but it does nothing.

[14500](https://github.com/apache/druid/pull/14500)


# Credits
Thanks to everyone who contributed to this release!

<list of gh ids>

