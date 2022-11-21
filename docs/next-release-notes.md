# New features

## Updated Kafka support

Updated the Apache Kafka core dependency to version 3.3.1.

https://github.com/apache/druid/pull/13176

## Query engine

### BIG_SUM SQL function

Added SQL function `BIG_SUM` that uses the [Compressed Big Decimal](https://github.com/apache/druid/pull/10705) Druid extension.

https://github.com/apache/druid/pull/13102

### Added Compressed Big Decimal min and max functions

Added min and max functions for Compressed Big Decimal and exposed these functions via SQL: BIG_MIN and BIG_MAX.

https://github.com/apache/druid/pull/13141

### Metrics used to downsample bucket

Changed the way the MSQ task engine determines whether or not to downsample data, to improve accuracy. The task engine now uses the number of bytes instead of number of keys.

https://github.com/apache/druid/pull/12998

### MSQ Docker improvement

Enabled MSQ query engine for Docker by default.

https://github.com/apache/druid/pull/13069

### Improved MSQ warnings

For disallowed MSQ warnings of certain types, the warning is now surfaced as the error.

https://github.com/apache/druid/pull/13198

### Added support for indexSpec

The MSQ task engine now supports the `indexSpec` context parameter.

https://github.com/apache/druid/pull/13275

### Added task start status to the worker report

Added `pendingTasks` and `runningTasks` fields to the worker report for the MSQ task engine.
See [Query task status information](#query-task-status-information) for related web console changes.

https://github.com/apache/druid/pull/13263

### Improved handling of secrets

When MSQ submits tasks containing SQL with sensitive keys, the keys can get logged in the file.
Druid now masks the sensitive keys in the log files using regular expressions.

https://github.com/apache/druid/pull/13231

### Use worker number to communicate between tasks

Changed the way WorkerClient communicates between the worker tasks, to abstract away the complexity of resolving the `workerNumber` to the `taskId` from the callers.
Once the WorkerClient writes it's outputs to the durable storage, it adds a file with `__success` in the `workerNumber` output directory for that stage and with its `taskId`. This allows you to determine the worker, which has successfully written its outputs to the durable storage, and differentiate from the partial outputs by orphan or failed worker tasks.

https://github.com/apache/druid/pull/13062

## Querying

### Improvements to querying user experience

This release includes several improvements for querying:

* Exposed HTTP response headers for SQL queries (https://github.com/apache/druid/pull/13052)
* Added the `shouldFinalize` feature for HLL and quantiles sketches. Druid will no longer finalize aggregators when:
    - aggregators appear in the outer level of a query
    - aggregators are used as input to an expression or finalizing-field-access post-aggregator

    To provide backwards compatibility, we added a `sqlFinalizeOuterSketches` query context parameter that restores the old behavior (https://github.com/apache/druid/pull/13247)

### Enabled async reads for JDBC

Prevented JDBC timeouts on long queries by returning empty batches when a batch fetch takes too long. Uses an async model to run the result fetch concurrently with JDBC requests.

https://github.com/apache/druid/pull/13196

### Enabled composite approach for checking in-filter values set in column dictionary

To accommodate large value sets arising from large in-filters or from joins pushed down as in-filters, Druid now uses sorted merge algorithm for merging the set and dictionary for larger values.

https://github.com/apache/druid/pull/13133

### Added new configuration keys to query context security model

Added the following configuration keys that refine the query context security model controlled by `druid.auth.authorizeQueryContextParams`:
* `druid.auth.unsecuredContextKeys`: The set of query context keys that do not require a security check.
* `druid.auth.securedContextKeys`: The set of query context keys that do require a security check.

## Nested columns

### Refactored a data source before unnest 

When data requires "flattening" during processing, the operator now takes in an array and then flattens the array into N (N=number of elements in the array) rows where each row has one of the values from the array.

https://github.com/apache/druid/pull/13085

## Ingestion

### Improved control for how Druid reads JSON data for streaming ingestion

You can now better control how Druid reads JSON data for streaming ingestion by setting the following fields in the input format specification:

* `assumedNewlineDelimited` to parse lines of JSON independently.
* `useJsonNodeReader` to retain valid JSON events when parsing multi-line JSON events when a parsing exception occurs.

https://github.com/apache/druid/pull/13089

### Kafka Consumer improvement

Allowed Kafka Consumer's custom deserializer to be configured after its instantiation.

https://github.com/apache/druid/pull/13097

### Fixed Overlord leader election

Fixed a problem where Overlord leader election failed due to lock reacquisition issues. Druid now fails these tasks and clears all locks so that the Overlord leader election isn't blocked.

https://github.com/apache/druid/pull/13172

### Support for inline protobuf descriptor

Added a new `inline` type `protoBytesDecoder` that allows a user to pass inline the contents of a Protobuf descriptor file, encoded as a Base64 string.

https://github.com/apache/druid/pull/13192

### When a Kafka stream becomes inactive, prevent Supervisor from creating new indexing tasks

Added Idle feature to `SeekableStreamSupervisor` for inactive stream.

https://github.com/apache/druid/pull/13144

### Sampling from stream input now respects the configured timeout

Fixed a problem where sampling from a stream input, such as Kafka or Kinesis, failed to respect the configured timeout when the stream had no records available. You can now set the maximum amount of time in which the entry iterator will return results.

https://github.com/apache/druid/pull/13296

### Streaming tasks resume on Overlord switch

Fixed a problem where streaming ingestion tasks continued to run until their duration elapsed after the Overlord leader had issued a pause to the tasks. Now, when the Overlord switch occurs right after it has issued a pause to the task, the task remains in a paused state even after the Overlord re-election.

https://github.com/apache/druid/pull/13223

### Fixed Parquet list conversion

Fixes an issue with Parquet list conversion, where lists of complex objects could unexpectedly be wrapped in an extra object, appearing as `[{"element":<actual_list_element>},{"element":<another_one>}...]` instead of the direct list. This changes the behavior of the parquet reader for lists of structured objects to be consistent with other parquet logical list conversions. The data is now fetched directly, more closely matching its expected structure.

https://github.com/apache/druid/pull/13294

### Introduced a tree type to flattenSpec

Introduced a `tree` type to `flattenSpec`. In the event that a simple hierarchical lookup is required, the `tree` type allows for faster JSON parsing than `jq` and `path` parsing types.

https://github.com/apache/druid/pull/12177

## Operations

### Segment discovery

The default segment discovery method now uses HTTP instead of ZooKeeper.

https://github.com/apache/druid/pull/13092

### Memory estimates

The task context flag `useMaxMemoryEstimates` is now set to false by default to improve memory usage estimation.

https://github.com/apache/druid/pull/13178

### Docker improvements

Updated dependencies for the Druid image for Docker, including JRE 11. Docker BuildKit cache is enabled to speed up building.

https://github.com/apache/druid/pull/13059

### Segment replication

Improved the process of checking server inventory to prevent over-replication of segments during segment balancing.

https://github.com/apache/druid/pull/13114

### Kill tasks do not include markAsUnuseddone

When you kill a task, Druid no longer automatically marks segments as unused. You must explicitly mark them as unused with `POST /druid/coordinator/v1/datasources/{dataSourceName}/markUnused`. 
For more information, see the [API reference](https://druid.apache.org/docs/latest/operations/api-reference.html#coordinator)

https://github.com/apache/druid/pull/13104

### Nested columns performance improvement

Improved `NestedDataColumnSerializer` to no longer explicitly write null values to the field writers for the missing values of every row. Instead, passing the row counter is moved to the field writers so that they can backfill null values in bulk.

https://github.com/apache/druid/pull/13101

### Provided service specific log4j overrides in containerized deployments

Provided an option to override log4j configs setup at the service level directories so that it works with Druid-operator based deployments.

https://github.com/apache/druid/pull/13020

### Various Docker improvements

* Updated Docker to run with JRE 11 by default.
* Updated Docker to use [`gcr.io/distroless/java11-debian11`](https://github.com/GoogleContainerTools/distroless) image as base by default.
* Enabled Docker buildkit cache to speed up building.
* Downloaded [`bash-static`](https://github.com/robxu9/bash-static) to the Docker image so that scripts that require bash can be executed.
* Bumped builder image from `3.8.4-jdk-11-slim` to `3.8.6-jdk-11-slim`.
* Switched busybox from `amd64/busybox:1.30.0-glibc` to `busybox:1.35.0-glibc`.
* Added support to build arm64-based image.

https://github.com/apache/druid/pull/13059

### Improved supervisor termination

Fixed issues with delayed supervisor termination during certain transient states.

https://github.com/apache/druid/pull/13072

### Fixed a problem when running Druid with JDK11+

Export `com.sun.management.internal` when running Druid under JRE11 and JRE17.

https://github.com/apache/druid/pull/13068

### Enabled cleaner JSON for various input sources and formats

Added `JsonInclude` to various properties, to avoid population of default values in serialized JSON.

https://github.com/apache/druid/pull/13064

### Improved metric reporting

Improved global-cached-lookups metric reporting.

https://github.com/apache/druid/pull/13219

### Fixed a bug in HttpPostEmitter

Fixed a bug in HttpPostEmitter where the emitting thread was prematurely stopped while there was data to be flushed.

https://github.com/apache/druid/pull/13237

### Improved direct memory check on startup

Improved direct memory check on startup by providing better support for Java 9+ in `RuntimeInfo`, and clearer log messages where validation fails.

https://github.com/apache/druid/pull/13207

### Added a new way of storing STRING type columns

Added support for 'front coded' string dictionaries for smaller string columns.

https://github.com/apache/druid/pull/12277

### Improved the run time of the MarkAsUnusedOvershadowedSegments duty

Improved the run time of the MarkAsUnusedOvershadowedSegments duty by iterating over all overshadowed segments and marking segments as unused in batches.

https://github.com/apache/druid/pull/13287

## Extensions

### Extension optimization

Optimized the `compareTo` function in `CompressedBigDecimal`.

https://github.com/apache/druid/pull/13086

### CompressedBigDecimal cleanup and extension

Removed unnecessary generic type from CompressedBigDecimal, added support for number input types, added support for reading aggregator input types directly (uningested data), and fixed scaling bug in buffer aggregator.

https://github.com/apache/druid/pull/13048

### Support for running tasks as Kubernetes jobs

Added an extension that allows Druid to use Kubernetes for launching and managing tasks, eliminating the need for MiddleManagers.
To use this extension, [include](../extensions.md#loading-extensions) `druid-kubernetes-overlord-extensions` in the extensions load list for your Overlord process.

https://github.com/apache/druid/pull/13156

### Support for Kubernetes discovery

Added `POD_NAME` and `POD_NAMESPACE` env variables to all Kubernetes Deployments and StatefulSets.
Helm deployment is now compatible with `druid-kubernetes-extension`.

https://github.com/apache/druid/pull/13262

## Web console

### Removed the old query view

The old query view is removed. Use the new query view with tabs.
For more information, see [Web console](https://druid.apache.org/docs/latest/operations/web-console.html#query).

https://github.com/apache/druid/pull/13169

### Filter column values in query results

The web console now allows you to add to existing filters for a selected column.

https://github.com/apache/druid/pull/13169

### Ability to add issue comments

You can now add an issue comment in SQL, for example `--:ISSUE: this is an issue` that is rendered in red and prevents the SQL from running. The comments are used by the spec-to-SQL converter to indicate that something could not be converted.

https://github.com/apache/druid/pull/13136

### Support for Kafka lookups

Added support for Kafka-based lookups rendering and input in the web console.

https://github.com/apache/druid/pull/13098

### Improved array detection

Added better detection for arrays containing objects.

https://github.com/apache/druid/pull/13077

### Updated Druid Query Toolkit version

[Druid Query Toolkit](https://www.npmjs.com/package/druid-query-toolkit) version 0.16.1 adds quotes to references in auto generated queries by default.

https://github.com/apache/druid/pull/13243

### Query task status information

The web console now exposes a textual indication about running and pending tasks when a query is stuck due to lack of task slots.

https://github.com/apache/druid/pull/13291