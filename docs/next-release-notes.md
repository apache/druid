# Highlights

## Multi-stage query (MSQ) task engine

### Improved table functions

Improved table functions as follows:

* Added new table functions for HTTP, inline, local, and S3.
* Added support for array-valued parameters for table functions and SQL queries. You can now reuse the same SQL for every ingestion, only passing in a different set of input files as query parameters.

https://github.com/apache/druid/pull/13627

### MSQ controller task retry function

Added the ability for MSQ controller task to retry worker task in case of failures. To enable, pass `faultTolerance:true` in the query context.

https://github.com/apache/druid/pull/13353

### Fixed bucket downsampling

Added a check to prevent the collector from downsampling the same bucket indefinitely.

https://github.com/apache/druid/pull/13663

## Querying

### Implementations of semantic interfaces

Added implementations of semantic interfaces to optimize the window processing on top of an `ArrayListSegment`.

https://github.com/apache/druid/pull/13652

### JDBC lookups with reserved identifiers

Added a new parameter with reserved identifiers for table, key, and column names. You can now parse JDBC lookups with reserved identifiers.

https://github.com/apache/druid/pull/13632

### Improved handling of query metrics over JDBC

Perviously, emitting metrics for parallel merge on an empty result set over JDBC would cause the `Yielders.each` call to close the sequence on the main thread instead of the query executor thread.

The fix creates the `yielder` on the query executor thread as well so that, if the sequence is consumed, any baggages that might be emitting metrics will not be called in the wrong thread.

https://github.com/apache/druid/pull/13608

## Ingestion

### Swapped Singleton scope for LazySingleton

Replaced some use of Guice's Singleton scope with LazySingleton.

https://github.com/apache/druid/pull/13673

### Nested column indexer for schema discovery

You can now use the nested column indexer for schema discovery. This schema discovery mode allows queries to see discovered columns as their correct type rather than limiting them to `STRING`.

https://github.com/apache/druid/pull/13653

### Parquet Hadoop Parser improvement

Parquet Hadoop Parser now also looks at `flattenSpec` and `transformSpec` when reading from Parquet files.

https://github.com/apache/druid/pull/13612

### Added context to HadoopIngestionSpec

Added `context` map to `HadoopIngestionSpec`.

You can set the `context` map directly in `HadoopIngestionSpec` using the command line (non-task) version or in the `context` map for `HadoopIndexTask` which is then automatically added to `HadoopIngestionSpec`.

https://github.com/apache/druid/pull/13624

### Ingestion tests for SQL-based ingestion

Added ingestion tests for SQL-based ingestion from S3, GCS, and Azure deep storage.

https://github.com/apache/druid/pull/13535

### Fixed exception logging

Perviously, a class of `QueryException` would throw away the causes making it hard to determine what failed in the SQL planner. Corrected that behavior and adjusted tests to enhance validation of response headers.

https://github.com/apache/druid/pull/13609

### Fixed shutdown in HttpRemoteTaskRunner

Previously, `shutdown` in `HttpRemoteTaskRunner` did not execute shutdown hooks.
Now, Druid retains the task in the set of tasks within `HttpRemoteTaskRunner` and clears them only when the state is completed.

https://github.com/apache/druid/pull/13558

### JsonLineReaderBenchmark benchmark

Introduced a new benchmark `JsonLineReaderBenchmark`.

Additionally, switched `TextReader` to use `FastLineIterator` which minimizes the repeated creation of buffers.

https://github.com/apache/druid/pull/13545

## Performance and operational improvements

### Added support for Indexer and MiddleManager in integration tests

You can now run integration tests on either the MiddleManager or Indexer processes.

https://github.com/apache/druid/pull/13660

### Updated forbidden APIs

Added `java.util.concurrent.Executors#newFixedThreadPool(int)` to the forbidden APIs list.

https://github.com/apache/druid/pull/13633

### Added sort operator for Window functions

Added `NaiveSortMaker` and `NaiveSortOperator` to handle sort operations.

https://github.com/apache/druid/pull/13619

### Updated the Operator interface

Changed the Operator interface for Window functions to a pull-based model.

https://github.com/apache/druid/pull/13600

### Updated Docker Compose

This release includes several improvements to the `docker-compose.yml` file:

* Added configuration to bind Postgres instance on the default port ("5432") to the `docker-compose.yml` file.
* Updated Broker, Historical, MiddleManager, and Router instances to use Druid 24.0.1 on the `docker-compose.yml` file.
* Removed trailing space on the `docker-compose.yml` file.

https://github.com/apache/druid/pull/13623

## Extensions

### Improved default fetch settings for Kinesis

Updated the following fetch settings for the Kinesis indexing service:

- `fetchThreads`: Twice the number of processors available to the task.
- `fetchDelayMillis`: 0 (no delay between fetches).
- `recordsPerFetch`: 100 MB or an estimated 5% of available heap, whichever is smaller, divided by `fetchThreads`.
- `recordBufferSize`: 100 MB or an estimated 10% of available heap, whichever is smaller.
- `maxRecordsPerPoll`: 100 for regular records, 1 for aggregated records.

https://github.com/apache/druid/pull/13539

## Web console

Notable web console improvements:

* Improved error reporting. https://github.com/apache/druid/pull/13636
* Improved the look of **Group by** totals in the **Services** view. https://github.com/apache/druid/pull/13631

## Security

### Support for the HTTP Strict-Transport-Security response header

Added support for the HTTP `Strict-Transport-Security` response header.

Druid does not include this header by default. You must enable it in runtime properties.

https://github.com/apache/druid/pull/13489