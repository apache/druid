---
id: known-issues
title: SQL-based ingestion known issues
sidebar_label: Known issues
---

> SQL-based ingestion and the multi-stage query task engine are experimental features available starting in Druid 24.0. You can use it in place of the existing native batch and Hadoop based ingestion systems. As an experimental feature, functionality documented on this page is subject to change or removal in future releases. Review the release notes and this page to stay up to date on changes.

### General query execution

- There's no fault tolerance. If any task fails, the entire query fails. 

- In case of a worker crash, stage outputs on S3 are not deleted automatically. You may need to delete
  the file using an external process or create an S3 lifecycle policy to remove the objects
  under `druid.msq.intermediate.storage.prefix`. A good start would be to delete the objects after 3 days if they are
  not automatically deleted.

- Only one local filesystem per server is used for stage output data during multi-stage query
  execution. If your servers have multiple local filesystems, this causes queries to exhaust
  available disk space earlier than expected. As a workaround, you can use [durable storage for shuffle meshes](./msq-durable-storage.md).

- When `msqMaxNumTasks` (formerly `msqNumTasks`, formerly `talariaNumTasks`) is higher than the total
  capacity of the cluster, more tasks may be launched than can run at once. This leads to a
  [TaskStartTimeout](./msq-reference.md#context-parameters) error code, as there is never enough capacity to run the query.
  To avoid this, set `msqMaxNumTasks` to a number of tasks that can run simultaneously on your cluster.

- When `msqTaskAssignment` is set to `auto`, the system generates one task per input file for certain splittable
  input sources where file sizes are not known ahead of time. This includes the `http` input source, where the system
  generates one task per URI.

### Memory usage

- INSERT queries can consume excessive memory when using complex types due to inaccurate footprint
  estimation. This can appear as an OutOfMemoryError during the SegmentGenerator stage when using
  sketches. If you run into this issue, try manually lowering the value of the
  [`msqRowsInMemory`](./msq-reference.md#context-parameters) parameter.

- EXTERN loads an entire row group into memory at once when reading from Parquet files. Row groups
  can be up to 1 GB in size, which can lead to excessive heap usage when reading many files in
  parallel. This can appear as an OutOfMemoryError during stages that read Parquet input files. If
  you run into this issue, try using a smaller number of worker tasks or you can increase the heap
  size of your Indexers or of your Middle Manager-launched indexing tasks.

- Ingesting a very long row may consume excessive memory and result in an OutOfMemoryError. If a row is read 
  which requires more memory than is available, the service might throw OutOfMemoryError. If you run into this
  issue, allocate enough memory to be able to store the largest row to the indexer. (16919)

### SELECT queries

- SELECT query results do not include real-time data until it has been published.

<!-- 
- SELECT query results are funneled through the controller task
  so they can be written to the query report.
  This is a bottleneck for queries with large resultsets. In the future,
  we will provide a mechanism for writing query results to multiple
  files in parallel. (14728)
-->
<!--
- SELECT query results are materialized in memory on the Broker when
  using the query results API. Large result sets
  can cause the Broker to run out of memory. (15963)
-->

- TIMESTAMP types are formatted as numbers rather than ISO8601 timestamp
  strings, which differs from Druid's standard result format. 

- BOOLEAN types are formatted as numbers like `1` and `0` rather
  than `true` or `false`, which differs from Druid's standard result
  format. 

- TopN is not implemented. The context parameter
  `useApproximateTopN` is ignored and always treated as if it
  were `false`. Therefore, topN-shaped queries will
  always run using the groupBy engine. There is no loss of
  functionality, but there may be a performance impact, since
  these queries will run using an exact algorithm instead of an
  approximate one.
- GROUPING SETS is not implemented. Queries that use GROUPING SETS
  will fail.
- The numeric flavors of the EARLIEST and LATEST aggregators do not work properly. Attempting to use the numeric flavors of these aggregators will lead to an error like `java.lang.ClassCastException: class java.lang.Double cannot be cast to class org.apache.druid.collections.SerializablePair`. The string flavors, however, do work properly.

###  INSERT queries

- The [schemaless dimensions](https://docs.imply.io/latest/druid/ingestion/ingestion-spec.html#inclusions-and-exclusions)
feature is not available. All columns and their types must be specified explicitly.

- [Segment metadata queries](https://docs.imply.io/latest/druid/querying/segmentmetadataquery.html)
  on datasources ingested with the Multi-Stage Query Engine will return values for`timestampSpec` that are not usable
  for introspection.
- Figuring out `rollup`, `query-granularity`, and `aggregatorFactories` is on a best effort basis. In
  particular, Pivot will not be able to automatically create data cubes that properly reflect the
  rollup configurations if the insert query does not meet the conditions defined in [Rollup](./index.md#group-by). Proper data cubes
  can still be created manually.

- When INSERT with GROUP BY does the match the criteria mentioned in [GROUP BY](./index.md#group-by),  the multi-stage engine generates segments that Druid's compaction
  functionality is not able to further roll up. This applies to autocompaction as well as manually
  issued `compact` tasks. Individual queries executed with the multi-stage engine always guarantee
  perfect rollup for their output, so this only matters if you are performing a sequence of INSERT
  queries that each append data to the same time chunk. If necessary, you can compact such data
  using another SQL query instead of a `compact` task.

- When using INSERT with GROUP BY, splitting of large partitions is not currently
  implemented. If a single partition key appears in a
  very large number of rows, an oversized segment will be created.
  You can mitigate this by adding additional columns to your
  partition key. Note that partition splitting _does_ work properly
  when performing INSERT without GROUP BY.

- INSERT with column lists, like
  `INSERT INTO tbl (a, b, c) SELECT ...`, is not implemented.

### EXTERN queries

- EXTERN does not accept `druid` input sources.

### Missing guardrails

- Maximum amount of local disk space to use for temporary data when durable storage is turned off. No guardrail today means worker tasks may exhaust all available disk space. In this case, you will receive an [UnknownError](./msq-reference.md#error-codes)) with a message including "No space left on device".