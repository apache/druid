---
id: concepts
title: SQL-based ingestion concepts
sidebar_label: Key concepts
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

> SQL-based ingestion using the multi-stage query task engine is our recommended solution starting in Druid 24.0. Alternative ingestion solutions, such as native batch and Hadoop-based ingestion systems, will still be supported. We recommend you read all [known issues](./msq-known-issues.md) and test the feature in a development environment before rolling it out in production. Using the multi-stage query task engine with `SELECT` statements that do not write to a datasource is experimental.

This topic covers the main concepts and terminology of the multi-stage query architecture.

## Vocabulary

You might see the following terms in the documentation or while you're using the multi-stage query architecture and task engine, such as when you view the report for a query:

- **Controller**: An indexing service task of type `query_controller` that manages
  the execution of a query. There is one controller task per query.

- **Worker**: Indexing service tasks of type `query_worker` that execute a
  query. There can be multiple worker tasks per query. Internally,
  the tasks process items in parallel using their processing pools (up to `druid.processing.numThreads` of execution parallelism
  within a worker task).

- **Stage**: A stage of query execution that is parallelized across
  worker tasks. Workers exchange data with each other between stages.

- **Partition**: A slice of data output by worker tasks. In INSERT or REPLACE
  queries, the partitions of the final stage become Druid segments.

- **Shuffle**: Workers exchange data between themselves on a per-partition basis in a process called
  shuffling. During a shuffle, each output partition is sorted by a clustering key.

## How the MSQ task engine works

Query tasks, specifically queries for INSERT, REPLACE, and SELECT, execute using indexing service tasks. Every query occupies at least two task slots while running. 

When you submit a query task to the MSQ task engine, the following happens:

1.  The Broker plans your SQL query into a native query, as usual.

2.  The Broker wraps the native query into a task of type `query_controller`
    and submits it to the indexing service.

3. The Broker returns the task ID to you and exits.

4.  The controller task launches some number of worker tasks determined by
    the `maxNumTasks` and `taskAssignment` [context parameters](./msq-reference.md#context-parameters). You can set these settings individually for each query.

5.  The worker tasks execute the query.

6.  If the query is a SELECT query, the worker tasks send the results
    back to the controller task, which writes them into its task report.
    If the query is an INSERT or REPLACE query, the worker tasks generate and
    publish new Druid segments to the provided datasource.


## Parallelism

Parallelism affects performance.

The [`maxNumTasks`](./msq-reference.md#context-parameters) query parameter determines the maximum number of tasks (workers and one controller) your query will use. Generally, queries perform better with more workers. The lowest possible value of `maxNumTasks` is two (one worker and one controller), and the highest possible value is equal to the number of free task slots in your cluster.

The `druid.worker.capacity` server property on each Middle Manager determines the maximum number
of worker tasks that can run on each server at once. Worker tasks run single-threaded, which
also determines the maximum number of processors on the server that can contribute towards
multi-stage queries. Since data servers are shared between Historicals and
Middle Managers, the default setting for `druid.worker.capacity` is lower than the number of
processors on the server. Advanced users may consider enhancing parallelism by increasing this
value to one less than the number of processors on the server. In most cases, this increase must
be accompanied by an adjustment of the memory allotment of the Historical process,
Middle-Manager-launched tasks, or both, to avoid memory overcommitment and server instability. If
you are not comfortable tuning these memory usage parameters to avoid overcommitment, it is best
to stick with the default `druid.worker.capacity`.

## Memory usage

Increasing the amount of available memory can improve performance as follows:

- Segment generation becomes more efficient when data doesn't spill to disk as often.
- Sorting stage output data becomes more efficient since available memory affects the
  number of required sorting passes.

Worker tasks use both JVM heap memory and off-heap ("direct") memory.

On Peons launched by Middle Managers, the bulk of the JVM heap (75%) is split up into two bundles of equal size: one processor bundle and one worker bundle. Each one comprises 37.5% of the available JVM heap.

The processor memory bundle is used for query processing and segment generation. Each processor bundle must
also provides space to buffer I/O between stages. Specifically, each downstream stage requires 1 MB of buffer space for
each upstream worker. For example, if you have 100 workers running in stage 0, and stage 1 reads from stage 0,
then each worker in stage 1 requires 1M * 100 = 100 MB of memory for frame buffers.

The worker memory bundle is used for sorting stage output data prior to shuffle. Workers can sort
more data than fits in memory; in this case, they will switch to using disk.

Worker tasks also use off-heap ("direct") memory. Set the amount of direct
memory available (`-XX:MaxDirectMemorySize`) to at least
`(druid.processing.numThreads + 1) * druid.processing.buffer.sizeBytes`. Increasing the
amount of direct memory available beyond the minimum does not speed up processing.

It may be necessary to override one or more memory-related parameters if you run into one of the [known issues around memory usage](./msq-known-issues.md#memory-usage).

## Limits

Knowing the limits for the MSQ task engine can help you troubleshoot any [errors](#error-codes) that you encounter. Many of the errors occur as a result of reaching a limit.

The following table lists query limits:

|Limit|Value|Error if exceeded|
|-----|-----|-----------------|
| Size of an individual row written to a frame. Row size when written to a frame may differ from the original row size. | 1 MB | `RowTooLarge` |
| Number of segment-granular time chunks encountered during ingestion. | 5,000 | `TooManyBuckets` |
| Number of input files/segments per worker. | 10,000 | `TooManyInputFiles` |
| Number of output partitions for any one stage. Number of segments generated during ingestion. |25,000 | `TooManyPartitions` |
| Number of output columns for any one stage. | 2,000 | `TooManyColumns` |
| Number of workers for any one stage. | Hard limit is 1,000. Memory-dependent soft limit may be lower. | `TooManyWorkers` |
| Maximum memory occupied by broadcasted tables. | 30% of each [processor memory bundle](#memory-usage). | `BroadcastTablesTooLarge` |

## Error codes

The following table describes error codes you may encounter in the `multiStageQuery.payload.status.errorReport.error.errorCode` field:

|Code|Meaning|Additional fields|
|----|-----------|----|
|  BroadcastTablesTooLarge  | The size of the broadcast tables, used in right hand side of the joins, exceeded the memory reserved for them in a worker task.  | `maxBroadcastTablesSize`: Memory reserved for the broadcast tables, measured in bytes. |
|  Canceled  |  The query was canceled. Common reasons for cancellation:<br /><br /><ul><li>User-initiated shutdown of the controller task via the `/druid/indexer/v1/task/{taskId}/shutdown` API.</li><li>Restart or failure of the server process that was running the controller task.</li></ul>|    |
|  CannotParseExternalData |  A worker task could not parse data from an external datasource.  |    |
|  ColumnNameRestricted|  The query uses a restricted column name.  |    |
|  ColumnTypeNotSupported|  Support for writing or reading from a particular column type is not supported. |    |
|  ColumnTypeNotSupported | The query attempted to use a column type that is not supported by the frame format. This occurs with ARRAY types, which are not yet implemented for frames.  | `columnName`<br /> <br />`columnType`   |
|  InsertCannotAllocateSegment |  The controller task could not allocate a new segment ID due to conflict with existing segments or pending segments. Common reasons for such conflicts:<br /> <br /><ul><li>Attempting to mix different granularities in the same intervals of the same datasource.</li><li>Prior ingestions that used non-extendable shard specs.</li></ul>| `dataSource`<br /> <br />`interval`: The interval for the attempted new segment allocation.  |
|  InsertCannotBeEmpty |  An INSERT or REPLACE query did not generate any output rows in a situation where output rows are required for success. This can happen for INSERT or REPLACE queries with `PARTITIONED BY` set to something other than `ALL` or `ALL TIME`.  |  `dataSource`  |
|  InsertCannotOrderByDescending  |  An INSERT query contained a `CLUSTERED BY` expression in descending order. Druid's segment generation code only supports ascending order.  |   `columnName` |
|  InsertCannotReplaceExistingSegment |  A REPLACE query cannot proceed because an existing segment partially overlaps those bounds, and the portion within the bounds is not fully overshadowed by query results. <br /> <br />There are two ways to address this without modifying your query:<ul><li>Shrink the OVERLAP filter to match the query results.</li><li>Expand the OVERLAP filter to fully contain the existing segment.</li></ul>| `segmentId`: The existing segment <br /> 
|  InsertLockPreempted  | An INSERT or REPLACE query was canceled by a higher-priority ingestion job, such as a real-time ingestion task.  | |
|  InsertTimeNull  | An INSERT or REPLACE query encountered a null timestamp in the `__time` field.<br /><br />This can happen due to using an expression like `TIME_PARSE(timestamp) AS __time` with a timestamp that cannot be parsed. (TIME_PARSE returns null when it cannot parse a timestamp.) In this case, try parsing your timestamps using a different function or pattern.<br /><br />If your timestamps may genuinely be null, consider using COALESCE to provide a default value. One option is CURRENT_TIMESTAMP, which represents the start time of the job. |
| InsertTimeOutOfBounds  |  A REPLACE query generated a timestamp outside the bounds of the TIMESTAMP parameter for your OVERWRITE WHERE clause.<br /> <br />To avoid this error, verify that the   you specified is valid.  |  `interval`: time chunk interval corresponding to the out-of-bounds timestamp  |
|  InvalidNullByte  | A string column included a null byte. Null bytes in strings are not permitted. |  `column`: The column that included the null byte |
| QueryNotSupported   | QueryKit could not translate the provided native query to a multi-stage query.<br /> <br />This can happen if the query uses features that aren't supported, like GROUPING SETS. |    |
|  RowTooLarge  |  The query tried to process a row that was too large to write to a single frame. See the [Limits](#limits) table for the specific limit on frame size. Note that the effective maximum row size is smaller than the maximum frame size due to alignment considerations during frame writing.  |   `maxFrameSize`: The limit on the frame size. |
|  TaskStartTimeout  | Unable to launch all the worker tasks in time. <br /> <br />There might be insufficient available slots to start all the worker tasks simultaneously.<br /> <br /> Try splitting up the query into smaller chunks with lesser `maxNumTasks` number. Another option is to increase capacity.  | |
|  TooManyBuckets  |  Exceeded the number of partition buckets for a stage. Partition buckets are only used for `segmentGranularity` during INSERT queries. The most common reason for this error is that your `segmentGranularity` is too narrow relative to the data. See the [Limits](./msq-concepts.md#limits) table for the specific limit.  |  `maxBuckets`: The limit on buckets.  |
| TooManyInputFiles | Exceeded the number of input files/segments per worker. See the [Limits](./msq-concepts.md#limits) table for the specific limit. | `umInputFiles`: The total number of input files/segments for the stage.<br /><br />`maxInputFiles`: The maximum number of input files/segments per worker per stage.<br /><br />`minNumWorker`: The minimum number of workers required for a successful run. |
|  TooManyPartitions   |  Exceeded the number of partitions for a stage. The most common reason for this is that the final stage of an INSERT or REPLACE query generated too many segments. See the [Limits](./msq-concepts.md#limits) table for the specific limit.  | `maxPartitions`: The limit on partitions which was exceeded    |
|  TooManyColumns |  Exceeded the number of columns for a stage. See the [Limits](#limits) table for the specific limit.  | `maxColumns`: The limit on columns which was exceeded.  |
|  TooManyWarnings |  Exceeded the allowed number of warnings of a particular type. | `rootErrorCode`: The error code corresponding to the exception that exceeded the required limit. <br /><br />`maxWarnings`: Maximum number of warnings that are allowed for the corresponding `rootErrorCode`.   |
|  TooManyWorkers |  Exceeded the supported number of workers running simultaneously. See the [Limits](#limits) table for the specific limit.  | `workers`: The number of simultaneously running workers that exceeded a hard or soft limit. This may be larger than the number of workers in any one stage if multiple stages are running simultaneously. <br /><br />`maxWorkers`: The hard or soft limit on workers that was exceeded.  |
|  NotEnoughMemory  |  Insufficient memory to launch a stage.  |  `serverMemory`: The amount of memory available to a single process.<br /><br />`serverWorkers`: The number of workers running in a single process.<br /><br />`serverThreads`: The number of threads in a single process.  |
|  WorkerFailed  |  A worker task failed unexpectedly.  |  `workerTaskId`: The ID of the worker task.  |
|  WorkerRpcFailed  |  A remote procedure call to a worker task failed and could not recover.  |  `workerTaskId`: the id of the worker task  |
|  UnknownError   |  All other errors.  |    |