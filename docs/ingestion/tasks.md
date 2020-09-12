---
id: tasks
title: "Task reference"
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

Tasks do all [ingestion](index.md)-related work in Druid.

For batch ingestion, you will generally submit tasks directly to Druid using the
[Task APIs](../operations/api-reference.md#tasks). For streaming ingestion, tasks are generally submitted for you by a
supervisor.

## Task API

Task APIs are available in two main places:

- The [Overlord](../design/overlord.md) process offers HTTP APIs to submit tasks, cancel tasks, check their status,
review logs and reports, and more. Refer to the [Tasks API reference page](../operations/api-reference.md#tasks) for a
full list.
- Druid SQL includes a [`sys.tasks`](../querying/sql.md#tasks-table) table that provides information about currently
running tasks. This table is read-only, and has a limited (but useful!) subset of the full information available through
the Overlord APIs.

<a name="reports"></a>

## Task reports

A report containing information about the number of rows ingested, and any parse exceptions that occurred is available for both completed tasks and running tasks.

The reporting feature is supported by the [simple native batch task](../ingestion/native-batch.md#simple-task), the Hadoop batch task, and Kafka and Kinesis ingestion tasks.

### Completion report

After a task completes, a completion report can be retrieved at:

```
http://<OVERLORD-HOST>:<OVERLORD-PORT>/druid/indexer/v1/task/<task-id>/reports
```

An example output is shown below:

```json
{
  "ingestionStatsAndErrors": {
    "taskId": "compact_twitter_2018-09-24T18:24:23.920Z",
    "payload": {
      "ingestionState": "COMPLETED",
      "unparseableEvents": {},
      "rowStats": {
        "determinePartitions": {
          "processed": 0,
          "processedWithError": 0,
          "thrownAway": 0,
          "unparseable": 0
        },
        "buildSegments": {
          "processed": 5390324,
          "processedWithError": 0,
          "thrownAway": 0,
          "unparseable": 0
        }
      },
      "errorMsg": null
    },
    "type": "ingestionStatsAndErrors"
  }
}
```

### Live report

When a task is running, a live report containing ingestion state, unparseable events and moving average for number of events processed for 1 min, 5 min, 15 min time window can be retrieved at:

```
http://<OVERLORD-HOST>:<OVERLORD-PORT>/druid/indexer/v1/task/<task-id>/reports
```

and 

```
http://<middlemanager-host>:<worker-port>/druid/worker/v1/chat/<task-id>/liveReports
```

An example output is shown below:

```json
{
  "ingestionStatsAndErrors": {
    "taskId": "compact_twitter_2018-09-24T18:24:23.920Z",
    "payload": {
      "ingestionState": "RUNNING",
      "unparseableEvents": {},
      "rowStats": {
        "movingAverages": {
          "buildSegments": {
            "5m": {
              "processed": 3.392158326408501,
              "unparseable": 0,
              "thrownAway": 0,
              "processedWithError": 0
            },
            "15m": {
              "processed": 1.736165476881023,
              "unparseable": 0,
              "thrownAway": 0,
              "processedWithError": 0
            },
            "1m": {
              "processed": 4.206417693750045,
              "unparseable": 0,
              "thrownAway": 0,
              "processedWithError": 0
            }
          }
        },
        "totals": {
          "buildSegments": {
            "processed": 1994,
            "processedWithError": 0,
            "thrownAway": 0,
            "unparseable": 0
          }
        }
      },
      "errorMsg": null
    },
    "type": "ingestionStatsAndErrors"
  }
}
```

A description of the fields:

The `ingestionStatsAndErrors` report provides information about row counts and errors.

The `ingestionState` shows what step of ingestion the task reached. Possible states include:
* `NOT_STARTED`: The task has not begun reading any rows
* `DETERMINE_PARTITIONS`: The task is processing rows to determine partitioning
* `BUILD_SEGMENTS`: The task is processing rows to construct segments
* `COMPLETED`: The task has finished its work.

Only batch tasks have the DETERMINE_PARTITIONS phase. Realtime tasks such as those created by the Kafka Indexing Service do not have a DETERMINE_PARTITIONS phase.

`unparseableEvents` contains lists of exception messages that were caused by unparseable inputs. This can help with identifying problematic input rows. There will be one list each for the DETERMINE_PARTITIONS and BUILD_SEGMENTS phases. Note that the Hadoop batch task does not support saving of unparseable events.

the `rowStats` map contains information about row counts. There is one entry for each ingestion phase. The definitions of the different row counts are shown below:
* `processed`: Number of rows successfully ingested without parsing errors
* `processedWithError`: Number of rows that were ingested, but contained a parsing error within one or more columns. This typically occurs where input rows have a parseable structure but invalid types for columns, such as passing in a non-numeric String value for a numeric column.
* `thrownAway`: Number of rows skipped. This includes rows with timestamps that were outside of the ingestion task's defined time interval and rows that were filtered out with a [`transformSpec`](index.md#transformspec), but doesn't include the rows skipped by explicit user configurations. For example, the rows skipped by `skipHeaderRows` or `hasHeaderRow` in the CSV format are not counted.
* `unparseable`: Number of rows that could not be parsed at all and were discarded. This tracks input rows without a parseable structure, such as passing in non-JSON data when using a JSON parser.

The `errorMsg` field shows a message describing the error that caused a task to fail. It will be null if the task was successful.

## Live reports

### Row stats

The non-parallel [simple native batch task](../ingestion/native-batch.md#simple-task), the Hadoop batch task, and Kafka and Kinesis ingestion tasks support retrieval of row stats while the task is running.

The live report can be accessed with a GET to the following URL on a Peon running a task:

```
http://<middlemanager-host>:<worker-port>/druid/worker/v1/chat/<task-id>/rowStats
```

An example report is shown below. The `movingAverages` section contains 1 minute, 5 minute, and 15 minute moving averages of increases to the four row counters, which have the same definitions as those in the completion report. The `totals` section shows the current totals.

```
{
  "movingAverages": {
    "buildSegments": {
      "5m": {
        "processed": 3.392158326408501,
        "unparseable": 0,
        "thrownAway": 0,
        "processedWithError": 0
      },
      "15m": {
        "processed": 1.736165476881023,
        "unparseable": 0,
        "thrownAway": 0,
        "processedWithError": 0
      },
      "1m": {
        "processed": 4.206417693750045,
        "unparseable": 0,
        "thrownAway": 0,
        "processedWithError": 0
      }
    }
  },
  "totals": {
    "buildSegments": {
      "processed": 1994,
      "processedWithError": 0,
      "thrownAway": 0,
      "unparseable": 0
    }
  }
}
```

For the Kafka Indexing Service, a GET to the following Overlord API will retrieve live row stat reports from each task being managed by the supervisor and provide a combined report.

```
http://<OVERLORD-HOST>:<OVERLORD-PORT>/druid/indexer/v1/supervisor/<supervisor-id>/stats
```

### Unparseable events

Lists of recently-encountered unparseable events can be retrieved from a running task with a GET to the following Peon API:

```
http://<middlemanager-host>:<worker-port>/druid/worker/v1/chat/<task-id>/unparseableEvents
```

Note that this functionality is not supported by all task types. Currently, it is only supported by the
non-parallel [native batch task](../ingestion/native-batch.md) (type `index`) and the tasks created by the Kafka
and Kinesis indexing services.

<a name="locks"></a>

## Task lock system

This section explains the task locking system in Druid. Druid's locking system
and versioning system are tightly coupled with each other to guarantee the correctness of ingested data.

## "Overshadowing" between segments

You can run a task to overwrite existing data. The segments created by an overwriting task _overshadows_ existing segments.
Note that the overshadow relation holds only for the same time chunk and the same data source.
These overshadowed segments are not considered in query processing to filter out stale data.

Each segment has a _major_ version and a _minor_ version. The major version is
represented as a timestamp in the format of [`"yyyy-MM-dd'T'hh:mm:ss"`](https://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html)
while the minor version is an integer number. These major and minor versions
are used to determine the overshadow relation between segments as seen below. 

A segment `s1` overshadows another `s2` if

- `s1` has a higher major version than `s2`, or
- `s1` has the same major version and a higher minor version than `s2`.

Here are some examples.

- A segment of the major version of `2019-01-01T00:00:00.000Z` and the minor version of `0` overshadows
 another of the major version of `2018-01-01T00:00:00.000Z` and the minor version of `1`.
- A segment of the major version of `2019-01-01T00:00:00.000Z` and the minor version of `1` overshadows
 another of the major version of `2019-01-01T00:00:00.000Z` and the minor version of `0`.

## Locking

If you are running two or more [druid tasks](./tasks.html) which generate segments for the same data source and the same time chunk,
the generated segments could potentially overshadow each other, which could lead to incorrect query results.

To avoid this problem, tasks will attempt to get locks prior to creating any segment in Druid.
There are two types of locks, i.e., _time chunk lock_ and _segment lock_.

When the time chunk lock is used, a task locks the entire time chunk of a data source where generated segments will be written.
For example, suppose we have a task ingesting data into the time chunk of `2019-01-01T00:00:00.000Z/2019-01-02T00:00:00.000Z` of the `wikipedia` data source.
With the time chunk locking, this task will lock the entire time chunk of `2019-01-01T00:00:00.000Z/2019-01-02T00:00:00.000Z` of the `wikipedia` data source
before it creates any segments. As long as it holds the lock, any other tasks will be unable to create segments for the same time chunk of the same data source.
The segments created with the time chunk locking have a _higher_ major version than existing segments. Their minor version is always `0`.

When the segment lock is used, a task locks individual segments instead of the entire time chunk.
As a result, two or more tasks can create segments for the same time chunk of the same data source simultaneously
if they are reading different segments.
For example, a Kafka indexing task and a compaction task can always write segments into the same time chunk of the same data source simultaneously.
The reason for this is because a Kafka indexing task always appends new segments, while a compaction task always overwrites existing segments.
The segments created with the segment locking have the _same_ major version and a _higher_ minor version.

> The segment locking is still experimental. It could have unknown bugs which potentially lead to incorrect query results.

To enable segment locking, you may need to set `forceTimeChunkLock` to `false` in the [task context](#context).
Once `forceTimeChunkLock` is unset, the task will choose a proper lock type to use automatically.
Please note that segment lock is not always available. The most common use case where time chunk lock is enforced is
when an overwriting task changes the segment granularity.
Also, the segment locking is supported by only native indexing tasks and Kafka/Kinesis indexing tasks.
Hadoop indexing tasks and `index_realtime` tasks (used by [Tranquility](tranquility.md)) don't support it yet.

`forceTimeChunkLock` in the task context is only applied to individual tasks.
If you want to unset it for all tasks, you would want to set `druid.indexer.tasklock.forceTimeChunkLock` to false in the [overlord configuration](../configuration/index.html#overlord-operations).

Lock requests can conflict with each other if two or more tasks try to get locks for the overlapped time chunks of the same data source.
Note that the lock conflict can happen between different locks types.

The behavior on lock conflicts depends on the [task priority](#lock-priority).
If all tasks of conflicting lock requests have the same priority, then the task who requested first will get the lock.
Other tasks will wait for the task to release the lock.

If a task of a lower priority asks a lock later than another of a higher priority,
this task will also wait for the task of a higher priority to release the lock.
If a task of a higher priority asks a lock later than another of a lower priority,
then this task will _preempt_ the other task of a lower priority. The lock
of the lower-prioritized task will be revoked and the higher-prioritized task will acquire a new lock.

This lock preemption can happen at any time while a task is running except
when it is _publishing segments_ in a critical section. Its locks become preemptible again once publishing segments is finished.

Note that locks are shared by the tasks of the same groupId.
For example, Kafka indexing tasks of the same supervisor have the same groupId and share all locks with each other.

<a name="priority"></a>

## Lock priority

Each task type has a different default lock priority. The below table shows the default priorities of different task types. Higher the number, higher the priority.

|task type|default priority|
|---------|----------------|
|Realtime index task|75|
|Batch index task|50|
|Merge/Append/Compaction task|25|
|Other tasks|0|

You can override the task priority by setting your priority in the task context as below.

```json
"context" : {
  "priority" : 100
}
```

<a name="context"></a>

## Context parameters

The task context is used for various individual task configuration. The following parameters apply to all task types.

|property|default|description|
|--------|-------|-----------|
|taskLockTimeout|300000|task lock timeout in millisecond. For more details, see [Locking](#locking).|
|forceTimeChunkLock|true|_Setting this to false is still experimental_<br/> Force to always use time chunk lock. If not set, each task automatically chooses a lock type to use. If this set, it will overwrite the `druid.indexer.tasklock.forceTimeChunkLock` [configuration for the overlord](../configuration/index.html#overlord-operations). See [Locking](#locking) for more details.|
|priority|Different based on task types. See [Priority](#priority).|Task priority|

> When a task acquires a lock, it sends a request via HTTP and awaits until it receives a response containing the lock acquisition result.
> As a result, an HTTP timeout error can occur if `taskLockTimeout` is greater than `druid.server.http.maxIdleTime` of Overlords.

## All task types

### `index`

See [Native batch ingestion (simple task)](native-batch.md#simple-task).

### `index_parallel`

See [Native batch ingestion (parallel task)](native-batch.md#parallel-task).

### `index_sub`

Submitted automatically, on your behalf, by an [`index_parallel`](#index_parallel) task.

### `index_hadoop`

See [Hadoop-based ingestion](hadoop.md).

### `index_kafka`

Submitted automatically, on your behalf, by a
[Kafka-based ingestion supervisor](../development/extensions-core/kafka-ingestion.md).

### `index_kinesis`

Submitted automatically, on your behalf, by a
[Kinesis-based ingestion supervisor](../development/extensions-core/kinesis-ingestion.md).

### `index_realtime`

Submitted automatically, on your behalf, by [Tranquility](tranquility.md).

### `compact`

Compaction tasks merge all segments of the given interval. See the documentation on
[compaction](data-management.md#compaction-and-reindexing) for details.

### `kill`

Kill tasks delete all metadata about certain segments and removes them from deep storage.
See the documentation on [deleting data](../ingestion/data-management.md#delete) for details.
