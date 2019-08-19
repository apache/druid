---
layout: doc_page
title: "Task Locking & Priority"
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

# Task Locking & Priority

This document explains the task locking system in Druid. Druid's locking system
and versioning system are tighly coupled with each other to guarantee the correctness of ingested data.

## Overshadow Relation between Segments

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

<div class="note caution">
The segment locking is still experimental. It could have unknown bugs which potentially lead to incorrect query results.
</div>

To enable segment locking, you may need to set `forceTimeChunkLock` to `false` in the [task context](#task-context).
Once `forceTimeChunkLock` is unset, the task will choose a proper lock type to use automatically.
Please note that segment lock is not always available. The most common use case where time chunk lock is enforced is
when an overwriting task changes the segment granularity.
Also, the segment locking is supported by only native indexing tasks and Kafka/Kinesis indexing tasks.
The Hadoop indexing tasks and realtime indexing tasks (with [Tranquility](./stream-push.html)) don't support it yet.

`forceTimeChunkLock` in the task context is only applied to individual tasks.
If you want to unset it for all tasks, you would want to set `druid.indexer.tasklock.forceTimeChunkLock` to false in the [overlord configuration](../configuration/index.html#overlord-operations).

Lock requests can conflict with each other if two or more tasks try to get locks for the overlapped time chunks of the same data source.
Note that the lock conflict can happen between different locks types.

The behavior on lock conflicts depends on the [task priority](#task-lock-priority).
If all tasks of conflicting lock requests have the same priority, then the task who requested first will get the lock.
Other tasks will wait for the task to release the lock.

If a task of a lower priority asks a lock later than another of a higher priority,
this task will also wait for the task of a higher priority to release the lock.
If a task of a higher priority asks a lock later than another of a lower priority,
then this task will _preempt_ the other task of a lower priority. The lock
of the lower-prioritized task will be revoked and the higher-prioritized task will acquire a new lock.

This lock preemption can happen at any time while a task is running except
when it is _publishing segments_ in a critical section. Its locks become preemptable again once publishing segments is finished.

Note that locks are shared by the tasks of the same groupId.
For example, Kafka indexing tasks of the same supervisor have the same groupId and share all locks with each other.

## Task Lock Priority

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

## Task Context

The task context is used for various individual task configuration. The following parameters apply to all task types.

|property|default|description|
|--------|-------|-----------|
|taskLockTimeout|300000|task lock timeout in millisecond. For more details, see [Locking](#locking).|
|forceTimeChunkLock|true|_**Setting this to false is still experimental**_<br/> Force to always use time chunk lock. If not set, each task automatically chooses a lock type to use. If this is set, it will overwrite the `druid.indexer.tasklock.forceTimeChunkLock` [configuration for the overlord](../configuration/index.html#overlord-operations). See [Locking](#locking) for more details.|
|priority|Different based on task types. See [Priority](#priority).|Task priority|

<div class="note caution">
When a task acquires a lock, it sends a request via HTTP and awaits until it receives a response containing the lock acquisition result.
As a result, an HTTP timeout error can occur if `taskLockTimeout` is greater than `druid.server.http.maxIdleTime` of Overlords.
</div>
