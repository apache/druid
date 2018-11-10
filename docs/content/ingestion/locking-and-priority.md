---
layout: doc_page
---

# Task Locking & Priority

## Locking

Once an overlord node accepts a task, the task acquires locks for the data source and intervals specified in the task.

There are two lock types, i.e., _shared lock_ and _exclusive lock_.

- A task needs to acquire a shared lock before it reads segments of an interval. Multiple shared locks can be acquired for the same dataSource and interval. Shared locks are always preemptable, but they don't preempt each other.
- A task needs to acquire an exclusive lock before it writes segments for an interval. An exclusive lock is also preemptable except while the task is publishing segments.

Each task can have different lock priorities. The locks of higher-priority tasks can preempt the locks of lower-priority tasks. The lock preemption works based on _optimistic locking_. When a lock is preempted, it is not notified to the owner task immediately. Instead, it's notified when the owner task tries to acquire the same lock again. (Note that lock acquisition is idempotent unless the lock is preempted.) In general, tasks don't compete for acquiring locks because they usually targets different dataSources or intervals.

A task writing data into a dataSource must acquire exclusive locks for target intervals. Note that exclusive locks are still preemptable. That is, they also be able to be preempted by higher priority locks unless they are _publishing segments_ in a critical section. Once publishing segments is finished, those locks become preemptable again.

Tasks do not need to explicitly release locks, they are released upon task completion. Tasks may potentially release 
locks early if they desire. Task ids are unique by naming them using UUIDs or the timestamp in which the task was created. 
Tasks are also part of a "task group", which is a set of tasks that can share interval locks.

## Priority

Druid's indexing tasks use locks for atomic data ingestion. Each lock is acquired for the combination of a dataSource and an interval. Once a task acquires a lock, it can write data for the dataSource and the interval of the acquired lock unless the lock is released or preempted. Please see [the below Locking section](#locking)

Each task has a priority which is used for lock acquisition. The locks of higher-priority tasks can preempt the locks of lower-priority tasks if they try to acquire for the same dataSource and interval. If some locks of a task are preempted, the behavior of the preempted task depends on the task implementation. Usually, most tasks finish as failed if they are preempted.

Tasks can have different default priorities depening on their types. Here are a list of default priorities. Higher the number, higher the priority.

|task type|default priority|
|---------|----------------|
|Realtime index task|75|
|Batch index task|50|
|Merge/Append/Compaction task|25|
|Other tasks|0|

You can override the task priority by setting your priority in the task context like below.

```json
"context" : {
  "priority" : 100
}
```

## Task Context

The task context is used for various task configuration parameters. The following parameters apply to all task types.

|property|default|description|
|--------|-------|-----------|
|taskLockTimeout|300000|task lock timeout in millisecond. For more details, see [Locking](#locking).|
|priority|Different based on task types. See [Priority](#priority).|Task priority|

<div class="note caution">
When a task acquires a lock, it sends a request via HTTP and awaits until it receives a response containing the lock acquisition result.
As a result, an HTTP timeout error can occur if `taskLockTimeout` is greater than `druid.server.http.maxIdleTime` of overlords.
</div>
