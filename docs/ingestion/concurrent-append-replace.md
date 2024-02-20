---
id: concurrent-append-replace
title: Concurrent append and replace
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

:::info
Concurrent append and replace is an [experimental feature](../development/experimental.md) available for JSON-based batch and streaming. It is not currently available for SQL-based ingestion.
:::

Concurrent append and replace safely replaces the existing data in an interval of a datasource while new data is being appended to that interval. One of the most common applications of this feature is appending new data (such as with streaming ingestion) to an interval while compaction of that interval is already in progress. Druid segments the data ingested during this time dynamically. The subsequent compaction run segments the data into the  granularity you specified.

To set up concurrent append and replace, use the context flag `useConcurrentLocks`. Druid will then determine the correct lock type for you, either append or replace. Although you can set the type of lock manually, we don't recommend it. 

## Update the compaction settings 

If you want to append data to a datasource while compaction is running, you need to enable concurrent append and replace for the datasource by updating the compaction settings.

### Update the compaction settings with the UI

In the **Compaction config** for a datasource, enable  **Use concurrent locks (experimental)**.

For details on accessing the compaction config in the UI, see [Enable automatic compaction with the web console](../data-management/automatic-compaction.md#web-console).

### Update the compaction settings with the API
 
Add the `taskContext` like you would any other automatic compaction setting through the API:

```shell
curl --location --request POST 'http://localhost:8081/druid/coordinator/v1/config/compaction' \
--header 'Content-Type: application/json' \
--data-raw '{
    "dataSource": "YOUR_DATASOURCE",
    "taskContext": {
        "useConcurrentLocks": true
    }
}'
```

## Configure a task lock type for your ingestion job

You also need to configure the ingestion job to allow concurrent tasks.

You can provide the context parameter like any other parameter for ingestion jobs through the API or the UI.

### Add a task lock using the Druid console

As part of the  **Load data** wizard for classic batch (JSON-based ingestion) and streaming ingestion, enable the following config on the **Publish** step: **Use concurrent locks (experimental)**.

### Add the task lock through the API

Add the following JSON snippet to your supervisor or ingestion spec if you're using the API:

```json
"context": {
   "useConcurrentLocks": true
}
```
 

## Task lock types

We recommend that you use the `useConcurrentLocks` context parameter so that Druid automatically determines the task lock types for you. If, for some reason, you need to manually set the task lock types explicitly, you can read more about them in this section.

<details><summary>Click here to read more about the lock types.</summary>

Druid uses task locks to make sure that multiple conflicting operations don't happen at once.
There are two task lock types: `APPEND` and `REPLACE`. The type of lock you use is determined by what you're trying to accomplish.

When setting task lock types manually, be aware of the following:
- The segment granularity of the append task must be equal to or finer than the segment granularity of the replace task.
- Concurrent append and replace fails if the task with `APPEND` lock uses a coarser segment granularity than the task with the `REPLACE` lock. For example, if the `APPEND` task uses a segment granularity of YEAR and the `REPLACE` task uses a segment granularity of MONTH, you should not use concurrent append and replace.
-  Only a single task can hold a `REPLACE` lock on a given interval of a datasource.
  - Multiple tasks can hold `APPEND` locks on a given interval of a datasource and append data to that interval simultaneously.

#### Add a task lock type to your ingestion job

You configure the task lock type for your ingestion job as follows:

- For streaming jobs, the `taskLockType` context parameter goes in your supervisor spec, and the lock type is always `APPEND`.
- For classic JSON-based batch ingestion, the `taskLockType` context parameter goes in your ingestion spec, and the lock type can be either `APPEND` or `REPLACE`. 
 
You can provide the context parameter through the API like any other parameter for ingestion job or through the UI.

##### Add a task lock using the Druid console

As part of the  **Load data** wizard for classic batch (JSON-based ingestion) and streaming ingestion, you can configure the task lock type for the ingestion during the **Publish** step:

- If you set **Append to existing** to **True**, you can then set **Allow concurrent append tasks (experimental)** to **True**.
- If you set **Append to existing** to **False**, you can then set **Allow concurrent replace tasks (experimental)** to **True**.

##### Add the task lock type through the API

Add the following JSON snippet to your supervisor or ingestion spec if you're using the API:

```json
"context": {
   "taskLockType": LOCK_TYPE
}   
```
 
The `LOCK_TYPE` depends on what you're trying to accomplish.

Set `taskLockType` to  `APPEND` if either of the following are true:

- Dynamic partitioning with append to existing is set to `true`
- The ingestion job is a streaming ingestion job

If you have multiple ingestion jobs that append all targeting the same datasource and want them to run simultaneously, you need to also include the following context parameter:

```json
"useSharedLock": "true"
```

Keep in mind that `taskLockType` takes precedence over `useSharedLock`. Do not use `useSharedLock` with `REPLACE` task locks.


Set  `taskLockType` to `REPLACE` if you're replacing data. For example, if you use any of the following partitioning types, use `REPLACE`:

- hash partitioning 
- range partitioning
- dynamic partitioning with append to existing set to `false`

</details>
