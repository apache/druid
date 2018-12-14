---
layout: doc_page
title: "Miscellaneous Tasks"
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

# Miscellaneous Tasks

## Noop Task

These tasks start, sleep for a time and are used only for testing. The available grammar is:

```json
{
    "type": "noop",
    "id": <optional_task_id>,
    "interval" : <optional_segment_interval>,
    "runTime" : <optional_millis_to_sleep>,
    "firehose": <optional_firehose_to_test_connect>
}
```


## Segment Merging Tasks (Deprecated)

### Append Task

Append tasks append a list of segments together into a single segment (one after the other). The grammar is:

```json
{
    "type": "append",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "segments": <JSON list of DataSegment objects to append>,
    "aggregations": <optional list of aggregators>,
    "context": <task context>
}
```

### Merge Task

Merge tasks merge a list of segments together. Any common timestamps are merged.
If rollup is disabled as part of ingestion, common timestamps are not merged and rows are reordered by their timestamp.

The grammar is:

```json
{
    "type": "merge",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "aggregations": <list of aggregators>,
    "rollup": <whether or not to rollup data during a merge>,
    "segments": <JSON list of DataSegment objects to merge>,
    "context": <task context>
}
```

### Same Interval Merge Task

Same Interval Merge task is a shortcut of merge task, all segments in the interval are going to be merged.

The grammar is:

```json
{
    "type": "same_interval_merge",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "aggregations": <list of aggregators>,
    "rollup": <whether or not to rollup data during a merge>,
    "interval": <DataSegment objects in this interval are going to be merged>,
    "context": <task context>
}
```
