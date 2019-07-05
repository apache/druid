---
layout: doc_page
title: "Deleting Data"
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

# Deleting Data

Permanent deletion of a segment in Apache Druid (incubating) has two steps:

1. The segment must first be marked as "unused". This occurs when a segment is dropped by retention rules, and when a user manually disables a segment through the Coordinator API.
2. After segments have been marked as "unused", a Kill Task will delete any "unused" segments from Druid's metadata store as well as deep storage.

For documentation on retention rules, please see [Data Retention](../operations/rule-configuration.html).

For documentation on disabling segments using the Coordinator API, please see [Coordinator Delete API](../operations/api-reference.html#coordinator-delete)

A data deletion tutorial is available at [Tutorial: Deleting data](../tutorials/tutorial-delete-data.html)

## Kill Task

Kill tasks delete all information about a segment and removes it from deep storage. Killable segments must be disabled (used==0) in the Druid segment table. The available grammar is:

```json
{
    "type": "kill",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "interval" : <all_segments_in_this_interval_will_die!>,
    "context": <task context>
}
```
