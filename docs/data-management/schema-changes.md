---
id: schema-changes
title: "Schema changes"
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


## For new data

Apache Druid allows you to provide a new schema for new data without the need to update the schema of any existing data.
It is sufficient to update your supervisor spec, if using [streaming ingestion](../ingestion/index.md#streaming), or to
provide the new schema the next time you do a [batch ingestion](../ingestion/index.md#batch). This is made possible by
the fact that each [segment](../design/segments.md), at the time it is created, stores a
copy of its own schema. Druid reconciles all of these individual segment schemas automatically at query time.

## For existing data

Schema changes are sometimes necessary for existing data. For example, you may want to change the type of a column in
previously-ingested data, or drop a column entirely. Druid handles this using [reindexing](update.md), the same method
it uses to handle updates of existing data. Reindexing involves rewriting all affected segments and can be a
time-consuming operation.
