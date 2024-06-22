---
id: index
title: "Data management"
sidebar_label: "Overview"
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

Apache Druid stores data [partitioned by time chunk](../design/storage.md) in immutable
files called [segments](../design/segments.md). Data management operations involving replacing, or deleting,
these segments include:

- [Updates](update.md) to existing data.
- [Deletion](delete.md) of existing data.
- [Schema changes](schema-changes.md) for new and existing data.
- [Compaction](compaction.md) and [automatic compaction](automatic-compaction.md), which reindex existing data to
  optimize storage footprint and performance.
