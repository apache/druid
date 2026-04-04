---
id: hadoop
title: "Hadoop-based ingestion"
sidebar_label: "Hadoop-based"
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

Support for Apache Hadoop-based ingestion was removed from Apache Druid 37.0.0. Please use
[SQL-based ingestion](../multi-stage-query/index.md) or [native batch](../ingestion/native-batch.md) instead.

The associated `materialized-view-selection` and `materialized-view-maintenance` contrib extensions were also removed
as part of this since they only supported Hadoop based ingestion.

Note that Druid still supports using [`druid-hdfs-storage`](../development/extensions-core/hdfs.md) as deep storage and other Hadoop ecosystem extensions and
functionality that was not specific to Hadoop-based ingestion.
