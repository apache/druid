---
id: streaming
title: "Streaming ingestion"
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

Apache Druid can consume data streams from the following external streaming sources:

* Apache Kafka through the bundled [Kafka indexing service](kafka-ingestion.md) extension.
* Amazon Kinesis through the bundled [Kinesis indexing service](kinesis-ingestion.md) extension.

Each indexing service provides real-time data ingestion with exactly-once stream processing guarantee.
To use either of the streaming ingestion methods, you must first load the associated extension on both the Overlord and the MiddleManager. See [Loading extensions](../configuration/extensions.md#loading-extensions) for more information.

Streaming ingestion is controlled by a continuously running [supervisor](supervisor.md).
The supervisor oversees the state of indexing tasks to coordinate handoffs, manage failures, and ensure that scalability and replication requirements are maintained.
You start a supervisor by submitting a JSON specification, often referred to as the supervisor spec, either though the Druid web console or using the [Supervisor API](../api-reference/supervisor-api.md).