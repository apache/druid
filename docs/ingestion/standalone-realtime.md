---
id: standalone-realtime
layout: doc_page
title: "Realtime Process"
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

Older versions of Apache Druid supported a standalone 'Realtime' process to query and index 'stream pull'
modes of real-time ingestion. These processes would periodically build segments for the data they had collected over
some span of time and then set up hand-off to [Historical](../design/historical.md) servers.

This processes could be invoked by

```
org.apache.druid.cli.Main server realtime
```

This model of stream pull ingestion was deprecated for a number of both operational and architectural reasons, and
removed completely in Druid 0.16.0. Operationally, realtime nodes were difficult to configure, deploy, and scale because
each node required an unique configuration. The design of the stream pull ingestion system for realtime nodes also
suffered from limitations which made it not possible to achieve exactly once ingestion.

The extensions `druid-kafka-eight`, `druid-kafka-eight-simpleConsumer`, `druid-rabbitmq`, and `druid-rocketmq` were also
removed at this time, since they were built to operate on the realtime nodes.

Please consider using the [Kafka Indexing Service](../ingestion/kafka-ingestion.md) or
[Kinesis Indexing Service](../ingestion/kinesis-ingestion.md) for stream pull ingestion instead.
