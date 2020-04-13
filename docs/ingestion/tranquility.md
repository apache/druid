---
id: tranquility
title: "Tranquility"
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

[Tranquility](https://github.com/druid-io/tranquility/) is a package for pushing
streams to Druid in real-time. Druid does not come bundled with Tranquility; it is available as a separate download.

Note that as of this writing, the latest available version of Tranquility is built against the rather old Druid 0.9.2
release. It will still work with the latest Druid servers, but not all features and functionality will be available
due to limitations of older Druid APIs on the Tranquility side.

For new projects that require streaming ingestion, we recommend using Druid's native support for
[Apache Kafka](../development/extensions-core/kafka-ingestion.md) or
[Amazon Kinesis](../development/extensions-core/kinesis-ingestion.md).

For more details, check out the [Tranquility GitHub page](https://github.com/druid-io/tranquility/).
