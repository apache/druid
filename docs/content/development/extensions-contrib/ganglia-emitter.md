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

---
layout: doc_page
---

# Ganglia Emitter

To use this extension, make sure to [include](../../operations/including-extensions.html) `ganglia-emitter` extension.

## Introduction

This extension emits druid metrics to a Ganglia server.
(http://ganglia.info/)

## Configuration

All the configuration parameters for the Ganglia emitter are under `druid.emitter.ganglia`.

|property|description|required?|default|
|--------|-----------|---------|-------|
|`druid.emitter.ganglia.hostname`|The hostname of the Ganglia server.|yes|none|
|`druid.emitter.ganglia.port`|The port of the Ganglia server.|yes|none|
|`druid.emitter.ganglia.separator`|Metric name separator|no|.|
|`druid.emitter.ganglia.includeHost`|Flag to include the hostname as part of the metric name.|no|false|
|`druid.emitter.ganglia.dimensionMapPath`|JSON file defining the Ganglia type, and desired dimensions for every Druid metric|no|Default mapping provided. See below.|
|`druid.emitter.ganglia.blankHolder`|The blank character replacement as Ganglia does not support path with blank character|no|"-"|
|`druid.emitter.ganglia.flushPeriod` | Queue flushing period in milliseconds. |no|1 minute|
|`druid.emitter.ganglia.loadPeriod` | Load metrics config period in milliseconds. |no|1 minute|
|`druid.emitter.ganglia.maxQueueSize`| Maximum size of the queue used to buffer events. |no|`MAX_INT`|
|`druid.emitter.ganglia.emitWaitTime` | wait time in milliseconds to try to send the event otherwise emitter will throwing event. |no|0|
|`druid.emitter.ganglia.waitForEventTime` | waiting time in milliseconds if necessary for an event to become available. |no|1000 (1 sec)|

### Druid to Ganglia Event Converter
Ganglia Emitter expects this mapping to
be provided as a JSON file.  Additionally, this mapping specifies which dimensions should be included for each metric.

Ganglia metric path is organized using the following schema:
`<druid metric name> : { "dimensions" : <dimension list>}`
e.g.
`query/time" : { "dimensions" : ["dataSource", "type"]}`

For metrics which are emitted from multiple services with different dimensions, the metric name is prefixed with
the service name.
e.g.
`"coordinator-segment/count" : { "dimensions" : ["dataSource"]},
 "historical-segment/count" : { "dimensions" : ["dataSource", "tier", "priority"]}`

For most use-cases, the default mapping is sufficient.
