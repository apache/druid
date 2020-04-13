---
id: graphite
title: "Graphite Emitter"
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


To use this Apache Druid extension, make sure to [include](../../development/extensions.md#loading-extensions) `graphite-emitter` extension.

## Introduction

This extension emits druid metrics to a graphite carbon server.
Metrics can be sent by using [plaintext](http://graphite.readthedocs.io/en/latest/feeding-carbon.html#the-plaintext-protocol) or [pickle](http://graphite.readthedocs.io/en/latest/feeding-carbon.html#the-pickle-protocol) protocol.
The pickle protocol is more efficient and supports sending batches of metrics (plaintext protocol send only one metric) in one request; batch size is configurable.

## Configuration

All the configuration parameters for graphite emitter are under `druid.emitter.graphite`.

|property|description|required?|default|
|--------|-----------|---------|-------|
|`druid.emitter.graphite.hostname`|The hostname of the graphite server.|yes|none|
|`druid.emitter.graphite.port`|The port of the graphite server.|yes|none|
|`druid.emitter.graphite.batchSize`|Number of events to send as one batch (only for pickle protocol)|no|100|
|`druid.emitter.graphite.protocol`|Graphite protocol; available protocols: pickle, plaintext.|no|pickle|
|`druid.emitter.graphite.eventConverter`| Filter and converter of druid events to graphite event (please see next section).|yes|none|
|`druid.emitter.graphite.flushPeriod` | Queue flushing period in milliseconds. |no|1 minute|
|`druid.emitter.graphite.maxQueueSize`| Maximum size of the queue used to buffer events. |no|`MAX_INT`|
|`druid.emitter.graphite.alertEmitters`| List of emitters where alerts will be forwarded to. This is a JSON list of emitter names, e.g. `["logging", "http"]`|no| empty list (no forwarding)|
|`druid.emitter.graphite.requestLogEmitters`| List of emitters where request logs (i.e., query logging events sent to emitters when `druid.request.logging.type` is set to `emitter`) will be forwarded to. This is a JSON list of emitter names, e.g. `["logging", "http"]`|no| empty list (no forwarding)|
|`druid.emitter.graphite.emitWaitTime` | wait time in milliseconds to try to send the event otherwise emitter will throwing event. |no|0|
|`druid.emitter.graphite.waitForEventTime` | waiting time in milliseconds if necessary for an event to become available. |no|1000 (1 sec)|

### Supported event types

The graphite emitter only emits service metric events to graphite (See [Druid Metrics](../../operations/metrics.md) for a list of metrics).

Alerts and request logs are not sent to graphite. These event types are not well represented in Graphite, which is more suited for timeseries views on numeric metrics, vs. storing non-numeric log events.

Instead, alerts and request logs are optionally forwarded to other emitter implementations, specified by `druid.emitter.graphite.alertEmitters` and `druid.emitter.graphite.requestLogEmitters` respectively.

### Druid to Graphite Event Converter

Graphite Event Converter defines a mapping between druid metrics name plus dimensions to a Graphite metric path.
Graphite metric path is organized using the following schema:
`<namespacePrefix>.[<druid service name>].[<druid hostname>].<druid metrics dimensions>.<druid metrics name>`
Properly naming the metrics is critical to avoid conflicts, confusing data and potentially wrong interpretation later on.

Example `druid.historical.hist-host1_yahoo_com:8080.MyDataSourceName.GroupBy.query/time`:

 * `druid` -> namespace prefix
 * `historical` -> service name
 * `hist-host1.yahoo.com:8080` -> druid hostname
 * `MyDataSourceName` -> dimension value
 * `GroupBy` -> dimension value
 * `query/time` -> metric name

We have two different implementation of event converter:

#### Send-All converter

The first implementation called `all`, will send all the druid service metrics events.
The path will be in the form `<namespacePrefix>.[<druid service name>].[<druid hostname>].<dimensions values ordered by dimension's name>.<metric>`
User has control of `<namespacePrefix>.[<druid service name>].[<druid hostname>].`

You can omit the hostname by setting `ignoreHostname=true`
`druid.SERVICE_NAME.dataSourceName.queryType.query/time`

You can omit the service name by setting `ignoreServiceName=true`
`druid.HOSTNAME.dataSourceName.queryType.query/time`

Elements in metric name by default are separated by "/", so graphite will create all metrics on one level. If you want to have metrics in the tree structure, you have to set `replaceSlashWithDot=true`
Original: `druid.HOSTNAME.dataSourceName.queryType.query/time`
Changed: `druid.HOSTNAME.dataSourceName.queryType.query.time`


```json

druid.emitter.graphite.eventConverter={"type":"all", "namespacePrefix": "druid.test", "ignoreHostname":true, "ignoreServiceName":true}

```

#### White-list based converter

The second implementation called `whiteList`, will send only the white listed metrics and dimensions.
Same as for the `all` converter user has control of `<namespacePrefix>.[<druid service name>].[<druid hostname>].`
White-list based converter comes with the following  default white list map located under resources in `./src/main/resources/defaultWhiteListMap.json`

Although user can override the default white list map by supplying a property called `mapPath`.
This property is a String containing the path for the file containing **white list map JSON object**.
For example the following converter will read the map from the file `/pathPrefix/fileName.json`.

```json

druid.emitter.graphite.eventConverter={"type":"whiteList", "namespacePrefix": "druid.test", "ignoreHostname":true, "ignoreServiceName":true, "mapPath":"/pathPrefix/fileName.json"}

```

**Druid emits a huge number of metrics we highly recommend to use the `whiteList` converter**
