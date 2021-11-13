---
layout: doc_page
title: "Dropwizard metrics emitter"
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

# Dropwizard Emitter

To use this extension, make sure to [include](../../development/extensions.md#loading-extensions) `dropwizard-emitter` in the extensions load list.

## Introduction

This extension integrates [Dropwizard](http://metrics.dropwizard.io/3.1.0/getting-started/#) metrics library with druid so that dropwizard users can easily absorb druid into their monitoring ecosystem.
It accumulates druid metrics as dropwizard metrics, and emits them to various sinks via dropwizard supported reporters.
Currently supported dropwizard metrics types counter, gauge, meter, timer and histogram. 
These metrics can be emitted using either Console or JMX reporter. 

To use this emitter, set

```
druid.emitter=dropwizard
```

## Configuration

All the configuration parameters for Dropwizard emitter are under `druid.emitter.dropwizard`.
    
|property|description|required?|default|
|--------|-----------|---------|-------|
|`druid.emitter.dropwizard.reporters`|List of dropwizard reporters to be used. Here is a list of [Supported Reporters](#supported-dropwizard-reporters)|yes|none|
|`druid.emitter.dropwizard.prefix`|Optional prefix to be used for metrics name|no|none|
|`druid.emitter.dropwizard.includeHost`|Flag to include the host and port as part of the metric name.|no|yes|
|`druid.emitter.dropwizard.dimensionMapPath`|Path to JSON file defining the dropwizard metric type, and desired dimensions for every Druid metric|no|Default mapping provided. See below.|
|`druid.emitter.dropwizard.alertEmitters`| List of emitters where alerts will be forwarded to. |no| empty list (no forwarding)|
|`druid.emitter.dropwizard.maxMetricsRegistrySize`| Maximum size of metrics registry to be cached at any time. |no| 100 Mb|


### Druid to Dropwizard Event Conversion

Each metric emitted using Dropwizard must specify a type, one of `[timer, counter, guage, meter, histogram]`. Dropwizard Emitter expects this mapping to
be provided as a JSON file.  Additionally, this mapping specifies which dimensions should be included for each metric.
If the user does not specify their own JSON file, a [default mapping](#default-metrics-mapping) is used.
All metrics are expected to be mapped. Metrics which are not mapped will be ignored.
Dropwizard metric path is organized using the following schema:

`<druid metric name> : { "dimensions" : <dimension list>, "type" : <Dropwizard metric type>, "timeUnit" : <For timers, timeunit in which metric is emitted>}`

e.g.
```json
"query/time" : { "dimensions" : ["dataSource", "type"], "type" : "timer", "timeUnit": "MILLISECONDS"},
"segment/scan/pending" : { "dimensions" : [], "type" : "gauge"}
```

For most use-cases, the default mapping is sufficient.

### Supported Dropwizard reporters

#### JMX Reporter
Used to report druid metrics via JMX.
```

druid.emitter.dropwizard.reporters=[{"type":"jmx"}]

```

#### Console Reporter
Used to print Druid Metrics to console logs.

```

druid.emitter.dropwizard.reporters=[{"type":"console","emitIntervalInSecs":30}"}]

```

### Default Metrics Mapping
Latest default metrics mapping can be found [here] (https://github.com/apache/druid/blob/master/extensions-contrib/dropwizard-emitter/src/main/resources/defaultMetricDimensions.json)
```json
{
  "query/time": {
    "dimensions": [
      "dataSource",
      "type"
    ],
    "type": "timer",
    "timeUnit": "MILLISECONDS"
  },
  "query/node/time": {
    "dimensions": [
      "server"
    ],
    "type": "timer",
    "timeUnit": "MILLISECONDS"
  },
  "query/node/ttfb": {
    "dimensions": [
      "server"
    ],
    "type": "timer",
    "timeUnit": "MILLISECONDS"
  },
  "query/node/backpressure": {
    "dimensions": [
      "server"
    ],
    "type": "timer",
    "timeUnit": "MILLISECONDS"
  },
  "query/segment/time": {
    "dimensions": [],
    "type": "timer",
    "timeUnit": "MILLISECONDS"
  },
  "query/wait/time": {
    "dimensions": [],
    "type": "timer",
    "timeUnit": "MILLISECONDS"
  },
  "segment/scan/pending": {
    "dimensions": [],
    "type": "gauge"
  },
  "query/segmentAndCache/time": {
    "dimensions": [],
    "type": "timer",
    "timeUnit": "MILLISECONDS"
  },
  "query/cpu/time": {
    "dimensions": [
      "dataSource",
      "type"
    ],
    "type": "timer",
    "timeUnit": "NANOSECONDS"
  },
  "query/cache/delta/numEntries": {
    "dimensions": [],
    "type": "counter"
  },
  "query/cache/delta/sizeBytes": {
    "dimensions": [],
    "type": "counter"
  },
  "query/cache/delta/hits": {
    "dimensions": [],
    "type": "counter"
  },
  "query/cache/delta/misses": {
    "dimensions": [],
    "type": "counter"
  },
  "query/cache/delta/evictions": {
    "dimensions": [],
    "type": "counter"
  },
  "query/cache/delta/hitRate": {
    "dimensions": [],
    "type": "counter"
  },
  "query/cache/delta/averageBytes": {
    "dimensions": [],
    "type": "counter"
  },
  "query/cache/delta/timeouts": {
    "dimensions": [],
    "type": "counter"
  },
  "query/cache/delta/errors": {
    "dimensions": [],
    "type": "counter"
  },
  "query/cache/total/numEntries": {
    "dimensions": [],
    "type": "gauge"
  },
  "query/cache/total/sizeBytes": {
    "dimensions": [],
    "type": "gauge"
  },
  "query/cache/total/hits": {
    "dimensions": [],
    "type": "gauge"
  },
  "query/cache/total/misses": {
    "dimensions": [],
    "type": "gauge"
  },
  "query/cache/total/evictions": {
    "dimensions": [],
    "type": "gauge"
  },
  "query/cache/total/hitRate": {
    "dimensions": [],
    "type": "gauge"
  },
  "query/cache/total/averageBytes": {
    "dimensions": [],
    "type": "gauge"
  },
  "query/cache/total/timeouts": {
    "dimensions": [],
    "type": "gauge"
  },
  "query/cache/total/errors": {
    "dimensions": [],
    "type": "gauge"
  },
  "ingest/events/thrownAway": {
    "dimensions": [
      "dataSource"
    ],
    "type": "counter"
  },
  "ingest/events/unparseable": {
    "dimensions": [
      "dataSource"
    ],
    "type": "counter"
  },
  "ingest/events/duplicate": {
    "dimensions": [
      "dataSource"
    ],
    "type": "counter"
  },
  "ingest/events/processed": {
    "dimensions": [
      "dataSource"
    ],
    "type": "counter"
  },
  "ingest/rows/output": {
    "dimensions": [
      "dataSource"
    ],
    "type": "counter"
  },
  "ingest/persist/counter": {
    "dimensions": [
      "dataSource"
    ],
    "type": "counter"
  },
  "ingest/persist/time": {
    "dimensions": [
      "dataSource"
    ],
    "type": "timer",
    "timeUnit": "MILLISECONDS"
  },
  "ingest/persist/cpu": {
    "dimensions": [
      "dataSource"
    ],
    "type": "timer",
    "timeUnit": "NANOSECONDS"
  },
  "ingest/persist/backPressure": {
    "dimensions": [
      "dataSource"
    ],
    "type": "gauge"
  },
  "ingest/persist/failed": {
    "dimensions": [
      "dataSource"
    ],
    "type": "counter"
  },
  "ingest/handoff/failed": {
    "dimensions": [
      "dataSource"
    ],
    "type": "counter"
  },
  "ingest/merge/time": {
    "dimensions": [
      "dataSource"
    ],
    "type": "timer",
    "timeUnit": "MILLISECONDS"
  },
  "ingest/merge/cpu": {
    "dimensions": [
      "dataSource"
    ],
    "type": "timer",
    "timeUnit": "NANOSECONDS"
  },
  "task/run/time": {
    "dimensions": [
      "dataSource",
      "taskType"
    ],
    "type": "timer",
    "timeUnit": "MILLISECONDS"
  },
  "segment/added/bytes": {
    "dimensions": [
      "dataSource",
      "taskType"
    ],
    "type": "counter"
  },
  "segment/moved/bytes": {
    "dimensions": [
      "dataSource",
      "taskType"
    ],
    "type": "counter"
  },
  "segment/nuked/bytes": {
    "dimensions": [
      "dataSource",
      "taskType"
    ],
    "type": "counter"
  },
  "segment/assigned/counter": {
    "dimensions": [
      "tier"
    ],
    "type": "counter"
  },
  "segment/moved/counter": {
    "dimensions": [
      "tier"
    ],
    "type": "counter"
  },
  "segment/dropped/counter": {
    "dimensions": [
      "tier"
    ],
    "type": "counter"
  },
  "segment/deleted/counter": {
    "dimensions": [
      "tier"
    ],
    "type": "counter"
  },
  "segment/unneeded/counter": {
    "dimensions": [
      "tier"
    ],
    "type": "counter"
  },
  "segment/cost/raw": {
    "dimensions": [
      "tier"
    ],
    "type": "counter"
  },
  "segment/cost/normalization": {
    "dimensions": [
      "tier"
    ],
    "type": "counter"
  },
  "segment/cost/normalized": {
    "dimensions": [
      "tier"
    ],
    "type": "counter"
  },
  "segment/loadQueue/size": {
    "dimensions": [
      "server"
    ],
    "type": "gauge"
  },
  "segment/loadQueue/failed": {
    "dimensions": [
      "server"
    ],
    "type": "gauge"
  },
  "segment/loadQueue/counter": {
    "dimensions": [
      "server"
    ],
    "type": "gauge"
  },
  "segment/dropQueue/counter": {
    "dimensions": [
      "server"
    ],
    "type": "gauge"
  },
  "segment/size": {
    "dimensions": [
      "dataSource"
    ],
    "type": "gauge"
  },
  "segment/overShadowed/counter": {
    "dimensions": [],
    "type": "gauge"
  },
  "segment/max": {
    "dimensions": [],
    "type": "gauge"
  },
  "segment/used": {
    "dimensions": [
      "dataSource",
      "tier",
      "priority"
    ],
    "type": "gauge"
  },
  "segment/usedPercent": {
    "dimensions": [
      "dataSource",
      "tier",
      "priority"
    ],
    "type": "gauge"
  },
  "jvm/pool/committed": {
    "dimensions": [
      "poolKind",
      "poolName"
    ],
    "type": "gauge"
  },
  "jvm/pool/init": {
    "dimensions": [
      "poolKind",
      "poolName"
    ],
    "type": "gauge"
  },
  "jvm/pool/max": {
    "dimensions": [
      "poolKind",
      "poolName"
    ],
    "type": "gauge"
  },
  "jvm/pool/used": {
    "dimensions": [
      "poolKind",
      "poolName"
    ],
    "type": "gauge"
  },
  "jvm/bufferpool/counter": {
    "dimensions": [
      "bufferpoolName"
    ],
    "type": "gauge"
  },
  "jvm/bufferpool/used": {
    "dimensions": [
      "bufferpoolName"
    ],
    "type": "gauge"
  },
  "jvm/bufferpool/capacity": {
    "dimensions": [
      "bufferpoolName"
    ],
    "type": "gauge"
  },
  "jvm/mem/init": {
    "dimensions": [
      "memKind"
    ],
    "type": "gauge"
  },
  "jvm/mem/max": {
    "dimensions": [
      "memKind"
    ],
    "type": "gauge"
  },
  "jvm/mem/used": {
    "dimensions": [
      "memKind"
    ],
    "type": "gauge"
  },
  "jvm/mem/committed": {
    "dimensions": [
      "memKind"
    ],
    "type": "gauge"
  },
  "jvm/gc/counter": {
    "dimensions": [
      "gcName",
      "gcGen"
    ],
    "type": "counter"
  },
  "jvm/gc/cpu": {
    "dimensions": [
      "gcName",
      "gcGen"
    ],
    "type": "timer",
    "timeUnit": "NANOSECONDS"
  },
  "ingest/events/buffered": {
    "dimensions": [
      "serviceName",
      "bufferCapacity"
    ],
    "type": "gauge"
  },
  "sys/swap/free": {
    "dimensions": [],
    "type": "gauge"
  },
  "sys/swap/max": {
    "dimensions": [],
    "type": "gauge"
  },
  "sys/swap/pageIn": {
    "dimensions": [],
    "type": "gauge"
  },
  "sys/swap/pageOut": {
    "dimensions": [],
    "type": "gauge"
  },
  "sys/disk/write/counter": {
    "dimensions": [
      "fsDevName"
    ],
    "type": "counter"
  },
  "sys/disk/read/counter": {
    "dimensions": [
      "fsDevName"
    ],
    "type": "counter"
  },
  "sys/disk/write/size": {
    "dimensions": [
      "fsDevName"
    ],
    "type": "counter"
  },
  "sys/disk/read/size": {
    "dimensions": [
      "fsDevName"
    ],
    "type": "counter"
  },
  "sys/net/write/size": {
    "dimensions": [],
    "type": "counter"
  },
  "sys/net/read/size": {
    "dimensions": [],
    "type": "counter"
  },
  "sys/fs/used": {
    "dimensions": [
      "fsDevName",
      "fsDirName",
      "fsTypeName",
      "fsSysTypeName",
      "fsOptions"
    ],
    "type": "gauge"
  },
  "sys/fs/max": {
    "dimensions": [
      "fsDevName",
      "fsDirName",
      "fsTypeName",
      "fsSysTypeName",
      "fsOptions"
    ],
    "type": "gauge"
  },
  "sys/mem/used": {
    "dimensions": [],
    "type": "gauge"
  },
  "sys/mem/max": {
    "dimensions": [],
    "type": "gauge"
  },
  "sys/storage/used": {
    "dimensions": [
      "fsDirName"
    ],
    "type": "gauge"
  },
  "sys/cpu": {
    "dimensions": [
      "cpuName",
      "cpuTime"
    ],
    "type": "gauge"
  },
  "coordinator-segment/counter": {
    "dimensions": [
      "dataSource"
    ],
    "type": "gauge"
  },
  "historical-segment/counter": {
    "dimensions": [
      "dataSource",
      "tier",
      "priority"
    ],
    "type": "gauge"
  },
  "jetty/numOpenConnections": {
    "dimensions": [],
    "type": "gauge"
  },
  "jetty/threadPool/total": {
    "dimensions": [],
    "type": "gauge"
  },
  "jetty/threadPool/idle": {
    "dimensions": [],
    "type": "gauge"
  },
  "jetty/threadPool/busy": {
    "dimensions": [],
    "type": "gauge"
  },
  "jetty/threadPool/isLowOnThreads": {
    "dimensions": [],
    "type": "gauge"
  },
  "jetty/threadPool/min": {
    "dimensions": [],
    "type": "gauge"
  },
  "jetty/threadPool/max": {
    "dimensions": [],
    "type": "gauge"
  },
  "jetty/threadPool/queueSize": {
    "dimensions": [],
    "type": "gauge"
  }
}
```
