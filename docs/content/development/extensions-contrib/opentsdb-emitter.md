---
layout: doc_page
---

# Opentsdb Emitter

To use this extension, make sure to [include](../../operations/including-extensions.html) `opentsdb-emitter` extension.

## Introduction

This extension emits druid metrics to [OpenTSDB](https://github.com/OpenTSDB/opentsdb) over HTTP (Using `Jersey client`). And this emitter only emits service metric events to OpenTSDB (See http://druid.io/docs/latest/operations/metrics.html for a list of metrics).

## Configuration

All the configuration parameters for the opentsdb emitter are under `druid.emitter.opentsdb`.

|property|description|required?|default|
|--------|-----------|---------|-------|
|`druid.emitter.opentsdb.host`|The host of the OpenTSDB server.|yes|none|
|`druid.emitter.opentsdb.port`|The port of the OpenTSDB server.|yes|none|
|`druid.emitter.opentsdb.connectionTimeout`|`Jersey client` connection timeout(in milliseconds).|no|2000|
|`druid.emitter.opentsdb.readTimeout`|`Jersey client` read timeout(in milliseconds).|no|2000|
|`druid.emitter.opentsdb.flushThreshold`|Queue flushing threshold.(Events will be sent as one batch)|no|100|
|`druid.emitter.opentsdb.maxQueueSize`|Maximum size of the queue used to buffer events.|no|1000|
|`druid.emitter.opentsdb.consumeDelay`|Queue consuming delay(in milliseconds). Actually, we use `ScheduledExecutorService` to schedule consuming events, so this `consumeDelay` means the delay between the termination of one execution and the commencement of the next. If your druid nodes produce metric events fast, then you should decrease this `consumeDelay` or increase the `maxQueueSize`.|no|10000|
|`druid.emitter.opentsdb.metricMapPath`|JSON file defining the desired metrics and dimensions for every Druid metric|no|./src/main/resources/defaultMetrics.json|

### Druid to OpenTSDB Event Converter

The opentsdb emitter will send only the desired metrics and dimensions which is defined in a JSON file.
If the user does not specify their own JSON file, a default file is used.  All metrics are expected to be configured in the JSON file. Metrics which are not configured will be logged.
Desired metrics and dimensions is organized using the following schema:`<druid metric name> : [ <dimension list> ]`<br />
e.g.

```json
"query/time": [
    "dataSource",
    "type"
]
```
 
For most use-cases, the default configuration is sufficient.

