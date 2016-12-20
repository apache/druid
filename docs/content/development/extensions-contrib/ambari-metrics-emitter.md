---
layout: doc_page
---

# Ambari Metrics Emitter

To use this extension, make sure to [include](../../operations/including-extensions.html) `ambari-metrics-emitter` extension.

## Introduction

This extension emits druid metrics to a ambari-metrics carbon server.
Events are sent after been [pickled](http://ambari-metrics.readthedocs.org/en/latest/feeding-carbon.html#the-pickle-protocol); the size of the batch is configurable. 

## Configuration

All the configuration parameters for ambari-metrics emitter are under `druid.emitter.ambari-metrics`.

|property|description|required?|default|
|--------|-----------|---------|-------|
|`druid.emitter.ambari-metrics.hostname`|The hostname of the ambari-metrics server.|yes|none|
|`druid.emitter.ambari-metrics.port`|The port of the ambari-metrics server.|yes|none|
|`druid.emitter.ambari-metrics.protocol`|The protocol used to send metrics to ambari metrics collector. One of http/https|no|http|
|`druid.emitter.ambari-metrics.trustStorePath`|Path to trustStore to be used for https|no|none|
|`druid.emitter.ambari-metrics.trustStoreType`|trustStore type to be used for https|no|none|
|`druid.emitter.ambari-metrics.trustStoreType`|trustStore password to be used for https|no|none|
|`druid.emitter.ambari-metrics.batchSize`|Number of events to send as one batch.|no|100|
|`druid.emitter.ambari-metrics.eventConverter`| Filter and converter of druid events to ambari-metrics timeline event(please see next section). |yes|none|  
|`druid.emitter.ambari-metrics.flushPeriod` | Queue flushing period in milliseconds. |no|1 minute|
|`druid.emitter.ambari-metrics.maxQueueSize`| Maximum size of the queue used to buffer events. |no|`MAX_INT`|
|`druid.emitter.ambari-metrics.alertEmitters`| List of emitters where alerts will be forwarded to. |no| empty list (no forwarding)|
|`druid.emitter.ambari-metrics.emitWaitTime` | wait time in milliseconds to try to send the event otherwise emitter will throwing event. |no|0|
|`druid.emitter.ambari-metrics.waitForEventTime` | waiting time in milliseconds if necessary for an event to become available. |no|1000 (1 sec)|

### Druid to Ambari Metrics Timeline Event Converter
 
Ambari Metrics Timeline Event Converter defines a mapping between druid metrics name plus dimensions to a timeline event metricName.
ambari-metrics metric path is organized using the following schema:
`<namespacePrefix>.[<druid service name>].[<druid hostname>].<druid metrics dimensions>.<druid metrics name>`
Properly naming the metrics is critical to avoid conflicts, confusing data and potentially wrong interpretation later on.

Example `druid.historical.hist-host1:8080.MyDataSourceName.GroupBy.query/time`:

 * `druid` -> namespace prefix 
 * `historical` -> service name 
 * `hist-host1:8080` -> druid hostname
 * `MyDataSourceName` -> dimension value 
 * `GroupBy` -> dimension value
 * `query/time` -> metric name

We have two different implementation of event converter:

#### Send-All converter

The first implementation called `all`, will send all the druid service metrics events. 
The path will be in the form `<namespacePrefix>.[<druid service name>].[<druid hostname>].<dimensions values ordered by dimension's name>.<metric>`
User has control of `<namespacePrefix>.[<druid service name>].[<druid hostname>].`

```json

druid.emitter.ambari-metrics.eventConverter={"type":"all", "namespacePrefix": "druid.test", "appName":"druid"}

```

#### White-list based converter

The second implementation called `whiteList`, will send only the white listed metrics and dimensions.
Same as for the `all` converter user has control of `<namespacePrefix>.[<druid service name>].[<druid hostname>].`
White-list based converter comes with the following  default white list map located under resources in `./src/main/resources/defaultWhiteListMap.json`

Although user can override the default white list map by supplying a property called `mapPath`.
This property is a String containing  the path for the file containing **white list map Json object**.
For example the following converter will read the map from the file `/pathPrefix/fileName.json`.  

```json

druid.emitter.ambari-metrics.eventConverter={"type":"whiteList", "namespacePrefix": "druid.test", "ignoreHostname":true, "appName":"druid", "mapPath":"/pathPrefix/fileName.json"}

```

**Druid emits a huge number of metrics we highly recommend to use the `whiteList` converter**
