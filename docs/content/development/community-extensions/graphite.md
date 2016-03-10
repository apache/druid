---
layout: doc_page
---

# Graphite Emitter

## Introduction

This extension emits druid metrics to a graphite carbon server.
Events are sent after been [pickled](http://graphite.readthedocs.org/en/latest/feeding-carbon.html#the-pickle-protocol); the size of the batch is configurable. 

## Configuration

All the configuration parameters for graphite emitter are under `druid.emitter.graphite`.

|property|description|required?|default|
|--------|-----------|---------|-------|
|`druid.emitter.graphite.hostname`|The hostname of the graphite server.|yes|none|
|`druid.emitter.graphite.port`|The port of the graphite server.|yes|none|
|`druid.emitter.graphite.batchSize`|Number of events to send as one batch.|no|100|
|`druid.emitter.graphite.eventConverter`| Filter and converter of druid events to graphite event(please see next section). |yes|none|  
|`druid.emitter.graphite.flushPeriod` | Queue flushing period in milliseconds. |no|1 minute|
|`druid.emitter.graphite.maxQueueSize`| Maximum size of the queue used to buffer events. |no|`MAX_INT`|
|`druid.emitter.graphite.alertEmitters`| List of emitters where alerts will be forwarded to. |no| empty list (no forwarding)|
 
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
`druid.SERVICE_NAME.dataSourceName.queryType.query.time`

You can omit the service name by setting `ignoreServiceName=true`
`druid.HOSTNAME.dataSourceName.queryType.query.time`

```json

druid.emitter.graphite.eventConverter={"type":"all", "namespacePrefix": "druid.test", "ignoreHostname":true, "ignoreServiceName":true}

```

#### White-list based converter

The second implementation called `whiteList`, will send only the white listed metrics and dimensions.
Same as for the `all` converter user has control of `<namespacePrefix>.[<druid service name>].[<druid hostname>].`
White-list based converter comes with the following  default white list map located under resources [defaultWhiteListMap.json](./src/main/resources/defaultWhiteListMap.json)

Although user can override the default white list map by supplying a property called `mapPath`.
This property is a String containing  the path for the file containing **white list map Json object**.
For example the following converter will read the map from the file `/pathPrefix/fileName.json`.  

```json

druid.emitter.graphite.eventConverter={"type":"whiteList", "namespacePrefix": "druid.test", "ignoreHostname":true, "ignoreServiceName":true, "mapPath":"/pathPrefix/fileName.json"}

```

**Druid emits a huge number of metrics we highly recommend to use the `whiteList` converter**
