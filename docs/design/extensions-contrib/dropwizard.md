---
layout: doc_page
---

# Dropwizard Emitter

To use this extension, make sure to [include](../../operations/including-extensions.html) `dropwizard-emitter` extension.

## Introduction

The intent of this extension is to integrate [Dropwizard](http://metrics.dropwizard.io/3.1.0/getting-started/#) metrics library with druid so that dropwizard users can easily absorb druid into their monitoring ecosystem.
It accumulates druid metrics in a dropwizard histogram and emits them to various sinks via dropwizard supported reporters.
A histogram measures the statistical distribution of values in a stream of data. In addition to minimum, maximum, mean, etc., it also measures median, 75th, 90th, 95th, 98th, 99th, and 99.9th percentiles.
Currently dropwizard metrics can be emitted to these sinks:
Console,HTTP,JMX,Graphite,CSV,Slf4jLogger,Ganglia and various other community supported [sinks](http://metrics.dropwizard.io/3.1.0/manual/third-party/).

## Configuration

All the configuration parameters for Dropwizard emitter are under `druid.emitter.dropwizard`.

|property|description|required?|default|
|--------|-----------|---------|-------|
|`druid.emitter.dropwizard.metric`|The metric manager to be used.Currently supported metric manager is histogram|no|histogram|
|`druid.emitter.dropwizard.reporter`|The dropwizard reporter to be used.|yes|none|
|`druid.emitter.dropwizard.eventConverter`| Filter and converter of druid events to dropwizard event(please see next section). |yes|none|
|`druid.emitter.dropwizard.alertEmitters`| List of emitters where alerts will be forwarded to. |no| empty list (no forwarding)|


### Druid to Dropwizard Event Converter
 
Dropwizard Event Converter defines a mapping between druid metrics name plus dimensions to a Dropwizard metric name.
Dropwizard metric name is organized using the following schema:
`<namespacePrefix>.[<druid service name>].[<druid hostname>].<druid metrics dimensions>.<druid metrics name>`
Properly naming the metrics is critical to avoid conflicts, confusing data and potentially wrong interpretation later on.

Example `druid.historical.abc_com:8080.MyDataSourceName.GroupBy.query/time`:

 * `druid` -> namespace prefix 
 * `historical` -> service name 
 * `abc.com:8080` -> druid hostname
 * `MyDataSourceName` -> dimension value 
 * `GroupBy` -> dimension value
 * `query/time` -> metric name

We have two different implementation of event converter:

#### Send-All converter

The first implementation called `all`, will send all the druid service metrics events. 
The metric name will be in the form `<namespacePrefix>.[<druid service name>].[<druid hostname>].<dimensions values ordered by dimension's name>.<metric>`
User has control of `<namespacePrefix>.[<druid service name>].[<druid hostname>].`

You can omit the hostname by setting `ignoreHostname=true`
`druid.SERVICE_NAME.dataSourceName.queryType.query.time`

You can omit the service name by setting `ignoreServiceName=true`
`druid.HOSTNAME.dataSourceName.queryType.query.time`

```json

druid.emitter.dropwizard.eventConverter={"type":"all", "namespacePrefix": "druid.test", "ignoreHostname":true, "ignoreServiceName":true}

```

#### White-list based converter

The second implementation called `whiteList`, will send only the white listed metrics and dimensions.
Same as for the `all` converter user has control of `<namespacePrefix>.[<druid service name>].[<druid hostname>].`
White-list based converter comes with the following  default white list map located under resources in `./src/main/resources/defaultWhiteListMap.json`

Although user can override the default white list map by supplying a property called `mapPath`.
This property is a String containing  the path for the file containing **white list map Json object**.
For example the following converter will read the map from the file `/pathPrefix/fileName.json`.  

```json

druid.emitter.dropwizard.eventConverter={"type":"whiteList", "namespacePrefix": "druid.test", "ignoreHostname":true, "ignoreServiceName":true, "mapPath":"/pathPrefix/fileName.json"}

```

**Druid emits a huge number of metrics we highly recommend to use the `whiteList` converter**

### Metric Manager

Metric manager defines the dropwizard accumulator that would be used to accumulate druid metric events.
For eg : Gauge,Counter,Histogram,Meter etc.

### Dropwizard reporter

```json

druid.emitter.dropwizard.reporter={"type":"jmx"}

```

```json

druid.emitter.dropwizard.reporter={"type":"console","emitIntervalInSecs":30}"}

```