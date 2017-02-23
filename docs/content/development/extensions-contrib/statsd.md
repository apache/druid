---
layout: doc_page
---

# StatsD Emitter

To use this extension, make sure to [include](../../operations/including-extensions.html) `statsd-emitter` extension.

## Introduction

This extension emits druid metrics to a StatsD server.
(https://github.com/etsy/statsd)
(https://github.com/armon/statsite)

## Configuration

All the configuration parameters for the StatsD emitter are under `druid.emitter.statsd`.

|property|description|required?|default|
|--------|-----------|---------|-------|
|`druid.emitter.statsd.hostname`|The hostname of the StatsD server.|yes|none|
|`druid.emitter.statsd.port`|The port of the StatsD server.|yes|none|
|`druid.emitter.statsd.prefix`|Optional metric name prefix.|no|""|
|`druid.emitter.statsd.separator`|Metric name separator|no|.|  
|`druid.emitter.statsd.includeHost`|Flag to include the hostname as part of the metric name.|no|false|  
|`druid.emitter.statsd.dimensionMapPath`|JSON file defining the StatsD type, and desired dimensions for every Druid metric|no|Default mapping provided. See below.|  

### Druid to StatsD Event Converter

Each metric sent to StatsD must specify a type, one of `[timer, counter, guage]`. StatsD Emitter expects this mapping to
be provided as a JSON file.  Additionally, this mapping specifies which dimensions should be included for each metric.
StatsD expects that metric values be integers.  Druid emits some metrics with values between the range 0 and 1. To accommodate these metrics they are converted
into the range 0 to 100.  This conversion can be enabled by setting the optional "convertRange" field true in the JSON mapping file.
If the user does not specify their own JSON file, a default mapping is used.  All
metrics are expected to be mapped. Metrics which are not mapped will log an error.
StatsD metric path is organized using the following schema:
`<druid metric name> : { "dimensions" : <dimension list>, "type" : <StatsD type>, "convertRange" : true/false}`
e.g.
`query/time" : { "dimensions" : ["dataSource", "type"], "type" : "timer"}`

For metrics which are emitted from multiple services with different dimensions, the metric name is prefixed with 
the service name. 
e.g.
`"coordinator-segment/count" : { "dimensions" : ["dataSource"], "type" : "gauge" },
 "historical-segment/count" : { "dimensions" : ["dataSource", "tier", "priority"], "type" : "gauge" }`
 
For most use-cases, the default mapping is sufficient.

