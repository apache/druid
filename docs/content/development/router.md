---
layout: doc_page
---

Router Node
===========

You should only ever need the router node if you have a Druid cluster well into the terabyte range. The router node can be used to route queries to different broker nodes. By default, the broker routes queries based on how [Rules](../operations/rule-configuration.html) are set up. For example, if 1 month of recent data is loaded into a `hot` cluster, queries that fall within the recent month can be routed to a dedicated set of brokers. Queries outside this range are routed to another set of brokers. This set up provides query isolation such that queries for more important data are not impacted by queries for less important data. 

Running
-------

```
io.druid.cli.Main server router
```

Example Production Configuration
--------------------------------

In this example, we have two tiers in our production cluster: `hot` and `_default_tier`. Queries for the `hot` tier are routed through the `broker-hot` set of brokers, and queries for the `_default_tier` are routed through the `broker-cold` set of brokers. If any exceptions or network problems occur, queries are routed to the `broker-cold` set of brokers. In our example, we are running with a c3.2xlarge EC2 node. We assume a `common.runtime.properties` already exists.

JVM settings:

```
-server
-Xmx13g
-Xms13g
-XX:NewSize=256m
-XX:MaxNewSize=256m
-XX:+UseConcMarkSweepGC
-XX:+PrintGCDetails
-XX:+PrintGCTimeStamps
-XX:+UseLargePages
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/mnt/galaxy/deploy/current/
-Duser.timezone=UTC
-Dfile.encoding=UTF-8
-Djava.io.tmpdir=/mnt/tmp

-Dcom.sun.management.jmxremote.port=17071
-Dcom.sun.management.jmxremote.authenticate=false
-Dcom.sun.management.jmxremote.ssl=false
```

Runtime.properties:

```
druid.host=#{IP_ADDR}:8080
druid.port=8080
druid.service=druid/router

druid.router.defaultBrokerServiceName=druid:broker-cold
druid.router.coordinatorServiceName=druid:coordinator
druid.router.tierToBrokerMap={"hot":"druid:broker-hot","_default_tier":"druid:broker-cold"}
druid.router.http.numConnections=50
druid.router.http.readTimeout=PT5M

# Number of threads used by the router proxy http client
druid.router.http.numMaxThreads=100

druid.server.http.numThreads=100
```

Runtime Configuration
---------------------

The router module uses several of the default modules in [Configuration](../configuration/index.html) and has the following set of configurations as well:

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.router.defaultBrokerServiceName`|Any string.|The default broker to connect to in case service discovery fails.|druid/broker|
|`druid.router.tierToBrokerMap`|An ordered JSON map of tiers to broker names. The priority of brokers is based on the ordering.|Queries for a certain tier of data are routed to their appropriate broker.|{"_default": "<defaultBrokerServiceName>"}|
|`druid.router.defaultRule`|Any string.|The default rule for all datasources.|"_default"|
|`druid.router.rulesEndpoint`|Any string.|The coordinator endpoint to extract rules from.|"/druid/coordinator/v1/rules"|
|`druid.router.coordinatorServiceName`|Any string.|The service discovery name of the coordinator.|druid/coordinator|
|`druid.router.pollPeriod`|Any ISO8601 duration.|How often to poll for new rules.|PT1M|
|`druid.router.strategies`|An ordered JSON array of objects.|All custom strategies to use for routing.|[{"type":"timeBoundary"},{"type":"priority"}]|

Router Strategies
-----------------
The router has a configurable list of strategies for how it selects which brokers to route queries to. The order of the strategies matter because as soon as a strategy condition is matched, a broker is selected.

### timeBoundary

```json
{
  "type":"timeBoundary"
}
```

Including this strategy means all timeBoundary queries are always routed to the highest priority broker.

### priority

```json
{
  "type":"priority",
  "minPriority":0,
  "maxPriority":1
}
```

Queries with a priority set to less than minPriority are routed to the lowest priority broker. Queries with priority set to greater than maxPriority are routed to the highest priority broker. By default, minPriority is 0 and maxPriority is 1. Using these default values, if a query with priority 0 (the default query priority is 0) is sent, the query skips the priority selection logic.

### JavaScript

Allows defining arbitrary routing rules using a JavaScript function. The function is passed the configuration and the query to be executed, and returns the tier it should be routed to, or null for the default tier.

*Example*: a function that sends queries containing more than three aggregators to the lowest priority broker.

```json
{
  "type" : "javascript",
  "function" : "function (config, query) { if (query.getAggregatorSpecs && query.getAggregatorSpecs().size() >= 3) { var size = config.getTierToBrokerMap().values().size(); if (size > 0) { return config.getTierToBrokerMap().values().toArray()[size-1] } else { return config.getDefaultBrokerServiceName() } } else { return null } }"
}
```

<div class="note info">
JavaScript-based functionality is disabled by default. Please refer to the Druid <a href="../development/javascript.html">JavaScript programming guide</a> for guidelines about using Druid's JavaScript functionality, including instructions on how to enable it.
</div>

HTTP Endpoints
--------------

The router node exposes several HTTP endpoints for interactions.

### GET

* `/status`

Returns the Druid version, loaded extensions, memory used, total memory and other useful information about the node.

* `/druid/v2/datasources`

Returns a list of queryable datasources.

* `/druid/v2/datasources/{dataSourceName}`

Returns the dimensions and metrics of the datasource.

* `/druid/v2/datasources/{dataSourceName}/dimensions`

Returns the dimensions of the datasource.

* `/druid/v2/datasources/{dataSourceName}/metrics`

Returns the metrics of the datasource.
