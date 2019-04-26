---
layout: doc_page
title: "Router Process"
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

# Router Process

The Apache Druid (incubating) Router process can be used to route queries to different Broker processes. By default, the broker routes queries based on how [Rules](../operations/rule-configuration.html) are set up. For example, if 1 month of recent data is loaded into a `hot` cluster, queries that fall within the recent month can be routed to a dedicated set of brokers. Queries outside this range are routed to another set of brokers. This set up provides query isolation such that queries for more important data are not impacted by queries for less important data. 

For query routing purposes, you should only ever need the Router process if you have a Druid cluster well into the terabyte range. 

In addition to query routing, the Router also runs the [Druid Console](../operations/management-uis.html#druid-console), a management UI for datasources, segments, tasks, data processes (Historicals and MiddleManagers), and coordinator dynamic configuration. The user can also run SQL and native Druid queries within the console.


Running
-------

```
org.apache.druid.cli.Main server router
```

Example Production Configuration
--------------------------------

In this example, we have two tiers in our production cluster: `hot` and `_default_tier`. Queries for the `hot` tier are routed through the `broker-hot` set of Brokers, and queries for the `_default_tier` are routed through the `broker-cold` set of Brokers. If any exceptions or network problems occur, queries are routed to the `broker-cold` set of brokers. In our example, we are running with a c3.2xlarge EC2 instance. We assume a `common.runtime.properties` already exists.

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
druid.plaintextPort=8080
druid.service=druid/router

druid.router.defaultBrokerServiceName=druid:broker-cold
druid.router.coordinatorServiceName=druid:coordinator
druid.router.tierToBrokerMap={"hot":"druid:broker-hot","_default_tier":"druid:broker-cold"}
druid.router.http.numConnections=50
druid.router.http.readTimeout=PT5M

# Number of threads used by the Router proxy http client
druid.router.http.numMaxThreads=100

druid.server.http.numThreads=100
```

Runtime Configuration
---------------------

The Router module uses several of the default modules in [Configuration](../configuration/index.html) and has the following set of configurations as well:

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.router.defaultBrokerServiceName`|Any string.|The default Broker to connect to in case service discovery fails.|druid/broker|
|`druid.router.tierToBrokerMap`|An ordered JSON map of tiers to Broker names. The priority of Brokers is based on the ordering.|Queries for a certain tier of data are routed to their appropriate Broker.|{"_default_tier": "<defaultBrokerServiceName>"}|
|`druid.router.defaultRule`|Any string.|The default rule for all datasources.|"_default"|
|`druid.router.pollPeriod`|Any ISO8601 duration.|How often to poll for new rules.|PT1M|
|`druid.router.strategies`|An ordered JSON array of objects.|All custom strategies to use for routing.|[{"type":"timeBoundary"},{"type":"priority"}]|
|`druid.router.avatica.balancer.type`|String representing an AvaticaConnectionBalancer name|Class to use for balancing Avatica queries across Brokers|rendezvousHash|
|`druid.router.http.maxRequestBufferSize`|Maximum size of the buffer used to write requests when forwarding them to the Broker. This should be set to atleast the maxHeaderSize allowed on the Broker|8 * 1024|


Router Strategies
-----------------
The Router has a configurable list of strategies for how it selects which Brokers to route queries to. The order of the strategies matter because as soon as a strategy condition is matched, a Broker is selected.

### timeBoundary

```json
{
  "type":"timeBoundary"
}
```

Including this strategy means all timeBoundary queries are always routed to the highest priority Broker.

### priority

```json
{
  "type":"priority",
  "minPriority":0,
  "maxPriority":1
}
```

Queries with a priority set to less than minPriority are routed to the lowest priority Broker. Queries with priority set to greater than maxPriority are routed to the highest priority Broker. By default, minPriority is 0 and maxPriority is 1. Using these default values, if a query with priority 0 (the default query priority is 0) is sent, the query skips the priority selection logic.

### JavaScript

Allows defining arbitrary routing rules using a JavaScript function. The function is passed the configuration and the query to be executed, and returns the tier it should be routed to, or null for the default tier.

*Example*: a function that sends queries containing more than three aggregators to the lowest priority Broker.

```json
{
  "type" : "javascript",
  "function" : "function (config, query) { if (query.getAggregatorSpecs && query.getAggregatorSpecs().size() >= 3) { var size = config.getTierToBrokerMap().values().size(); if (size > 0) { return config.getTierToBrokerMap().values().toArray()[size-1] } else { return config.getDefaultBrokerServiceName() } } else { return null } }"
}
```

<div class="note info">
JavaScript-based functionality is disabled by default. Please refer to the Druid <a href="../development/javascript.html">JavaScript programming guide</a> for guidelines about using Druid's JavaScript functionality, including instructions on how to enable it.
</div>


Avatica Query Balancing
--------------

All Avatica JDBC requests with a given connection ID must be routed to the same Broker, since Druid Brokers do not share connection state with each other.

To accomplish this, Druid provides two built-in balancers that use rendezvous hashing and consistent hashing of a request's connection ID respectively to assign requests to Brokers.

Note that when multiple Routers are used, all Routers should have identical balancer configuration to ensure that they make the same routing decisions.

### Rendezvous Hash Balancer

This balancer uses [Rendezvous Hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing) on an Avatica request's connection ID to assign the request to a Broker.

To use this balancer, specify the following property:

```
druid.router.avatica.balancer.type=rendezvousHash
```

If no `druid.router.avatica.balancer` property is set, the Router will also default to using the Rendezvous Hash Balancer.

### Consistent Hash Balancer

This balancer uses [Consistent Hashing](https://en.wikipedia.org/wiki/Consistent_hashing) on an Avatica request's connection ID to assign the request to a Broker.

To use this balancer, specify the following property:

```
druid.router.avatica.balancer.type=consistentHash
```

This is a non-default implementation that is provided for experimentation purposes. The consistent hasher has longer setup times on initialization and when the set of Brokers changes, but has a faster Broker assignment time than the rendezous hasher when tested with 5 Brokers. Benchmarks for both implementations have been provided in `ConsistentHasherBenchmark` and `RendezvousHasherBenchmark`. The consistent hasher also requires locking, while the rendezvous hasher does not.

HTTP Endpoints
--------------

The Router process exposes several HTTP endpoints for interactions.

### GET

* `/status`

Returns the Druid version, loaded extensions, memory used, total memory and other useful information about the process.

* `/druid/v2/datasources`

Returns a list of queryable datasources.

* `/druid/v2/datasources/{dataSourceName}`

Returns the dimensions and metrics of the datasource.

* `/druid/v2/datasources/{dataSourceName}/dimensions`

Returns the dimensions of the datasource.

* `/druid/v2/datasources/{dataSourceName}/metrics`

Returns the metrics of the datasource.

Router as Management Proxy
--------------------------

The Router can be configured to forward requests to the active Coordinator or Overlord process. This may be useful for
setting up a highly available cluster in situations where the HTTP redirect mechanism of the inactive -> active
Coordinator/Overlord does not function correctly (servers are behind a load balancer, the hostname used in the redirect
is only resolvable internally, etc.).

### Enabling the Management Proxy

To enable this functionality, set the following in the Router's runtime.properties:

```
druid.router.managementProxy.enabled=true
```

### Routing

The management proxy supports implicit and explicit routes. Implicit routes are those where the destination can be
determined from the original request path based on Druid API path conventions. For the Coordinator the convention is
`/druid/coordinator/*` and for the Overlord the convention is `/druid/indexer/*`. These are convenient because they mean
that using the management proxy does not require modifying the API request other than issuing the request to the Router
instead of the Coordinator or Overlord. Most Druid API requests can be routed implicitly.

Explicit routes are those where the request to the Router contains a path prefix indicating which process the request
should be routed to. For the Coordinator this prefix is `/proxy/coordinator` and for the Overlord it is `/proxy/overlord`.
This is required for API calls with an ambiguous destination. For example, the `/status` API is present on all Druid
processes, so explicit routing needs to be used to indicate the proxy destination.

This is summarized in the table below:

|Request Route|Destination|Rewritten Route|Example|
|-------------|-----------|---------------|-------|
|`/druid/coordinator/*`|Coordinator|`/druid/coordinator/*`|`router:8888/druid/coordinator/v1/datasources` -> `coordinator:8081/druid/coordinator/v1/datasources`|
|`/druid/indexer/*`|Overlord|`/druid/indexer/*`|`router:8888/druid/indexer/v1/task` -> `overlord:8090/druid/indexer/v1/task`|
|`/proxy/coordinator/*`|Coordinator|`/*`|`router:8888/proxy/coordinator/status` -> `coordinator:8081/status`|
|`/proxy/overlord/*`|Overlord|`/*`|`router:8888/proxy/overlord/druid/indexer/v1/isLeader` -> `overlord:8090/druid/indexer/v1/isLeader`|
