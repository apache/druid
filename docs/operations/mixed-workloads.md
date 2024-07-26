---
id: mixed-workloads
title: Configure Druid for mixed workloads
sidebar_label: Mixed workloads
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

If you frequently run concurrent, heterogeneous workloads on your Apache Druid cluster, configure Druid to properly allocate cluster resources to optimize your overall query performance.

Each Druid query consumes a certain amount of cluster resources, such as processing threads, memory buffers for intermediate query results, and HTTP threads for communicating between Brokers and data servers.
"Heavy" queries that return large results are more resource-intensive than short-running, "light" queries.
You typically do not want these long resource-intensive queries to throttle the performance of short interactive queries.
For example, if you run both sets of queries in the same Druid cluster, heavy queries may employ all available HTTP threads.
This situation slows down subsequent queries—heavy and light—and may trigger timeout errors for those queries.
With proper resource isolation, you can execute long-running, low priority queries without interfering with short-running, high priority queries.

Druid provides the following strategies to isolate resources and improve query concurrency:
- **Query laning** where you set a limit on the maximum number of long-running queries executed on each Broker. 
- **Service tiering** which defines separate groups of Historicals and Brokers to receive different query assignments based on query priority.

You can profile Druid queries using normal performance profiling techniques such as Druid query metrics analysis, thread dumps of JVM, or flame graphs to identify what resources are affected by mixed workloads.
The largest bottleneck will likely be in the Broker HTTP threads.
Mitigate resource contention of the Broker HTTP threads with query laning.
However, mixed workloads also affect other resources, including processing threads and merge buffers.
To reduce the burden on these resources, apply both service tiering and query laning.


## Query laning

When you need to run many concurrent queries having heterogeneous workloads, start with query laning to optimize your query performance.
Query laning restricts resource usage for less urgent queries to ensure dedicated resources for high priority queries.

Query lanes are analogous to carpool and normal lanes on the freeway. With query laning, Druid sets apart prioritized lanes from other general lanes.
Druid restricts low priority queries to the general lanes and allows high priority queries to run wherever possible, whether in a VIP or general lane.

In Druid, query lanes reserve resources for Broker HTTP threads. Each Druid query requires one Broker thread. The number of threads on a Broker is defined by the `druid.server.http.numThreads` parameter. Broker threads may be occupied by tasks other than queries, such as health checks. You can use query laning to limit the number of HTTP threads designated for resource-intensive queries, leaving other threads available for short-running queries and other tasks.

### General properties

Set the following query laning properties in the `broker/runtime.properties` file.

* `druid.query.scheduler.laning.strategy` – The strategy used to assign queries to lanes.
You can use the built-in [“high/low” laning strategy](../configuration/index.md#highlow-laning-strategy), or [define your own laning strategy manually](../configuration/index.md#manual-laning-strategy).
* `druid.query.scheduler.numThreads` – The total number of queries that can be served per Broker. We recommend setting this value to 1-2 less than `druid.server.http.numThreads`.

The query scheduler by default does not limit the number of queries that a Broker can serve. Setting this property to a bounded number limits the thread count. If the allocated threads are all occupied, any incoming query, including interactive queries, will be queued on the broker and will timeout after the request stays in the queue for more than the configured timeout. This configured timeout is equal to `MIN(Integer.MAX_VALUE, druid.server.http.maxQueryTimeout)`. If the value of `druid.server.http.maxQueryTimeout` is negative, the request is queued forever. 

### Lane-specific properties

If you use the __high/low laning strategy__, set the following:

* `druid.query.scheduler.laning.maxLowPercent` – The maximum percent of query threads to handle low priority queries. The remaining query threads are dedicated to high priority queries.

Consider also defining a [prioritization strategy](../configuration/index.md#prioritization-strategies) for the Broker to label queries as high or low priority. Otherwise, manually set the priority for incoming queries on the [query context](../querying/query-context.md).

If you use a __manual laning strategy__, set the following:

* `druid.query.scheduler.laning.lanes.{name}` – The limit for how many queries can run in the `name` lane. Define as many named lanes as needed.
* `druid.query.scheduler.laning.isLimitPercent` – Whether to treat the lane limit as an exact number or a percent of the minimum of `druid.server.http.numThreads` or `druid.query.scheduler.numThreads`.

With manual laning, incoming queries can be labeled with the desired lane in the `lane` parameter of the [query context](../querying/query-context.md).

See [Query prioritization and laning](../configuration/index.md#query-prioritization-and-laning) for additional details on query laning configuration.

### Example

Example config for query laning with the high/low laning strategy:

```
# Laning strategy
druid.query.scheduler.laning.strategy=hilo
druid.query.scheduler.laning.maxLowPercent=20

# Limit the number of HTTP threads for query processing
# This value should be less than druid.server.http.numThreads
druid.query.scheduler.numThreads=40
```


## Service tiering

In service tiering, you define separate groups of Historicals and Brokers to manage queries based on the segments and resource requirements of the query.
You can limit the resources that are set aside for certain types of queries.
Many heavy queries involving complex subqueries or large result sets can hog resources away from high priority, interactive queries.
Minimize the impact of these heavy queries by limiting them to a separate Broker tier.
When all Brokers set aside for heavy queries are occupied, subsequent heavy queries must wait until the designated resources become available.
A prolonged wait results in the later queries failing with a timeout error.

Note that you can separate Historical processes into tiers without having separate Broker tiers.
Historical-only tiering is not sufficient to meet the demands of mixed workloads on a Druid cluster.
However, it is useful when you query certain segments more frequently than others, such as often analyzing the most recent data.
Historical tiering assigns data from specific time intervals to specific tiers in order to support higher concurrency on hot data. 

The examples below demonstrate two tiers—hot and cold—for both the Historicals and Brokers. The Brokers will serve short-running, light queries before long-running, heavy queries. Light queries will be routed to the hot tiers, and heavy queries will be routed to the cold tiers.

### Historical tiering

This section describes how to configure segment loading and how to assign Historical services into tiers.

#### Configure segment loading

The Coordinator service assigns segments to different tiers of Historicals using load rules.
Define a [load rule](rule-configuration.md#load-rules) to indicate how segment replicas should be assigned to different Historical tiers. For example, you may store segments of more recent data on more powerful hardware for better performance.

There are several types of load rules: forever, interval, and period. Select the load rule that matches your use case for each Historical, whether you want all segments to be loaded, segments within a certain time interval, or segments within a certain time period.
Interval and period load rules must be accompanied by corresponding [drop rules](rule-configuration.md#drop-rules).

In the load rule, define tiers in the `tieredReplicants` property. Provide descriptive names for your tiers, and specify how many replicas each tier should have. You can designate a higher number of replicas for the hot tier to increase the concurrency for processing queries.

The following example shows a period load rule with two Historical tiers, named “hot” and “\_default\_tier”.
For the most recent month of data, Druid loads three replicas in the hot tier and one replica in the default cold tier.
Incoming queries that rely on this month of data can use the single replica in the cold Historical tier or any of the three replicas in the hot Historical tier.

```
{
  "type" : "loadByPeriod",
  "period" : "P1M",
  "includeFuture" : true,
  "tieredReplicants": {
    "hot": 3,
    "_default_tier" : 1
  }
}
```

See [Load rules](rule-configuration.md#load-rules) for more information on segment load rules. Visit [Tutorial: Configuring data retention](../tutorials/tutorial-retention.md) for an example of setting retention rules from the Druid web console.

#### Assign Historicals to tiers

To assign a Historical to a tier, add a label for the tier name and set the priority value in the  `historical/runtime.properties` for the Historical.

Example Historical in the hot tier:

```
druid.server.tier=hot
druid.server.priority=1
```

Example Historical in the cold tier:

```
druid.server.tier=_default_tier
druid.server.priority=0
```

See [Historical general configuration](../configuration/index.md#historical-general-configuration) for more details on these properties.

### Broker tiering

You must set up Historical tiering before you can use Broker tiering.
To set up Broker tiering, assign Brokers to tiers, and configure query routing by the Router.

#### Assign Brokers to tiers

For each of the Brokers, define the Broker group in the `broker/runtime.properties` files.

Example config for a Broker in the hot tier:
```
druid.service=druid:broker-hot
```

Example config for a Broker in the cold tier:
```
druid.service=druid:broker-cold
```

Also in the `broker/runtime.properties` files, instruct the Broker to select Historicals by priority so that the Broker will select Historicals in the hot tier before Historicals in the cold tier.

Example Broker config to prioritize hot tier Historicals:
```
druid.broker.select.tier=highestPriority
```

See [Broker configuration](../configuration/index.md#broker-process-configs) for more details on these properties.

#### Configure query routing

Direct the Router to route queries appropriately by setting the default Broker tier and the map of Historical tier to Broker tier in the `router/runtime.properties` file.

Example Router config to map hot/cold tier Brokers to hot/cold tier Historicals, respectively:

```
druid.router.defaultBrokerServiceName=druid:broker-cold
druid.router.tierToBrokerMap={"hot":"druid:broker-hot","_default_tier":"druid:broker-cold"}
```

If you plan to run Druid SQL queries, also [enable routing of SQL queries](../design/router.md#routing-of-sql-queries-using-strategies) by setting the following:
```
druid.router.sql.enable=true
```

See [Router process](../design/router.md#example-production-configuration) for an example production configuration.

## Learn more

See [Multitenancy considerations](../querying/multitenancy.md) for applying query concurrency to multitenant workloads.
