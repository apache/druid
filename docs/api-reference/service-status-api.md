---
id: service-status-api
title: Service status API
sidebar_label: Service status
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

This document describes the API endpoints to retrieve service (process) status, cluster information for Apache Druid

## Common

All processes support the following endpoints.

### Process information

`GET /status`

Returns the Druid version, loaded extensions, memory used, total memory, and other useful information about the process.

`GET /status/health`

Always returns a boolean `true` value with a 200 OK response, useful for automated health checks.

`GET /status/properties`

Returns the current configuration properties of the process.

`GET /status/selfDiscovered/status`

Returns a JSON map of the form `{"selfDiscovered": true/false}`, indicating whether the node has received a confirmation
from the central node discovery mechanism (currently ZooKeeper) of the Druid cluster that the node has been added to the
cluster. It is recommended to not consider a Druid node "healthy" or "ready" in automated deployment/container
management systems until it returns `{"selfDiscovered": true}` from this endpoint. This is because a node may be
isolated from the rest of the cluster due to network issues and it doesn't make sense to consider nodes "healthy" in
this case. Also, when nodes such as Brokers use ZooKeeper segment discovery for building their view of the Druid cluster
(as opposed to HTTP segment discovery), they may be unusable until the ZooKeeper client is fully initialized and starts
to receive data from the ZooKeeper cluster. `{"selfDiscovered": true}` is a proxy event indicating that the ZooKeeper
client on the node has started to receive data from the ZooKeeper cluster and it's expected that all segments and other
nodes will be discovered by this node timely from this point.

`GET /status/selfDiscovered`

Similar to `/status/selfDiscovered/status`, but returns 200 OK response with empty body if the node has discovered itself
and 503 SERVICE UNAVAILABLE if the node hasn't discovered itself yet. This endpoint might be useful because some
monitoring checks such as AWS load balancer health checks are not able to look at the response body.

## Master server

### Coordinator

#### Leadership

`GET /druid/coordinator/v1/leader`

Returns the current leader Coordinator of the cluster.

`GET /druid/coordinator/v1/isLeader`

Returns a JSON object with `leader` parameter, either true or false, indicating if this server is the current leader
Coordinator of the cluster. In addition, returns HTTP 200 if the server is the current leader and HTTP 404 if not.
This is suitable for use as a load balancer status check if you only want the active leader to be considered in-service
at the load balancer.

<a name="coordinator-segment-loading"></a>

### Overlord

#### Leadership

`GET /druid/indexer/v1/leader`

Returns the current leader Overlord of the cluster. If you have multiple Overlords, just one is leading at any given time. The others are on standby.

`GET /druid/indexer/v1/isLeader`

This returns a JSON object with field `leader`, either true or false. In addition, this call returns HTTP 200 if the
server is the current leader and HTTP 404 if not. This is suitable for use as a load balancer status check if you
only want the active leader to be considered in-service at the load balancer.

## Data server

### MiddleManager

`GET /druid/worker/v1/enabled`

Check whether a MiddleManager is in an enabled or disabled state. Returns JSON object keyed by the combined `druid.host`
and `druid.port` with the boolean state as the value.

```json
{"localhost:8091":true}
```

`GET /druid/worker/v1/tasks`

Retrieve a list of active tasks being run on MiddleManager. Returns JSON list of taskid strings. Normal usage should
prefer to use the `/druid/indexer/v1/tasks` [Tasks API](./tasks-api.md) or one of it's task state specific variants instead.

```json
["index_wikiticker_2019-02-11T02:20:15.316Z"]
```

`GET /druid/worker/v1/task/{taskid}/log`

Retrieve task log output stream by task id. Normal usage should prefer to use the `/druid/indexer/v1/task/{taskId}/log`
[Tasks API](./tasks-api.md) instead.

`POST /druid/worker/v1/disable`

Disable a MiddleManager, causing it to stop accepting new tasks but complete all existing tasks. Returns JSON  object
keyed by the combined `druid.host` and `druid.port`:

```json
{"localhost:8091":"disabled"}
```

`POST /druid/worker/v1/enable`

Enable a MiddleManager, allowing it to accept new tasks again if it was previously disabled. Returns JSON  object
keyed by the combined `druid.host` and `druid.port`:

```json
{"localhost:8091":"enabled"}
```

`POST /druid/worker/v1/task/{taskid}/shutdown`

Shutdown a running task by `taskid`. Normal usage should prefer to use the `/druid/indexer/v1/task/{taskId}/shutdown`
[Tasks API](./tasks-api.md) instead. Returns JSON:

```json
{"task":"index_kafka_wikiticker_f7011f8ffba384b_fpeclode"}
```


## Historical  
### Segment loading

`GET /druid/historical/v1/loadstatus`

Returns JSON of the form `{"cacheInitialized":<value>}`, where value is either `true` or `false` indicating if all
segments in the local cache have been loaded. This can be used to know when a Historical process is ready
to be queried after a restart.

`GET /druid/historical/v1/readiness`

Similar to `/druid/historical/v1/loadstatus`, but instead of returning JSON with a flag, responses 200 OK if segments
in the local cache have been loaded, and 503 SERVICE UNAVAILABLE, if they haven't.


## Load Status

`GET /druid/broker/v1/loadstatus`

Returns a flag indicating if the Broker knows about all segments in the cluster. This can be used to know when a Broker process is ready to be queried after a restart.

`GET /druid/broker/v1/readiness`

Similar to `/druid/broker/v1/loadstatus`, but instead of returning a JSON, responses 200 OK if its ready and otherwise 503 SERVICE UNAVAILABLE.
