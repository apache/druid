---
id: dynamic-configuration-api
title: Dynamic configuration API
sidebar_label: Dynamic configuration
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


<!--

  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements. See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership. The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License. You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied. See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

This document describes the API endpoints to retrieve and manage dynamic configurations for the [Coordinator](../design/coordinator.md) and [Overlord](../design/overlord.md) in Apache Druid.

In this topic, `http://ROUTER_IP:ROUTER_PORT` is a placeholder for your Router service address and port.
Replace it with the information for your deployment.
For example, use `http://localhost:8888` for quickstart deployments.

## Coordinator dynamic configuration

The Coordinator has dynamic configurations to tune certain behavior on the fly, without requiring a service restart.
For information on the supported properties, see [Coordinator dynamic configuration](../configuration/index.md#dynamic-configuration).

### Get dynamic configuration

Retrieves the current Coordinator dynamic configuration. Returns a JSON object with the dynamic configuration properties.

#### URL

<code class="getAPI">GET</code> <code>/druid/coordinator/v1/config</code>

#### Responses

<Tabs>

<TabItem value="1" label="200 SUCCESS">


*Successfully retrieved dynamic configuration*

</TabItem>
</Tabs>

---

#### Sample request

<Tabs>

<TabItem value="2" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/config"
```

</TabItem>
<TabItem value="3" label="HTTP">


```HTTP
GET /druid/coordinator/v1/config HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

#### Sample response

<details>
<summary>Click to show sample response</summary>

```json
{
    "millisToWaitBeforeDeleting": 900000,
    "mergeBytesLimit": 524288000,
    "mergeSegmentsLimit": 100,
    "maxSegmentsToMove": 100,
    "replicantLifetime": 15,
    "replicationThrottleLimit": 500,
    "balancerComputeThreads": 1,
    "killDataSourceWhitelist": [],
    "killPendingSegmentsSkipList": [],
    "maxSegmentsInNodeLoadingQueue": 500,
    "decommissioningNodes": [],
    "decommissioningMaxPercentOfMaxSegmentsToMove": 70,
    "pauseCoordination": false,
    "replicateAfterLoadTimeout": false,
    "maxNonPrimaryReplicantsToLoad": 2147483647,
    "useRoundRobinSegmentAssignment": true,
    "smartSegmentLoading": true,
    "debugDimensions": null
}
```

</details>

### Update dynamic configuration

Submits a JSON-based dynamic configuration spec to the Coordinator.
For information on the supported properties, see [Dynamic configuration](../configuration/index.md#dynamic-configuration).

#### URL

<code class="postAPI">POST</code> <code>/druid/coordinator/v1/config</code>

#### Header parameters

The endpoint supports a set of optional header parameters to populate the `author` and `comment` fields in the configuration history.

* `X-Druid-Author`
  * Type: String
  * Author of the configuration change.
* `X-Druid-Comment`
  * Type: String
  * Description for the update.

#### Responses

<Tabs>

<TabItem value="4" label="200 SUCCESS">


*Successfully updated dynamic configuration*

</TabItem>
</Tabs>

---

#### Sample request

<Tabs>

<TabItem value="5" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/config" \
--header 'Content-Type: application/json' \
--data '{
  "millisToWaitBeforeDeleting": 900000,
  "mergeBytesLimit": 524288000,
  "mergeSegmentsLimit": 100,
  "maxSegmentsToMove": 5,
  "percentOfSegmentsToConsiderPerMove": 100,
  "useBatchedSegmentSampler": true,
  "replicantLifetime": 15,
  "replicationThrottleLimit": 10,
  "balancerComputeThreads": 1,
  "emitBalancingStats": true,
  "killDataSourceWhitelist": [],
  "killPendingSegmentsSkipList": [],
  "maxSegmentsInNodeLoadingQueue": 100,
  "decommissioningNodes": [],
  "decommissioningMaxPercentOfMaxSegmentsToMove": 70,
  "pauseCoordination": false,
  "replicateAfterLoadTimeout": false,
  "maxNonPrimaryReplicantsToLoad": 2147483647,
  "useRoundRobinSegmentAssignment": true
}'
```

</TabItem>
<TabItem value="6" label="HTTP">


```HTTP
POST /druid/coordinator/v1/config HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
Content-Type: application/json
Content-Length: 683

{
  "millisToWaitBeforeDeleting": 900000,
  "mergeBytesLimit": 524288000,
  "mergeSegmentsLimit": 100,
  "maxSegmentsToMove": 5,
  "percentOfSegmentsToConsiderPerMove": 100,
  "useBatchedSegmentSampler": true,
  "replicantLifetime": 15,
  "replicationThrottleLimit": 10,
  "balancerComputeThreads": 1,
  "emitBalancingStats": true,
  "killDataSourceWhitelist": [],
  "killPendingSegmentsSkipList": [],
  "maxSegmentsInNodeLoadingQueue": 100,
  "decommissioningNodes": [],
  "decommissioningMaxPercentOfMaxSegmentsToMove": 70,
  "pauseCoordination": false,
  "replicateAfterLoadTimeout": false,
  "maxNonPrimaryReplicantsToLoad": 2147483647,
  "useRoundRobinSegmentAssignment": true
}
```

</TabItem>
</Tabs>

#### Sample response

A successful request returns an HTTP `200 OK` message code and an empty response body.

### Get dynamic configuration history

Retrieves the history of changes to Coordinator dynamic configuration over an interval of time. Returns an empty array if there are no history records available.

#### URL

<code class="getAPI">GET</code> <code>/druid/coordinator/v1/config/history</code>

#### Query parameters

The endpoint supports a set of optional query parameters to filter results.

* `interval`
  * Type: String
  * Limit the results to the specified time interval in ISO 8601 format delimited with `/`. For example, `2023-07-13/2023-07-19`. The default interval is one week. You can change this period by setting `druid.audit.manager.auditHistoryMillis` in the `runtime.properties` file for the Coordinator.

* `count`
  * Type: Integer
  * Limit the number of results to the last `n` entries.

#### Responses

<Tabs>

<TabItem value="7" label="200 SUCCESS">


*Successfully retrieved history*


</TabItem>
</Tabs>

---

#### Sample request

The following example retrieves the dynamic configuration history between `2022-07-13` and `2024-07-19`. The response is limited to 10 entries.

<Tabs>

<TabItem value="8" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/config/history?interval=2022-07-13%2F2024-07-19&count=10"
```

</TabItem>
<TabItem value="9" label="HTTP">


```HTTP
GET /druid/coordinator/v1/config/history?interval=2022-07-13/2024-07-19&count=10 HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

#### Sample response

<details>
  <summary>Click to show sample response</summary>

```json
[
    {
        "key": "coordinator.config",
        "type": "coordinator.config",
        "auditInfo": {
            "author": "",
            "comment": "",
            "ip": "127.0.0.1"
        },
        "payload": "{\"millisToWaitBeforeDeleting\":900000,\"mergeBytesLimit\":524288000,\"mergeSegmentsLimit\":100,\"maxSegmentsToMove\":5,\"replicantLifetime\":15,\"replicationThrottleLimit\":10,\"balancerComputeThreads\":1,\"killDataSourceWhitelist\":[],\"killPendingSegmentsSkipList\":[],\"maxSegmentsInNodeLoadingQueue\":100,\"decommissioningNodes\":[],\"decommissioningMaxPercentOfMaxSegmentsToMove\":70,\"pauseCoordination\":false,\"replicateAfterLoadTimeout\":false,\"maxNonPrimaryReplicantsToLoad\":2147483647,\"useRoundRobinSegmentAssignment\":true,\"smartSegmentLoading\":true,\"debugDimensions\":null}",
        "auditTime": "2023-10-03T20:59:51.622Z"
    }
]
```
</details>

## Overlord dynamic configuration

The Overlord has dynamic configurations to tune how Druid assigns tasks to workers.
For information on the supported properties, see [Overlord dynamic configuration](../configuration/index.md#overlord-dynamic-configuration).

### Get dynamic configuration

Retrieves the current Overlord dynamic configuration.
Returns a JSON object with the dynamic configuration properties.
Returns an empty response body if there is no current Overlord dynamic configuration.

#### URL

<code class="getAPI">GET</code> <code>/druid/indexer/v1/worker</code>

#### Responses

<Tabs>

<TabItem value="10" label="200 SUCCESS">


*Successfully retrieved dynamic configuration*

</TabItem>
</Tabs>

#### Sample request

<Tabs>

<TabItem value="11" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/worker"
```

</TabItem>
<TabItem value="12" label="HTTP">


```HTTP
GET /druid/indexer/v1/worker HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

#### Sample response

<details>
  <summary>Click to show sample response</summary>

```json
{
    "type": "default",
    "selectStrategy": {
        "type": "fillCapacityWithCategorySpec",
        "workerCategorySpec": {
            "categoryMap": {},
            "strong": true
        }
    },
    "autoScaler": null
}
```

</details>

### Update dynamic configuration

Submits a JSON-based dynamic configuration spec to the Overlord.
For information on the supported properties, see [Overlord dynamic configuration](../configuration/index.md#overlord-dynamic-configuration).

#### URL

<code class="postAPI">POST</code><code>/druid/indexer/v1/worker</code>

#### Header parameters

The endpoint supports a set of optional header parameters to populate the `author` and `comment` fields in the configuration history.

* `X-Druid-Author`
  * Type: String
  * Author of the configuration change.
* `X-Druid-Comment`
  * Type: String
  * Description for the update.

#### Responses

<Tabs>

<TabItem value="13" label="200 SUCCESS">


*Successfully updated dynamic configuration*

</TabItem>
</Tabs>

---

#### Sample request

<Tabs>

<TabItem value="14" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/worker" \
--header 'Content-Type: application/json' \
--data '{
  "type": "default",
  "selectStrategy": {
    "type": "fillCapacityWithCategorySpec",
    "workerCategorySpec": {
      "categoryMap": {},
      "strong": true
    }
  },
  "autoScaler": null
}'
```

</TabItem>
<TabItem value="15" label="HTTP">


```HTTP
POST /druid/indexer/v1/worker HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
Content-Type: application/json
Content-Length: 196

{
  "type": "default",
  "selectStrategy": {
    "type": "fillCapacityWithCategorySpec",
    "workerCategorySpec": {
      "categoryMap": {},
      "strong": true
    }
  },
  "autoScaler": null
}
```

</TabItem>
</Tabs>

#### Sample response

A successful request returns an HTTP `200 OK` message code and an empty response body.

### Get dynamic configuration history

Retrieves the history of changes to Overlord dynamic configuration over an interval of time. Returns an empty array if there are no history records available.

#### URL

<code class="getAPI">GET</code> <code>/druid/indexer/v1/worker/history</code>


#### Query parameters

The endpoint supports a set of optional query parameters to filter results.

* `interval`
  * Type: String
  * Limit the results to the specified time interval in ISO 8601 format delimited with `/`. For example, `2023-07-13/2023-07-19`. The default interval is one week. You can change this period by setting `druid.audit.manager.auditHistoryMillis` in the `runtime.properties` file for the Overlord.

* `count`
  * Type: Integer
  * Limit the number of results to the last `n` entries.

#### Responses

<Tabs>

<TabItem value="16" label="200 SUCCESS">


*Successfully retrieved history*

</TabItem>
</Tabs>

---

#### Sample request

The following example retrieves the dynamic configuration history between `2022-07-13` and `2024-07-19`. The response is limited to 10 entries.

<Tabs>

<TabItem value="17" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/worker/history?interval=2022-07-13%2F2024-07-19&count=10"
```

</TabItem>
<TabItem value="18" label="HTTP">


```HTTP
GET /druid/indexer/v1/worker/history?interval=2022-07-13%2F2024-07-19&count=10 HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

#### Sample response

<details>
  <summary>Click to show sample response</summary>

```json
[
    {
        "key": "worker.config",
        "type": "worker.config",
        "auditInfo": {
            "author": "",
            "comment": "",
            "ip": "127.0.0.1"
        },
        "payload": "{\"type\":\"default\",\"selectStrategy\":{\"type\":\"fillCapacityWithCategorySpec\",\"workerCategorySpec\":{\"categoryMap\":{},\"strong\":true}},\"autoScaler\":null}",
        "auditTime": "2023-10-03T21:49:49.991Z"
    }
]
```

</details>

### Get an array of worker nodes in the cluster

Returns an array of all the worker nodes in the cluster along with its corresponding metadata.

<code class="getAPI">GET</code><code>/druid/indexer/v1/workers</code>

#### Responses

<Tabs>

<TabItem value="19" label="200 SUCCESS">


*Successfully retrieved worker nodes*

</TabItem>
</Tabs>

---

#### Sample request

<Tabs>

<TabItem value="20" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/workers"
```

</TabItem>
<TabItem value="21" label="HTTP">


```HTTP
GET /druid/indexer/v1/workers HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

#### Sample response

<details>
  <summary>Click to show sample response</summary>

```json
[
    {
        "worker": {
            "scheme": "http",
            "host": "localhost:8091",
            "ip": "198.51.100.0",
            "capacity": 2,
            "version": "0",
            "category": "_default_worker_category"
        },
        "currCapacityUsed": 0,
        "currParallelIndexCapacityUsed": 0,
        "availabilityGroups": [],
        "runningTasks": [],
        "lastCompletedTaskTime": "2023-09-29T19:13:05.505Z",
        "blacklistedUntil": null
    }
]
```

</details>

### Get scaling events

Returns Overlord scaling events if autoscaling runners are in use.
Returns an empty response body if there are no Overlord scaling events.

#### URL

<code class="getAPI">GET</code><code>/druid/indexer/v1/scaling</code>

#### Responses

<Tabs>

<TabItem value="22" label="200 SUCCESS">


*Successfully retrieved scaling events*

</TabItem>
</Tabs>

---

#### Sample request

<Tabs>

<TabItem value="23" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/scaling"
```

</TabItem>
<TabItem value="24" label="HTTP">


```HTTP
GET /druid/indexer/v1/scaling HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

#### Sample response

A successful request returns a `200 OK` response and an array of scaling events.
