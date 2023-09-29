---
id: dynamic-configuration-api
title: Dynamic configuration API
sidebar_label: Dynamic configuration
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

This document describes the API endpoints to retrieve and manage the dynamic configurations for the [Coordinator](../configuration/index.md#overlord-dynamic-configuration) and [Overlord](../configuration/index.md#dynamic-configuration) in Apache Druid.

In this document, `http://SERVICE_IP:SERVICE_PORT` is a placeholder for the server address of deployment and the service port. For example, on the quickstart configuration, replace `http://ROUTER_IP:ROUTER_PORT` with `http://localhost:8888`.

## Coordinator dynamic configuration

The Coordinator has dynamic configurations to tune certain behavior on the fly, without requiring a service restart. For more information, see [Coordinator Dynamic Configuration](../configuration/index.md#dynamic-configuration).

### Update Coordinator dynamic configuration

Update Coordinator dynamic worker configuration. Pass the dynamic configuration spec as a JSON request body. For information on constructing a dynamic configuration spec, see [Dynamic configuration](../configuration/index.md#dynamic-configuration).

#### URL

<code class="postAPI">POST</code> <code>/druid/coordinator/v1/config</code>

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully updated dynamic configuration* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

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

<!--HTTP-->

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

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

A successful request returns an HTTP `200 OK` message code and an empty response body.

### Get Coordinator dynamic configuration

Retrieves current Coordinator dynamic configuration. Returns a JSON object with the dynamic configuration properties and values. For information on the response properties, see [Dynamic configuration](../configuration/index.md#dynamic-configuration).

#### URL

<code class="getAPI">GET</code> <code>/druid/coordinator/v1/config</code>

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved dynamic configuration* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/config"
```

<!--HTTP-->

```HTTP
GET /druid/coordinator/v1/config HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

```json
{
    "millisToWaitBeforeDeleting": 900000,
    "mergeBytesLimit": 524288000,
    "mergeSegmentsLimit": 100,
    "maxSegmentsToMove": 5,
    "percentOfSegmentsToConsiderPerMove": 100.0,
    "useBatchedSegmentSampler": true,
    "replicantLifetime": 15,
    "replicationThrottleLimit": 10,
    "balancerComputeThreads": 1,
    "emitBalancingStats": false,
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
</details>

### Get Coordinator dynamic configuration history

Retrieves the history of changes to Coordinator dynamic configuration over an interval of time. Returns an empty array if there are no history records available.

#### URL 

<code class="getAPI">GET</code> <code>/druid/coordinator/v1/config/history</code>

#### Query parameters

* `interval` (optional)
  * Type: ISO 8601
  * Limits the number of results to the specified time interval. Delimited with `/`. For example, `2023-07-13/2023-07-19`.

* `count` (optional)
  * Type: Int
  * Limits the number of results to the last `n` entries.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved history* 


<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following example retrieves the dynamic configuration history between `2022-07-13` and `2024-07-19`. The response is limited to 10 entries.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/config/history?interval=2022-07-13%2F2024-07-19&count=10"
```

<!--HTTP-->

```HTTP
GET /druid/coordinator/v1/config/history?interval=2022-07-13/2024-07-19&count=10 HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

```json
[
    {
        "key": "coordinator.config",
        "type": "coordinator.config",
        "auditInfo": {
            "author": "console",
            "comment": "testing config change",
            "ip": "127.0.0.1"
        },
        "payload": "{\"millisToWaitBeforeDeleting\":900000,\"mergeBytesLimit\":524288000,\"mergeSegmentsLimit\":100,\"maxSegmentsToMove\":5,\"percentOfSegmentsToConsiderPerMove\":100.0,\"useBatchedSegmentSampler\":true,\"replicantLifetime\":15,\"replicationThrottleLimit\":10,\"balancerComputeThreads\":1,\"emitBalancingStats\":false,\"killDataSourceWhitelist\":[],\"killPendingSegmentsSkipList\":[],\"maxSegmentsInNodeLoadingQueue\":100,\"decommissioningNodes\":[],\"decommissioningMaxPercentOfMaxSegmentsToMove\":70,\"pauseCoordination\":false,\"replicateAfterLoadTimeout\":false,\"maxNonPrimaryReplicantsToLoad\":2147483647,\"useRoundRobinSegmentAssignment\":true}",
        "auditTime": "2023-08-12T07:51:36.306Z"
    }
]
```
</details>

## Overlord dynamic configuration

The Overlord can be dynamically configured to specify how tasks are assigned to workers. For more information, see [Overlord dynamic configuration](../configuration/index.md#overlord-dynamic-configuration).

### Update Overlord dynamic configuration.

Updates overlord dynamic worker configuration. Pass the dynamic configuration spec as a JSON request body. For information on constructing an Overlord dynamic configuration spec, see [Overlord dynamic configuration](../configuration/index.md#overlord-dynamic-configuration).

#### URL

<code class="postAPI">POST</code><code>/druid/indexer/v1/worker</code>

#### Header parameters

The endpoint supports a set of optional header parameters to populate the `author` and `comment` fields in the configuration history. 

* `X-Druid-Author` (optional)
  * Type: String
  * A string representing the author making the configuration change.
* `X-Druid-Comment` (optional)
  * Type: String
  * A string describing the update.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully updated dynamic configuration* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

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

<!--HTTP-->

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

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

A successful request returns an HTTP `200 OK` message code and an empty response body.

### Get Overlord dynamic configuration

Returns current Overlord dynamic configuration. Returns an empty response body if there are no current Overlord dynamic configuration.

#### URL 

<code class="getAPI">GET</code> <code>/druid/indexer/v1/worker</code>

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved dynamic configuration* 

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/worker"
```

<!--HTTP-->

```HTTP
GET /druid/indexer/v1/worker HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

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


### Get Overlord dynamic configuration history

Retrieves history of changes to Overlord dynamic configuration. Returns an empty array if there are no history records available.


#### URL

<code class="getAPI">GET</code> <code>/druid/indexer/v1/worker/history</code>

#### Query parameters

* `interval` (optional)
  * Type: ISO 8601
  * Limits the number of results to the specified time interval. Delimited with `/`. For example, `2023-07-13/2023-07-19`.

* `count` (optional)
  * Type: Int
  * Limits the number of results to the last `n` entries.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved history* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following example retrieves the dynamic configuration history between `2022-07-13` and `2024-07-19`. The response is limited to 10 entries.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/worker/history?interval=2022-07-13%2F2024-07-19&count=10"
```

<!--HTTP-->

```HTTP
GET /druid/indexer/v1/worker/history?interval=2022-07-13%2F2024-07-19&count=10 HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

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
        "auditTime": "2023-08-15T20:32:06.899Z"
    }
]
```
</details>

### Get an array of worker nodes in the cluster

Returns an array of all the worker nodes in the cluster along with its corresponding metadata.

<code class="getAPI">GET</code><code>/druid/indexer/v1/workers</code>

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved worker nodes* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/workers"
```

<!--HTTP-->

```HTTP
GET /druid/indexer/v1/workers HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

```json
[
    {
        "worker": {
            "scheme": "http",
            "host": "localhost:8091",
            "ip": "192.168.1.4",
            "capacity": 2,
            "version": "0",
            "category": "_default_worker_category"
        },
        "currCapacityUsed": 0,
        "currParallelIndexCapacityUsed": 0,
        "availabilityGroups": [],
        "runningTasks": [],
        "lastCompletedTaskTime": "2023-08-15T18:19:37.254Z",
        "blacklistedUntil": null
    }
]
```
</details>

### Get scaling events

Returns Overlord scaling events if auto-scaling runners are in use.

#### URL

<code class="getAPI">GET</code><code>/druid/indexer/v1/scaling</code>

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved scaling events* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/scaling"
```

<!--HTTP-->

```HTTP
GET /druid/indexer/v1/scaling HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

```json

```
</details>