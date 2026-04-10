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

`GET` `/druid/coordinator/v1/config`

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
<summary>View the response</summary>

```json
{
    "millisToWaitBeforeDeleting": 900000,
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
    "debugDimensions": null,
    "turboLoadingNodes": [],
    "cloneServers": {}

}
```

</details>

### Update dynamic configuration

Submits a JSON-based dynamic configuration spec to the Coordinator.
For information on the supported properties, see [Dynamic configuration](../configuration/index.md#dynamic-configuration).

#### URL

`POST` `/druid/coordinator/v1/config`

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
  "useRoundRobinSegmentAssignment": true,
  "turboLoadingNodes": [],
  "cloneServers": {}
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
  "useRoundRobinSegmentAssignment": true,
  "turboLoadingNodes": [],
  "cloneServers": {}
}
```

</TabItem>
</Tabs>

#### Sample response

A successful request returns an HTTP `200 OK` message code and an empty response body.

### Get dynamic configuration history

Retrieves the history of changes to Coordinator dynamic configuration over an interval of time. Returns an empty array if there are no history records available.

#### URL

`GET` `/druid/coordinator/v1/config/history`

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
  <summary>View the response</summary>

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
        "payload": "{\"millisToWaitBeforeDeleting\":900000,\"maxSegmentsToMove\":5,\"replicantLifetime\":15,\"replicationThrottleLimit\":10,\"balancerComputeThreads\":1,\"killDataSourceWhitelist\":[],\"killPendingSegmentsSkipList\":[],\"maxSegmentsInNodeLoadingQueue\":100,\"decommissioningNodes\":[],\"decommissioningMaxPercentOfMaxSegmentsToMove\":70,\"pauseCoordination\":false,\"replicateAfterLoadTimeout\":false,\"maxNonPrimaryReplicantsToLoad\":2147483647,\"useRoundRobinSegmentAssignment\":true,\"smartSegmentLoading\":true,\"debugDimensions\":null,\"decommissioningNodes\":[]}",
        "auditTime": "2023-10-03T20:59:51.622Z"
    }
]
```
</details>

## Broker dynamic configuration

Broker dynamic configuration is managed through the Coordinator but consumed by Brokers.
These settings control broker behavior such as query blocking rules and default query context values.

> **Note:** Broker dynamic configuration is best-effort. Settings may not be applied in certain
> cases, such as when a Broker has recently started and hasn't received the config yet, or if the
> Broker cannot contact the Coordinator. Brokers poll the configuration periodically (default every
> 1 minute) and also receive push updates from the Coordinator for immediate propagation. If a
> setting is critical and must always be applied, use the equivalent static runtime property instead.

### Get broker dynamic configuration

Retrieves the current Broker dynamic configuration. Returns a JSON object with the dynamic configuration properties.

#### URL

`GET` `/druid/coordinator/v1/broker/config`

#### Responses

<Tabs>

<TabItem value="25" label="200 SUCCESS">


*Successfully retrieved broker dynamic configuration*

</TabItem>
</Tabs>

---

#### Sample request

<Tabs>

<TabItem value="26" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/broker/config"
```

</TabItem>
<TabItem value="27" label="HTTP">


```HTTP
GET /druid/coordinator/v1/broker/config HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

#### Sample response

<details>
<summary>View the response</summary>

```json
{
  "queryBlocklist": [
    {
      "ruleName": "block-expensive-scans",
      "dataSources": ["large_table"],
      "queryTypes": ["scan"]
    }
  ],
  "queryContext": {
    "priority": 0,
    "timeout": 300000
  },
  "perSegmentTimeoutConfig": {
    "large_table": {
      "perSegmentTimeoutMs": 10000,
      "monitorOnly": false
    }
  }
}
```

</details>

### Update broker dynamic configuration

Updates the Broker dynamic configuration.

#### URL

`POST` `/druid/coordinator/v1/broker/config`

#### Header parameters

The endpoint supports a set of optional header parameters to populate the audit log information.

* `X-Druid-Author`
  * Type: String
  * Author making the config change.

* `X-Druid-Comment`
  * Type: String
  * Comment describing the change.

#### Responses

<Tabs>

<TabItem value="28" label="200 SUCCESS">


*Successfully updated configuration*

</TabItem>
</Tabs>

---

#### Sample request

<Tabs>

<TabItem value="29" label="cURL">


```shell
curl -X POST "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/broker/config" \
-H "Content-Type: application/json" \
-H "X-Druid-Author: admin" \
-H "X-Druid-Comment: Add query blocklist rules and set default context" \
-d '{
  "queryBlocklist": [
    {
      "ruleName": "block-expensive-scans",
      "dataSources": ["large_table", "huge_dataset"],
      "queryTypes": ["scan"]
    },
    {
      "ruleName": "block-debug-queries",
      "contextMatches": {
        "debug": "true"
      }
    }
  ],
  "queryContext": {
    "priority": 0,
    "timeout": 300000
  },
  "perSegmentTimeoutConfig": {
    "large_table": {
      "perSegmentTimeoutMs": 10000
    },
    "huge_dataset": {
      "perSegmentTimeoutMs": 5000,
      "monitorOnly": true
    }
  }
}'
```

</TabItem>
<TabItem value="30" label="HTTP">


```HTTP
POST /druid/coordinator/v1/broker/config HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
Content-Type: application/json
X-Druid-Author: admin
X-Druid-Comment: Add query blocklist rules and set default context

{
  "queryBlocklist": [
    {
      "ruleName": "block-expensive-scans",
      "dataSources": ["large_table", "huge_dataset"],
      "queryTypes": ["scan"]
    },
    {
      "ruleName": "block-debug-queries",
      "contextMatches": {
        "debug": "true"
      }
    }
  ],
  "queryContext": {
    "priority": 0,
    "timeout": 300000
  },
  "perSegmentTimeoutConfig": {
    "large_table": {
      "perSegmentTimeoutMs": 10000
    },
    "huge_dataset": {
      "perSegmentTimeoutMs": 5000,
      "monitorOnly": true
    }
  }
}
```

</TabItem>
</Tabs>

#### Sample response

A successful request returns an HTTP `200 OK` message code and an empty response body.

### Broker dynamic configuration properties

The following table shows the dynamic configuration properties for the Broker.

|Property|Description|Default|
|--------|-----------|-------|
|`queryBlocklist`| List of rules to block queries based on datasource, query type, and/or query context parameters. Each rule defines criteria that are combined with AND logic. Blocked queries return an HTTP 403 error. See [Query blocklist rules](#query-blocklist-rules) for details.|none|
|`queryContext`| Map of default query context key-value pairs applied to all queries on this broker. These values override static defaults set via runtime properties (`druid.query.default.context.*`) but are overridden by context values supplied in individual query payloads. Useful for setting cluster-wide defaults such as `priority` or `timeout` without restarting. See [Query context reference](../querying/query-context-reference.md) for available keys.|none|
|`perSegmentTimeoutConfig`| Map of datasource names to per-segment timeout configurations. When a query targets a datasource in this map, the Broker injects the configured `perSegmentTimeout` into the query context before forwarding to Historicals. See [Per-segment timeout configuration](#per-segment-timeout-configuration) for details.|none|

#### Query blocklist rules

Query blocklist rules allow you to block specific queries based on datasource, query type, and/or query context parameters. This feature is useful for preventing expensive or problematic queries from impacting cluster performance.

Each rule in the `queryBlocklist` array is a JSON object with the following properties:

|Property|Description|Required|Default|
|--------|-----------|--------|-------|
|`ruleName`|Unique name identifying this blocklist rule. Used in error messages when queries are blocked.|Yes|N/A|
|`dataSources`|List of datasource names to match. A query matches if it references any datasource in this list.|No|Matches all datasources|
|`queryTypes`|List of query types to match (e.g., `scan`, `timeseries`, `groupBy`, `topN`). A query matches if its type is in this list.|No|Matches all query types|
|`contextMatches`|Map of query context parameter key-value pairs to match. A query matches if all specified context parameters match the provided values (case-sensitive string comparison).|No|Matches all contexts|

**Rule matching behavior:**

- A query must match ALL specified criteria within a rule (AND logic) to be blocked by that rule
- If any criterion is omitted, empty or null, it matches everything (e.g., omitting `queryTypes` or setting it to null matches all query types)
- For context matching: if a rule specifies context parameters, queries with missing or null values for those keys will not match
- At least one criterion must be specified per rule to prevent accidentally blocking all queries
- A query is blocked if it matches ANY rule in the blocklist (OR logic between rules)

**Error response:**

When a query is blocked, the Broker returns an HTTP 403 error with a message indicating the query ID and the rule that blocked it:

```json
{
  "error": "Forbidden",
  "errorMessage": "Query[abc-123-def] blocked by rule[block-expensive-scans]",
  "persona": "USER",
  "category": "FORBIDDEN"
}
```

#### Per-segment timeout configuration

Per-segment timeout configuration allows operators to set per-datasource segment processing timeouts without restarting the cluster. This is useful when different datasources have different performance characteristics — for example, allowing longer timeouts for larger datasets.

Each entry in the `perSegmentTimeoutConfig` map is keyed by datasource name and has the following properties:

|Property|Description|Required|Default|
|--------|-----------|--------|-------|
|`perSegmentTimeoutMs`|Per-segment processing timeout in milliseconds. Must be greater than 0.|Yes|N/A|
|`monitorOnly`|When `true`, the timeout value is logged but not enforced. Useful for observing the impact of a timeout before enabling enforcement.|No|`false`|

**Precedence order** (highest to lowest):

1. User-supplied `perSegmentTimeout` in the query context
2. Per-datasource value from `perSegmentTimeoutConfig`
3. Cluster-wide default from `druid.query.default.context.perSegmentTimeout`

> **Note:** For queries involving multiple datasources (e.g. joins or unions), the timeout from the
> first matching datasource is applied. The match order is non-deterministic.
> To avoid this, configure the same timeout for all datasources involved in such queries, or set `perSegmentTimeout` explicitly in the query context.

**Example configuration:**

```json
{
  "perSegmentTimeoutConfig": {
    "large_datasource": {
      "perSegmentTimeoutMs": 10000
    },
    "critical_datasource": {
      "perSegmentTimeoutMs": 5000,
      "monitorOnly": true
    }
  }
}
```

To clear all per-datasource timeouts, POST an empty map:

```json
{
  "perSegmentTimeoutConfig": {}
}
```

### Get broker dynamic configuration history

Retrieves the history of changes to Broker dynamic configuration over an interval of time. Returns an empty array if there are no history records available.

#### URL

`GET` `/druid/coordinator/v1/broker/config/history`

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

<TabItem value="31" label="200 SUCCESS">


*Successfully retrieved history*

</TabItem>
</Tabs>

---

#### Sample request

<Tabs>

<TabItem value="32" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/broker/config/history?count=10"
```

</TabItem>
<TabItem value="33" label="HTTP">


```HTTP
GET /druid/coordinator/v1/broker/config/history?count=10 HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

#### Sample response

<details>
<summary>View the response</summary>

```json
[
  {
    "key": "broker.config",
    "type": "broker.config",
    "auditInfo": {
      "author": "admin",
      "comment": "Add query blocklist rules",
      "ip": "127.0.0.1"
    },
    "payload": "{\"queryBlocklist\":[{\"ruleName\":\"block-expensive-scans\",\"dataSources\":[\"large_table\"],\"queryTypes\":[\"scan\"]}],\"queryContext\":{\"priority\":0,\"timeout\":300000},\"perSegmentTimeoutConfig\":{\"large_table\":{\"perSegmentTimeoutMs\":10000,\"monitorOnly\":false}}}",
    "auditTime": "2024-03-06T12:00:00.000Z"
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

`GET` `/druid/indexer/v1/worker`

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
  <summary>View the response</summary>

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

`POST` `/druid/indexer/v1/worker`

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

`GET` `/druid/indexer/v1/worker/history`

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
  <summary>View the response</summary>

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

`GET` `/druid/indexer/v1/workers`

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
  <summary>View the response</summary>

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

`GET` `/druid/indexer/v1/scaling`

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
