---
id: troubleshooting
title: "Troubleshooting query execution in Druid"
sidebar_label: "Troubleshooting"
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

This topic describes issues that may affect query execution in Druid, how to identify those issues, and strategies to resolve them.

## Query fails due to internal communication timeout

In Druid's query processing, when the Broker sends a query to the data servers, the data servers process the query and push their intermediate results back to the Broker.
Because calls from the Broker to the data servers are synchronous, the Jetty server can time out in data servers in certain cases: 

1. The data servers don't push any results to the Broker before the maximum idle time.
2. The data servers started to push data but paused for longer than the maximum idle time such as due to [Broker backpressure](../operations/basic-cluster-tuning.md#broker-backpressure).

When such timeout occurs, the server interrupts the connection between the Broker and data servers which causes the query to fail with a channel disconnection error. For example,

```json
{
   "error": {
      "error": "Unknown exception",
      "errorMessage": "Query[6eee73a6-a95f-4bdc-821d-981e99e39242] url[https://localhost:8283/druid/v2/] failed with exception msg [Channel disconnected] (through reference chain: org.apache.druid.query.scan.ScanResultValue[\"segmentId\"])",
      "errorClass": "com.fasterxml.jackson.databind.JsonMappingException",
      "host": "localhost:8283"
   }
}
```

Channel disconnection occurs for various reasons.
To verify that the error is due to web server timeout, search for the query ID in the Historical logs.
The query ID in the example above is `6eee73a6-a95f-4bdc-821d-981e99e39242`.
The `"host"` field in the error message above indicates the IP address of the Historical in question.
In the Historical logs, you will see a raised exception indicating `Idle timeout expired`:

```text
2021-09-14T19:52:27,685 ERROR [qtp475526834-85[scan_[test_large_table]_6eee73a6-a95f-4bdc-821d-981e99e39242]] org.apache.druid.server.QueryResource - Unable to send query response. (java.io.IOException: java.util.concurrent.TimeoutException: Idle timeout expired: 300000/300000 ms)
2021-09-14T19:52:27,685 ERROR [qtp475526834-85] org.apache.druid.server.QueryLifecycle - Exception while processing queryId [6eee73a6-a95f-4bdc-821d-981e99e39242] (java.io.IOException: java.util.concurrent.TimeoutException: Idle timeout expired: 300000/300000 ms)
2021-09-14T19:52:27,686 WARN [qtp475526834-85] org.eclipse.jetty.server.HttpChannel - handleException /druid/v2/ java.io.IOException: java.util.concurrent.TimeoutException: Idle timeout expired: 300000/300000 ms
```

To mitigate query failure due to web server timeout:
* Increase the max idle time for the web server.
Set the max idle time in the `druid.server.http.maxIdleTime` property in the `historical/runtime.properties` file.
You must restart the Druid cluster for this change to take effect.
See [Configuration reference](../configuration/index.md) for more information on configuring the server. 
* If the timeout occurs because the data servers have not pushed any results to the Broker, consider optimizing data server performance. Significant slowdown in the data servers may be a result of spilling too much data to disk in [groupBy v2 queries](groupbyquery.md#performance-tuning-for-groupby-v2), large [`IN` filters](filters.md#in-filter) in the query, or an under scaled cluster. Analyze your [Druid query metrics](../operations/metrics.md#query-metrics) to determine the bottleneck.
* If the timeout is caused by Broker backpressure, consider optimizing Broker performance. Check whether the connection is fast enough between the Broker and deep storage.

