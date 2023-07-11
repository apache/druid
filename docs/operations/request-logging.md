---
id: request-logging
title: Request logging
sidebar_label: Request logging
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

All Apache Druid services that can serve queries can also log the query requests they process.
Request logs contain information on query metrics, including execution time and memory usage.
You can use information in the request logs to monitor query performance, determine bottlenecks, and analyze and improve slow queries.

Request logging is disabled by default.
This topic describes how to configure Druid to generate request logs to track query metrics.

## Configure request logging

To enable request logging, determine the type of request logger to use, then set the configurations specific to the request logger type.

The following request logger types are available:

- `noop`: Disables request logging, the default behavior.
- [`file`](../configuration/index.md#file-request-logging): Stores logs to disk.
- [`emitter`](../configuration/index.md#emitter-request-logging): Logs request to an external location, which is configured through an emitter.
- [`slf4j`](../configuration/index.md#slf4j-request-logging): Logs queries via the SLF4J Java logging API.
- [`filtered`](../configuration/index.md#filtered-request-logging): Filters requests by query type or execution time before logging the filtered queries by the delegated request logger.
- [`composing`](../configuration/index.md#composing-request-logging): Logs all requests to multiple request loggers.
- [`switching`](../configuration/index.md#switching-request-logging): Logs native queries and SQL queries to separate request loggers.

Define the type of request logger in `druid.request.logging.type`.
See the [Request logging configuration](../configuration/index.md#request-logging) for properties to set for each type of request logger.
Specify these properties in the `common.runtime.properties` file.
You must restart Druid for the changes to take effect.

Druid stores the results in the Broker logs, unless the request logging type is `emitter`.
If you use emitter request logging, you must also configure metrics emission.

## Configure metrics emission

Druid includes various emitters to send metrics and alerts. 
To emit query metrics, set `druid.request.logging.feed=emitter`, and define the emitter type in the `druid.emitter` property.

You can use any of the following emitters in Druid:

- `noop`: Disables metric emission, the default behavior.
- [`logging`](../configuration/index.md#logging-emitter-module): Emits metrics to Log4j 2. See [Logging](../configuration/logging.md) to configure Log4j 2 for use with Druid.
- [`http`](../configuration/index.md#http-emitter-module): Sends HTTP `POST` requests containing the metrics in JSON format to a user-defined endpoint.
- [`parametrized`](../configuration/index.md#parametrized-http-emitter-module): Operates like the `http` emitter but fine-tunes the recipient URL based on the event feed.
- [`composing`](../configuration/index.md#composing-emitter-module): Emits metrics to multiple emitter types.
- [`graphite`](../configuration/index.md#graphite-emitter): Emits metrics to a [Graphite](https://graphiteapp.org/) Carbon service.

Specify these properties in the `common.runtime.properties` file.
See the [Metrics emitters configuration](../configuration/index.md#metrics-emitters) for properties to set for each type of metrics emitter.
You must restart Druid for the changes to take effect.


## Example

The following configuration shows how to enable request logging and post query metrics to the endpoint `http://example.com:8080/path`.
```
# Enable request logging and configure the emitter request logger
druid.request.logging.type=emitter
druid.request.logging.feed=myRequestLogFeed

# Enable metrics emission and tell Druid where to emit messages
druid.emitter=http
druid.emitter.http.recipientBaseUrl=http://example.com:8080/path

# Authenticate to the base URL, if needed
druid.emitter.http.basicAuthentication=username:password
```

The following shows an example log emitter output:
```
[
    {
        "feed": "metrics",
        "timestamp": "2022-01-06T20:32:06.628Z",
        "service": "druid/broker",
        "host": "localhost:8082",
        "version": "2022.01.0-iap-SNAPSHOT",
        "metric": "sqlQuery/bytes",
        "value": 9351,
        "dataSource": "[wikipedia]",
        "id": "56e8317b-31cc-443d-b109-47f51b21d4c3",
        "nativeQueryIds": "[2b9cbced-11fc-4d78-a58c-c42863dff3c8]",
        "remoteAddress": "127.0.0.1",
        "success": "true"
    },
    {
        "feed": "myRequestLogFeed",
        "timestamp": "2022-01-06T20:32:06.585Z",
        "remoteAddr": "127.0.0.1",
        "service": "druid/broker",
        "sqlQueryContext":
        {
            "useApproximateCountDistinct": false,
            "sqlQueryId": "56e8317b-31cc-443d-b109-47f51b21d4c3",
            "useApproximateTopN": false,
            "useCache": false,
            "sqlOuterLimit": 101,
            "populateCache": false,
            "nativeQueryIds": "[2b9cbced-11fc-4d78-a58c-c42863dff3c8]"
        },
        "queryStats":
        {
            "sqlQuery/time": 43,
            "sqlQuery/planningTimeMs": 5,
            "sqlQuery/bytes": 9351,
            "success": true,
            "context":
            {
                "useApproximateCountDistinct": false,
                "sqlQueryId": "56e8317b-31cc-443d-b109-47f51b21d4c3",
                "useApproximateTopN": false,
                "useCache": false,
                "sqlOuterLimit": 101,
                "populateCache": false,
                "nativeQueryIds": "[2b9cbced-11fc-4d78-a58c-c42863dff3c8]"
            },
            "identity": "allowAll"
        },
        "query": null,
        "host": "localhost:8082",
        "sql": "SELECT * FROM wikipedia WHERE cityName = 'Buenos Aires'"
    },
    {
        "feed": "myRequestLogFeed",
        "timestamp": "2022-01-06T20:32:07.652Z",
        "remoteAddr": "",
        "service": "druid/broker",
        "sqlQueryContext":
        {},
        "queryStats":
        {
            "query/time": 16,
            "query/bytes": -1,
            "success": true,
            "identity": "allowAll"
        },
        "query":
        {
            "queryType": "scan",
            "dataSource":
            {
                "type": "table",
                "name": "wikipedia"
            },
            "intervals":
            {
                "type": "intervals",
                "intervals":
                [
                    "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
                ]
            },
            "virtualColumns":
            [
                {
                    "type": "expression",
                    "name": "v0",
                    "expression": "'Buenos Aires'",
                    "outputType": "STRING"
                }
            ],
            "resultFormat": "compactedList",
            "batchSize": 20480,
            "limit": 101,
            "filter":
            {
                "type": "selector",
                "dimension": "cityName",
                "value": "Buenos Aires",
                "extractionFn": null
            },
            "columns":
            [
                "__time",
                "added",
                "channel",
                "comment",
                "commentLength",
                "countryIsoCode",
                "countryName",
                "deleted",
                "delta",
                "deltaBucket",
                "diffUrl",
                "flags",
                "isAnonymous",
                "isMinor",
                "isNew",
                "isRobot",
                "isUnpatrolled",
                "metroCode",
                "namespace",
                "page",
                "regionIsoCode",
                "regionName",
                "user",
                "v0"
            ],
            "legacy": false,
            "context":
            {
                "populateCache": false,
                "queryId": "62e3d373-6e50-41b4-873b-1e56347c2950",
                "sqlOuterLimit": 101,
                "sqlQueryId": "cbb3d519-aee9-4566-8920-dbbeab6269f5",
                "useApproximateCountDistinct": false,
                "useApproximateTopN": false,
                "useCache": false
            },
            "descending": false,
            "granularity":
            {
                "type": "all"
            }
        },
        "host": "localhost:8082",
        "sql": null
    },
    ...
]
``` 

## Learn more

See the following topics for more information.
* [Query metrics](metrics.md#query-metrics)
* [Request logging configuration](../configuration/index.md#request-logging)
* [Metrics emitters configuration](../configuration/index.md#metrics-emitters) 

