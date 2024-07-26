---
id: prometheus
title: "Prometheus Emitter"
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


To use this Apache Druid extension, [include](../../configuration/extensions.md#loading-extensions) `prometheus-emitter` in the extensions load list.

## Introduction

This extension exposes [Druid metrics](https://druid.apache.org/docs/latest/operations/metrics.html) for collection by a Prometheus server (https://prometheus.io/).

Emitter is enabled by setting `druid.emitter=prometheus` [configs](https://druid.apache.org/docs/latest/configuration/index.html#enabling-metrics) or include `prometheus` in the composing emitter list.

## Configuration

All the configuration parameters for the Prometheus emitter are under `druid.emitter.prometheus`.

| property                                      | description                                                                                                                                                                                                                            | required? | default                              |
|-----------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|--------------------------------------|
| `druid.emitter.prometheus.strategy`           | The strategy to expose prometheus metrics. <br/>Should be one of `exporter` and `pushgateway`. Default strategy `exporter` would expose metrics for scraping purpose. Peon tasks (short-lived jobs) should use `pushgateway` strategy. | yes       | exporter                             |
| `druid.emitter.prometheus.port`               | The port on which to expose the prometheus HTTPServer. Required if using `exporter` strategy.                                                                                                                                          | no        | none                                 |
| `druid.emitter.prometheus.namespace`          | Optional metric namespace. Must match the regex `[a-zA-Z_:][a-zA-Z0-9_:]*`                                                                                                                                                             | no        | druid                                |
| `druid.emitter.prometheus.dimensionMapPath`   | JSON file defining the Prometheus metric type, desired dimensions, help text, and conversionFactor for every Druid metric.                                                                                                             | no        | Default mapping provided. See below. |
| `druid.emitter.prometheus.addHostAsLabel`     | Flag to include the hostname as a prometheus label.                                                                                                                                                                                    | no        | false                                |
| `druid.emitter.prometheus.addServiceAsLabel`  | Flag to include the druid service name (e.g. `druid/broker`, `druid/coordinator`, etc.) as a prometheus label.                                                                                                                         | no        | false                                |
| `druid.emitter.prometheus.pushGatewayAddress` | Pushgateway address. Required if using `pushgateway` strategy.                                                                                                                                                                         | no        | none                                 |
| `druid.emitter.prometheus.flushPeriod`        | Emit metrics to Pushgateway every `flushPeriod` seconds. Required if `pushgateway` strategy is used.                                                                                                                                   | no        | 15                                   |
| `druid.emitter.prometheus.extraLabels`        | JSON key-value pairs for additional labels on all metrics. Keys (label names) must match the regex `[a-zA-Z_:][a-zA-Z0-9_:]*`. Example: `{"cluster_name": "druid_cluster1", "env": "staging"}`.                                        | no        | none                                 |
| `druid.emitter.prometheus.deletePushGatewayMetricsOnShutdown` | Flag to delete metrics from Pushgateway on task shutdown. Works only if `pushgateway` strategy is used. This feature allows to delete a stale metrics from batch executed tasks. Otherwise, the Pushgateway will store these stale metrics indefinitely as there is [no time to live mechanism](https://github.com/prometheus/pushgateway/issues/117), using the memory to hold data that was already scraped by Prometheus. | no | false |
| `druid.emitter.prometheus.waitForShutdownDelay` | Time in milliseconds to wait for peon tasks to delete metrics from the Pushgateway on shutdown (e.g. 60_000). Applicable only when `pushgateway` strategy is used and `deletePushGatewayMetricsOnShutdown` is set to true. There is no guarantee that a peon task will delete metrics from the gateway if the configured delay is more than the [Peon's `druid.indexer.task.gracefulShutdownTimeout`](https://druid.apache.org/docs/latest/configuration/#additional-peon-configuration) value. For best results, set this value is 1.2 times the configured Prometheus `scrape_interval` of Pushgateway to ensure that  Druid scrapes the metrics before cleanup. | no | none |

### Ports for colocated Druid processes

In certain instances, Druid processes may be colocated on the same host. For example, the Broker and Router may share the same server. Other colocated processes include the Historical and MiddleManager or the Coordinator and Overlord. When you have colocated processes, specify `druid.emitter.prometheus.port` separately for each process on each host. For example, even if the Broker and Router share the same host, the Broker runtime properties and the Router runtime properties each need to list `druid.emitter.prometheus.port`, and the port value for both must be different.

### Override properties for Peon Tasks

Peon tasks are created dynamically by middle managers and have dynamic host and port addresses. Since the `exporter` strategy allows Prometheus to read only from a fixed address, it cannot be used for peon tasks.
So, these tasks need to be configured to use `pushgateway` strategy to push metrics from Druid to prometheus gateway.

If this emitter is configured to use `exporter` strategy globally, some of the above configurations need to be overridden in the middle manager so that spawned peon tasks can still use the `pushgateway` strategy.

```
#
# Override global prometheus emitter configuration for peon tasks to use `pushgateway` strategy.
# Other configurations can also be overridden by adding `druid.indexer.fork.property.` prefix to above configuration properties.
# 
druid.indexer.fork.property.druid.emitter.prometheus.strategy=pushgateway
druid.indexer.fork.property.druid.emitter.prometheus.pushGatewayAddress=http://<push-gateway-address>
```

### Metric names

All metric names and labels are reformatted to match Prometheus standards.
- For names: all characters which are not alphanumeric, underscores, or colons (matching `[^a-zA-Z_:][^a-zA-Z0-9_:]*`) are replaced with `_`
- For labels: all characters which are not alphanumeric or underscores (matching `[^a-zA-Z0-9_][^a-zA-Z0-9_]*`) are replaced with `_`

### Metric mapping

Each metric to be collected by Prometheus must specify a type, one of `[timer, counter, guage]`. Prometheus Emitter expects this mapping to
be provided as a JSON file.  Additionally, this mapping specifies which dimensions should be included for each metric.  Prometheus expects
histogram timers to use Seconds as the base unit.  Timers which do not use seconds as a base unit can use the `conversionFactor` to set
the base time unit. If the user does not specify their own JSON file, a default mapping is used.  All
metrics are expected to be mapped. Metrics which are not mapped will not be tracked.

Prometheus metric path is organized using the following schema:

```json
<druid metric name> : { 
  "dimensions" : <dimension list>, 
  "type" : <timer|counter|gauge>, 
  "conversionFactor": <conversionFactor>, 
  "help" : <help text>
}
```

For example:
```json
"query/time" : { 
  "dimensions" : ["dataSource", "type"],
  "type" : "timer",
  "conversionFactor": 1000.0,
  "help": "Seconds taken to complete a query."
}
```

For metrics which are emitted from multiple services with different dimensions, the metric name is prefixed with
the service name. For example:

```json
"druid/coordinator-segment/count" : { "dimensions" : ["dataSource"], "type" : "gauge" },
"druid/historical-segment/count" : { "dimensions" : ["dataSource", "tier", "priority"], "type" : "gauge" }
```

For most use cases, the default mapping is sufficient.
