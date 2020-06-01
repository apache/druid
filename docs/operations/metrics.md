---
id: metrics
title: "Metrics"
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


Druid generates metrics related to queries, ingestion, and coordination.

Metrics are emitted as JSON objects to a runtime log file or over HTTP (to a service such as Apache Kafka). Metric emission is disabled by default.

All Druid metrics share a common set of fields:

* `timestamp` - the time the metric was created
* `metric` - the name of the metric
* `service` - the service name that emitted the metric
* `host` - the host name that emitted the metric
* `value` - some numeric value associated with the metric

Metrics may have additional dimensions beyond those listed above.

Most metric values reset each emission period. By default druid emission period is 1 minute, this can be changed by setting the property `druid.monitoring.emissionPeriod`.

Available Metrics
-----------------

## Query metrics

### Broker

|Metric|Description|Dimensions|Normal Value|
|------|-----------|----------|------------|
|`query/time`|Milliseconds taken to complete a query.|Common: dataSource, type, interval, hasFilters, duration, context, remoteAddress, id. Aggregation Queries: numMetrics, numComplexMetrics. GroupBy: numDimensions. TopN: threshold, dimension.|< 1s|
|`query/bytes`|number of bytes returned in query response.|Common: dataSource, type, interval, hasFilters, duration, context, remoteAddress, id. Aggregation Queries: numMetrics, numComplexMetrics. GroupBy: numDimensions. TopN: threshold, dimension.| |
|`query/node/time`|Milliseconds taken to query individual historical/realtime processes.|id, status, server.|< 1s|
|`query/node/bytes`|number of bytes returned from querying individual historical/realtime processes.|id, status, server.| |
|`query/node/ttfb`|Time to first byte. Milliseconds elapsed until Broker starts receiving the response from individual historical/realtime processes.|id, status, server.|< 1s|
|`query/node/backpressure`|Milliseconds that the channel to this process has spent suspended due to backpressure.|id, status, server.| |
|`query/count`|number of total queries|This metric is only available if the QueryCountStatsMonitor module is included.||
|`query/success/count`|number of queries successfully processed|This metric is only available if the QueryCountStatsMonitor module is included.||
|`query/failed/count`|number of failed queries|This metric is only available if the QueryCountStatsMonitor module is included.||
|`query/interrupted/count`|number of queries interrupted due to cancellation or timeout|This metric is only available if the QueryCountStatsMonitor module is included.||
|`sqlQuery/time`|Milliseconds taken to complete a SQL query.|id, nativeQueryIds, dataSource, remoteAddress, success.|< 1s|
|`sqlQuery/bytes`|number of bytes returned in SQL query response.|id, nativeQueryIds, dataSource, remoteAddress, success.| |

### Historical

|Metric|Description|Dimensions|Normal Value|
|------|-----------|----------|------------|
|`query/time`|Milliseconds taken to complete a query.|Common: dataSource, type, interval, hasFilters, duration, context, remoteAddress, id. Aggregation Queries: numMetrics, numComplexMetrics. GroupBy: numDimensions. TopN: threshold, dimension.|< 1s|
|`query/segment/time`|Milliseconds taken to query individual segment. Includes time to page in the segment from disk.|id, status, segment.|several hundred milliseconds|
|`query/wait/time`|Milliseconds spent waiting for a segment to be scanned.|id, segment.|< several hundred milliseconds|
|`segment/scan/pending`|Number of segments in queue waiting to be scanned.||Close to 0|
|`query/segmentAndCache/time`|Milliseconds taken to query individual segment or hit the cache (if it is enabled on the Historical process).|id, segment.|several hundred milliseconds|
|`query/cpu/time`|Microseconds of CPU time taken to complete a query|Common: dataSource, type, interval, hasFilters, duration, context, remoteAddress, id. Aggregation Queries: numMetrics, numComplexMetrics. GroupBy: numDimensions. TopN: threshold, dimension.|Varies|
|`query/count`|number of total queries|This metric is only available if the QueryCountStatsMonitor module is included.||
|`query/success/count`|number of queries successfully processed|This metric is only available if the QueryCountStatsMonitor module is included.||
|`query/failed/count`|number of failed queries|This metric is only available if the QueryCountStatsMonitor module is included.||
|`query/interrupted/count`|number of queries interrupted due to cancellation or timeout|This metric is only available if the QueryCountStatsMonitor module is included.||

### Real-time

|Metric|Description|Dimensions|Normal Value|
|------|-----------|----------|------------|
|`query/time`|Milliseconds taken to complete a query.|Common: dataSource, type, interval, hasFilters, duration, context, remoteAddress, id. Aggregation Queries: numMetrics, numComplexMetrics. GroupBy: numDimensions. TopN: threshold, dimension.|< 1s|
|`query/wait/time`|Milliseconds spent waiting for a segment to be scanned.|id, segment.|several hundred milliseconds|
|`segment/scan/pending`|Number of segments in queue waiting to be scanned.||Close to 0|
|`query/count`|number of total queries|This metric is only available if the QueryCountStatsMonitor module is included.||
|`query/success/count`|number of queries successfully processed|This metric is only available if the QueryCountStatsMonitor module is included.||
|`query/failed/count`|number of failed queries|This metric is only available if the QueryCountStatsMonitor module is included.||
|`query/interrupted/count`|number of queries interrupted due to cancellation or timeout|This metric is only available if the QueryCountStatsMonitor module is included.||

### Jetty

|Metric|Description|Normal Value|
|------|-----------|------------|
|`jetty/numOpenConnections`|Number of open jetty connections.|Not much higher than number of jetty threads.|

### Cache

|Metric|Description|Normal Value|
|------|-----------|------------|
|`query/cache/delta/*`|Cache metrics since the last emission.||N/A|
|`query/cache/total/*`|Total cache metrics.||N/A|


|Metric|Description|Dimensions|Normal Value|
|------|-----------|----------|------------|
|`*/numEntries`|Number of cache entries.||Varies.|
|`*/sizeBytes`|Size in bytes of cache entries.||Varies.|
|`*/hits`|Number of cache hits.||Varies.|
|`*/misses`|Number of cache misses.||Varies.|
|`*/evictions`|Number of cache evictions.||Varies.|
|`*/hitRate`|Cache hit rate.||~40%|
|`*/averageByte`|Average cache entry byte size.||Varies.|
|`*/timeouts`|Number of cache timeouts.||0|
|`*/errors`|Number of cache errors.||0|
|`*/put/ok`|Number of new cache entries successfully cached.||Varies, but more than zero.|
|`*/put/error`|Number of new cache entries that could not be cached due to errors.||Varies, but more than zero.|
|`*/put/oversized`|Number of potential new cache entries that were skipped due to being too large (based on `druid.{broker,historical,realtime}.cache.maxEntrySize` properties).||Varies.|

#### Memcached only metrics

Memcached client metrics are reported as per the following. These metrics come directly from the client as opposed to from the cache retrieval layer.

|Metric|Description|Dimensions|Normal Value|
|------|-----------|----------|------------|
|`query/cache/memcached/total`|Cache metrics unique to memcached (only if `druid.cache.type=memcached`) as their actual values|Variable|N/A|
|`query/cache/memcached/delta`|Cache metrics unique to memcached (only if `druid.cache.type=memcached`) as their delta from the prior event emission|Variable|N/A|

## SQL Metrics

If SQL is enabled, the Broker will emit the following metrics for SQL.

|Metric|Description|Dimensions|Normal Value|
|------|-----------|----------|------------|
|`sqlQuery/time`|Milliseconds taken to complete a SQL.|id, nativeQueryIds, dataSource, remoteAddress, success.|< 1s|
|`sqlQuery/bytes`|number of bytes returned in SQL response.|id, nativeQueryIds, dataSource, remoteAddress, success.| |

## Ingestion Metrics (Kafka Indexing Service)

These metrics are applicable for the Kafka Indexing Service.

|Metric|Description|Dimensions|Normal Value|
|------|-----------|----------|------------|
|`ingest/kafka/lag`|Total lag between the offsets consumed by the Kafka indexing tasks and latest offsets in Kafka brokers across all partitions. Minimum emission period for this metric is a minute.|dataSource.|Greater than 0, should not be a very high number |
|`ingest/kafka/maxLag`|Max lag between the offsets consumed by the Kafka indexing tasks and latest offsets in Kafka brokers across all partitions. Minimum emission period for this metric is a minute.|dataSource.|Greater than 0, should not be a very high number |
|`ingest/kafka/avgLag`|Average lag between the offsets consumed by the Kafka indexing tasks and latest offsets in Kafka brokers across all partitions. Minimum emission period for this metric is a minute.|dataSource.|Greater than 0, should not be a very high number |

## Ingestion metrics (Realtime process)

These metrics are only available if the RealtimeMetricsMonitor is included in the monitors list for the Realtime process. These metrics are deltas for each emission period.

|Metric|Description|Dimensions|Normal Value|
|------|-----------|----------|------------|
|`ingest/events/thrownAway`|Number of events rejected because they are outside the windowPeriod.|dataSource, taskId, taskType.|0|
|`ingest/events/unparseable`|Number of events rejected because the events are unparseable.|dataSource, taskId, taskType.|0|
|`ingest/events/duplicate`|Number of events rejected because the events are duplicated.|dataSource, taskId, taskType.|0|
|`ingest/events/processed`|Number of events successfully processed per emission period.|dataSource, taskId, taskType.|Equal to your # of events per emission period.|
|`ingest/rows/output`|Number of Druid rows persisted.|dataSource, taskId, taskType.|Your # of events with rollup.|
|`ingest/persists/count`|Number of times persist occurred.|dataSource, taskId, taskType.|Depends on configuration.|
|`ingest/persists/time`|Milliseconds spent doing intermediate persist.|dataSource, taskId, taskType.|Depends on configuration. Generally a few minutes at most.|
|`ingest/persists/cpu`|Cpu time in Nanoseconds spent on doing intermediate persist.|dataSource, taskId, taskType.|Depends on configuration. Generally a few minutes at most.|
|`ingest/persists/backPressure`|Milliseconds spent creating persist tasks and blocking waiting for them to finish.|dataSource, taskId, taskType.|0 or very low|
|`ingest/persists/failed`|Number of persists that failed.|dataSource, taskId, taskType.|0|
|`ingest/handoff/failed`|Number of handoffs that failed.|dataSource, taskId, taskType.|0|
|`ingest/merge/time`|Milliseconds spent merging intermediate segments|dataSource, taskId, taskType.|Depends on configuration. Generally a few minutes at most.|
|`ingest/merge/cpu`|Cpu time in Nanoseconds spent on merging intermediate segments.|dataSource, taskId, taskType.|Depends on configuration. Generally a few minutes at most.|
|`ingest/handoff/count`|Number of handoffs that happened.|dataSource, taskId, taskType.|Varies. Generally greater than 0 once every segment granular period if cluster operating normally|
|`ingest/sink/count`|Number of sinks not handoffed.|dataSource, taskId, taskType.|1~3|
|`ingest/events/messageGap`|Time gap between the data time in event and current system time.|dataSource, taskId, taskType.|Greater than 0, depends on the time carried in event |


Note: If the JVM does not support CPU time measurement for the current thread, ingest/merge/cpu and ingest/persists/cpu will be 0.

### Indexing service

|Metric|Description|Dimensions|Normal Value|
|------|-----------|----------|------------|
|`task/run/time`|Milliseconds taken to run a task.|dataSource, taskId, taskType, taskStatus.|Varies.|
|`task/action/log/time`|Milliseconds taken to log a task action to the audit log.|dataSource, taskId, taskType|< 1000 (subsecond)|
|`task/action/run/time`|Milliseconds taken to execute a task action.|dataSource, taskId, taskType|Varies from subsecond to a few seconds, based on action type.|
|`segment/added/bytes`|Size in bytes of new segments created.|dataSource, taskId, taskType, interval.|Varies.|
|`segment/moved/bytes`|Size in bytes of segments moved/archived via the Move Task.|dataSource, taskId, taskType, interval.|Varies.|
|`segment/nuked/bytes`|Size in bytes of segments deleted via the Kill Task.|dataSource, taskId, taskType, interval.|Varies.|
|`task/success/count`|Number of successful tasks per emission period. This metric is only available if the TaskCountStatsMonitor module is included.|dataSource.|Varies.|
|`task/failed/count`|Number of failed tasks per emission period. This metric is only available if the TaskCountStatsMonitor module is included.|dataSource.|Varies.|
|`task/running/count`|Number of current running tasks. This metric is only available if the TaskCountStatsMonitor module is included.|dataSource.|Varies.|
|`task/pending/count`|Number of current pending tasks. This metric is only available if the TaskCountStatsMonitor module is included.|dataSource.|Varies.|
|`task/waiting/count`|Number of current waiting tasks. This metric is only available if the TaskCountStatsMonitor module is included.|dataSource.|Varies.|

## Coordination

These metrics are for the Druid Coordinator and are reset each time the Coordinator runs the coordination logic.

|Metric|Description|Dimensions|Normal Value|
|------|-----------|----------|------------|
|`segment/assigned/count`|Number of segments assigned to be loaded in the cluster.|tier.|Varies.|
|`segment/moved/count`|Number of segments moved in the cluster.|tier.|Varies.|
|`segment/dropped/count`|Number of segments dropped due to being overshadowed.|tier.|Varies.|
|`segment/deleted/count`|Number of segments dropped due to rules.|tier.|Varies.|
|`segment/unneeded/count`|Number of segments dropped due to being marked as unused.|tier.|Varies.|
|`segment/cost/raw`|Used in cost balancing. The raw cost of hosting segments.|tier.|Varies.|
|`segment/cost/normalization`|Used in cost balancing. The normalization of hosting segments.|tier.|Varies.|
|`segment/cost/normalized`|Used in cost balancing. The normalized cost of hosting segments.|tier.|Varies.|
|`segment/loadQueue/size`|Size in bytes of segments to load.|server.|Varies.|
|`segment/loadQueue/failed`|Number of segments that failed to load.|server.|0|
|`segment/loadQueue/count`|Number of segments to load.|server.|Varies.|
|`segment/dropQueue/count`|Number of segments to drop.|server.|Varies.|
|`segment/size`|Total size of used segments in a data source. Emitted only for data sources to which at least one used segment belongs.|dataSource.|Varies.|
|`segment/count`|Number of used segments belonging to a data source. Emitted only for data sources to which at least one used segment belongs.|dataSource.|< max|
|`segment/overShadowed/count`|Number of overshadowed segments.||Varies.|
|`segment/unavailable/count`|Number of segments (not including replicas) left to load until segments that should be loaded in the cluster are available for queries.|dataSource.|0|
|`segment/underReplicated/count`|Number of segments (including replicas) left to load until segments that should be loaded in the cluster are available for queries.|tier, dataSource.|0|
|`tier/historical/count`|Number of available historical nodes in each tier.|tier.|Varies.|
|`tier/replication/factor`|Configured maximum replication factor in each tier.|tier.|Varies.|
|`tier/required/capacity`|Total capacity in bytes required in each tier.|tier.|Varies.|
|`tier/total/capacity`|Total capacity in bytes available in each tier.|tier.|Varies.|

If `emitBalancingStats` is set to `true` in the Coordinator [dynamic configuration](
../configuration/index.html#dynamic-configuration), then [log entries](../configuration/logging.md) for class
`org.apache.druid.server.coordinator.duty.EmitClusterStatsAndMetrics` will have extra information on balancing
decisions.

## General Health

### Historical

|Metric|Description|Dimensions|Normal Value|
|------|-----------|----------|------------|
|`segment/max`|Maximum byte limit available for segments.||Varies.|
|`segment/used`|Bytes used for served segments.|dataSource, tier, priority.|< max|
|`segment/usedPercent`|Percentage of space used by served segments.|dataSource, tier, priority.|< 100%|
|`segment/count`|Number of served segments.|dataSource, tier, priority.|Varies.|
|`segment/pendingDelete`|On-disk size in bytes of segments that are waiting to be cleared out|Varies.|

### JVM

These metrics are only available if the JVMMonitor module is included.

|Metric|Description|Dimensions|Normal Value|
|------|-----------|----------|------------|
|`jvm/pool/committed`|Committed pool.|poolKind, poolName.|close to max pool|
|`jvm/pool/init`|Initial pool.|poolKind, poolName.|Varies.|
|`jvm/pool/max`|Max pool.|poolKind, poolName.|Varies.|
|`jvm/pool/used`|Pool used.|poolKind, poolName.|< max pool|
|`jvm/bufferpool/count`|Bufferpool count.|bufferPoolName.|Varies.|
|`jvm/bufferpool/used`|Bufferpool used.|bufferPoolName.|close to capacity|
|`jvm/bufferpool/capacity`|Bufferpool capacity.|bufferPoolName.|Varies.|
|`jvm/mem/init`|Initial memory.|memKind.|Varies.|
|`jvm/mem/max`|Max memory.|memKind.|Varies.|
|`jvm/mem/used`|Used memory.|memKind.|< max memory|
|`jvm/mem/committed`|Committed memory.|memKind.|close to max memory|
|`jvm/gc/count`|Garbage collection count.|gcName (cms/g1/parallel/etc.), gcGen (old/young)|Varies.|
|`jvm/gc/cpu`|Count of CPU time in Nanoseconds spent on garbage collection. Note: `jvm/gc/cpu` represents the total time over multiple GC cycles; divide by `jvm/gc/count` to get the mean GC time per cycle|gcName, gcGen|Sum of `jvm/gc/cpu` should be within 10-30% of sum of `jvm/cpu/total`, depending on the GC algorithm used (reported by [`JvmCpuMonitor`](../configuration/index.html#enabling-metrics)) |

### EventReceiverFirehose

The following metric is only available if the EventReceiverFirehoseMonitor module is included.

|Metric|Description|Dimensions|Normal Value|
|------|-----------|----------|------------|
|`ingest/events/buffered`|Number of events queued in the EventReceiverFirehose's buffer|serviceName, dataSource, taskId, taskType, bufferCapacity.|Equal to current # of events in the buffer queue.|
|`ingest/bytes/received`|Number of bytes received by the EventReceiverFirehose.|serviceName, dataSource, taskId, taskType.|Varies.|

## Sys

These metrics are only available if the SysMonitor module is included.

|Metric|Description|Dimensions|Normal Value|
|------|-----------|----------|------------|
|`sys/swap/free`|Free swap.||Varies.|
|`sys/swap/max`|Max swap.||Varies.|
|`sys/swap/pageIn`|Paged in swap.||Varies.|
|`sys/swap/pageOut`|Paged out swap.||Varies.|
|`sys/disk/write/count`|Writes to disk.|fsDevName, fsDirName, fsTypeName, fsSysTypeName, fsOptions.|Varies.|
|`sys/disk/read/count`|Reads from disk.|fsDevName, fsDirName, fsTypeName, fsSysTypeName, fsOptions.|Varies.|
|`sys/disk/write/size`|Bytes written to disk. Can we used to determine how much paging is occurring with regards to segments.|fsDevName, fsDirName, fsTypeName, fsSysTypeName, fsOptions.|Varies.|
|`sys/disk/read/size`|Bytes read from disk. Can we used to determine how much paging is occurring with regards to segments.|fsDevName, fsDirName, fsTypeName, fsSysTypeName, fsOptions.|Varies.|
|`sys/net/write/size`|Bytes written to the network.|netName, netAddress, netHwaddr|Varies.|
|`sys/net/read/size`|Bytes read from the network.|netName, netAddress, netHwaddr|Varies.|
|`sys/fs/used`|Filesystem bytes used.|fsDevName, fsDirName, fsTypeName, fsSysTypeName, fsOptions.|< max|
|`sys/fs/max`|Filesystesm bytes max.|fsDevName, fsDirName, fsTypeName, fsSysTypeName, fsOptions.|Varies.|
|`sys/mem/used`|Memory used.||< max|
|`sys/mem/max`|Memory max.||Varies.|
|`sys/storage/used`|Disk space used.|fsDirName.|Varies.|
|`sys/cpu`|CPU used.|cpuName, cpuTime.|Varies.|

