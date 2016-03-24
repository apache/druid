---
layout: doc_page
---
Coordinator Node Configuration
==============================
For general Coordinator Node information, see [here](../design/coordinator.html).

Runtime Configuration
---------------------

The coordinator node uses several of the global configs in [Configuration](../configuration/index.html) and has the following set of configurations as well:

### Node Config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current node. This is used to advertise the current processes location as reachable from another node and should generally be specified such that `http://${druid.host}/` could actually talk to this process|InetAddress.getLocalHost().getCanonicalHostName()|
|`druid.port`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8081|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|druid/coordinator|

### Coordinator Operation

|Property|Description|Default|
|--------|-----------|-------|
|`druid.coordinator.period`|The run period for the coordinator. The coordinator’s operates by maintaining the current state of the world in memory and periodically looking at the set of segments available and segments being served to make decisions about whether any changes need to be made to the data topology. This property sets the delay between each of these runs.|PT60S|
|`druid.coordinator.period.indexingPeriod`|How often to send indexing tasks to the indexing service. Only applies if merge or conversion is turned on.|PT1800S (30 mins)|
|`druid.coordinator.startDelay`|The operation of the Coordinator works on the assumption that it has an up-to-date view of the state of the world when it runs, the current ZK interaction code, however, is written in a way that doesn’t allow the Coordinator to know for a fact that it’s done loading the current state of the world. This delay is a hack to give it enough time to believe that it has all the data.|PT300S|
|`druid.coordinator.merge.on`|Boolean flag for whether or not the coordinator should try and merge small segments into a more optimal segment size.|false|
|`druid.coordinator.conversion.on`|Boolean flag for converting old segment indexing versions to the latest segment indexing version.|false|
|`druid.coordinator.load.timeout`|The timeout duration for when the coordinator assigns a segment to a historical node.|PT15M|
|`druid.coordinator.kill.on`|Boolean flag for whether or not the coordinator should submit kill task for unused segments, that is, hard delete them from metadata store and deep storage. If set to true, then for all the whitelisted dataSources, coordinator will submit tasks periodically based on `period` specified. These kill tasks will delete all segments except for the last `durationToRetain` period. Whitelist can be set via dynamic configuration `killDataSourceWhitelist` described later.|false|
|`druid.coordinator.kill.period`|How often to send kill tasks to the indexing service. Value must be greater than `druid.coordinator.period.indexingPeriod`. Only applies if kill is turned on.|PT1D (1 Day)|
|`druid.coordinator.kill.durationToRetain`| Do not kill segments in last `durationToRetain`, must be greater or equal to 0. Only applies and MUST be specified if kill is turned on. Note that default value is invalid.|PT-1S (-1 seconds)|
|`druid.coordinator.kill.maxSegments`|Kill at most n segments per kill task submission, must be greater than 0. Only applies and MUST be specified if kill is turned on. Note that default value is invalid.|0|

### Metadata Retrieval

|Property|Description|Default|
|--------|-----------|-------|
|`druid.manager.config.pollDuration`|How often the manager polls the config table for updates.|PT1m|
|`druid.manager.segments.pollDuration`|The duration between polls the Coordinator does for updates to the set of active segments. Generally defines the amount of lag time it can take for the coordinator to notice new segments.|PT1M|
|`druid.manager.rules.pollDuration`|The duration between polls the Coordinator does for updates to the set of active rules. Generally defines the amount of lag time it can take for the coordinator to notice rules.|PT1M|
|`druid.manager.rules.defaultTier`|The default tier from which default rules will be loaded from.|_default|
|`druid.manager.rules.alertThreshold`|The duration after a failed poll upon which an alert should be emitted.|PT10M|

Dynamic Configuration
---------------------

The coordinator has dynamic configuration to change certain behaviour on the fly. The coordinator uses a JSON spec object from the Druid [metadata storage](../dependencies/metadata-storage.html) config table. This object is detailed below:

It is recommended that you use the Coordinator Console to configure these parameters. However, if you need to do it via HTTP, the JSON object can be submitted to the coordinator via a POST request at:

```
http://<COORDINATOR_IP>:<PORT>/druid/coordinator/v1/config
```

Optional Header Parameters for auditing the config change can also be specified.

|Header Param Name| Description | Default |
|----------|-------------|---------|
|`X-Druid-Author`| author making the config change|""|
|`X-Druid-Comment`| comment describing the change being done|""|

A sample coordinator dynamic config JSON object is shown below:

```json
{
  "millisToWaitBeforeDeleting": 900000,
  "mergeBytesLimit": 100000000L,
  "mergeSegmentsLimit" : 1000,
  "maxSegmentsToMove": 5,
  "replicantLifetime": 15,
  "replicationThrottleLimit": 10,
  "emitBalancingStats": false,
  "killDataSourceWhitelist": ["wikipedia", "testDatasource"]
}
```

Issuing a GET request at the same URL will return the spec that is currently in place. A description of the config setup spec is shown below.

|Property|Description|Default|
|--------|-----------|-------|
|`millisToWaitBeforeDeleting`|How long does the coordinator need to be active before it can start removing (marking unused) segments in metadata storage.|900000 (15 mins)|
|`mergeBytesLimit`|The maximum total uncompressed size in bytes of segments to merge.|524288000L|
|`mergeSegmentsLimit`|The maximum number of segments that can be in a single [append task](../ingestion/tasks.html).|100|
|`maxSegmentsToMove`|The maximum number of segments that can be moved at any given time.|5|
|`replicantLifetime`|The maximum number of coordinator runs for a segment to be replicated before we start alerting.|15|
|`replicationThrottleLimit`|The maximum number of segments that can be replicated at one time.|10|
|`emitBalancingStats`|Boolean flag for whether or not we should emit balancing stats. This is an expensive operation.|false|
|`killDataSourceWhitelist`|List of dataSources for which kill tasks are sent if property `druid.coordinator.kill.on` is true.|none|

To view the audit history of coordinator dynamic config issue a GET request to the URL -

```
http://<COORDINATOR_IP>:<PORT>/druid/coordinator/v1/config/history?interval=<interval>
```

default value of interval can be specified by setting `druid.audit.manager.auditHistoryMillis` (1 week if not configured) in coordinator runtime.properties

To view last <n> entries of the audit history of coordinator dynamic config issue a GET request to the URL -

```
http://<COORDINATOR_IP>:<PORT>/druid/coordinator/v1/config/history?count=<n>
```


# Lookups Dynamic Config (EXPERIMENTAL)
These configuration options control the behavior of the Lookup dynamic configuration described in the [lookups page](../querying/lookups.html)

|Property|Description|Default|
|--------|-----------|-------|
|`druid.manager.lookups.hostDeleteTimeout`|How long to wait for a `DELETE` request to a particular node before considering the `DELETE` a failure|PT1s|
|`druid.manager.lookups.hostUpdateTimeout`|How long to wait for a `POST` request to a particular node before considering the `POST` a failure|PT10s|
|`druid.manager.lookups.deleteAllTimeout`|How long to wait for all `DELETE` requests to finish before considering the delete attempt a failure|PT10s|
|`druid.manager.lookups.updateAllTimeout`|How long to wait for all `POST` requests to finish before considering the attempt a failure|PT60s|
|`druid.manager.lookups.threadPoolSize`|How many nodes can be managed concurrently (concurrent POST and DELETE requests). Requests this limit will wait in a queue until a slot becomes available.|10|
|`druid.manager.lookups.period`|How many milliseconds between checks for configuration changes|30_000|
