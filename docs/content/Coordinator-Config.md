---
layout: doc_page
---
Coordinator Node Configuration
==============================
For general Coordinator Node information, see [here](Coordinator.html).

Runtime Configuration
---------------------

The coordinator node uses several of the global configs in [Configuration](Configuration.html) and has the following set of configurations as well:

### Node Config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current node. This is used to advertise the current processes location as reachable from another node and should generally be specified such that `http://${druid.host}/` could actually talk to this process|none|
|`druid.port`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|none|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|none|

### Coordinator Operation

|Property|Description|Default|
|--------|-----------|-------|
|`druid.coordinator.period`|The run period for the coordinator. The coordinator’s operates by maintaining the current state of the world in memory and periodically looking at the set of segments available and segments being served to make decisions about whether any changes need to be made to the data topology. This property sets the delay between each of these runs.|PT60S|
|`druid.coordinator.period.indexingPeriod`|How often to send indexing tasks to the indexing service. Only applies if merge or conversion is turned on.|PT1800S (30 mins)|
|`druid.coordinator.startDelay`|The operation of the Coordinator works on the assumption that it has an up-to-date view of the state of the world when it runs, the current ZK interaction code, however, is written in a way that doesn’t allow the Coordinator to know for a fact that it’s done loading the current state of the world. This delay is a hack to give it enough time to believe that it has all the data.|PT300S|
|`druid.coordinator.merge.on`|Boolean flag for whether or not the coordinator should try and merge small segments into a more optimal segment size.|false|
|`druid.coordinator.conversion.on`|Boolean flag for converting old segment indexing versions to the latest segment indexing version.|false|
|`druid.coordinator.load.timeout`|The timeout duration for when the coordinator assigns a segment to a historical node.|PT15M|
|`druid.coordinator.load.queue.size`|Size of Load Queue for each historical node, defines maximum no. of concurrent segments will be assigned for loading to a historical node. |10|

### Metadata Retrieval

|Property|Description|Default|
|--------|-----------|-------|
|`druid.manager.config.pollDuration`|How often the manager polls the config table for updates.|PT1m|
|`druid.manager.segment.pollDuration`|The duration between polls the Coordinator does for updates to the set of active segments. Generally defines the amount of lag time it can take for the coordinator to notice new segments.|PT1M|
|`druid.manager.rules.pollDuration`|The duration between polls the Coordinator does for updates to the set of active rules. Generally defines the amount of lag time it can take for the coordinator to notice rules.|PT1M|
|`druid.manager.rules.defaultTier`|The default tier from which default rules will be loaded from.|_default|

Dynamic Configuration
---------------------

The coordinator has dynamic configuration to change certain behaviour on the fly. The coordinator a JSON spec object from the Druid [metadata storage](Metadata-storage.html) config table. This object is detailed below:

It is recommended that you use the Coordinator Console to configure these parameters. However, if you need to do it via HTTP, the JSON object can be submitted to the overlord via a POST request at:

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
  "emitBalancingStats": false
}
```

Issuing a GET request at the same URL will return the spec that is currently in place. A description of the config setup spec is shown below.

|Property|Description|Default|
|--------|-----------|-------|
|`millisToWaitBeforeDeleting`|How long does the coordinator need to be active before it can start removing (marking unused) segments in metadata storage.|900000 (15 mins)|
|`mergeBytesLimit`|The maximum number of bytes to merge (for segments).|524288000L|
|`mergeSegmentsLimit`|The maximum number of segments that can be in a single [merge task](Tasks.html).|100|
|`maxSegmentsToMove`|The maximum number of segments that can be moved at any given time.|5|
|`replicantLifetime`|The maximum number of coordinator runs for a segment to be replicated before we start alerting.|15|
|`replicationThrottleLimit`|The maximum number of segments that can be replicated at one time.|10|
|`emitBalancingStats`|Boolean flag for whether or not we should emit balancing stats. This is an expensive operation.|false|

To view the audit history of coordinator dynamic config issue a GET request to the URL -

```
http://<COORDINATOR_IP>:<PORT>/druid/coordinator/v1/config/history?interval=<interval>
```

default value of interval can be specified by setting `druid.audit.manager.auditHistoryMillis` (1 week if not configured) in coordinator runtime.properties
