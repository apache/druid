---
layout: doc_page
---
Coordinator Node Configuration
==============================
For general Coordinator Node information, see [here](Coordinator.html).

Runtime Configuration
---------------------

The coordinator module uses several of the default modules in [Configuration](Configuration.html) and has the following set of configurations as well:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.coordinator.period`|The run period for the coordinator. The coordinator’s operates by maintaining the current state of the world in memory and periodically looking at the set of segments available and segments being served to make decisions about whether any changes need to be made to the data topology. This property sets the delay between each of these runs.|PT60S|
|`druid.coordinator.period.indexingPeriod`|How often to send indexing tasks to the indexing service. Only applies if merge or conversion is turned on.|PT1800S (30 mins)|
|`druid.coordinator.startDelay`|The operation of the Coordinator works on the assumption that it has an up-to-date view of the state of the world when it runs, the current ZK interaction code, however, is written in a way that doesn’t allow the Coordinator to know for a fact that it’s done loading the current state of the world. This delay is a hack to give it enough time to believe that it has all the data.|PT300S|
|`druid.coordinator.merge.on`|Boolean flag for whether or not the coordinator should try and merge small segments into a more optimal segment size.|PT300S|
|`druid.coordinator.conversion.on`|Boolean flag for converting old segment indexing versions to the latest segment indexing version.|false|
|`druid.coordinator.load.timeout`|The timeout duration for when the coordinator assigns a segment to a historical node.|15 minutes|
|`druid.manager.segment.pollDuration`|The duration between polls the Coordinator does for updates to the set of active segments. Generally defines the amount of lag time it can take for the coordinator to notice new segments.|PT1M|
|`druid.manager.rules.pollDuration`|The duration between polls the Coordinator does for updates to the set of active rules. Generally defines the amount of lag time it can take for the coordinator to notice rules.|PT1M|
|`druid.manager.rules.defaultTier`|The default tier from which default rules will be loaded from.|_default|

Dynamic Configuration
---------------------

The coordinator has dynamic configuration to change certain behaviour on the fly. The coordinator a JSON spec object from the Druid [MySQL](MySQL.html) config table. This object is detailed below:

It is recommended that you use the Coordinator Console to configure these parameters. However, if you need to do it via HTTP, the JSON object can be submitted to the overlord via a POST request at:

```
http://<COORDINATOR_IP>:<PORT>/coordinator/config
```

A sample worker setup spec is shown below:

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