---
layout: doc_page
---
# ZooKeeper
Druid uses [ZooKeeper](http://zookeeper.apache.org/) (ZK) for management of current cluster state. The operations that happen over ZK are

1.  [Coordinator](Coordinator.html) leader election
2.  Segment "publishing" protocol from [Historical](Historical.html) and [Realtime](Realtime.html)
3.  Segment load/drop protocol between [Coordinator](Coordinator.html) and [Historical](Historical.html)
4.  [Overlord](Indexing-Service.html) leader election
5.  [Indexing Service](Indexing-Service.html) task management

### Property Configuration

ZooKeeper paths are set via the `runtime.properties` configuration file. Druid will automatically create paths that do not exist, so typos in config files is a very easy way to become split-brained.


|Property|Description|Default|
|--------|-----------|-------|
|`druid.zk.service.host`|The ZooKeeper hosts to connect to. This is a REQUIRED property and therefore a host address must be supplied.|none|
|`druid.zk.service.sessionTimeoutMs`|ZooKeeper session timeout, in milliseconds.|`30000`|
|`druid.curator.compress`|Boolean flag for whether or not created Znodes should be compressed.|`false`|

### Path Configuration
Druid interacts with ZK through a set of standard path configurations. We recommend just setting the base ZK path, but all ZK paths that Druid uses can be overwritten to absolute paths.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.zk.paths.base`|Base Zookeeper path.|`/druid`|
|`druid.zk.paths.propertiesPath`|Zookeeper properties path.|`${druid.zk.paths.base}/properties`|
|`druid.zk.paths.announcementsPath`|Druid node announcement path.|`${druid.zk.paths.base}/announcements`|
|`druid.zk.paths.liveSegmentsPath`|Current path for where Druid nodes announce their segments.|`${druid.zk.paths.base}/segments`|
|`druid.zk.paths.loadQueuePath`|Entries here cause historical nodes to load and drop segments.|`${druid.zk.paths.base}/loadQueue`|
|`druid.zk.paths.coordinatorPath`|Used by the coordinator for leader election.|`${druid.zk.paths.base}/coordinator`|
|`druid.zk.paths.servedSegmentsPath`|@Deprecated. Legacy path for where Druid nodes announce their segments.|`${druid.zk.paths.base}/servedSegments`|

The indexing service also uses its own set of paths. These configs can be included in the common configuration.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.zk.paths.indexer.base`|Base zookeeper path for |`${druid.zk.paths.base}/indexer`|
|`druid.zk.paths.indexer.announcementsPath`|Middle managers announce themselves here.|`${druid.zk.paths.indexer.base}/announcements`|
|`druid.zk.paths.indexer.tasksPath`|Used to assign tasks to middle managers.|`${druid.zk.paths.indexer.base}/tasks`|
|`druid.zk.paths.indexer.statusPath`|Parent path for announcement of task statuses.|`${druid.zk.paths.indexer.base}/status`|
|`druid.zk.paths.indexer.leaderLatchPath`|Used for Overlord leader election.|`${druid.zk.paths.indexer.base}/leaderLatchPath`|

If `druid.zk.paths.base` and `druid.zk.paths.indexer.base` are both set, and none of the other `druid.zk.paths.*` or `druid.zk.paths.indexer.*` values are set, then the other properties will be evaluated relative to their respective `base`.
For example, if `druid.zk.paths.base` is set to `/druid1` and `druid.zk.paths.indexer.base` is set to `/druid2` then `druid.zk.paths.announcementsPath` will default to `/druid1/announcements` while `druid.zk.paths.indexer.announcementsPath` will default to `/druid2/announcements`.

The following path is used service discovery and are **not** affected by `druid.zk.paths.base` and **must** be specified separately.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.discovery.curator.path`|Services announce themselves under this ZooKeeper path.|`/druid/discovery`|

### Coordinator Leader Election

We use the Curator LeadershipLatch recipe to do leader election at path

```
${druid.zk.paths.coordinatorPath}/_COORDINATOR
```

### Segment "publishing" protocol from Historical and Realtime

The `announcementsPath` and `servedSegmentsPath` are used for this.

All [Historical](Historical.html) and [Realtime](Realtime.html) nodes publish themselves on the `announcementsPath`, specifically, they will create an ephemeral znode at

```
${druid.zk.paths.announcementsPath}/${druid.host}
```

Which signifies that they exist. They will also subsequently create a permanent znode at

```
${druid.zk.paths.servedSegmentsPath}/${druid.host}
```

And as they load up segments, they will attach ephemeral znodes that look like

```
${druid.zk.paths.servedSegmentsPath}/${druid.host}/_segment_identifier_
```

Nodes like the [Coordinator](Coordinator.html) and [Broker](Broker.html) can then watch these paths to see which nodes are currently serving which segments.

### Segment load/drop protocol between Coordinator and Historical

The `loadQueuePath` is used for this.

When the [Coordinator](Coordinator.html) decides that a [Historical](Historical.html) node should load or drop a segment, it writes an ephemeral znode to

```
${druid.zk.paths.loadQueuePath}/_host_of_historical_node/_segment_identifier
```

This node will contain a payload that indicates to the historical node what it should do with the given segment. When the historical node is done with the work, it will delete the znode in order to signify to the Coordinator that it is complete.
