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
