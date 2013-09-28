---
layout: doc_page
---
Druid uses ZooKeeper (ZK) for management of current cluster state. The operations that happen over ZK are

1.  [Master](Master.html) leader election
2.  Segment "publishing" protocol from [Compute](Compute.html) and [Realtime](Realtime.html)
3.  Segment load/drop protocol between [Master](Master.html) and [Compute](Compute.html)

### Property Configuration

ZooKeeper paths are set via the `runtime.properties` configuration file. Druid will automatically create paths that do not exist, so typos in config files is a very easy way to become split-brained.

There is a prefix path that is required and can be used as the only (well, kinda, see the note below) path-related zookeeper configuration parameter (everything else will be a default based on the prefix):

```
druid.zk.paths.base
```

You can also override each individual path (defaults are shown below):

```
druid.zk.paths.propertiesPath=${druid.zk.paths.base}/properties
druid.zk.paths.announcementsPath=${druid.zk.paths.base}/announcements
druid.zk.paths.servedSegmentsPath=${druid.zk.paths.base}/servedSegments
druid.zk.paths.loadQueuePath=${druid.zk.paths.base}/loadQueue
druid.zk.paths.masterPath=${druid.zk.paths.base}/master
druid.zk.paths.indexer.announcementsPath=${druid.zk.paths.base}/indexer/announcements
druid.zk.paths.indexer.tasksPath=${druid.zk.paths.base}/indexer/tasks
druid.zk.paths.indexer.statusPath=${druid.zk.paths.base}/indexer/status
druid.zk.paths.indexer.leaderLatchPath=${druid.zk.paths.base}/indexer/leaderLatchPath
```

NOTE: We also use Curator’s service discovery module to expose some services via zookeeper. This also uses a zookeeper path, but this path is **not** affected by `druid.zk.paths.base` and **must** be specified separately. This property is

```
druid.zk.paths.discoveryPath
```

### Master Leader Election

We use the Curator LeadershipLatch recipe to do leader election at path

```
${druid.zk.paths.masterPath}/_MASTER
```

### Segment "publishing" protocol from Compute and Realtime

The `announcementsPath` and `servedSegmentsPath` are used for this.

All [Compute](Compute.html) and [Realtime](Realtime.html) nodes publish themselves on the `announcementsPath`, specifically, they will create an ephemeral znode at

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

Nodes like the [Master](Master.html) and [Broker](Broker.html) can then watch these paths to see which nodes are currently serving which segments.

### Segment load/drop protocol between Master and Compute

The `loadQueuePath` is used for this.

When the [Master](Master.html) decides that a [Compute](Compute.html) node should load or drop a segment, it writes an ephemeral znode to

```
${druid.zk.paths.loadQueuePath}/_host_of_compute_node/_segment_identifier
```

This node will contain a payload that indicates to the Compute node what it should do with the given segment. When the Compute node is done with the work, it will delete the znode in order to signify to the Master that it is complete.
