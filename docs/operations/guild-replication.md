---
id: guild-replication
title: "Guild Replication Documentation"
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
# Guild Replication

## The Basics 

Guild replication is a way for a Druid Cluster to make its best effort to replicate segments across multiple operator defined Guilds.

If Guild replication is enabled, the Coordinator will try its best to load replicants on more than one Guild. However, this will not always be the case. The following are some reasons why a segment may not be replicated across more than one Guild:
* The `LoadRule` that applies to the used segment only calls for one replicant to be loaded on the cluster.
* There are no servers on other Guilds who have storage available for the used segment, but storage is available on a server in the already used Guild. In this case the Coordinator would rather meet the replicant count specified by the `LoadRule` then have less replicants than expected.

### Configuration

#### runtime.properties

`druid.server.guild`

* An operator can assign Historical servers a Guild via runtime.properties. This is a string value with a default of `_default_guild`

`druid.coordinator.guildReplication.on`

* An operator must set this to `true` in the Coordinator's runtime.properties if they want the Coordinator to attempt to load replicants across multiple Guilds. The default value is `false` meaning this behavior is off.

#### Dynamic Coordinator Configuration

`guildReplicationMaxPercentOfMaxSegmentsToMove`

* This is a floating-point value that an operator can utilize to control prioritization for balancing segments who are not located on more than one Guild. The default value is `0`, meaning that the Coordinator will not do any prioritization for such segments when performing balancing.
* The higher it is set, the more segments who are on a single Guild will be prioritized for moving during balancing.
* Note that `decommissioningMaxPercentOfMaxSegmentsToMove` takes precedence so if you are decommissioning servers, those segments will be treated with a higher priority if you are using that configuration.
* If all of your segments live on more than one Guild, balancing will happen without concern for Guild replication state.

`emitGuildReplicationMetrics`

* This is a boolean flag that controls if the Coordinator will periodically emit metrics with Guild replication information. Enabling this will come with compute cost as the Coordinator will have to perform calculations for these metrics.

## FAQ

### Why would I ever want to use this?

The initial driving force behind this functionality was to implement physical rack awareness on large on-prem Druid Clusters. The idea was to get a similar result to what HDFS allows for rack aware replication. Using Guild replication for this allows an operator multiple things. Most notably, the ability to restart racks of Historical servers at a time without losing data availability as well as the peace of mind knowing that a rack level failure in the data center can be tolerated with zero data unavailability.

Notice that the word "Guild" has been used in lieu of "Rack". That is because this is not a feature intended only for use on-prem to replicate across physical racks. There are likely many more reasons that using operator defined guilds can help an operator run a performant and resilient production cluster either on-prem or in the cloud.

### Can I enable this on a cluster with existing data?

Yes! However, you will almost certainly start off with a number of segments who are violating the goal of Guild replication by being located on a single Guild. Segment balancing will eventually rectify these violations. You should use `guildReplicationMaxPercentOfMaxSegmentsToMove` when possible to prioritize moving these segments to multiple Guilds. Enabling `emitGuildReplicationMetrics` can help you track the status of your cluster's Guild replication compliance.

### Can I disable this on a cluster with existing data?

Yes! When upgrading to a version of Druid with this functionality for the first time, it will be OFF by default. This means you do not need to worry about setting Guilds for your servers. If you have turned it on, but now wish to turn it off, you can certainly do that too. Once off, the Coordinator will go back to loading, dropping, and balancing segments without any concern for what Guild a Historical server belongs to.

## Known Shortfalls

* Segments whose applicable `LoadRule` only calls for one replicant can still be selected by the cluster balancing process when prioritizing segments who are only located on one Guild. This is not the most ideal situation because in this prioritized balancing the goal is to increase the number of segments that are replicated across Guilds. This situation does not help work towards that goal.

* `LoadRule#dropSegmentFromServers` does not account for guild replication counts within the method itself. When being called for a segment with > 1 replicants loaded on some specified set of guilds, it will drop segments based solely of calculated cost by the `BalancerStrategy`. This means we could make a sub-optimal drop when it comes to maximizing guild replication factors. If replication is reduced to a single guild, the Coordinator should rectify the sub-optimal drop during an upcoming segment balancing run.