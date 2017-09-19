---
layout: doc_page
---
# Retaining or Automatically Dropping Data

Coordinator nodes use rules to determine what data should be loaded to or dropped from the cluster. Rules are used for data retention and query execution, and are set on the coordinator console (http://coordinator_ip:port).

There are three types of rules, i.e., load rules, drop rules, and broadcast rules. Load rules indicate how segments should be assigned to different historical node tiers and how many replicas of a segment should exist in each tier. 
Drop rules indicate when segments should be dropped entirely from the cluster. Finally, broadcast rules indicate how segments of different data sources should be co-located in historical nodes.

The coordinator loads a set of rules from the metadata storage. Rules may be specific to a certain datasource and/or a default set of rules can be configured. Rules are read in order and hence the ordering of rules is important. The coordinator will cycle through all available segments and match each segment with the first rule that applies. Each segment may only match a single rule.

Note: It is recommended that the coordinator console is used to configure rules. However, the coordinator node does have HTTP endpoints to programmatically configure rules.

When a rule is updated, the change may not be reflected until the next time the coordinator runs. This will be fixed in the near future.

Load Rules
----------

Load rules indicate how many replicas of a segment should exist in a server tier.

### Forever Load Rule

Forever load rules are of the form:

```json
{
  "type" : "loadForever",  
  "tieredReplicants": {
    "hot": 1,
    "_default_tier" : 1
  }
}
```

* `type` - this should always be "loadForever"
* `tieredReplicants` - A JSON Object where the keys are the tier names and values are the number of replicas for that tier.


### Interval Load Rule

Interval load rules are of the form:

```json
{
  "type" : "loadByInterval",
  "interval": "2012-01-01/2013-01-01",
  "tieredReplicants": {
    "hot": 1,
    "_default_tier" : 1
  }
}
```

* `type` - this should always be "loadByInterval"
* `interval` - A JSON Object representing ISO-8601 Intervals
* `tieredReplicants` - A JSON Object where the keys are the tier names and values are the number of replicas for that tier.

### Period Load Rule

Period load rules are of the form:

```json
{
  "type" : "loadByPeriod",
  "period" : "P1M",
  "tieredReplicants": {
      "hot": 1,
      "_default_tier" : 1
  }
}
```

* `type` - this should always be "loadByPeriod"
* `period` - A JSON Object representing ISO-8601 Periods
* `tieredReplicants` - A JSON Object where the keys are the tier names and values are the number of replicas for that tier.

The interval of a segment will be compared against the specified period. The rule matches if the period overlaps the interval.

Drop Rules
----------

Drop rules indicate when segments should be dropped from the cluster.

### Forever Drop Rule

Forever drop rules are of the form:

```json
{
  "type" : "dropForever"  
}
```

* `type` - this should always be "dropForever"

All segments that match this rule are dropped from the cluster.


### Interval Drop Rule

Interval drop rules are of the form:

```json
{
  "type" : "dropByInterval",
  "interval" : "2012-01-01/2013-01-01"
}
```

* `type` - this should always be "dropByInterval"
* `interval` - A JSON Object representing ISO-8601 Periods

A segment is dropped if the interval contains the interval of the segment.

### Period Drop Rule

Period drop rules are of the form:

```json
{
  "type" : "dropByPeriod",
  "period" : "P1M"
}
```

* `type` - this should always be "dropByPeriod"
* `period` - A JSON Object representing ISO-8601 Periods

The interval of a segment will be compared against the specified period. The period is from some time in the past to the current time. The rule matches if the period contains the interval.

Broadcast Rules
---------------

Broadcast rules indicate how segments of different data sources should be co-located in historical nodes. 
Once a broadcast rule is configured for a data source, all segments of the data source are broadcasted to the servers holding _any segments_ of the co-located data sources.

### Forever Broadcast Rule

Forever broadcast rules are of the form:

```json
{
  "type" : "broadcastForever",
  "colocatedDataSources" : [ "target_source1", "target_source2" ]
}
```

* `type` - this should always be "broadcastForever"
* `colocatedDataSources` - A JSON List containing data source names to be co-located. `null` and empty list means broadcasting to every node in the cluster.

### Interval Broadcast Rule

Interval broadcast rules are of the form:

```json
{
  "type" : "broadcastByInterval",
  "colocatedDataSources" : [ "target_source1", "target_source2" ],
  "interval" : "2012-01-01/2013-01-01"
}
```

* `type` - this should always be "broadcastByInterval"
* `colocatedDataSources` - A JSON List containing data source names to be co-located. `null` and empty list means broadcasting to every node in the cluster.
* `interval` - A JSON Object representing ISO-8601 Periods. Only the segments of the interval will be broadcasted.

### Period Broadcast Rule

Period broadcast rules are of the form:

```json
{
  "type" : "broadcastByPeriod",
  "colocatedDataSources" : [ "target_source1", "target_source2" ],
  "period" : "P1M"
}
```

* `type` - this should always be "broadcastByPeriod"
* `colocatedDataSources` - A JSON List containing data source names to be co-located. `null` and empty list means broadcasting to every node in the cluster.
* `period` - A JSON Object representing ISO-8601 Periods

The interval of a segment will be compared against the specified period. The period is from some time in the past to the current time. The rule matches if the period contains the interval.

<div class="note caution">
broadcast rules don't guarantee that segments of the data sources are always co-located because segments for the colocated data sources are not loaded together atomically.
If you want to always co-locate the segments of some data sources together, it is recommended to leave colocatedDataSources empty.
</div>

# Permanently Deleting Data
 
Druid can fully drop data from the cluster, wipe the metadata store entry, and remove the data from deep storage for any segments that are 
marked as unused (segments dropped from the cluster via rules are always marked as unused). You can submit a [kill task](../ingestion/tasks.html) to the [indexing service](../design/indexing-service.html) to do this.

# Reloading Dropped Data

Data that has been dropped from a Druid cluster cannot be reloaded using only rules. To reload dropped data in Druid, you must first set your retention period (i.e. changing the retention period from 1 month to 2 months), and 
then enable the datasource in the Druid coordinator console, or through the Druid coordinator endpoints.
