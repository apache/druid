---
layout: doc_page
---
# Configuring Rules for Coordinator Nodes

Rules indicate how segments should be assigned to different historical node tiers and how many replicas of a segment should exist in each tier. Rules may also indicate when segments should be dropped entirely from the cluster. The coordinator loads a set of rules from the metadata storage. Rules may be specific to a certain datasource and/or a default set of rules can be configured. Rules are read in order and hence the ordering of rules is important. The coordinator will cycle through all available segments and match each segment with the first rule that applies. Each segment may only match a single rule.

Note: It is recommended that the coordinator console is used to configure rules. However, the coordinator node does have HTTP endpoints to programmatically configure rules.


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

* `type` - this should always be "loadByInterval"
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

* `type` - this should always be "dropByPeriod"

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
