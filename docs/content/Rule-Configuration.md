---
layout: doc_page
---
# Configuring Rules for Coordinator Nodes
Note: It is recommended that the coordinator console is used to configure rules. However, the coordinator node does have HTTP endpoints to programmatically configure rules.

Load Rules
----------

Load rules indicate how many replicants of a segment should exist in a server tier.

### Interval Load Rule

Interval load rules are of the form:

```json
{
  "type" : "loadByInterval",
  "interval" : "2012-01-01/2013-01-01",
  "tier" : "hot"
}
```

* `type` - this should always be "loadByInterval"
* `interval` - A JSON Object representing ISO-8601 Intervals
* `replicants` - number of replicants to load
* `tier` - the configured historical node tier

### Period Load Rule

Period load rules are of the form:

```json
{
  "type" : "loadByPeriod",
  "period" : "P1M",
  "futurePeriod": "P0D",
  "tieredReplicants" : {
    "hot" : 2,
    "_default" : 1
  }
}
```

* `type` - this should always be "loadByPeriod"
* `period` - ISO-8601 period of past data to load
* `futurePeriod` - ISO-8601 period of future data to load
* `tieredReplicants` - a map of String to Integer for the number of replicants in each tier
* `replicants` - (deprecated, use `tieredReplicants`) number of replicants to load
* `tier` - (deprecated, use `tieredReplicants`) historical node tier to load in

The interval of a segment will be compared against the specified periods.
The rule matches if (now - period, now + futurePeriod) overlaps the segment interval.

Drop Rules
----------

Drop rules indicate when segments should be dropped from the cluster.

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
