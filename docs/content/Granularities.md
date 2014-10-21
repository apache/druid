---
layout: doc_page
---
# Aggregation Granularity
The granularity field determines how data gets bucketed across the time dimension, or how it gets aggregated by hour, day, minute, etc.

It can be specified either as a string for simple granularities or as an object for arbitrary granularities.

### Simple Granularities

Simple granularities are specified as a string and bucket timestamps by their UTC time (e.g., days start at 00:00 UTC).

Supported granularity strings are: `all`, `none`, `minute`, `fifteen_minute`, `thirty_minute`, `hour` and `day`

* `all` buckets everything into a single bucket
* `none` does not bucket data (it actually uses the granularity of the index - minimum here is `none` which means millisecond granularity). Using `none` in a [TimeSeriesQuery](TimeSeriesQuery.html) is currently not recommended (the system will try to generate 0 values for all milliseconds that didn’t exist, which is often a lot).

### Duration Granularities

Duration granularities are specified as an exact duration in milliseconds and timestamps are returned as UTC.

They also support specifying an optional origin, which defines where to start counting time buckets from (defaults to 1970-01-01T00:00:00Z).

```javascript
{"type": "duration", "duration": "7200000"}
```

This chunks up every 2 hours.

```javascript
{"type": "duration", "duration": "3600000", "origin": "2012-01-01T00:30:00Z"}
```

This chunks up every hour on the half-hour.

### Period Granularities

Period granularities are specified as arbitrary period combinations of years, months, weeks, hours, minutes and seconds (e.g. P2W, P3M, PT1H30M, PT0.750S) in ISO8601 format. They support specifying a time zone which determines where period boundaries start as well as the timezone of the returned timestamps. By default, years start on the first of January, months start on the first of the month and weeks start on Mondays unless an origin is specified.

Time zone is optional (defaults to UTC). Origin is optional (defaults to 1970-01-01T00:00:00 in the given time zone).

```javascript
{"type": "period", "period": "P2D", "timeZone": "America/Los_Angeles"}
```

This will bucket by two-day chunks in the Pacific timezone.

```javascript
{"type": "period", "period": "P3M", "timeZone": "America/Los_Angeles", "origin": "2012-02-01T00:00:00-08:00"}
```

This will bucket by 3-month chunks in the Pacific timezone where the three-month quarters are defined as starting from February.

#### Supported Time Zones
Timezone support is provided by the [Joda Time library](http://www.joda.org), which uses the standard IANA time zones. See the [Joda Time supported timezones](http://joda-time.sourceforge.net/timezones.html).
