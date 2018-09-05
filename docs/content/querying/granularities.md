---
layout: doc_page
---

# Aggregation Granularity
The granularity field determines how data gets bucketed across the time dimension, or how it gets aggregated by hour, day, minute, etc.

It can be specified either as a string for simple granularities or as an object for arbitrary granularities.

### Simple Granularities

Simple granularities are specified as a string and bucket timestamps by their UTC time (e.g., days start at 00:00 UTC).

Supported granularity strings are: `all`, `none`, `second`, `minute`, `fifteen_minute`, `thirty_minute`, `hour`, `day`, `week`, `month`, `quarter` and `year`.

* `all` buckets everything into a single bucket
* `none` does not bucket data (it actually uses the granularity of the index - minimum here is `none` which means millisecond granularity). Using `none` in a [TimeseriesQuery](../querying/timeseriesquery.html) is currently not recommended (the system will try to generate 0 values for all milliseconds that didn’t exist, which is often a lot).

#### Example:

Suppose you have data below stored in Druid with millisecond ingestion granularity,

``` json
{"timestamp": "2013-08-31T01:02:33Z", "page": "AAA", "language" : "en"}
{"timestamp": "2013-09-01T01:02:33Z", "page": "BBB", "language" : "en"}
{"timestamp": "2013-09-02T23:32:45Z", "page": "CCC", "language" : "en"}
{"timestamp": "2013-09-03T03:32:45Z", "page": "DDD", "language" : "en"}
```

After submitting a groupBy query with `hour` granularity,

``` json
{
   "queryType":"groupBy",
   "dataSource":"my_dataSource",
   "granularity":"hour",
   "dimensions":[
      "language"
   ],
   "aggregations":[
      {
         "type":"count",
         "name":"count"
      }
   ],
   "intervals":[
      "2000-01-01T00:00Z/3000-01-01T00:00Z"
   ]
}
```

you will get

``` json
[ {
  "version" : "v1",
  "timestamp" : "2013-08-31T01:00:00.000Z",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
}, {
  "version" : "v1",
  "timestamp" : "2013-09-01T01:00:00.000Z",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
}, {
  "version" : "v1",
  "timestamp" : "2013-09-02T23:00:00.000Z",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
}, {
  "version" : "v1",
  "timestamp" : "2013-09-03T03:00:00.000Z",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
} ]
```

Note that all the empty buckets are discarded.


If you change the granularity to `day`, you will get

``` json
[ {
  "version" : "v1",
  "timestamp" : "2013-08-31T00:00:00.000Z",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
}, {
  "version" : "v1",
  "timestamp" : "2013-09-01T00:00:00.000Z",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
}, {
  "version" : "v1",
  "timestamp" : "2013-09-02T00:00:00.000Z",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
}, {
  "version" : "v1",
  "timestamp" : "2013-09-03T00:00:00.000Z",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
} ]
```


If you change the granularity to `none`, you will get the same results as setting it to the ingestion granularity.

``` json
[ {
  "version" : "v1",
  "timestamp" : "2013-08-31T01:02:33.000Z",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
}, {
  "version" : "v1",
  "timestamp" : "2013-09-01T01:02:33.000Z",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
}, {
  "version" : "v1",
  "timestamp" : "2013-09-02T23:32:45.000Z",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
}, {
  "version" : "v1",
  "timestamp" : "2013-09-03T03:32:45.000Z",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
} ]
```

Having a query granularity smaller than the ingestion granularity doesn't make sense,
because information about that smaller granularity is not present in the indexed data.
So, if the query granularity is smaller than the ingestion granularity, druid produces
results that are equivalent to having set the query granularity to the ingestion granularity.
See `queryGranularity` in [Ingestion Spec](../ingestion/ingestion-spec.html#granularityspec).


If you change the granularity to `all`, you will get everything aggregated in 1 bucket,

``` json
[ {
  "version" : "v1",
  "timestamp" : "2000-01-01T00:00:00.000Z",
  "event" : {
    "count" : 4,
    "language" : "en"
  }
} ]
```


### Duration Granularities

Duration granularities are specified as an exact duration in milliseconds and timestamps are returned as UTC. Duration granularity values are in millis.

They also support specifying an optional origin, which defines where to start counting time buckets from (defaults to 1970-01-01T00:00:00Z).

```javascript
{"type": "duration", "duration": 7200000}
```

This chunks up every 2 hours.

```javascript
{"type": "duration", "duration": 3600000, "origin": "2012-01-01T00:30:00Z"}
```

This chunks up every hour on the half-hour.

#### Example:

Reusing the data in the previous example, after submitting a groupBy query with 24 hours duration,

``` json
{
   "queryType":"groupBy",
   "dataSource":"my_dataSource",
   "granularity":{"type": "duration", "duration": "86400000"},
   "dimensions":[
      "language"
   ],
   "aggregations":[
      {
         "type":"count",
         "name":"count"
      }
   ],
   "intervals":[
      "2000-01-01T00:00Z/3000-01-01T00:00Z"
   ]
}
```

you will get

``` json
[ {
  "version" : "v1",
  "timestamp" : "2013-08-31T00:00:00.000Z",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
}, {
  "version" : "v1",
  "timestamp" : "2013-09-01T00:00:00.000Z",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
}, {
  "version" : "v1",
  "timestamp" : "2013-09-02T00:00:00.000Z",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
}, {
  "version" : "v1",
  "timestamp" : "2013-09-03T00:00:00.000Z",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
} ]
```

if you set the origin for the granularity to `2012-01-01T00:30:00Z`,

``` javascript
   "granularity":{"type": "duration", "duration": "86400000", "origin":"2012-01-01T00:30:00Z"}
```

you will get

``` json
[ {
  "version" : "v1",
  "timestamp" : "2013-08-31T00:30:00.000Z",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
}, {
  "version" : "v1",
  "timestamp" : "2013-09-01T00:30:00.000Z",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
}, {
  "version" : "v1",
  "timestamp" : "2013-09-02T00:30:00.000Z",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
}, {
  "version" : "v1",
  "timestamp" : "2013-09-03T00:30:00.000Z",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
} ]
```

Note that the timestamp for each bucket starts at the 30th minute.

### Period Granularities

Period granularities are specified as arbitrary period combinations of years, months, weeks, hours, minutes and seconds (e.g. P2W, P3M, PT1H30M, PT0.750S) in [ISO8601](https://en.wikipedia.org/wiki/ISO_8601) format. They support specifying a time zone which determines where period boundaries start as well as the timezone of the returned timestamps. By default, years start on the first of January, months start on the first of the month and weeks start on Mondays unless an origin is specified.

Time zone is optional (defaults to UTC). Origin is optional (defaults to 1970-01-01T00:00:00 in the given time zone).

```javascript
{"type": "period", "period": "P2D", "timeZone": "America/Los_Angeles"}
```

This will bucket by two-day chunks in the Pacific timezone.

```javascript
{"type": "period", "period": "P3M", "timeZone": "America/Los_Angeles", "origin": "2012-02-01T00:00:00-08:00"}
```

This will bucket by 3-month chunks in the Pacific timezone where the three-month quarters are defined as starting from February.

#### Example

Reusing the data in the previous example, if you submit a groupBy query with 1 day period in Pacific timezone,

``` json
{
   "queryType":"groupBy",
   "dataSource":"my_dataSource",
   "granularity":{"type": "period", "period": "P1D", "timeZone": "America/Los_Angeles"},
   "dimensions":[
      "language"
   ],
   "aggregations":[
      {
         "type":"count",
         "name":"count"
      }
   ],
   "intervals":[
      "1999-12-31T16:00:00.000-08:00/2999-12-31T16:00:00.000-08:00"
   ]
}
```

you will get

``` json
[ {
  "version" : "v1",
  "timestamp" : "2013-08-30T00:00:00.000-07:00",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
}, {
  "version" : "v1",
  "timestamp" : "2013-08-31T00:00:00.000-07:00",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
}, {
  "version" : "v1",
  "timestamp" : "2013-09-02T00:00:00.000-07:00",
  "event" : {
    "count" : 2,
    "language" : "en"
  }
} ]
```

Note that the timestamp for each bucket has been converted to Pacific time. Row `{"timestamp": "2013-09-02T23:32:45Z", "page": "CCC", "language" : "en"}` and
`{"timestamp": "2013-09-03T03:32:45Z", "page": "DDD", "language" : "en"}` are put in the same bucket because they are in the same day in Pacific time.

Also note that the `intervals` in groupBy query will not be converted to the timezone specified, the timezone specified in granularity is only applied on the
query results.

If you set the origin for the granularity to `1970-01-01T20:30:00-08:00`,

``` javascript
   "granularity":{"type": "period", "period": "P1D", "timeZone": "America/Los_Angeles", "origin": "1970-01-01T20:30:00-08:00"}
```

you will get

``` json
[ {
  "version" : "v1",
  "timestamp" : "2013-08-29T20:30:00.000-07:00",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
}, {
  "version" : "v1",
  "timestamp" : "2013-08-30T20:30:00.000-07:00",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
}, {
  "version" : "v1",
  "timestamp" : "2013-09-01T20:30:00.000-07:00",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
}, {
  "version" : "v1",
  "timestamp" : "2013-09-02T20:30:00.000-07:00",
  "event" : {
    "count" : 1,
    "language" : "en"
  }
} ]
```

Note that the `origin` you specified has nothing to do with the timezone, it only serves as a starting point for locating the very first granularity bucket.
In this case, Row `{"timestamp": "2013-09-02T23:32:45Z", "page": "CCC", "language" : "en"}` and `{"timestamp": "2013-09-03T03:32:45Z", "page": "DDD", "language" : "en"}`
are not in the same bucket.

#### Supported Time Zones
Timezone support is provided by the [Joda Time library](http://www.joda.org), which uses the standard IANA time zones. See the [Joda Time supported timezones](http://joda-time.sourceforge.net/timezones.html).
