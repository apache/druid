---
layout: doc_page
---

# InfluxDB Line Protocol Parser

To use this extension, make sure to [include](../../operations/including-extensions.html) `druid-influx-extensions`.

This extension enables Druid to parse the [InfluxDB Line Protocol](https://docs.influxdata.com/influxdb/v1.5/write_protocols/line_protocol_tutorial/), a popular text-based timeseries metric serialization format. 

## Line Protocol

A typical line looks like this:

```cpu,application=dbhost=prdb123,region=us-east-1 usage_idle=99.24,usage_user=0.55 1520722030000000000```

which contains four parts:
  - measurement: A string indicating the name of the measurement represented (e.g. cpu, network, web_requests)
  - tags: zero or more key-value pairs (i.e. dimensions)
  - measurements: one or more key-value pairs; values can be numeric, boolean, or string
  - timestamp: nanoseconds since Unix epoch (the parser truncates it to milliseconds)

The parser extracts these fields into a map, giving the measurement the key `measurement` and the timestamp the key `_ts`. The tag and measurement keys are copied verbatim, so users should take care to avoid name collisions. It is up to the ingestion spec to decide which fields should be treated as dimensions and which should be treated as metrics (typically tags correspond to dimensions and measurements correspond to metrics).

The parser is configured like so:
```json
"parser": {
      "type": "string",
      "parseSpec": {
        "format": "influx",
        "timestampSpec": {
          "column": "__ts",
          "format": "millis"
        },
        "dimensionsSpec": {
          "dimensionExclusions": [
            "__ts"
          ]
        },
        "whitelistMeasurements": [
          "cpu"
        ]
      }
```

The `whitelistMeasurements` field is an optional list of strings. If present, measurements that do not match one of the strings in the list will be ignored.
