---
layout: doc_page
---

# DistinctCount aggregator

To use this extension, make sure to [include](../../operations/including-extensions.html) `druid-distinctcount` extension.

Additionally, follow these steps:

(1) First use single dimension hash-based partitioning to partition data by a dimension for example visitor_id, this to make sure all rows with a particular value for that dimension will go into the same segment or this might over count.
(2) Second use distinctCount to calculate exact distinct count, make sure queryGranularity is divide exactly by segmentGranularity or else the result will be wrong.
There is some limitations, when use with groupBy, the groupBy keys' numbers should not exceed maxIntermediateRows in every segment, if exceed the result will wrong. And when use with topN, numValuesPerPass should not too big, if too big the distinctCount will use many memory and cause the JVM out of service.

This has been used in production.

Example:
# Timeseries Query

```json
{
  "queryType": "timeseries",
  "dataSource": "sample_datasource",
  "granularity": "day",
  "aggregations": [
    {
      "type": "distinctCount",
      "name": "uv",
      "fieldName": "visitor_id"
    }
  ],
  "intervals": [
    "2016-03-01T00:00:00.000/2013-03-20T00:00:00.000"
  ]
}
```

# TopN Query

```json
{
  "queryType": "topN",
  "dataSource": "sample_datasource",
  "dimension": "sample_dim",
  "threshold": 5,
  "metric": "uv",
  "granularity": "all",
  "aggregations": [
    {
      "type": "distinctCount",
      "name": "uv",
      "fieldName": "visitor_id"
    }
  ],
  "intervals": [
    "2016-03-06T00:00:00/2016-03-06T23:59:59"
  ]
}
```

# GroupBy Query

```json
{
  "queryType": "groupBy",
  "dataSource": "sample_datasource",
  "dimensions": "[sample_dim]",
  "granularity": "all",
  "aggregations": [
    {
      "type": "distinctCount",
      "name": "uv",
      "fieldName": "visitor_id"
    }
  ],
  "intervals": [
    "2016-03-06T00:00:00/2016-03-06T23:59:59"
  ]
}
```
