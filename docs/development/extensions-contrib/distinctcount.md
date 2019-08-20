---
id: distinctcount
title: "DistinctCount Aggregator"
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


To use this Apache Druid (incubating) extension, make sure to [include](../../development/extensions.md#loading-extensions) the `druid-distinctcount` extension.

Additionally, follow these steps:

1. First, use a single dimension hash-based partition spec to partition data by a single dimension. For example visitor_id. This to make sure all rows with a particular value for that dimension will go into the same segment, or this might over count.
2. Second, use distinctCount to calculate the distinct count, make sure queryGranularity is divided exactly by segmentGranularity or else the result will be wrong.

There are some limitations, when used with groupBy, the groupBy keys' numbers should not exceed maxIntermediateRows in every segment. If exceeded the result will be wrong. When used with topN, numValuesPerPass should not be too big. If too big the distinctCount will use a lot of memory and might cause the JVM to go our of memory.

Example:

## Timeseries query

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

## TopN query

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

## GroupBy query

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
