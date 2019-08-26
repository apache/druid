---
id: time-min-max
title: "Timestamp Min/Max aggregators"
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


To use this Apache Druid (incubating) extension, make sure to [include](../../development/extensions.md#loading-extensions) `druid-time-min-max`.

These aggregators enable more precise calculation of min and max time of given events than `__time` column whose granularity is sparse, the same as query granularity.
To use this feature, a "timeMin" or "timeMax" aggregator must be included at indexing time.
They can apply to any columns that can be converted to timestamp, which include Long, DateTime, Timestamp, and String types.

For example, when a data set consists of timestamp, dimension, and metric value like followings.

```
2015-07-28T01:00:00.000Z  A  1
2015-07-28T02:00:00.000Z  A  1
2015-07-28T03:00:00.000Z  A  1
2015-07-28T04:00:00.000Z  B  1
2015-07-28T05:00:00.000Z  A  1
2015-07-28T06:00:00.000Z  B  1
2015-07-29T01:00:00.000Z  C  1
2015-07-29T02:00:00.000Z  C  1
2015-07-29T03:00:00.000Z  A  1
2015-07-29T04:00:00.000Z  A  1
```

At ingestion time, timeMin and timeMax aggregator can be included as other aggregators.

```json
{
    "type": "timeMin",
    "name": "tmin",
    "fieldName": "<field_name, typically column specified in timestamp spec>"
}
```

```json
{
    "type": "timeMax",
    "name": "tmax",
    "fieldName": "<field_name, typically column specified in timestamp spec>"
}
```

`name` is output name of aggregator and can be any string. `fieldName` is typically column specified in timestamp spec but can be any column that can be converted to timestamp.

To query for results, the same aggregators "timeMin" and "timeMax" is used.

```json
{
  "queryType": "groupBy",
  "dataSource": "timeMinMax",
  "granularity": "DAY",
  "dimensions": ["product"],
  "aggregations": [
    {
      "type": "count",
      "name": "count"
    },
    {
      "type": "timeMin",
      "name": "<output_name of timeMin>",
      "fieldName": "tmin"
    },
    {
      "type": "timeMax",
      "name": "<output_name of timeMax>",
      "fieldName": "tmax"
    }
  ],
  "intervals": [
    "2010-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z"
  ]
}
```

Then, result has min and max of timestamp, which is finer than query granularity.

```
2015-07-28T00:00:00.000Z A 4 2015-07-28T01:00:00.000Z 2015-07-28T05:00:00.000Z
2015-07-28T00:00:00.000Z B 2 2015-07-28T04:00:00.000Z 2015-07-28T06:00:00.000Z
2015-07-29T00:00:00.000Z A 2 2015-07-29T03:00:00.000Z 2015-07-29T04:00:00.000Z
2015-07-29T00:00:00.000Z C 2 2015-07-29T01:00:00.000Z 2015-07-29T02:00:00.000Z
```
