---
layout: doc_page
title: "AccurateCardinality Aggregator"
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

# AccurateCardinality Aggregator

In many case, we do need exactly count distinct.
Now, Druid offers the ability by nested group by query.
Its logic can be described as follows:

For a sql like:
```sql
select count(distinct pid) from DATASOURCE where col="val"
```
the exactly query will be like:
```sql
select count(*) from (
  select pid from DATASOURCE_segments_in_historical1 where col="val" group by pid
  UNION ALL
  select pid from DATASOURCE_segments_in_historical2 where col="val" group by pid
  UNION ALL
  select pid from DATASOURCE_segments_in_historical3 where col="val" group by pid
  ...
) group by pid
```

For high cardinality case, the size of result transfered from historical node to broker node can be really large and leads to poor performance.
So this extension try to use bitmap as the container for the result data from the historical.
The performance can be 10 times better the nested group by method.

To use this extension, make sure to [include](../../operations/including-extensions.html) the `accurate-cardinality` extension.

DSL query example:

```json
{
  "queryType": "timeseries",
  "dataSource": "sample_datasource",
  "granularity": "day",
  "aggregations": [
    {
      "type": "accurateCardinality",
      "name": "uv",
      "fieldName": "clientid"
    }
  ],
  "intervals": [
    "2018-12-01T00:00:00.000/2018-12-31T00:00:00.000"
  ]
}
```

SQL query example:

```json
{
  "query": "select ACCURATE_CARDINALITY(clientid) as uv from sample_datasource where __time >= '2018-12-01 00:00:00' and __time < '2018-12-31 00:00:00'",
  "context": {
        "sqlTimeZone":"Asia/Shanghai"
  }
}
```

There are some limitations:

1. `accurateCardinality` aggregator can only support dimension which is long type.
2. `ACCURATE_CARDINALITY` can only have one operand