---
layout: doc_page
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

# Lookback Queries

## Overview
**Lookback Query** is an extension which provides support for computing a metric from multiple different time periods. 

This new query type, processed by the brokers, recursively processes component queries, joins results based on time and 
dimensions, and uses standard post-aggregators to combine values from component queries.

#### High level algorithm example

Consider the following example data:

**Underlying data (date,dimension):**

| Jan,A | Jan,B | Feb,A | Feb,B | Feb,C | Mar,B | Mar,C |
|:-------:|:-------:|:-------:|:-------:|:-------:|:-------:|:-------:|
|   7   |   9   |    1  |   5   |   6   |   3   |   5   |

**Cohort query results (date,dimension):**

| Jan,A | Jan,B | Feb,A | Feb,B | Feb,C |
|:-------:|:-------:|:-------:|:-------:|:-------:|
|   7   |   9   |    1  |   5   |   6   |

**Measurement results (date,dimension):**

| Feb,A | Feb,B | Feb,C | Mar,B | Mar,C |
|:-------:|:-------:|:-------:|:-------:|:-------:|
|   1   |   5   |    6  |   3   |   5   |

**Query results (date,dimension):**

| Feb,A | Feb,B | Feb,C | Mar,B | Mar,C |
|:-------:|:-------:|:-------:|:-------:|:-------:|
|  7,1  |  9,5  |null,6 |  5,3  |  6,5  |

#### Main enhancements provided by this extension:
1. Functionality: Extending druid query functionality and reducing the need for other programs to do this.
2. Performance: Improving performance of not sending a lot of data back to the requestor.
3. Generic approach: Can be used by multiple data types and types of lookback queries in many different environments.

## Operations
To use this extension, make sure to [load](../../operations/including-extensions.html) `druid-lookback-query` only to 
the Broker (usually in `conf/broker/runtime.properties`).  **Do not load this in services other than in the broker, as it
will prevent the services from running.**

## Configuration
There are currently no configuration properties specific to Lookback Query.

## Limitations and drawbacks
* Overlapping periods between the primary and the lookback (cohort) periods result in Druid doing extra work since
each subquery is run independently and in parallel (the overlapping periods will be computed multiple times).

##Query spec:
* Most properties in the query spec derived from  [groupBy query](../../querying/groupbyquery.html) / [timeseries](../../querying/timeseriesquery.html), see documentation for these query types.

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "lookback"; this is the first thing Druid looks at to figure out how to interpret the query.|yes|
|dataSource|A String or Object defining the data source to query, very similar to a table in a relational database. See [DataSource](../../querying/datasource.html) for more information.|yes|
|postAggregations|Supports only aggregations as input; See [Post Aggregations](../../querying/post-aggregations.html)|no|
|context|An additional JSON Object which can be used to specify certain flags.|no|
|lookbackOffsets|A list of Periods to limit the lookback window.  For example: `P-1D`.|yes|
|lookbackPrefixes|A list of strings that prefixes the results, e.g. for a lookback of 1-D (`lookback_P-1D_`).|yes|
|limitSpec|See [LimitSpec](../../querying/limitspec.html)|no|
|having|See [Having](../../querying/having.html)|no|

## Examples

All examples are based on the Wikipedia dataset provided in the Druid [tutorials](../../tutorials/index.html).

### Basic example

Calculating an hour-by-hour trend of the percentage of lines deleted from wikipedia.

Query syntax:

```
{
  "queryType": "lookback",
  "dataSource": {
    "type": "query",
    "query": {
      "queryType": "timeseries",
      "dataSource": {
        "type": "table",
        "name": "wikipedia"
      },
      "context": {
        "queryId": "4b96a5a8-3760-4bbb-932c-5c61094d2922_1"
      },
      "granularity": {
        "type": "period",
        "period": "PT1H",
        "timeZone": "UTC"
      },
      "intervals": [
        "2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.001Z"
      ],
      "aggregations": [
        {
          "name": "a0",
          "fieldName": "deleted",
          "type": "longSum"
        }
      ],
      "postAggregations": []
    }
  },
  "context": {
    "timeout": 6000000,
    "queryId": "4b96a5a8-3760-4bbb-932c-5c61094d2922_1",
    "priority": 1
  },
  "lookbackPrefixes": [
    "lookback_PT1H_"
  ],
  "lookbackOffsets": [
    "PT1H"
  ],
  "postAggregations": [
    {
      "type": "arithmetic",
      "name": "linesDeletedHoH",
      "fn": "/",
      "fields": [
        {
          "type": "arithmetic",
          "name": "linesDeletedHoH",
          "fn": "-",
          "fields": [
            {
              "type": "fieldAccess",
              "fieldName": "a0"
            },
            {
              "type": "fieldAccess",
              "fieldName": "lookback_PT1H_a0"
            }
          ]
        },
        {
          "type": "fieldAccess",
          "fieldName": "lookback_PT1H_a0"
        },
        {
          "type": "constant",
          "name": "PCT_MULTIPLIER",
          "value": 0.01
        }
      ]
    }
  ]
}
```

Result:

```
[ {
  "timestamp" : "2015-09-12T00:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 16208,
    "linesDeletedHoH" : -89.13499506416585,
    "a0" : 1761
  }
}, {
  "timestamp" : "2015-09-12T01:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 14543,
    "linesDeletedHoH" : 11.448806986178917,
    "a0" : 16208
  }
}, {
  "timestamp" : "2015-09-12T02:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 13101,
    "linesDeletedHoH" : 11.006793374551561,
    "a0" : 14543
  }
}, {
  "timestamp" : "2015-09-12T03:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 12040,
    "linesDeletedHoH" : 8.812292358803987,
    "a0" : 13101
  }
}, {
  "timestamp" : "2015-09-12T04:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 6399,
    "linesDeletedHoH" : 88.15439912486326,
    "a0" : 12040
  }
}, {
  "timestamp" : "2015-09-12T05:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 9036,
    "linesDeletedHoH" : -29.183266932270918,
    "a0" : 6399
  }
}, {
  "timestamp" : "2015-09-12T06:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 11409,
    "linesDeletedHoH" : -20.799368919274258,
    "a0" : 9036
  }
}, {
  "timestamp" : "2015-09-12T07:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 11616,
    "linesDeletedHoH" : -1.7820247933884297,
    "a0" : 11409
  }
}, {
  "timestamp" : "2015-09-12T08:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 17509,
    "linesDeletedHoH" : -33.65697641213091,
    "a0" : 11616
  }
}, {
  "timestamp" : "2015-09-12T09:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 19406,
    "linesDeletedHoH" : -9.775327218386066,
    "a0" : 17509
  }
}, {
  "timestamp" : "2015-09-12T10:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 16284,
    "linesDeletedHoH" : 19.17219356423483,
    "a0" : 19406
  }
}, {
  "timestamp" : "2015-09-12T11:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 18672,
    "linesDeletedHoH" : -12.789203084832904,
    "a0" : 16284
  }
}, {
  "timestamp" : "2015-09-12T12:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 30520,
    "linesDeletedHoH" : -38.82044560943643,
    "a0" : 18672
  }
}, {
  "timestamp" : "2015-09-12T13:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 18025,
    "linesDeletedHoH" : 69.32038834951456,
    "a0" : 30520
  }
}, {
  "timestamp" : "2015-09-12T14:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 26399,
    "linesDeletedHoH" : -31.72089851888329,
    "a0" : 18025
  }
}, {
  "timestamp" : "2015-09-12T15:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 24759,
    "linesDeletedHoH" : 6.6238539520982265,
    "a0" : 26399
  }
}, {
  "timestamp" : "2015-09-12T16:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 19634,
    "linesDeletedHoH" : 26.102679026179075,
    "a0" : 24759
  }
}, {
  "timestamp" : "2015-09-12T17:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 17345,
    "linesDeletedHoH" : 13.196886710867686,
    "a0" : 19634
  }
}, {
  "timestamp" : "2015-09-12T18:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 19305,
    "linesDeletedHoH" : -10.152810152810153,
    "a0" : 17345
  }
}, {
  "timestamp" : "2015-09-12T19:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 22265,
    "linesDeletedHoH" : -13.294408264091622,
    "a0" : 19305
  }
}, {
  "timestamp" : "2015-09-12T20:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 16394,
    "linesDeletedHoH" : 35.81188239599853,
    "a0" : 22265
  }
}, {
  "timestamp" : "2015-09-12T21:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 16379,
    "linesDeletedHoH" : 0.09158068258135417,
    "a0" : 16394
  }
}, {
  "timestamp" : "2015-09-12T22:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : 15289,
    "linesDeletedHoH" : 7.129308653280137,
    "a0" : 16379
  }
}, {
  "timestamp" : "2015-09-12T23:00:00.000Z",
  "result" : {
    "lookback_PT1H_a0" : null,
    "linesDeletedHoH" : null,
    "a0" : 15289
  }
} ]
```