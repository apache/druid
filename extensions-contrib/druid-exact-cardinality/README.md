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

This module provides exact distinct count for long type column.

Usage for `Bitmap64ExactCountBuild` and `Bitmap64ExactCountMerge`:
Kafka ingestion task spec:
```
{
  "type": "kafka",
  "spec": {
    "dataSchema": {
      "dataSource": "ticker_event_bitmap64_exact_count_rollup",
      "timestampSpec": {
        "column": "timestamp",
        "format": "millis",
        "missingValue": null
      },
      "dimensionsSpec": {
        "dimensions": [
          {
            "type": "string",
            "name": "key"
          }
        ],
        "dimensionExclusions": []
      },
      "metricsSpec": [
        {
          "type": "Bitmap64ExactCountBuild",
          "name": "count",
          "fieldName": "value"
        }
      ],
      "granularitySpec": {
        "type": "uniform",
        "segmentGranularity": "HOUR",
        "queryGranularity": "HOUR",
        "rollup": true,
        "intervals": null
      },
      "transformSpec": {
        "filter": null,
        "transforms": []
      }
    },
    "ioConfig": {
      "topic": "ticker_event",
      "inputFormat": {
        "type": "json",
        "flattenSpec": {
          "useFieldDiscovery": true,
          "fields": []
        },
        "featureSpec": {}
      },
      "replicas": 1,
      "taskCount": 1,
      "taskDuration": "PT3600S",
      "consumerProperties": {
        "bootstrap.servers": "localhost:9092"
      },
      "pollTimeout": 100,
      "startDelay": "PT5S",
      "period": "PT30S",
      "useEarliestOffset": false,
      "completionTimeout": "PT1800S",
      "lateMessageRejectionPeriod": null,
      "earlyMessageRejectionPeriod": null,
      "lateMessageRejectionStartDateTime": null,
      "stream": "ticker_event",
      "useEarliestSequenceNumber": false,
      "type": "kafka"
    }
  }
}
```
Query from rollup datasource:
```
{
  "queryType": "timeseries",
  "dataSource": {
    "type": "table",
    "name": "ticker_event_bitmap64_exact_count_rollup"
  },
  "intervals": {
    "type": "intervals",
    "intervals": [
      "2020-09-13T06:35:35.000Z/146140482-04-24T15:36:27.903Z"
    ]
  },
  "descending": false,
  "virtualColumns": [],
  "filter": null,
  "granularity": {
    "type": "all"
  },
  "aggregations": [
    {
      "type": "Bitmap64ExactCountMerge",
      "name": "a0",
      "fieldName": "count"
    }
  ],
  "postAggregations": [],
  "limit": 2147483647
}
```
query with post aggregator:
```
{
  "queryType": "timeseries",
  "dataSource": {
    "type": "table",
    "name": "ticker_event_bitmap64_exact_count_rollup"
  },
  "intervals": {
    "type": "intervals",
    "intervals": [
      "2020-09-13T06:35:35.000Z/146140482-04-24T15:36:27.903Z"
    ]
  },
  "descending": false,
  "virtualColumns": [],
  "filter": null,
  "granularity": {
    "type": "all"
  },
  "aggregations": [
    {
      "type": "count",
      "name": "cnt"
    },
    {
      "type": "Bitmap64ExactCountMerge",
      "name": "a0",
      "fieldName": "count"
    }
  ],
  "postAggregations": [
    {
      "type": "arithmetic",
      "fn": "/",
      "fields": [
        {
          "type": "bitmap64ExactCountCardinality",
          "name": "a0",
          "fieldName": "a0"
        },
        {
          "type": "fieldAccess",
          "name": "cnt",
          "fieldName": "cnt"
        }
      ],
      "name": "rollup_rate"
    }
  ],
  "limit": 2147483647
}
```
sql query:
```
SELECT "key", BITMAP64_EXACT_COUNT("count")
FROM "ticker_event_bitmap64_exact_count_rollup"
WHERE "__time" >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
GROUP BY key
```

Note: this `longExactCount` aggregator is recommended to use in `timeseries` type query though it also works for `topN` 
and `groupBy` query. eg:
```
{
  "queryType": "timeseries",
  "dataSource": "ticker_event",
  "intervals": {
    "type": "intervals",
    "intervals": [
      "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
    ]
  },
  "descending": false,
  "virtualColumns": [],
  "filter": null,
  "granularity": {
    "type": "all"
  },
  "aggregations": [
    {
      "type": "longExactCount",
      "name": "exactCount",
      "fieldName": "value",
      "expression": null
    }
  ]
}
```
```
{
  "queryType": "groupBy",
  "dataSource": "ticker_event",
  "dimensions": ["key"],
  "intervals": {
    "type": "intervals",
    "intervals": [
      "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
    ]
  },
  "granularity": {
    "type": "all"
  },
  "aggregations": [
    {
      "type": "longExactCount",
      "name": "a0",
      "fieldName": "value"
    }
  ]
}
```


