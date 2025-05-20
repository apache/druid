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

This module provides exact cardinality count for long type column.

## How it Works

This extension calculates the exact cardinality (distinct count) of values in a column of `LONG` data type. It uses [Roaring Bitmaps](https://roaringbitmap.org/), specifically the `Roaring64NavigableMap` variant, as its underlying data structure.

Here's a brief overview of its operation:

1.  **Data Ingestion (`Bitmap64ExactCardinalityBuild`)**:
    *   When new data is ingested, and this aggregator is applied to a `LONG` column (specified by `fieldName`), each unique long value from the input column is added to a `Roaring64NavigableMap`.
    *   This bitmap efficiently stores the set of unique 64-bit integers encountered.

2.  **Aggregation and Rollup (`Bitmap64ExactCardinalityMerge`)**:
    *   During rollup or when combining results from different segments/queries, the aggregator merges multiple `Roaring64NavigableMap` instances.
    *   This merge operation is a bitwise `OR` on the bitmaps, which correctly computes the union of the distinct values from each source.
    *   The intermediate object stored and transferred is the serialized form of the `Roaring64NavigableMap`.

3.  **Query Time**:
    *   At query time, the `Bitmap64ExactCardinalityMerge` aggregator is typically used to combine the bitmaps from the selected segments.
    *   The final computation involves calling `getLongCardinality()` on the resulting `Roaring64NavigableMap`, which returns the exact count of distinct long values.
    *   A `Bitmap64ExactCardinalityPostAggregator` is also available for post-processing, allowing the cardinality result to be used in further calculations.

**Key Characteristics**:

*   **Exactness**: Provides precise distinct counts, unlike approximation algorithms (e.g., HyperLogLog).
*   **Input Type**: Designed for `LONG` (64-bit integer) columns. If your data is in string format, it must be converted to longs first (e.g., via dictionary encoding or hashing) to use this aggregator.
*   **Data Structure**: Leverages Roaring Bitmaps for memory efficiency and fast set operations (add, union, cardinality check) compared to traditional `HashSet` approaches, especially for certain data distributions.
*   **Trade-offs**:
    *   **Memory**: While Roaring Bitmaps are efficient, storing exact unique values will generally consume more memory than sketch-based approximate algorithms, especially for very high cardinality dimensions with uniformly distributed values.
    *   **Performance**: The performance can be very good, but for extremely high cardinalities, the cost of merging large bitmaps might be higher than that of merging smaller sketches.

This aggregator is suitable for use cases where the precision of the distinct count is paramount and the input data is or can be represented as long values.

## Usage Examples
Kafka ingestion task spec:
```
{
  "type": "kafka",
  "spec": {
    "dataSchema": {
      "dataSource": "ticker_event_bitmap64_exact_cardinality_rollup",
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
          "type": "Bitmap64ExactCardinalityBuild",
          "name": "cardinality",
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
    "name": "ticker_event_bitmap64_exact_cardinality_rollup"
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
      "type": "Bitmap64ExactCardinalityMerge",
      "name": "a0",
      "fieldName": "cardinality"
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
    "name": "ticker_event_bitmap64_exact_cardinality_rollup"
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
      "type": "cardinality",
      "name": "cnt"
    },
    {
      "type": "Bitmap64ExactCardinalityMerge",
      "name": "a0",
      "fieldName": "cardinality"
    }
  ],
  "postAggregations": [
    {
      "type": "arithmetic",
      "fn": "/",
      "fields": [
        {
          "type": "bitmap64ExactCardinality",
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
SELECT "key", BITMAP64_EXACT_CARDINALITY("cardinality")
FROM "ticker_event_bitmap64_exact_cardinality_rollup"
WHERE "__time" >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
GROUP BY key
```

Note: this `longExactCardinality` aggregator is recommended for use in `timeseries` type queries, though it also works for `topN` 
and `groupBy` queries. e.g.:
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
      "type": "longExactCardinality",
      "name": "ExactCardinality",
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
      "type": "longExactCardinality",
      "name": "a0",
      "fieldName": "value"
    }
  ]
}
```


