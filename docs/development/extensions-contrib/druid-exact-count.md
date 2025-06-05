---
id: druid-exact-count
title: "Exact Count"
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

This extension provides exact cardinality counting functionality for LONG type columns using [Roaring Bitmaps](https://roaringbitmap.org/). Unlike approximate cardinality aggregators like HyperLogLog, this aggregator provides precise distinct counts.

## Installation

To use this Apache Druid extension, [include](../../configuration/extensions.md#loading-extensions) `druid-exact-count` in the extensions load list.

## How it Works

The extension uses `Roaring64NavigableMap` as its underlying data structure to efficiently store and compute exact cardinality of 64-bit integers. It provides two types of aggregators that serve different purposes:

### Build Aggregator (Bitmap64ExactCountBuild)

The BUILD aggregator is used when you want to compute cardinality directly from raw LONG values:

- Used during ingestion or when querying raw data
- Must be used on columns of type LONG, otherwise the output will be 1.

Example:

```json
{
  "type": "Bitmap64ExactCountBuild",
  "name": "unique_values",
  "fieldName": "id"
}
```

### Merge Aggregator (Bitmap64ExactCountMerge)

The MERGE aggregator is used when working with pre-computed bitmaps:

- Used for querying pre-aggregated data (columns that were previously aggregated using BUILD)
- Combines multiple bitmaps using bitwise operations
- Must be used on columns that are aggregated using BUILD
- `Bitmap64ExactCountMerge` aggregator is recommended for use in `timeseries` type queries, though it also works for `topN` and `groupBy` queries.

Example:

```json
{
  "type": "Bitmap64ExactCountMerge",
  "name": "total_unique_values",
  "fieldName": "unique_values" // Must be a pre-computed bitmap
}
```

### Typical Workflow

1. During ingestion, use BUILD to create the initial bitmap:
    ```json
    {
      "type": "index",
      "spec": {
        "dataSchema": {
          "metricsSpec": [
            {
              "type": "Bitmap64ExactCountBuild",
              "name": "unique_users",
              "fieldName": "user_id"
            }
          ]
        }
      }
    }
    ```

2. When querying the aggregated data, use MERGE to combine bitmaps:
    ```json
    {
      "queryType": "timeseries",
      "aggregations": [
        {
          "type": "Bitmap64ExactCountMerge",
          "name": "total_unique_users",
          "fieldName": "unique_users"
        }
      ]
    }
    ```

## Usage

### SQL Query

You can use the `BITMAP64_EXACT_COUNT` function in SQL queries:

```sql
SELECT BITMAP64_EXACT_COUNT(column_name)
FROM datasource
WHERE ...
GROUP BY ...
```

### Post-Aggregator

You can also use the post-aggregator for further processing:

```json
{
  "type": "bitmap64ExactCount",
  "name": "<output_name>",
  "fieldName": "<aggregator_name>"
}
```

## Considerations

- **Memory Usage**: While Roaring Bitmaps are efficient, storing exact unique values will generally consume more memory than approximate algorithms like HyperLogLog.
- **Input Type**: This aggregator only works with LONG (64-bit integer) columns. String or other data types must be converted to longs before using this aggregator.
- **Build vs Merge**: Always use BUILD for raw data and MERGE for pre-aggregated data. Using BUILD on pre-aggregated data or MERGE on raw data will not work correctly.

## Example Use Cases

1. **User Analytics**: Count unique users over time

```sql
-- First ingest with BUILD aggregator
-- Then query with:
SELECT 
  TIME_FLOOR(__time, 'PT1H') AS hour,
  BITMAP64_EXACT_COUNT(unique_users) as distinct_users
FROM user_metrics
GROUP BY 1
```

2. **High-Precision Metrics**: When exact counts are required

```json
{
  "type": "groupBy",
  "dimensions": [
    "country"
  ],
  "aggregations": [
    {
      "type": "Bitmap64ExactCountMerge",
      "name": "exact_user_count",
      "fieldName": "unique_users"
    }
  ]
}
```

## Walkthrough Using Wikipedia datasource

### Batch Ingestion Task Spec

```json
{
  "type": "index",
  "spec": {
    "dataSchema": {
      "dataSource": "wikipedia_metrics",
      "timestampSpec": {
        "column": "__time",
        "format": "auto"
      },
      "dimensionsSpec": {
        "dimensions": [
          "channel",
          "namespace",
          "page",
          "user",
          "cityName",
          "countryName",
          "regionName",
          "isRobot",
          "isUnpatrolled",
          "isNew",
          "isAnonymous"
        ]
      },
      "metricsSpec": [
        {
          "type": "Bitmap64ExactCountBuild",
          "name": "unique_added_values",
          "fieldName": "added"
        },
        {
          "type": "Bitmap64ExactCountBuild",
          "name": "unique_delta_values",
          "fieldName": "delta"
        },
        {
          "type": "Bitmap64ExactCountBuild",
          "name": "unique_comment_lengths",
          "fieldName": "commentLength"
        },
        {
          "name": "count",
          "type": "count"
        },
        {
          "name": "sum_added",
          "type": "longSum",
          "fieldName": "added"
        },
        {
          "name": "sum_delta",
          "type": "longSum",
          "fieldName": "delta"
        }
      ],
      "granularitySpec": {
        "type": "uniform",
        "segmentGranularity": "DAY",
        "queryGranularity": "HOUR",
        "rollup": true,
        "intervals": [
          "2016-06-27/2016-06-28"
        ]
      }
    },
    "ioConfig": {
      "type": "index",
      "inputSource": {
        "type": "druid",
        "dataSource": "wikipedia",
        "interval": "2016-06-27/2016-06-28"
      },
      "inputFormat": {
        "type": "tsv",
        "findColumnsFromHeader": true
      }
    },
    "tuningConfig": {
      "type": "index",
      "maxRowsPerSegment": 5000000,
      "maxRowsInMemory": 25000
    }
  }
}
```

### Query from datasource with raw bytes

```json
{
  "queryType": "timeseries",
  "dataSource": {
    "type": "table",
    "name": "wikipedia_metrics"
  },
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
      "type": "Bitmap64ExactCountBuild",
      "name": "a0",
      "fieldName": "unique_added_values"
    }
  ]
}
```

### Query from datasource with pre-aggregated bitmap

```json
{
  "queryType": "timeseries",
  "dataSource": {
    "type": "table",
    "name": "wikipedia_metrics"
  },
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
      "type": "Bitmap64ExactCountMerge",
      "name": "a0",
      "fieldName": "unique_added_values"
    }
  ]
}
```

## Other Examples

### Kafka ingestion task spec

```json
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

### Query with Post-aggregator:

```json
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
          "type": "bitmap64ExactCount",
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
