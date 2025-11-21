---
id: projections
title: Query projections
sidebar_label: Projections
description: Speed up your queries by defining projections that pre-aggreate data for you.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

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

  :::info[Experimental]

Projections are experimental. We don't recommend them for production use.

  :::

Projections are a type of aggregation that is computed and stored as part of your datasource in a segment. When using rollups to pre-aggregate rows are based on a specific granularity, the source rows are no longer available. Projections, on the other hand, don't affect the source dimensions. They remain part of your datasource and are queryable.

The pre-aggregated data can speed up queries by reducing the number of rows that need to be processed for any shape that matches a projection. Thus, we recommend you build projections for commonly used queries. For example, you define the following projection in your datasource:


<details>
<summary>Show the projection</summary>

```json

     "projections": [
        {
          "type": "aggregate",
          "name": "channel_page_hourly_distinct_user_added_deleted",
          "groupingColumns": [
            {
              "type": "long",
              "name": "__gran"
            },
            {
              "type": "string",
              "name": "channel"
            },
            {
              "type": "string",
              "name": "page"
            }
          ],
          "virtualColumns": [
            {
              "type": "expression",
              "expression": "timestamp_floor(__time, 'PT1H')",
              "name": "__gran",
              "outputType": "LONG"
            }
          ],
          "aggregators": [
            {
              "type": "HLLSketchBuild",
              "name": "distinct_users",
              "fieldName": "user"
            },
            {
              "type": "longSum",
              "name": "sum_added",
              "fieldName": "added"
            },
            {
              "type": "longSum",
              "name": "sum_deleted",
              "fieldName": "deleted"
            }
          ]
        }
     ]
```

</details>

A query targeting the aggregated dimensions grouped in the same way uses the projection, for example:

<details>
<summary>Show the query</summary>

```sql
SELECT
  TIME_FLOOR(__time, 'PT1H') AS __gran,
  channel,
  page,
  APPROX_COUNT_DISTINCT_DS(user) AS distinct_users,
  SUM(added) AS sum_added,
  SUM(deleted) AS sum_deleted
FROM your_datasource
GROUP BY
  TIME_FLOOR(__time, 'PT1H'),
  channel,
  page
```

</details>

## Create a projection

You can either create a projection as part of your ingestion or manually add them to an existing datasource. 

You can create a projection:

- in the ingestion spec or query for your datasource
- in the catalog for an existing datasource
- in the compaction spec for an existing datasource

In addition to the columns in your datasource, a projection has three components:

- Virtual columns (`spec.projections.virtualColumns`) composed of multiple existing columns from your datasource. A projection can reference an existing column in your datasource or the virtual columns defined in this block.
- Grouping columns (`spec.projections.groupingColumns`) to sort the projection. They must either already exist in your datasource or be defined in `virtualColumns` of your ingestion spec. The order in which you define your grouping columns dictates the sort order for in the projection. Sort order is always ascending.
- Aggregators (`spec.projections.aggregators`) that define the columns you want to create projections for and the aggregator to use for that column. The columns must either already exist in your datasource or be defined in `virtualColumns`.

Note that any projection dimension you create becomes part of your datasource. You need to reingest the data to remove a projection from your datasource. Alternatively, you can use a query context parameter to avoid using projections for a specific query.

### Limitations

When creating a projection, keep the following limitations in mind:

- If your projection includes source columns that are type `float`, you need to use double aggregations, like `doubleSum`,  in the projection.
- The aggregator in your projection must match the aggregator in the query for a projection to get used. For example, the output type of the `cardinality` aggregator is different at ingestion time (string) and at query time (long)
- Since the source columns for a projection are unaffected by a projection, storage requirements can increase.
- 

### As part of your ingestion

To create a projection at ingestion time, use the [`projectionsSpec` block in your ingestion spec](../ingestion/ingestion-spec.md#projections).

<details>
<summary>Show the ingestion spec</summary>

```json

```

</details>

To create projections for SQL-based ingestion, you need to also have the [`druid-catalog`](../development/extensions-core/catalog.md) extension loaded.

### Manually add a projection

You can define a projection for an existing datasource. We recommend using the [`druid-catalog`](../development/extensions-core/catalog.md) extension, but you can also define the projection in a compaction spec.

The following API call includes a payload with the `properties.projections` block that defines your projections:

<details>
<summary>View the payload</summary>

```json {11,19,39} showLineNumbers
{
  "type": "datasource",
  "columns": [],
  "properties": {
    "segmentGranularity": "PT1H",
    "projections": [
      {
        "spec": {
          "name": "channel_page_hourly_distinct_user_added_deleted",
          "type": "aggregate",
          "virtualColumns": [
            {
              "type": "expression",
              "name": "__gran",
              "expression": "timestamp_floor(__time, 'PT1H')",
              "outputType": "LONG"
            }
          ],
          "groupingColumns": [
            {
              "type": "long",
              "name": "__gran",
              "multiValueHandling": "SORTED_ARRAY",
              "createBitmapIndex": false
            },
            {
              "type": "string",
              "name": "channel",
              "multiValueHandling": "SORTED_ARRAY",
              "createBitmapIndex": true
            },
            {
              "type": "string",
              "name": "page",
              "multiValueHandling": "SORTED_ARRAY",
              "createBitmapIndex": true
            }
          ],
          "aggregators": [
            {
              "type": "HLLSketchBuild",
              "name": "distinct_users",
              "fieldName": "user",
              "lgK": 12,
              "tgtHllType": "HLL_4"
            },
            {
              "type": "longSum",
              "name": "sum_added",
              "fieldName": "added"
            },
            {
              "type": "longSum",
              "name": "sum_deleted",
              "fieldName": "deleted"
            }
          ]
        }
      }
    ]
  }
}
```

</details>

In this example, Druid aggregates data into `distinct_user`, `sum_added`, and `sum_deleted` dimensions based on the aggregator that's specified and a source dimension. These aggregations are grouped by the columns you define in `groupingColumns`.

## Use a projection 

Druid automatically uses a projection if your query matches a projection you've defined. You can use the following query context parameters to override the default behavior for projections:

- `useProjection`: The name of a projection you defined. The query engine must use this specific projection. If the projection does not match the query, the query fails.
- `forceProjections`: Set to `true` or `false`. Requires the query engine to use a projection. If no projection matches the query, the query fails. Defaults to `false`, which means that Druid uses a projection if there is one that matches your query. If there isn't, Druid processes the query as usual.
-  `noProjections`:  Set to `true` or `false`. The query engine won't use any projections.

## Compaction

To use compaction on a datasource that includes projections, you need to set the spec type to catalog: `spec.type: catalog`:

<Tabs>
  <TabItem value="Coordinator duties">

```json
{
    "type": "catalog",
    "dataSource": YOUR_DATASOURCE,
    "engine": "native",
    "skipOffsetFromLatest": "PT0H",
    "taskPriority": 25,
    "inputSegmentSizeBytes": 100000000000000,
    "taskContext": null
  }
```

</TabItem>
  <TabItem value="Supervisors"> 
  
  ```json
  {
  "type": "autocompact",
  "spec": {
    "type": "catalog",
    "dataSource": YOUR_DATASOURCE,
    "engine": "native",
    "skipOffsetFromLatest": "PT0H",
    "taskPriority": 25,
    "inputSegmentSizeBytes": 100000000000000,
    "taskContext": null
  },
  "suspended": true
}
```

  </TabItem>
</Tabs>

