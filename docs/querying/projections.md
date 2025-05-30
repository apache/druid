---
id: projections
title: Query projections
sidebar_label: Projections
description: .
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

Projections are a type of aggregation that is computed and stored as part of a segment. The pre-aggregated data can speed up queries by reducing the number of rows that need to be processed for any query shape that matches a projection. 

## Create a projection

A projection has three components:

- Virtual columns (`spec.projections.virtualColumns`) that are used to compute a projection. The source data for the virtual columns must exist in your datasource.
- Grouping columns (`spec.projections.groupingColumns`) that are used to group a projection. They must either already exist in your datasource or be defined in `virtualColumns`. The order in which you define your grouping columns equates to the order in which data is sorted in teh projection, always ascending.
- Aggregators (`spec.projections.aggregators`) that define the columns you want to create projections for and which aggregator to use for that column. They must either already exist in your datasource or be defined in `virtualColumns`.

The aggregators are what Druid attempts to match when you run a query. If an aggregator in a query matches an aggregator you defined in your projection, Druid uses it.

You can either create a projection at ingestion time or after the datasource is created. 

Note that any projection dimension you create becomes part of your datasource. To remove a projection from your datasource, you need to reingest the data. Alternatively, you can use a query context parameter to not use projections for a specific query.



### At ingestion time

To create a projection at ingestion time, use the [`projectionsSpec` block in your ingestion spec](../ingestion/ingestion-spec.md#projections).

### After ingestion

:::info

To create a projection for an existing datasource, you must have the `druid-catalog` extension loaded.

:::

 `POST /druid/coordinator/v1/catalog/schemas/druid/tables/{datasource}` where the payload includes a `properties.projections` block like the following:

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

Druid automatically uses a projection if your query matches a projection you've defined. There are some query context parameters that give you some control on how projections are used and Druid's behavior:

- `useProjection`: The name of a projection you defined. The query engine must use that projection and will fail the query if the projection does not match the query.
- `forceProjections` `true` or `false`. The query engine must use a projection and will fail the query if there isn't a matching projection.
-  `noProjections`:  `true` or `false`. The query engine won't use any projections.

## Compaction

To use compaction on a datasource that includes projections, you need to set the type to catalog: `spec.type: catalog`:

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