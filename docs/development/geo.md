---
id: geo
title: "Spatial filters"
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

> Apache Druid supports two query languages: [Druid SQL](../querying/sql.md) and [native queries](../querying/querying.md).
> This document describes a feature that is only available in the native language.

Apache Druid supports filtering spatially indexed columns based on an origin and a bound.

This topic explains how to ingest and query spatial filters.
For information on other filters supported by Druid, see [Query filters](../querying/filters.md).

## Spatial indexing

Spatial indexing refers to ingesting data of a spatial data type, such as geometry or geography, into Druid.

Spatial dimensions are string columns that contain coordinates separated by a comma.
In the ingestion spec, you configure spatial dimensions in the `dimensionsSpec` object of the `dataSchema` component.

You can provide spatial dimensions in any of the [data formats](../ingestion/data-formats.md) supported by Druid.
The following example shows an ingestion spec with a spatial dimension named `coordinates`, which is constructed from the input fields `x` and `y`:

```json
{
  "type": "hadoop",
  "dataSchema": {
    "dataSource": "DatasourceName",
    "parser": {
      "type": "string",
      "parseSpec": {
        "format": "json",
        "timestampSpec": {
          "column": "timestamp",
          "format": "auto"
        },
        "dimensionsSpec": {
          "dimensions": [
            {
              "type": "double",
              "name": "x"
            },
            {
              "type": "double",
              "name": "y"
            }
          ],
          "spatialDimensions": [
            {
              "dimName": "coordinates",
              "dims": [
                "x",
                "y"
              ]
            }
          ]
        }
      }
    }
  }
}
```

Each spatial dimension object in the `spatialDimensions` array is defined by the following fields:

|Property|Description|Required|
|--------|-----------|--------|
|`dimName`|The name of a spatial dimension. You can construct a spatial dimension from other dimensions or it may already exist as part of an event. If a spatial dimension already exists, it must be an array of coordinate values.|yes|
|`dims`|The list of dimension names that comprise the spatial dimension.|no|

For information on how to use the ingestion spec to configure ingestion, see [Ingestion spec reference](../ingestion/ingestion-spec.md).
For general information on loading data in Druid, see [Ingestion](../ingestion/index.md).

## Spatial filters

A [filter](../querying/filters.md) is a JSON object indicating which rows of data should be included in the computation for a query.
You can filter on spatial structures, such as rectangles and polygons, using the spatial filter.

Spatial filters have the following structure:

```json
"filter": {
  "type": "spatial",
  "dimension": <name_of_spatial_dimension>,
  "bound": <bound_type>
}
```

The following example shows a spatial filter with a rectangular bound type:

```json
"filter" : {
    "type": "spatial",
    "dimension": "spatialDim",
    "bound": {
        "type": "rectangular",
        "minCoords": [10.0, 20.0],
        "maxCoords": [30.0, 40.0]
    }
```

The order of the dimension coordinates in the spatial filter must be equal to the order of the dimension coordinates in the `spatialDimensions` array.

### Bound types

The `bound` property of the spatial filter object lets you filter on ranges of dimension values. 
You can define rectangular, radius, and polygon filter bounds.

#### Rectangular

The `rectangular` bound has the following elements:

|Property|Description|Required|
|--------|-----------|--------|
|`minCoords`|The list of minimum dimension coordinates in the form [x, y]|yes|
|`maxCoords`|The list of maximum dimension coordinates in the form [x, y]|yes|

#### Radius

The `radius` bound has the following elements:

|Property|Description|Required|
|--------|-----------|--------|
|`coords`|Origin coordinates in the form [x, y]|yes|
|`radius`|The float radius value|yes|

#### Polygon

The `polygon` bound has the following elements:

|Property|Description|Required|
|--------|-----------|--------|
|`abscissa`|Horizontal coordinates for the corners of the polygon|yes|
|`ordinate`|Vertical coordinates for the corners of the polygon|yes|
