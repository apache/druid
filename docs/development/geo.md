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

Apache Druid supports filtering specially spatially indexed columns based on an origin and a bound.

## Spatial indexing

You can provide spatial dimensions in any data spec.
For example, you can specify spatial dimensions for a JSON data spec as follows:

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
					"dimensions": [],
					"spatialDimensions": [{
						"dimName": "coordinates",
						"dims": ["x", "y"]
					}]
				}
			}
		}
	}
}
```

## Spatial filters

|Property|Description|Required|
|--------|-----------|--------|
|`dimName`|The name of the spatial dimension. A spatial dimension may be constructed from multiple other dimensions or it may already exist as part of an event. If a spatial dimension already exists, it must be an array of coordinate values.|yes|
|`dims`|A list of dimension names that comprise a spatial dimension.|no|

The grammar for a spatial filter is as follows:

```json
"filter" : {
    "type": "spatial",
    "dimension": "spatialDim",
    "bound": {
        "type": "rectangular",
        "minCoords": [10.0, 20.0],
        "maxCoords": [30.0, 40.0]
    }
}
```

### Bound types

#### `rectangular`

|Property|Description|Required|
|--------|-----------|--------|
|`minCoords`|List of minimum dimension coordinates for coordinates [x, y, z, …]|yes|
|`maxCoords`|List of maximum dimension coordinates for coordinates [x, y, z, …]|yes|

#### `radius`

|Property|Description|Required|
|--------|-----------|--------|
|`coords`|Origin coordinates in the form [x, y, z, …]|yes|
|`radius`|The float radius value|yes|

#### `polygon`

|Property|Description|Required|
|--------|-----------|--------|
|`abscissa`|Horizontal coordinate for corners of the polygon|yes|
|`ordinate`|Vertical coordinate for corners of the polygon|yes|
