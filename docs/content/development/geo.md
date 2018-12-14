---
layout: doc_page
title: "Geographic Queries"
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

# Geographic Queries

Druid supports filtering specially spatially indexed columns based on an origin and a bound.

# Spatial Indexing
In any of the data specs, there is the option of providing spatial dimensions. For example, for a JSON data spec, spatial dimensions can be specified as follows:

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
						"dims": ["lat", "long"]
					}]
				}
			}
		}
	}
}
```

|property|description|required?|
|--------|-----------|---------|
|dimName|The name of the spatial dimension. A spatial dimension may be constructed from multiple other dimensions or it may already exist as part of an event. If a spatial dimension already exists, it must be an array of coordinate values.|yes|
|dims|A list of dimension names that comprise a spatial dimension.|no|

# Spatial Filters
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

Bounds
------

### Rectangular

|property|description|required?|
|--------|-----------|---------|
|minCoords|List of minimum dimension coordinates for coordinates [x, y, z, …]|yes|
|maxCoords|List of maximum dimension coordinates for coordinates [x, y, z, …]|yes|

### Radius

|property|description|required?|
|--------|-----------|---------|
|coords|Origin coordinates in the form [x, y, z, …]|yes|
|radius|The float radius value|yes|
