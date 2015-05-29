---
layout: doc_page
---
# Geographic Queries
Druid supports filtering specially spatially indexed columns based on an origin and a bound.

# Spatial Indexing
In any of the data specs, there is the option of providing spatial dimensions. For example, for a JSON data spec, spatial dimensions can be specified as follows:

```json
"dataSpec" : {
    "format": "JSON",
    "dimensions": <some_dims>,
    "spatialDimensions": [
        {
            "dimName": "coordinates",
            "dims": ["lat", "long"]
        },
        ...
    ]
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

