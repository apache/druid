---
layout: doc_page
---
Note: This feature is highly experimental and only works with spatially indexed dimensions.

The grammar for a spatial filter is as follows:

    <code>
    {
        "dimension": "spatialDim",
        "bound": {
            "type": "rectangular",
            "minCoords": [10.0, 20.0],
            "maxCoords": [30.0, 40.0]
        }
    }
    </code>

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

