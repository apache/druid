---
layout: doc_page
---
Note: This feature is highly experimental.

In any of the data specs, there is now the option of providing spatial dimensions. For example, for a JSON data spec, spatial dimensions can be specified as follows:

    <code>
    {
        "type": "JSON",
        "dimensions": <some_dims>,
        "spatialDimensions": [
            {
                "dimName": "coordinates",
                "dims": ["lat", "long"]
            },
            ...
        ]
    }
    </code>

|property|description|required?|
|--------|-----------|---------|
|dimName|The name of the spatial dimension. A spatial dimension may be constructed from multiple other dimensions or it may already exist as part of an event. If a spatial dimension already exists, it must be an array of dimension values.|yes|
|dims|A list of dimension names that comprise a spatial dimension.|no|

