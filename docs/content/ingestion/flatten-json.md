---
layout: doc_page
---

# JSON Flatten Spec

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| useFieldDiscovery | Boolean | If true, interpret all fields with singular values (not a map or list) and flat lists (lists of singular values) at the root level as columns. | no (default == true) |
| fields | JSON Object array | Specifies the fields of interest and how they are accessed | no (default == []) |

Defining the JSON Flatten Spec allows nested JSON fields to be flattened during ingestion time. Only the JSON ParseSpec supports flattening.

'fields' is a list of JSON Objects, describing the field names and how the fields are accessed:

## JSON Field Spec

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | Type of the field, "root" or "path". | yes |
| name | String | This string will be used as the column name when the data has been ingested.  | yes |
| expr | String | Defines an expression for accessing the field within the JSON object, using [JsonPath](https://github.com/jayway/JsonPath) notation. | yes |

Suppose the event JSON has the following form:

```json
{
 "timestamp": "2015-09-12T12:10:53.155Z",
 "dim1": "qwerty",
 "dim2": "asdf",
 "dim3": "zxcv",
 "ignore_me": "ignore this",
 "metrica": 9999,
 "foo": {"bar": "abc"},
 "foo.bar": "def",
 "nestmet": {"val": 42},
 "hello": [1.0, 2.0, 3.0, 4.0, 5.0],
 "mixarray": [1.0, 2.0, 3.0, 4.0, {"last": 5}],
 "world": [{"hey": "there"}, {"tree": "apple"}],
 "thing": {"food": ["sandwich", "pizza"]}
}
```

The column "metrica" is a Long metric column, "hello" is an array of Double metrics, and "nestmet.val" is a nested Long metric. All other columns are dimensions.

To flatten this JSON, the parseSpec could be defined as follows:

```json
"parseSpec": {
  "format": "json",
  "flattenSpec": {
    "useFieldDiscovery": true,
    "fields": [
      {
        "type": "root",
        "name": "dim1",
        "expr": "dim1"
      },
      "dim2",
      {
        "type": "path",
        "name": "foo.bar",
        "expr": "$.foo.bar"
      },
      {
        "type": "root",
        "name": "root-foo.bar",
        "expr": "foo.bar"
      },
      {
        "type": "path",
        "name": "path-metric",
        "expr": "$.nestmet.val"
      },
      {
        "type": "path",
        "name": "hello-0",
        "expr": "$.hello[0]"
      },
      {
        "type": "path",
        "name": "hello-4",
        "expr": "$.hello[4]"
      },
      {
        "type": "path",
        "name": "world-hey",
        "expr": "$.world.hey"
      },
      {
        "type": "path",
        "name": "worldtree",
        "expr": "$.world.tree"
      },
      {
        "type": "path",
        "name": "first-food",
        "expr": "$.thing.food[0]"
      },
      {
        "type": "path",
        "name": "second-food",
        "expr": "$.thing.food[1]"
      }
    ]
  },
  "dimensionsSpec" : {
   "dimensions" : [],
   "dimensionsExclusions": ["ignore_me"]
  },
  "timestampSpec" : {
   "format" : "auto",
   "column" : "timestamp"
  }
}
```

Fields "dim3", "ignore_me", and "metrica" will be automatically discovered because 'useFieldDiscovery' is true, so they have been omitted from the field spec list.

"ignore_me" will be automatically discovered but excluded as specified by dimensionsExclusions.

Aggregators should use the metric column names as defined in the flattenSpec. Using the example above:

```json
"metricsSpec" : [ 
{
  "type" : "longSum",
  "name" : "path-metric-sum",
  "fieldName" : "path-metric"
}, 
{
  "type" : "doubleSum",
  "name" : "hello-0-sum",
  "fieldName" : "hello-0"
},
{
  "type" : "longSum",
  "name" : "metrica-sum",
  "fieldName" : "metrica"
}
]
```

Note that:

* For convenience, when defining a root-level field, it is possible to define only the field name, as a string, shown with "dim2" above.
* Enabling 'useFieldDiscovery' will only autodetect fields at the root level with a single value (not a map or list), as well as fields referring to a list of single values. In the example above, "dim1", "dim2", "dim3", "ignore_me", "metrica", and "foo.bar" (at the root) would be automatically detected as columns. The "hello" field is a list of Doubles and will be autodiscovered, but note that the example ingests the individual list members as separate fields. The "world" field must be explicitly defined because its value is a map. The "mixarray" field, while similar to "hello", must also be explicitly defined because its last value is a map.  
* Duplicate field definitions are not allowed, an exception will be thrown.
* If auto field discovery is enabled, any discovered field with the same name as one already defined in the field specs will be skipped and not added twice.
* The JSON input must be a JSON object at the root, not an array. e.g., {"valid": "true"} and {"valid":[1,2,3]} are supported but [{"invalid": "true"}] and [1,2,3] are not.