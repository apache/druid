---
layout: doc_page
---
#Query Filters
A filter is a JSON object indicating which rows of data should be included in the computation for a query. It’s essentially the equivalent of the WHERE clause in SQL. Druid supports the following types of filters.

### Selector filter

The simplest filter is a selector filter. The selector filter will match a specific dimension with a specific value. Selector filters can be used as the base filters for more complex Boolean expressions of filters.

The grammar for a SELECTOR filter is as follows:

``` json
"filter": { "type": "selector", "dimension": <dimension_string>, "value": <dimension_value_string> }
```

This is the equivalent of `WHERE <dimension_string> = '<dimension_value_string>'`.

### Regular expression filter

The regular expression filter is similar to the selector filter, but using regular expressions. It matches the specified dimension with the given pattern. The pattern can be any standard [Java regular expression](http://docs.oracle.com/javase/6/docs/api/java/util/regex/Pattern.html).

``` json
"filter": { "type": "regex", "dimension": <dimension_string>, "pattern": <pattern_string> }
```

### Logical expression filters

#### AND

The grammar for an AND filter is as follows:

``` json
"filter": { "type": "and", "fields": [<filter>, <filter>, ...] }
```

The filters in fields can be any other filter defined on this page.

#### OR

The grammar for an OR filter is as follows:

``` json
"filter": { "type": "or", "fields": [<filter>, <filter>, ...] }
```

The filters in fields can be any other filter defined on this page.

#### NOT

The grammar for a NOT filter is as follows:

```json
"filter": { "type": "not", "field": <filter> }
```

The filter specified at field can be any other filter defined on this page.

### JavaScript filter

The JavaScript filter matches a dimension against the specified JavaScript function predicate. The filter matches values for which the function returns true.

The function takes a single argument, the dimension value, and returns either true or false.

```json
{
  "type" : "javascript",
  "dimension" : <dimension_string>,
  "function" : "function(value) { <...> }"
}
```

**Example**
The following matches any dimension values for the dimension `name` between `'bar'` and `'foo'`

```json
{
  "type" : "javascript",
  "dimension" : "name",
  "function" : "function(x) { return(x >= 'bar' && x <= 'foo') }"
}
```

### Extraction filter

Extraction filter matches a dimension using some specific [Extraction function](./dimensionspecs.html#extraction-functions).
The following filter matches the values for which the extraction function has transformation entry `input_key=output_value` where
 `output_value` is equal to the filter `value` and `input_key` is present as dimension.

**Example**
The following matches dimension values in `[product_1, product_3, product_5]` for the column `product`

```json
{
    "filter": {
        "type": "extraction",
        "dimension": "product",
        "value": "bar_1",
        "extractionFn": {
            "type": "lookup",
            "lookup": {
                "type": "map",
                "map": {
                    "product_1": "bar_1",
                    "product_5": "bar_1",
                    "product_3": "bar_1"
                }
            }
        }
    }
}
```
### Search filter

Search filters can be used to filter on partial string matches. 

```json
{
    "filter": {
        "type": "search",
        "dimension": "product",
        "query": {
          "type": "insensitive_contains",
          "value": "foo" 
        }        
    }
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This String should always be "search".|yes|
|dimension|The dimension to perform the search over.|yes|
|query|A JSON object for the type of search. See below for more information.|yes|

#### Search Query Spec

##### Insensitive Contains

|property|description|required?|
|--------|-----------|---------|
|type|This String should always be "insensitive_contains".|yes|
|value|A String value to run the search over.|yes|

##### Fragment

|property|description|required?|
|--------|-----------|---------|
|type|This String should always be "fragment".|yes|
|values|A JSON array of String values to run the search over. Case insensitive.|yes|
