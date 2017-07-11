---
layout: doc_page
---

# Transforming Dimension Values

The following JSON fields can be used in a query to operate on dimension values.

## DimensionSpec

`DimensionSpec`s define how dimension values get transformed prior to aggregation.

### Default DimensionSpec

Returns dimension values as is and optionally renames the dimension.

```json
{
  "type" : "default",
  "dimension" : <dimension>,
  "outputName": <output_name>,
  "outputType": <"STRING"|"LONG"|"FLOAT">
}
```

When specifying a DimensionSpec on a numeric column, the user should include the type of the column in the `outputType` field. If left unspecified, the `outputType` defaults to STRING.

Please refer to the [Output Types](#output-types) section for more details.

### Extraction DimensionSpec

Returns dimension values transformed using the given [extraction function](#extraction-functions).

```json
{
  "type" : "extraction",
  "dimension" : <dimension>,
  "outputName" :  <output_name>,
  "outputType": <"STRING"|"LONG"|"FLOAT">,
  "extractionFn" : <extraction_function>
}
```

`outputType` may also be specified in an ExtractionDimensionSpec to apply type conversion to results before merging. If left unspecified, the `outputType` defaults to STRING.

Please refer to the [Output Types](#output-types) section for more details.

### Filtered DimensionSpecs

These are only useful for multi-value dimensions. If you have a row in druid that has a multi-value dimension with values ["v1", "v2", "v3"] and you send a groupBy/topN query grouping by that dimension with [query filter](filters.html) for value "v1". In the response you will get 3 rows containing "v1", "v2" and "v3". This behavior might be unintuitive for some use cases.

It happens because "query filter" is internally used on the bitmaps and only used to match the row to be included in the query result processing. With multi-value dimensions, "query filter" behaves like a contains check, which will match the row with dimension value ["v1", "v2", "v3"]. Please see the section on "Multi-value columns" in [segment](../design/segments.html) for more details.
Then groupBy/topN processing pipeline "explodes" all multi-value dimensions resulting 3 rows for "v1", "v2" and "v3" each.

In addition to "query filter" which efficiently selects the rows to be processed, you can use the filtered dimension spec to filter for specific values within the values of a multi-value dimension. These dimensionSpecs take a delegate DimensionSpec and a filtering criteria. From the "exploded" rows, only rows matching the given filtering criteria are returned in the query result.

The following filtered dimension spec acts as a whitelist or blacklist for values as per the "isWhitelist" attribute value.

```json
{ "type" : "listFiltered", "delegate" : <dimensionSpec>, "values": <array of strings>, "isWhitelist": <optional attribute for true/false, default is true> }
```

Following filtered dimension spec retains only the values matching regex. Note that `listFiltered` is faster than this and one should use that for whitelist or blacklist usecase.

```json
{ "type" : "regexFiltered", "delegate" : <dimensionSpec>, "pattern": <java regex pattern> }
```

For more details and examples, see [multi-value dimensions](multi-value-dimensions.html).

### Lookup DimensionSpecs

<div class="note caution">
Lookups are an <a href="../development/experimental.html">experimental</a> feature.
</div>

Lookup DimensionSpecs can be used to define directly a lookup implementation as dimension spec.
Generally speaking there is two different kind of lookups implementations.
The first kind is passed at the query time like `map` implementation.

```json
{
  "type":"lookup",
  "dimension":"dimensionName",
  "outputName":"dimensionOutputName",
  "replaceMissingValueWith":"missing_value",
  "retainMissingValue":false,
  "lookup":{"type": "map", "map":{"key":"value"}, "isOneToOne":false}
}
```

A property of `retainMissingValue` and `replaceMissingValueWith` can be specified at query time to hint how to handle missing values. Setting `replaceMissingValueWith` to `""` has the same effect as setting it to `null` or omitting the property.
Setting `retainMissingValue` to true will use the dimension's original value if it is not found in the lookup.
The default values are `replaceMissingValueWith = null` and `retainMissingValue = false` which causes missing values to be treated as missing.

It is illegal to set `retainMissingValue = true` and also specify a `replaceMissingValueWith`.

A property of `injective` specifies if optimizations can be used which assume there is no combining of multiple names into one. For example: If ABC123 is the only key that maps to SomeCompany, that can be optimized since it is a unique lookup. But if both ABC123 and DEF456 BOTH map to SomeCompany, then that is NOT a unique lookup. Setting this value to true and setting `retainMissingValue` to FALSE (the default) may cause undesired behavior.

A property `optimize` can be supplied to allow optimization of lookup based extraction filter (by default `optimize = true`).

The second kind where it is not possible to pass at query time due to their size, will be based on an external lookup table or resource that is already registered via configuration file or/and coordinator.

```json
{
  "type":"lookup",
  "dimension":"dimensionName",
  "outputName":"dimensionOutputName",
  "name":"lookupName"
}
```

## Output Types

The dimension specs provide an option to specify the output type of a column's values. This is necessary as it is possible for a column with given name to have different value types in different segments; results will be converted to the type specified by `outputType` before merging.

Note that not all use cases for DimensionSpec currently support `outputType`, the table below shows which use cases support this option:

|Query Type|Supported?|
|--------|---------|
|GroupBy (v1)|no|
|GroupBy (v2)|yes|
|TopN|yes|
|Search|no|
|Select|no|
|Cardinality Aggregator|no|

## Extraction Functions

Extraction functions define the transformation applied to each dimension value.

Transformations can be applied to both regular (string) dimensions, as well
as the special `__time` dimension, which represents the current time bucket
according to the query [aggregation granularity](../querying/granularities.html).

**Note**: for functions taking string values (such as regular expressions),
`__time` dimension values will be formatted in [ISO-8601 format](https://en.wikipedia.org/wiki/ISO_8601)
before getting passed to the extraction function.

### Regular Expression Extraction Function

Returns the first matching group for the given regular expression.
If there is no match, it returns the dimension value as is.

```json
{
  "type" : "regex",
  "expr" : <regular_expression>,
  "index" : <group to extract, default 1>
  "replaceMissingValue" : true,
  "replaceMissingValueWith" : "foobar"
}
```

For example, using `"expr" : "(\\w\\w\\w).*"` will transform
`'Monday'`, `'Tuesday'`, `'Wednesday'` into `'Mon'`, `'Tue'`, `'Wed'`.

If "index" is set, it will control which group from the match to extract. Index zero extracts the string matching the
entire pattern.

If the `replaceMissingValue` property is true, the extraction function will transform dimension values that do not match the regex pattern to a user-specified String. Default value is `false`.

The `replaceMissingValueWith` property sets the String that unmatched dimension values will be replaced with, if `replaceMissingValue` is true. If `replaceMissingValueWith` is not specified, unmatched dimension values will be replaced with nulls.

For example, if `expr` is `"(a\w+)"` in the example JSON above, a regex that matches words starting with the letter `a`, the extraction function will convert a dimension value like `banana` to `foobar`.


### Partial Extraction Function

Returns the dimension value unchanged if the regular expression matches, otherwise returns null.

```json
{ "type" : "partial", "expr" : <regular_expression> }
```

### Search Query Extraction Function

Returns the dimension value unchanged if the given [`SearchQuerySpec`](../querying/searchqueryspec.html)
matches, otherwise returns null.

```json
{ "type" : "searchQuery", "query" : <search_query_spec> }
```

### Substring Extraction Function

Returns a substring of the dimension value starting from the supplied index and of the desired length. Both index
and length are measured in the number of Unicode code units present in the string as if it were encoded in UTF-16.
Note that some Unicode characters may be represented by two code units. This is the same behavior as the Java String
class's "substring" method.

If the desired length exceeds the length of the dimension value, the remainder of the string starting at index will
be returned. If index is greater than the length of the dimension value, null will be returned.

```json
{ "type" : "substring", "index" : 1, "length" : 4 }
```

The length may be omitted for substring to return the remainder of the dimension value starting from index, 
or null if index greater than the length of the dimension value.

```json
{ "type" : "substring", "index" : 3 }
```

### Strlen Extraction Function

Returns the length of dimension values, as measured in the number of Unicode code units present in the string as if it
were encoded in UTF-16. Note that some Unicode characters may be represented by two code units. This is the same
behavior as the Java String class's "length" method.

null strings are considered as having zero length.

```json
{ "type" : "strlen" }
```

### Time Format Extraction Function

Returns the dimension value formatted according to the given format string, time zone, and locale.

For `__time` dimension values, this formats the time value bucketed by the
[aggregation granularity](../querying/granularities.html)

For a regular dimension, it assumes the string is formatted in
[ISO-8601 date and time format](https://en.wikipedia.org/wiki/ISO_8601).

* `format` : date time format for the resulting dimension value, in [Joda Time DateTimeFormat](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html), or null to use the default ISO8601 format.
* `locale` : locale (language and country) to use, given as a [IETF BCP 47 language tag](http://www.oracle.com/technetwork/java/javase/java8locales-2095355.html#util-text), e.g. `en-US`, `en-GB`, `fr-FR`, `fr-CA`, etc.
* `timeZone` : time zone to use in [IANA tz database format](http://en.wikipedia.org/wiki/List_of_tz_database_time_zones), e.g. `Europe/Berlin` (this can possibly be different than the aggregation time-zone)
* `granularity` : [granularity](granularities.html) to apply before formatting, or omit to not apply any granularity.
* `asMillis` : boolean value, set to true to treat input strings as millis rather than ISO8601 strings. Additionally, if `format` is null or not specified, output will be in millis rather than ISO8601.

```json
{ "type" : "timeFormat",
  "format" : <output_format> (optional),
  "timeZone" : <time_zone> (optional, default UTC),
  "locale" : <locale> (optional, default current locale),
  "granularity" : <granularity> (optional, default none) },
  "asMillis" : <true or false> (optional) }
```

For example, the following dimension spec returns the day of the week for Montréal in French:

```json
{
  "type" : "extraction",
  "dimension" : "__time",
  "outputName" :  "dayOfWeek",
  "extractionFn" : {
    "type" : "timeFormat",
    "format" : "EEEE",
    "timeZone" : "America/Montreal",
    "locale" : "fr"
  }
}
```

### Time Parsing Extraction Function

Parses dimension values as timestamps using the given input format,
and returns them formatted using the given output format.

Note, if you are working with the `__time` dimension, you should consider using the
[time extraction function instead](#time-format-extraction-function) instead,
which works on time value directly as opposed to string values.

Time formats are described in the
[SimpleDateFormat documentation](http://icu-project.org/apiref/icu4j/com/ibm/icu/text/SimpleDateFormat.html)

```json
{ "type" : "time",
  "timeFormat" : <input_format>,
  "resultFormat" : <output_format> }
```


### Javascript Extraction Function

Returns the dimension value, as transformed by the given JavaScript function.

For regular dimensions, the input value is passed as a string.

For the `__time` dimension, the input value is passed as a number
representing the number of milliseconds since January 1, 1970 UTC.

Example for a regular dimension

```json
{
  "type" : "javascript",
  "function" : "function(str) { return str.substr(0, 3); }"
}
```

```json
{
  "type" : "javascript",
  "function" : "function(str) { return str + '!!!'; }",
  "injective" : true
}
```

A property of `injective` specifies if the javascript function preserves uniqueness. The default value is `false` meaning uniqueness is not preserved

Example for the `__time` dimension:

```json
{
  "type" : "javascript",
  "function" : "function(t) { return 'Second ' + Math.floor((t % 60000) / 1000); }"
}
```

<div class="note info">
JavaScript-based functionality is disabled by default. Please refer to the Druid <a href="../development/javascript.html">JavaScript programming guide</a> for guidelines about using Druid's JavaScript functionality, including instructions on how to enable it.
</div>

### Lookup extraction function

Lookups are a concept in Druid where dimension values are (optionally) replaced with new values. 
For more documentation on using lookups, please see [here](../querying/lookups.html). 
Explicit lookups allow you to specify a set of keys and values to use when performing the extraction.

```json
{
  "type":"lookup",
  "lookup":{
    "type":"map",
    "map":{"foo":"bar", "baz":"bat"}
  },
  "retainMissingValue":true,
  "injective":true
}
```

```json
{
  "type":"lookup",
  "lookup":{
    "type":"map",
    "map":{"foo":"bar", "baz":"bat"}
  },
  "retainMissingValue":false,
  "injective":false,
  "replaceMissingValueWith":"MISSING"
}
```

```json
{
  "type":"lookup",
  "lookup":{"type":"namespace","namespace":"some_lookup"},
  "replaceMissingValueWith":"Unknown",
  "injective":false
}
```

```json
{
  "type":"lookup",
  "lookup":{"type":"namespace","namespace":"some_lookup"},
  "retainMissingValue":true,
  "injective":false
}
```

A lookup can be of type `namespace` or `map`. A `map` lookup is passed as part of the query. 
A `namespace` lookup is populated on all the nodes which handle queries as per [lookups](../querying/lookups.html)

A property of `retainMissingValue` and `replaceMissingValueWith` can be specified at query time to hint how to handle missing values. Setting `replaceMissingValueWith` to `""` has the same effect as setting it to `null` or omitting the property. Setting `retainMissingValue` to true will use the dimension's original value if it is not found in the lookup. The default values are `replaceMissingValueWith = null` and `retainMissingValue = false` which causes missing values to be treated as missing.
 
It is illegal to set `retainMissingValue = true` and also specify a `replaceMissingValueWith`.

A property of `injective` specifies if optimizations can be used which assume there is no combining of multiple names into one. For example: If ABC123 is the only key that maps to SomeCompany, that can be optimized since it is a unique lookup. But if both ABC123 and DEF456 BOTH map to SomeCompany, then that is NOT a unique lookup. Setting this value to true and setting `retainMissingValue` to FALSE (the default) may cause undesired behavior.

A property `optimize` can be supplied to allow optimization of lookup based extraction filter (by default `optimize = true`). 
The optimization layer will run on the broker and it will rewrite the extraction filter as clause of selector filters.
For instance the following filter 

```json
{
    "filter": {
        "type": "selector",
        "dimension": "product",
        "value": "bar_1",
        "extractionFn": {
            "type": "lookup",
            "optimize": true,
            "lookup": {
                "type": "map",
                "map": {
                    "product_1": "bar_1",
                    "product_3": "bar_1"
                }
            }
        }
    }
}
```

will be rewritten as

```json
{
   "filter":{
      "type":"or",
      "fields":[
         {
            "filter":{
               "type":"selector",
               "dimension":"product",
               "value":"product_1"
            }
         },
         {
            "filter":{
               "type":"selector",
               "dimension":"product",
               "value":"product_3"
            }
         }
      ]
   }
}
```

A null dimension value can be mapped to a specific value by specifying the empty string as the key.
This allows distinguishing between a null dimension and a lookup resulting in a null.
For example, specifying `{"":"bar","bat":"baz"}` with dimension values `[null, "foo", "bat"]` and replacing missing values with `"oof"` will yield results of `["bar", "oof", "baz"]`.
Omitting the empty string key will cause the missing value to take over. For example, specifying `{"bat":"baz"}` with dimension values `[null, "foo", "bat"]` and replacing missing values with `"oof"` will yield results of `["oof", "oof", "baz"]`.

### Registered Lookup Extraction Function

While it is recommended that the [lookup dimension spec](#lookup-dimensionspecs) be used whenever possible, any lookup that is registered for use as a lookup dimension spec can be used as a dimension extraction.

The specification for dimension extraction using dimension specification named lookups is formatted as per the following example:

```json
{
  "type":"registeredLookup",
  "lookup":"some_lookup_name",
  "retainMissingValue":true,
  "injective":false
}
```

All the flags for [lookup extraction function](#lookup-extraction-function) apply here as well.

In general, the dimension specification should be used. This dimension **extraction** implementation is made available for testing, validation, and transitioning from dimension extraction to the dimension specification style lookups.
There is also a chance that a feature uses dimension extraction in such a way that it is not applied to dimension specification lookups. Such a scenario should be brought to the attention of the development mailing list.

### Cascade Extraction Function

Provides chained execution of extraction functions.

A property of `extractionFns` contains an array of any extraction functions, which is executed in the array index order.

Example for chaining [regular expression extraction function](#regular-expression-extraction-function), [javascript extraction function](#javascript-extraction-function), and [substring extraction function](#substring-extraction-function) is as followings.

```json
{
  "type" : "cascade", 
  "extractionFns": [
    { 
      "type" : "regex", 
      "expr" : "/([^/]+)/", 
      "replaceMissingValue": false,
      "replaceMissingValueWith": null
    },
    { 
      "type" : "javascript", 
      "function" : "function(str) { return \"the \".concat(str) }" 
    },
    { 
      "type" : "substring", 
      "index" : 0, "length" : 7 
    }
  ]
}
```

It will transform dimension values with specified extraction functions in the order named.
For example, `'/druid/prod/historical'` is transformed to `'the dru'` as regular expression extraction function first transforms it to `'druid'` and then, javascript extraction function transforms it to `'the druid'`, and lastly, substring extraction function transforms it to `'the dru'`. 

### String Format Extraction Function

Returns the dimension value formatted according to the given format string.

```json
{ "type" : "stringFormat", "format" : <sprintf_expression>, "nullHandling" : <optional attribute for handling null value> }
```

For example if you want to concat "[" and "]" before and after the actual dimension value, you need to specify "[%s]" as format string. "nullHandling" can be one of `nullString`, `emptyString` or `returnNull`. With "[%s]" format, each configuration will result `[null]`, `[]`, `null`. Default is `nullString`.

### Upper and Lower extraction functions.

Returns the dimension values as all upper case or lower case.
Optionally user can specify the language to use in order to perform upper or lower transformation 

```json
{
  "type" : "upper",
  "locale":"fr"
}
```

or without setting "locale" (in this case, the current value of the default locale for this instance of the Java Virtual Machine.)

```json
{
  "type" : "lower"
}
```

### Bucket Extraction Function

Bucket extraction function is used to bucket numerical values in each range of the given size by converting them to the same base value. Non numeric values are converted to null.

* `size` : the size of the buckets (optional, default 1)
* `offset` : the offset for the buckets (optional, default 0)

The following extraction function creates buckets of 5 starting from 2. In this case, values in the range of [2, 7) will be converted to 2, values in [7, 12) will be converted to 7, etc.

```json
{
  "type" : "bucket",
  "size" : 5,
  "offset" : 2
}
```
