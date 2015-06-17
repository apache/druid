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
{ "type" : "default", "dimension" : <dimension>, "outputName": <output_name> }
```

### Extraction DimensionSpec

Returns dimension values transformed using the given [extraction function](#extraction-functions).

```json
{
  "type" : "extraction",
  "dimension" : <dimension>,
  "outputName" :  <output_name>,
  "extractionFn" : <extraction_function>
}
```

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
{ "type" : "regex", "expr" : <regular_expression> }
```

For example, using `"expr" : "(\\w\\w\\w).*"` will transform
`'Monday'`, `'Tuesday'`, `'Wednesday'` into `'Mon'`, `'Tue'`, `'Wed'`.

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

### Time Format Extraction Function

Returns the dimension value formatted according to the given format string, time zone, and locale.

For `__time` dimension values, this formats the time value bucketed by the
[aggregation granularity](../querying/granularities.html)

For a regular dimension, it assumes the string is formatted in
[ISO-8601 date and time format](https://en.wikipedia.org/wiki/ISO_8601).

* `format` : date time format for the resulting dimension value, in [Joda Time DateTimeFormat](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html).
* `locale` : locale (language and country) to use, given as a [IETF BCP 47 language tag](http://www.oracle.com/technetwork/java/javase/java8locales-2095355.html#util-text), e.g. `en-US`, `en-GB`, `fr-FR`, `fr-CA`, etc.
* `timeZone` : time zone to use in [IANA tz database format](http://en.wikipedia.org/wiki/List_of_tz_database_time_zones), e.g. `Europe/Berlin` (this can possibly be different than the aggregation time-zone)

```json
{ "type" : "timeFormat",
  "format" : <output_format>,
  "timeZone" : <time_zone> (optional),
  "locale" : <locale> (optional) }
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

### Lookup lookup extraction function
Explicit lookups allow you to specify a set of keys and values to use when performing the extraction
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

A lookup can be of type `namespace` or `map`. A `map` lookup is passed as part of the query. A `namespace` lookup is populated on all the nodes which handle queries as per [lookups](../querying/lookups.html)

A property of `retainMissingValue` and `replaceMissingValueWith` can be specified at query time to hint how to handle missing values. Setting `replaceMissingValueWith` to `""` has the same effect of setting it to `null` or omitting the property. Setting `retainMissingValue` to true will use the dimension's original value if it is not found in the lookup. The default values are `replaceMissingValueWith = null` and `retainMissingValue = false` which causes missing values to be treated as missing.
 
It is illegal to set `retainMissingValue = true` and also specify a `replaceMissingValueWith`

A property of `injective` specifies if optimizations can be used which assume there is no combining of multiple names into one. For example: If ABC123 is the only key that maps to SomeCompany, that can be optimized since it is a unique lookup. But if both ABC123 and DEF456 BOTH map to SomeCompany, then that is NOT a unique lookup. Setting this value to true and setting `retainMissingValue` to FALSE (the default) may cause undesired behavior.

A null dimension value can be mapped to a specific value by specifying the empty string as the key.
This allows distinguishing between a null dimension and a lookup resulting in a null.
For example, specifying `{"":"bar","bat":"baz"}` with dimension values `[null, "foo", "bat"]` and replacing missing values with `"oof"` will yield results of `["bar", "oof", "baz"]`.
Omitting the empty string key will cause the missing value to take over. For example, specifying `{"bat":"baz"}` with dimension values `[null, "foo", "bat"]` and replacing missing values with `"oof"` will yield results of `["oof", "oof", "baz"]`.
