---
layout: doc_page
---
# Query Filters
A filter is a JSON object indicating which rows of data should be included in the computation for a query. It’s essentially the equivalent of the WHERE clause in SQL. Druid supports the following types of filters.

### Selector filter

The simplest filter is a selector filter. The selector filter will match a specific dimension with a specific value. Selector filters can be used as the base filters for more complex Boolean expressions of filters.

The grammar for a SELECTOR filter is as follows:

``` json
"filter": { "type": "selector", "dimension": <dimension_string>, "value": <dimension_value_string> }
```

This is the equivalent of `WHERE <dimension_string> = '<dimension_value_string>'`.

The selector filter supports the use of extraction functions, see [Filtering with Extraction Functions](#filtering-with-extraction-functions) for details.

### Column Comparison filter

The column comparison filter is similar to the selector filter, but instead compares dimensions to each other. For example:

``` json
"filter": { "type": "columnComparison", "dimensions": [<dimension_a>, <dimension_b>] }
```

This is the equivalent of `WHERE <dimension_a> = <dimension_b>`.

`dimensions` is list of [DimensionSpecs](./dimensionspecs.html), making it possible to apply an extraction function if needed.

### Regular expression filter

The regular expression filter is similar to the selector filter, but using regular expressions. It matches the specified dimension with the given pattern. The pattern can be any standard [Java regular expression](http://docs.oracle.com/javase/6/docs/api/java/util/regex/Pattern.html).

``` json
"filter": { "type": "regex", "dimension": <dimension_string>, "pattern": <pattern_string> }
```

The regex filter supports the use of extraction functions, see [Filtering with Extraction Functions](#filtering-with-extraction-functions) for details.


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

The JavaScript filter supports the use of extraction functions, see [Filtering with Extraction Functions](#filtering-with-extraction-functions) for details.

<div class="note info">
JavaScript-based functionality is disabled by default. Please refer to the Druid <a href="../development/javascript.html">JavaScript programming guide</a> for guidelines about using Druid's JavaScript functionality, including instructions on how to enable it.
</div>

### Extraction filter

<div class="note caution">
The extraction filter is now deprecated. The selector filter with an extraction function specified
provides identical functionality and should be used instead.
</div>

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
|extractionFn|[Extraction function](#filtering-with-extraction-functions) to apply to the dimension|no|

The search filter supports the use of extraction functions, see [Filtering with Extraction Functions](#filtering-with-extraction-functions) for details.

#### Search Query Spec

##### Contains

|property|description|required?|
|--------|-----------|---------|
|type|This String should always be "contains".|yes|
|value|A String value to run the search over.|yes|
|caseSensitive|Whether two string should be compared as case sensitive or not|no (default == false)|

##### Insensitive Contains

|property|description|required?|
|--------|-----------|---------|
|type|This String should always be "insensitive_contains".|yes|
|value|A String value to run the search over.|yes|

Note that an "insensitive_contains" search is equivalent to a "contains" search with "caseSensitive": false (or not
provided).

##### Fragment

|property|description|required?|
|--------|-----------|---------|
|type|This String should always be "fragment".|yes|
|values|A JSON array of String values to run the search over.|yes|
|caseSensitive|Whether strings should be compared as case sensitive or not. Default: false(insensitive)|no|

### In filter

In filter can be used to express the following SQL query:

```sql
 SELECT COUNT(*) AS 'Count' FROM `table` WHERE `outlaw` IN ('Good', 'Bad', 'Ugly')
```

The grammar for a IN filter is as follows:

```json
{
    "type": "in",
    "dimension": "outlaw",
    "values": ["Good", "Bad", "Ugly"]
}
```

The IN filter supports the use of extraction functions, see [Filtering with Extraction Functions](#filtering-with-extraction-functions) for details.

### Like filter

Like filters can be used for basic wildcard searches. They are equivalent to the SQL LIKE operator. Special characters
supported are "%" (matches any number of characters) and "\_" (matches any one character).

|property|type|description|required?|
|--------|-----------|---------|---------|
|type|String|This should always be "like".|yes|
|dimension|String|The dimension to filter on|yes|
|pattern|String|LIKE pattern, such as "foo%" or "___bar".|yes|
|escape|String|An escape character that can be used to escape special characters.|no|
|extractionFn|[Extraction function](#filtering-with-extraction-functions)| Extraction function to apply to the dimension|no|

Like filters support the use of extraction functions, see [Filtering with Extraction Functions](#filtering-with-extraction-functions) for details.

This Like filter expresses the condition `last_name LIKE "D%"` (i.e. last_name starts with "D").

```json
{
    "type": "like",
    "dimension": "last_name",
    "pattern": "D%"
}
```

### Bound filter

Bound filters can be used to filter on ranges of dimension values. It can be used for comparison filtering like
greater than, less than, greater than or equal to, less than or equal to, and "between" (if both "lower" and
"upper" are set).

|property|type|description|required?|
|--------|-----------|---------|---------|
|type|String|This should always be "bound".|yes|
|dimension|String|The dimension to filter on|yes|
|lower|String|The lower bound for the filter|no|
|upper|String|The upper bound for the filter|no|
|lowerStrict|Boolean|Perform strict comparison on the lower bound ("<" instead of "<=")|no, default: false|
|upperStrict|Boolean|Perform strict comparison on the upper bound (">" instead of ">=")|no, default: false|
|ordering|String|Specifies the sorting order to use when comparing values against the bound. Can be one of the following values: "lexicographic", "alphanumeric", "numeric", "strlen". See [Sorting Orders](./sorting-orders.html) for more details.|no, default: "lexicographic"|
|extractionFn|[Extraction function](#filtering-with-extraction-functions)| Extraction function to apply to the dimension|no|
  
Bound filters support the use of extraction functions, see [Filtering with Extraction Functions](#filtering-with-extraction-functions) for details.

The following bound filter expresses the condition `21 <= age <= 31`:
```json
{
    "type": "bound",
    "dimension": "age",
    "lower": "21",
    "upper": "31" ,
    "ordering": "numeric"
}
```

This filter expresses the condition `foo <= name <= hoo`, using the default lexicographic sorting order.
```json
{
    "type": "bound",
    "dimension": "name",
    "lower": "foo",
    "upper": "hoo"
}
```

Using strict bounds, this filter expresses the condition `21 < age < 31`
```json
{
    "type": "bound",
    "dimension": "age",
    "lower": "21",
    "lowerStrict": true,
    "upper": "31" ,
    "upperStrict": true,
    "ordering": "numeric"
}
```

The user can also specify a one-sided bound by omitting "upper" or "lower". This filter expresses `age < 31`.
```json
{
    "type": "bound",
    "dimension": "age",
    "upper": "31" ,
    "upperStrict": true,
    "ordering": "numeric"
}
```

Likewise, this filter expresses `age >= 18`
```json
{
    "type": "bound",
    "dimension": "age",
    "lower": "18" ,
    "ordering": "numeric"
}
```


### Interval Filter

The Interval filter enables range filtering on columns that contain long millisecond values, with the boundaries specified as ISO 8601 time intervals. It is suitable for the `__time` column, long metric columns, and dimensions with values that can be parsed as long milliseconds.

This filter converts the ISO 8601 intervals to long millisecond start/end ranges and translates to an OR of Bound filters on those millisecond ranges, with numeric comparison. The Bound filters will have left-closed and right-open matching (i.e., start <= time < end).

|property|type|description|required?|
|--------|-----------|---------|---------|
|type|String|This should always be "interval".|yes|
|dimension|String|The dimension to filter on|yes|
|intervals|Array|A JSON array containing ISO-8601 interval strings. This defines the time ranges to filter on.|yes|
|extractionFn|[Extraction function](#filtering-with-extraction-functions)| Extraction function to apply to the dimension|no|
  
The interval filter supports the use of extraction functions, see [Filtering with Extraction Functions](#filtering-with-extraction-functions) for details.

If an extraction function is used with this filter, the extraction function should output values that are parseable as long milliseconds.

The following example filters on the time ranges of October 1-7, 2014 and November 15-16, 2014.
```json
{
    "type" : "interval",
    "dimension" : "__time",
    "intervals" : [
      "2014-10-01T00:00:00.000Z/2014-10-07T00:00:00.000Z",
      "2014-11-15T00:00:00.000Z/2014-11-16T00:00:00.000Z"
    ]
}
```

The filter above is equivalent to the following OR of Bound filters:

```json
{
    "type": "or",
    "fields": [
      {
        "type": "bound",
        "dimension": "__time",
        "lower": "1412121600000",
        "lowerStrict": false,
        "upper": "1412640000000" ,
        "upperStrict": true,
        "ordering": "numeric"
      },
      {
         "type": "bound",
         "dimension": "__time",
         "lower": "1416009600000",
         "lowerStrict": false,
         "upper": "1416096000000" ,
         "upperStrict": true,
         "ordering": "numeric"
      }
    ]
}
```

### Filtering with Extraction Functions
All filters except the "spatial" filter support extraction functions.
An extraction function is defined by setting the "extractionFn" field on a filter.
See [Extraction function](./dimensionspecs.html#extraction-functions) for more details on extraction functions.

If specified, the extraction function will be used to transform input values before the filter is applied.
The example below shows a selector filter combined with an extraction function. This filter will transform input values
according to the values defined in the lookup map; transformed values will then be matched with the string "bar_1".


**Example**
The following matches dimension values in `[product_1, product_3, product_5]` for the column `product`

```json
{
    "filter": {
        "type": "selector",
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

## Column types

Druid supports filtering on timestamp, string, long, and float columns.

Note that only string columns have bitmap indexes. Therefore, queries that filter on other column types will need to
scan those columns.

### Filtering on numeric columns

When filtering on numeric columns, you can write filters as if they were strings. In most cases, your filter will be
converted into a numeric predicate and will be applied to the numeric column values directly. In some cases (such as
the "regex" filter) the numeric column values will be converted to strings during the scan.

For example, filtering on a specific value, `myFloatColumn = 10.1`:

```json
"filter": {
  "type": "selector",
  "dimension": "myFloatColumn",
  "value": "10.1"
}
```

Filtering on a range of values, `10 <= myFloatColumn < 20`:

```json
"filter": {
  "type": "bound",
  "dimension": "myFloatColumn",
  "ordering": "numeric",
  "lowerBound": "10",
  "lowerStrict": false,
  "upperBound": "20",
  "upperStrict": true
}
```

### Filtering on the Timestamp Column

Query filters can also be applied to the timestamp column. The timestamp column has long millisecond values. To refer
to the timestamp column, use the string `__time` as the dimension name. Like numeric dimensions, timestamp filters
should be specified as if the timestamp values were strings.

If the user wishes to interpret the timestamp with a specific format, timezone, or locale, the [Time Format Extraction Function](./dimensionspecs.html#time-format-extraction-function) is useful.

For example, filtering on a long timestamp value:

```json
"filter": {
  "type": "selector",
  "dimension": "__time",
  "value": "124457387532"
}
```

Filtering on day of week:

```json
"filter": {
  "type": "selector",
  "dimension": "__time",
  "value": "Friday",
  "extractionFn": {
    "type": "timeFormat",
    "format": "EEEE",
    "timeZone": "America/New_York",
    "locale": "en"
  }
}
```

Filtering on a set of ISO 8601 intervals:

```json
{
    "type" : "interval",
    "dimension" : "__time",
    "intervals" : [
      "2014-10-01T00:00:00.000Z/2014-10-07T00:00:00.000Z",
      "2014-11-15T00:00:00.000Z/2014-11-16T00:00:00.000Z"
    ]
}
```


### Noop Filter
The noop filter is a filter which applies no conditions to your query.  Useful if you need to disable other filters when queries are generated programatically. 

```json
{ "type" : "noop" }
```