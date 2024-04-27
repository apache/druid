---
id: filters
title: "Query filters"
sidebar_label: "Filters"
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

:::info
 Apache Druid supports two query languages: [Druid SQL](sql.md) and [native queries](querying.md).
 This document describes the native
 language. For information about aggregators available in SQL, refer to the
 [SQL documentation](sql-scalar.md).
:::

A filter is a JSON object indicating which rows of data should be included in the computation for a query. It’s essentially the equivalent of the WHERE clause in SQL.
Filters are commonly applied on dimensions, but can be applied on aggregated metrics, for example, see [Filtered aggregator](./aggregations.md#filtered-aggregator) and [Having filters](./having.md).

By default, Druid uses SQL compatible three-value logic when filtering. See [Boolean logic](./sql-data-types.md#boolean-logic) for more details.

Apache Druid supports the following types of filters.

## Selector filter

The simplest filter is a selector filter. The selector filter matches a specific dimension with a specific value. Selector filters can be used as the base filters for more complex Boolean expressions of filters.

| Property | Description | Required |
| -------- | ----------- | -------- |
| `type` | Must be "selector".| Yes |
| `dimension` | Input column or virtual column name to filter on. | Yes |
| `value` | String value to match. | No. If not specified the filter matches NULL values. |
| `extractionFn` | [Extraction function](./dimensionspecs.md#extraction-functions) to apply to `dimension` prior to value matching. See [filtering with extraction functions](#filtering-with-extraction-functions) for details. | No |

The selector filter can only match against `STRING` (single and multi-valued), `LONG`, `FLOAT`, `DOUBLE` types. Use the newer null and equality filters to match against `ARRAY` or `COMPLEX` types.

When the selector filter matches against numeric inputs, the string `value` will be best-effort coerced into a numeric value.

### Example: equivalent of `WHERE someColumn = 'hello'`

``` json
{ "type": "selector", "dimension": "someColumn", "value": "hello" }
```


### Example: equivalent of `WHERE someColumn IS NULL`

``` json
{ "type": "selector", "dimension": "someColumn", "value": null }
```


## Equality Filter

The equality filter is a replacement for the selector filter with the ability to match against any type of column. The equality filter is designed to have more SQL compatible behavior than the selector filter and so can not match null values. To match null values use the null filter.

Druid's SQL planner uses the equality filter by default instead of selector filter whenever `druid.generic.useDefaultValueForNull=false`, or if `sqlUseBoundAndSelectors` is set to false on the [SQL query context](./sql-query-context.md).

| Property | Description | Required |
| -------- | ----------- | -------- |
| `type` | Must be "equality".| Yes |
| `column` | Input column or virtual column name to filter on. | Yes |
| `matchValueType` | String specifying the type of value to match. For example `STRING`, `LONG`, `DOUBLE`, `FLOAT`, `ARRAY<STRING>`, `ARRAY<LONG>`, or any other Druid type. The `matchValueType` determines how Druid interprets the `matchValue` to assist in converting to the type of the matched `column`. | Yes |
| `matchValue` | Value to match, must not be null. | Yes |

### Example: equivalent of `WHERE someColumn = 'hello'`

```json
{ "type": "equals", "column": "someColumn", "matchValueType": "STRING", "matchValue": "hello" }
```

### Example: equivalent of `WHERE someNumericColumn = 1.23`

```json
{ "type": "equals", "column": "someNumericColumn", "matchValueType": "DOUBLE", "matchValue": 1.23 }
```

### Example: equivalent of `WHERE someArrayColumn = ARRAY[1, 2, 3]`

```json
{ "type": "equals", "column": "someArrayColumn", "matchValueType": "ARRAY<LONG>", "matchValue": [1, 2, 3] }
```


## Null Filter

The null filter is a partial replacement for the selector filter. It is dedicated to matching NULL values.

Druid's SQL planner uses the null filter by default instead of selector filter whenever `druid.generic.useDefaultValueForNull=false`, or if `sqlUseBoundAndSelectors` is set to false on the [SQL query context](./sql-query-context.md).

| Property | Description | Required |
| -------- | ----------- | -------- |
| `type` | Must be "null".| Yes |
| `column` | Input column or virtual column name to filter on. | Yes |

### Example: equivalent of `WHERE someColumn IS NULL`

```json
{ "type": "null", "column": "someColumn" }
```


## Column comparison filter

The column comparison filter is similar to the selector filter, but compares dimensions to each other. For example:

| Property | Description | Required |
| -------- | ----------- | -------- |
| `type` | Must be "selector".| Yes |
| `dimensions` | List of [`DimensionSpec`](./dimensionspecs.md) to compare. | Yes |

`dimensions` is list of [DimensionSpecs](./dimensionspecs.md), making it possible to apply an extraction function if needed.

Note that the column comparison filter converts all values to strings prior to comparison. This allows differently-typed input columns to match without a cast operation.

### Example: equivalent of `WHERE someColumn = someLongColumn`

``` json
{
  "type": "columnComparison",
  "dimensions": [
    "someColumn",
    {
      "type" : "default",
      "dimension" : someLongColumn,
      "outputType": "LONG"
    }
  ]
}
```


## Logical expression filters

### AND

| Property | Description | Required |
| -------- | ----------- | -------- |
| `type` | Must be "and".| Yes |
| `fields` | List of filter JSON objects, such as any other filter defined on this page or provided by extensions. | Yes |


#### Example: equivalent of `WHERE someColumn = 'a' AND otherColumn = 1234 AND anotherColumn IS NULL`

``` json
{
  "type": "and",
  "fields": [
    { "type": "equals", "column": "someColumn", "matchValue": "a", "matchValueType": "STRING" },
    { "type": "equals", "column": "otherColumn", "matchValue": 1234, "matchValueType": "LONG" },
    { "type": "null", "column": "anotherColumn" } 
  ]
}
```

### OR

| Property | Description | Required |
| -------- | ----------- | -------- |
| `type` | Must be "or".| Yes |
| `fields` | List of filter JSON objects, such as any other filter defined on this page or provided by extensions. | Yes |

#### Example: equivalent of `WHERE someColumn = 'a' OR otherColumn = 1234 OR anotherColumn IS NULL`

``` json
{
  "type": "or",
  "fields": [
    { "type": "equals", "column": "someColumn", "matchValue": "a", "matchValueType": "STRING" },
    { "type": "equals", "column": "otherColumn", "matchValue": 1234, "matchValueType": "LONG" },
    { "type": "null", "column": "anotherColumn" } 
  ]
}
```

### NOT

| Property | Description | Required |
| -------- | ----------- | -------- |
| `type` | Must be "not".| Yes |
| `field` | Filter JSON objects, such as any other filter defined on this page or provided by extensions. | Yes |

#### Example: equivalent of `WHERE someColumn IS NOT NULL`

```json
{ "type": "not", "field": { "type": "null", "column": "someColumn" }}
```


## In filter
The in filter can match input rows against a set of values, where a match occurs if the value is contained in the set.

| Property | Description | Required |
| -------- | ----------- | -------- |
| `type` | Must be "in".| Yes |
| `dimension` | Input column or virtual column name to filter on. | Yes |
| `values` | List of string value to match. | Yes |
| `extractionFn` | [Extraction function](./dimensionspecs.md#extraction-functions) to apply to `dimension` prior to value matching. See [filtering with extraction functions](#filtering-with-extraction-functions) for details. | No |


If an empty `values` array is passed to the "in" filter, it will simply return an empty result.

If the `values` array contains `null`, the "in" filter matches null values. This differs from the SQL IN filter, which
does not match NULL values.

### Example: equivalent of `WHERE `outlaw` IN ('Good', 'Bad', 'Ugly')`

```json
{
    "type": "in",
    "dimension": "outlaw",
    "values": ["Good", "Bad", "Ugly"]
}
```


## Bound filter

Bound filters can be used to filter on ranges of dimension values. It can be used for comparison filtering like
greater than, less than, greater than or equal to, less than or equal to, and "between" (if both "lower" and
"upper" are set).

| Property | Description | Required |
| -------- | ----------- | -------- |
| `type` | Must be "bound". | Yes |
| `dimension` | Input column or virtual column name to filter on. | Yes |
| `lower` | The lower bound string match value for the filter. | No |
| `upper`| The upper bound string match value for the filter. | No |
| `lowerStrict` | Boolean indicating whether to perform strict comparison on the `lower` bound (">" instead of ">="). | No, default: `false` |
| `upperStrict` | Boolean indicating whether to perform strict comparison on the upper bound ("<" instead of "<="). | No, default: `false`|
| `ordering` | String that specifies the sorting order to use when comparing values against the bound. Can be one of the following values: `"lexicographic"`, `"alphanumeric"`, `"numeric"`, `"strlen"`, `"version"`. See [Sorting Orders](./sorting-orders.md) for more details. | No, default: `"lexicographic"`|
| `extractionFn` | [Extraction function](./dimensionspecs.md#extraction-functions) to apply to `dimension` prior to value matching. See [filtering with extraction functions](#filtering-with-extraction-functions) for details. | No |

When the bound filter matches against numeric inputs, the string `lower` and `upper` bound values are best-effort coerced into a numeric value when using the `"numeric"` mode of ordering.

The bound filter can only match against `STRING` (single and multi-valued), `LONG`, `FLOAT`, `DOUBLE` types. Use the newer range to match against `ARRAY` or `COMPLEX` types.

Note that the bound filter matches null values if you don't specify a lower bound. Use the range filter if SQL-compatible behavior.

### Example: equivalent to `WHERE 21 <= age <= 31`

```json
{
    "type": "bound",
    "dimension": "age",
    "lower": "21",
    "upper": "31" ,
    "ordering": "numeric"
}
```

### Example: equivalent to `WHERE 'foo' <= name <= 'hoo'`, using the default lexicographic sorting order

```json
{
    "type": "bound",
    "dimension": "name",
    "lower": "foo",
    "upper": "hoo"
}
```

### Example: equivalent to `WHERE 21 < age < 31`

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

### Example: equivalent to `WHERE age < 31`

```json
{
    "type": "bound",
    "dimension": "age",
    "upper": "31" ,
    "upperStrict": true,
    "ordering": "numeric"
}
```

### Example: equivalent to `WHERE age >= 18`

```json
{
    "type": "bound",
    "dimension": "age",
    "lower": "18" ,
    "ordering": "numeric"
}
```


## Range filter

The range filter is a replacement for the bound filter. It compares against any type of column and is designed to have has more SQL compliant behavior than the bound filter. It won't match null values, even if you don't specify a lower bound.

Druid's SQL planner uses the range filter by default instead of bound filter whenever `druid.generic.useDefaultValueForNull=false`, or if `sqlUseBoundAndSelectors` is set to false on the [SQL query context](./sql-query-context.md).

| Property | Description | Required |
| -------- | ----------- | -------- |
| `type` | Must be "range".| Yes |
| `column` | Input column or virtual column name to filter on. | Yes |
| `matchValueType` | String specifying the type of bounds to match. For example `STRING`, `LONG`, `DOUBLE`, `FLOAT`, `ARRAY<STRING>`, `ARRAY<LONG>`, or any other Druid type. The `matchValueType` determines how Druid interprets the `matchValue` to assist in converting to the type of the matched `column` and also defines the type of comparison used when matching values. | Yes |
| `lower` | Lower bound value to match. | No. At least one of `lower` or `upper` must not be null. |
| `upper` | Upper bound value to match. | No. At least one of `lower` or `upper` must not be null. |
| `lowerOpen` | Boolean indicating if lower bound is open in the interval of values defined by the range (">" instead of ">="). | No |
| `upperOpen` | Boolean indicating if upper bound is open on the interval of values defined by range ("<" instead of "<="). | No |

### Example: equivalent to `WHERE 21 <= age <= 31`

```json
{
    "type": "range",
    "column": "age",
    "matchValueType": "LONG",
    "lower": 21,
    "upper": 31
}
```

### Example: equivalent to `WHERE 'foo' <= name <= 'hoo'`, using STRING comparison

```json
{
    "type": "range",
    "column": "name",
    "matchValueType": "STRING",
    "lower": "foo",
    "upper": "hoo"
}
```

### Example: equivalent to `WHERE 21 < age < 31`

```json
{
    "type": "range",
    "column": "age",
    "matchValueType": "LONG",
    "lower": "21",
    "lowerOpen": true,
    "upper": "31" ,
    "upperOpen": true
}
```

### Example: equivalent to `WHERE age < 31`

```json
{
    "type": "range",
    "column": "age",
    "matchValueType": "LONG",
    "upper": "31" ,
    "upperOpen": true
}
```

### Example: equivalent to `WHERE age >= 18`

```json
{
    "type": "range",
    "column": "age",
    "matchValueType": "LONG",
    "lower": 18
}
```

### Example: equivalent to `WHERE ARRAY['a','b','c'] < arrayColumn < ARRAY['d','e','f']`, using ARRAY comparison

```json
{
    "type": "range",
    "column": "name",
    "matchValueType": "ARRAY<STRING>",
    "lower": ["a","b","c"],
    "lowerOpen": true,
    "upper": ["d","e","f"],
    "upperOpen": true
}
```


## Like filter

Like filters can be used for basic wildcard searches. They are equivalent to the SQL LIKE operator. Special characters
supported are "%" (matches any number of characters) and "\_" (matches any one character).

| Property | Description | Required |
| -------- | ----------- | -------- |
| `type` | Must be "like".| Yes |
| `dimension` | Input column or virtual column name to filter on. | Yes |
| `pattern` | String LIKE pattern, such as "foo%" or "___bar".| Yes |
| `escape`| A string escape character that can be used to escape special characters. | No |
| `extractionFn` | [Extraction function](./dimensionspecs.md#extraction-functions) to apply to `dimension` prior to value matching. See [filtering with extraction functions](#filtering-with-extraction-functions) for details. | No |

Like filters support the use of extraction functions, see [Filtering with Extraction Functions](#filtering-with-extraction-functions) for details.

### Example: equivalent of `WHERE last_name LIKE "D%"` (last_name starts with "D")

```json
{
    "type": "like",
    "dimension": "last_name",
    "pattern": "D%"
}
```

## Regular expression filter

The regular expression filter is similar to the selector filter, but using regular expressions. It matches the specified dimension with the given pattern.

| Property | Description | Required |
| -------- | ----------- | -------- |
| `type` | Must be "regex".| Yes |
| `dimension` | Input column or virtual column name to filter on. | Yes |
| `pattern` | String pattern to match - any standard [Java regular expression](http://docs.oracle.com/javase/6/docs/api/java/util/regex/Pattern.html). | Yes |
| `extractionFn` | [Extraction function](./dimensionspecs.md#extraction-functions) to apply to `dimension` prior to value matching. See [filtering with extraction functions](#filtering-with-extraction-functions) for details. | No |

Note that it is often more optimal to use a like filter instead of a regex for simple matching of prefixes.

### Example: matches values that start with "50."

``` json
{ "type": "regex", "dimension": "someColumn", "pattern": ^50.* }
```

## Array contains element filter

The `arrayContainsElement` filter checks if an `ARRAY` contains a specific element but can also match against any type of column. When matching against scalar columns, scalar columns are treated as single-element arrays.

| Property | Description | Required |
| -------- | ----------- | -------- |
| `type` | Must be "arrayContainsElement".| Yes |
| `column` | Input column or virtual column name to filter on. | Yes |
| `elementMatchValueType` | String specifying the type of element value to match. For example `STRING`, `LONG`, `DOUBLE`, `FLOAT`, `ARRAY<STRING>`, `ARRAY<LONG>`, or any other Druid type. The `elementMatchValueType` determines how Druid interprets the `elementMatchValue` to assist in converting to the type of elements contained in the matched `column`. | Yes |
| `elementMatchValue` | Array element value to match. This value can be null. | Yes |

### Example: equivalent of `WHERE ARRAY_CONTAINS(someArrayColumn, 'hello')`

```json
{ "type": "arrayContainsElement", "column": "someArrayColumn", "elementMatchValueType": "STRING", "elementMatchValue": "hello" }
```

### Example: equivalent of `WHERE ARRAY_CONTAINS(someNumericArrayColumn, 1.23)`

```json
{ "type": "arrayContainsElement", "column": "someNumericArrayColumn", "elementMatchValueType": "DOUBLE", "elementMatchValue": 1.23 }
```

### Example: equivalent of `WHERE ARRAY_CONTAINS(someNumericArrayColumn, ARRAY[1, 2, 3])`

```json
{
  "type": "and",
  "fields": [
    { "type": "arrayContainsElement", "column": "someNumericArrayColumn", "elementMatchValueType": "LONG", "elementMatchValue": 1 },
    { "type": "arrayContainsElement", "column": "someNumericArrayColumn", "elementMatchValueType": "LONG", "elementMatchValue": 2 },
    { "type": "arrayContainsElement", "column": "someNumericArrayColumn", "elementMatchValueType": "LONG", "elementMatchValue": 3 }
  ]
}

```

### Example: equivalent of `WHERE ARRAY_OVERLAPS(someNumericArrayColumn, ARRAY[1, 2, 3])`

```json
{
 "type": "or",
 "fields": [
  { "type": "arrayContainsElement", "column": "someNumericArrayColumn", "elementMatchValueType": "LONG", "elementMatchValue": 1 },
  { "type": "arrayContainsElement", "column": "someNumericArrayColumn", "elementMatchValueType": "LONG", "elementMatchValue": 2 },
  { "type": "arrayContainsElement", "column": "someNumericArrayColumn", "elementMatchValueType": "LONG", "elementMatchValue": 3 }
 ]
}
```

## Interval filter

The Interval filter enables range filtering on columns that contain long millisecond values, with the boundaries specified as ISO 8601 time intervals. It is suitable for the `__time` column, long metric columns, and dimensions with values that can be parsed as long milliseconds.

This filter converts the ISO 8601 intervals to long millisecond start/end ranges and translates to an OR of Bound filters on those millisecond ranges, with numeric comparison. The Bound filters will have left-closed and right-open matching (i.e., start <= time < end).

| Property | Description | Required |
| -------- | ----------- | -------- |
| `type` | Must be "interval". | Yes |
| `dimension` | Input column or virtual column name to filter on. | Yes |
| `intervals` | A JSON array containing ISO-8601 interval strings that defines the time ranges to filter on. | Yes |
| `extractionFn` | [Extraction function](./dimensionspecs.md#extraction-functions) to apply to `dimension` prior to value matching. See [filtering with extraction functions](#filtering-with-extraction-functions) for details. | No |

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


## True filter
A filter which matches all values. You can use it to temporarily disable other filters without removing them.

```json
{ "type" : "true" }
```

## False filter
A filter matches no values. You can use it to force a query to match no values.

```json
{"type": "false" }
```


## Search filter

You can use search filters to filter on partial string matches.

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

| Property | Description | Required |
| -------- | ----------- | -------- |
| `type` | Must be "search". | Yes |
| `dimension` | Input column or virtual column name to filter on. | Yes |
| `query`| A JSON object for the type of search. See [search query spec](#search-query-spec) for more information. | Yes |
| `extractionFn` | [Extraction function](./dimensionspecs.md#extraction-functions) to apply to `dimension` prior to value matching. See [filtering with extraction functions](#filtering-with-extraction-functions) for details. | No |

### Search query spec

#### Contains

| Property | Description | Required |
| -------- | ----------- | -------- |
| `type` | Must be "contains". | Yes |
| `value` | A String value to search. | Yes |
| `caseSensitive` | Whether the string comparison is case-sensitive or not. | No, default is false (insensitive) |

#### Insensitive contains

| Property | Description | Required |
| -------- | ----------- | -------- |
| `type` | Must be "insensitive_contains". | Yes |
| `value` | A String value to search. | Yes |

Note that an "insensitive_contains" search is equivalent to a "contains" search with "caseSensitive": false (or not
provided).

#### Fragment

| Property | Description | Required |
| -------- | ----------- | -------- |
| `type` | Must be "fragment". | Yes |
| `values` | A JSON array of string values to search. | Yes |
| `caseSensitive` | Whether the string comparison is case-sensitive or not. | No, default is false (insensitive) |



## Expression filter

The expression filter allows for the implementation of arbitrary conditions, leveraging the Druid expression system. This filter allows for complete flexibility, but it might be less performant than a combination of the other filters on this page because it can't always use the same optimizations available to other filters.

| Property | Description | Required |
| -------- | ----------- | -------- |
| `type` | Must be "expression" | Yes |
| `expression` | Expression string to evaluate into true or false. See the [Druid expression system](math-expr.md) for more details. | Yes |

### Example: expression based matching

```json
{ 
    "type" : "expression" ,
    "expression" : "((product_type == 42) && (!is_deleted))"
}
```


## JavaScript filter

The JavaScript filter matches a dimension against the specified JavaScript function predicate. The filter matches values for which the function returns true.

| Property | Description | Required |
| -------- | ----------- | -------- |
| `type` | Must be "javascript" | Yes |
| `dimension` | Input column or virtual column name to filter on. | Yes |
| `function` | JavaScript function which accepts the dimension value as a single argument, and returns either true or false. | Yes |
| `extractionFn` | [Extraction function](./dimensionspecs.md#extraction-functions) to apply to `dimension` prior to value matching. See [filtering with extraction functions](#filtering-with-extraction-functions) for details. | No |

### Example: matching any dimension values for the dimension `name` between `'bar'` and `'foo'`

```json
{
  "type" : "javascript",
  "dimension" : "name",
  "function" : "function(x) { return(x >= 'bar' && x <= 'foo') }"
}
```

:::info
 JavaScript-based functionality is disabled by default. Please refer to the Druid [JavaScript programming guide](../development/javascript.md) for guidelines about using Druid's JavaScript functionality, including instructions on how to enable it.
:::

## Extraction filter

:::info
 The extraction filter is now deprecated. The selector filter with an extraction function specified
 provides identical functionality and should be used instead.
:::

Extraction filter matches a dimension using a specific [extraction function](./dimensionspecs.md#extraction-functions).
The following filter matches the values for which the extraction function has a transformation entry `input_key=output_value` where
`output_value` is equal to the filter `value` and `input_key` is present as a dimension.

| Property | Description | Required |
| -------- | ----------- | -------- |
| `type` | Must be "extraction" | Yes |
| `dimension` | Input column or virtual column name to filter on. | Yes |
| `value` | String value to match. | No. If not specified the filter will match NULL values. |
| `extractionFn` | [Extraction function](./dimensionspecs.md#extraction-functions) to apply to `dimension` prior to value matching. See [filtering with extraction functions](#filtering-with-extraction-functions) for details. | No |

### Example: matching dimension values in `[product_1, product_3, product_5]` for the column `product`

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

## Filtering with extraction functions

All filters except the "spatial" filter support extraction functions.
An extraction function is defined by setting the "extractionFn" field on a filter.
See [Extraction function](./dimensionspecs.md#extraction-functions) for more details on extraction functions.

If specified, the extraction function will be used to transform input values before the filter is applied.
The example below shows a selector filter combined with an extraction function. This filter will transform input values
according to the values defined in the lookup map; transformed values will then be matched with the string "bar_1".

### Example: matches dimension values in `[product_1, product_3, product_5]` for the column `product`

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

Note that only string columns and columns produced with the ['auto' ingestion spec](../ingestion/ingestion-spec.md#dimension-objects) also used by [type aware schema discovery](../ingestion/schema-design.md#type-aware-schema-discovery) have bitmap indexes. Queries that filter on other column types must
scan those columns.

### Filtering on multi-value string columns

All filters return true if any one of the dimension values is satisfies the filter.

#### Example: multi-value match behavior
Given a multi-value STRING row with values `['a', 'b', 'c']`, a filter such as

```json
{ "type": "equals", "column": "someMultiValueColumn", "matchValueType": "STRING", "matchValue": "b" }
```
will successfully match the entire row. This can produce sometimes unintuitive behavior when coupled with the implicit UNNEST functionality of Druid [GroupBy](./groupbyquery.md) and [TopN](./topnquery.md) queries.

Additionally, contradictory filters may be defined and perfectly legal in native queries which will not work in SQL.

#### Example: SQL "contradiction"
This query is impossible to express as is in SQL since it is a contradiction that the SQL planner will optimize to false and match nothing.

Given a multi-value STRING row with values `['a', 'b', 'c']`, and filter such as
```json
{
  "type": "and",
  "fields": [
    {
      "type": "equals",
      "column": "someMultiValueColumn",
      "matchValueType": "STRING",
      "matchValue": "a"
    },
    {
      "type": "equals",
      "column": "someMultiValueColumn",
      "matchValueType": "STRING",
      "matchValue": "b"
    }
  ]
}
```
will successfully match the entire row, but not match a row with value `['a', 'c']`.

To express this filter in SQL, use [SQL multi-value string functions](./sql-multivalue-string-functions.md) such as `MV_CONTAINS`, which can be optimized by the planner to the same native filters.

### Filtering on numeric columns

Some filters, such as equality and range filters allow accepting numeric match values directly since they include a secondary `matchValueType` parameter.

When filtering on numeric columns using string based filters such as the selector, in, and bounds filters, you can write filter match values as if they were strings. In most cases, your filter will be
converted into a numeric predicate and will be applied to the numeric column values directly. In some cases (such as
the "regex" filter) the numeric column values will be converted to strings during the scan.

#### Example: filtering on a specific value, `myFloatColumn = 10.1`

```json
{
  "type": "equals",
  "dimension": "myFloatColumn",
  "matchValueType": "FLOAT",
  "value": 10.1
}
```

or with a selector filter:

```json
{
  "type": "selector",
  "dimension": "myFloatColumn",
  "value": "10.1"
}
```

#### Example: filtering on a range of values, `10 <= myFloatColumn < 20`

```json
{
  "type": "range",
  "column": "myFloatColumn",
  "matchvalueType": "FLOAT",
  "lower": 10.1,
  "lowerOpen": false,
  "upper": 20.9,
  "upperOpen": true
}
```

or with a bound filter:

```json
{
  "type": "bound",
  "dimension": "myFloatColumn",
  "ordering": "numeric",
  "lower": "10",
  "lowerStrict": false,
  "upper": "20",
  "upperStrict": true
}
```

### Filtering on the timestamp column

Query filters can also be applied to the timestamp column. The timestamp column has long millisecond values. To refer
to the timestamp column, use the string `__time` as the dimension name. Like numeric dimensions, timestamp filters
should be specified as if the timestamp values were strings.

If you want to interpret the timestamp with a specific format, timezone, or locale, the [Time Format Extraction Function](./dimensionspecs.md#time-format-extraction-function) is useful.

#### Example: filtering on a long timestamp value

```json
{
  "type": "equals",
  "dimension": "__time",
  "matchValueType": "LONG",
  "value": 124457387532
}
```

or with a selector filter:

```json
{
  "type": "selector",
  "dimension": "__time",
  "value": "124457387532"
}
```

#### Example: filtering on day of week using an extraction function

```json
{
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

#### Example: filtering on a set of ISO 8601 intervals

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

