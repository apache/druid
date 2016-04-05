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

The selector filter supports the use of extraction functions, see [Filtering with Extraction Functions](#filtering-with-extraction-functions) for details.

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


### Bound filter

Bound filter can be used to filter by comparing dimension values to an upper value or/and a lower value. 
By default Comparison is string based and **case sensitive**.
To use numeric comparison you can set `alphaNumeric` to `true`.
By default the bound filter is a not a strict inclusion `inputString <= upper && inputSting >= lower`.

The bound filter supports the use of extraction functions, see [Filtering with Extraction Functions](#filtering-with-extraction-functions) for details.

The grammar for a bound filter is as follows:

```json
{
    "type": "bound",
    "dimension": "age",
    "lower": "21",
    "upper": "31" ,
    "alphaNumeric": true
}
```
Equivalent to retain column if `21 <= age <= 31`

```json
{
    "type": "bound",
    "dimension": "name",
    "lower": "foo",
    "upper": "hoo"
}
```

Equivalent to retain column if `foo <= name <= hoo`

In order to have a strict inclusion user can set `lowerStrict` or/and `upperStrict` to `true`

To have strict bounds:

```json
{
    "type": "bound",
    "dimension": "age",
    "lower": "21",
    "lowerStrict": true,
    "upper": "31" ,
    "upperStrict": true,
    "alphaNumeric": true
}
```
Equivalent to retain column if `21 < age < 31`

To have strict upper bound:

```json
{
    "type": "bound",
    "dimension": "age",
    "lower": "21",
    "upper": "31" ,
    "upperStrict": true,
    "alphaNumeric": true
}
```

Equivalent to retain column if `21 <= age < 31`

To compare to only an upper bound or lowe bound

```json
{
    "type": "bound",
    "dimension": "age",
    "upper": "31" ,
    "upperStrict": true,
    "alphaNumeric": true
}
```

Equivalent to retain column if `age < 31`

```json
{
    "type": "bound",
    "dimension": "age",
    "lower": "18" ,
    "alphaNumeric": true
}
```

Equivalent to retain column if ` 18 <= age`

For `alphaNumeric` comparator, in case of the dimension value includes none-digits you may expect **fuzzy matching**
If dimension value starts with a none digit, the filter will consider it out of range (`value < lowerBound` and `value > upperBound`)
If dimension value starts with digit and contains a none digits comparing will be done character wise.  
For instance suppose lower bound is `100` and value is `10K` the filter will match (`100 < 10K` returns `true`) since `K` is greater than any digit
Now suppose that the lower bound is `110` the filter will not match (`110 < 10K` returns `false`)




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
|values|A JSON array of String values to run the search over.|yes|
|caseSensitive|Whether strings should be compared as case sensitive or not. Default: false(insensitive)|no|

##### Contains

|property|description|required?|
|--------|-----------|---------|
|type|This String should always be "contains".|yes|
|value|A String value to run the search over.|yes|
|caseSensitive|Whether two string should be compared as case sensitive or not|yes|


### Filtering with Extraction Functions
Some filters optionally support the use of extraction functions.
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