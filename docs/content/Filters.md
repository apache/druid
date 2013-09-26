---
layout: doc_page
---
A filter is a JSON object indicating which rows of data should be included in the computation for a query. It’s essentially the equivalent of the WHERE clause in SQL. Druid supports the following types of filters.

### Selector filter

The simplest filter is a selector filter. The selector filter will match a specific dimension with a specific value. Selector filters can be used as the base filters for more complex Boolean expressions of filters.

The grammar for a SELECTOR filter is as follows:

    <code>"filter": {
        "type": "selector",
        "dimension": <dimension_string>,
        "value": <dimension_value_string>
    }
    </code>

This is the equivalent of `WHERE <dimension_string> = '<dimension_value_string>'`.

### Regular expression filter

The regular expression filter is similar to the selector filter, but using regular expressions. It matches the specified dimension with the given pattern. The pattern can be any standard [Java regular expression](http://docs.oracle.com/javase/6/docs/api/java/util/regex/Pattern.html).

    <code>"filter": {
        "type": "regex",
        "dimension": <dimension_string>,
        "pattern": <pattern_string>
    }
    </code>

### Logical expression filters

#### AND

The grammar for an AND filter is as follows:

    <code>"filter": {
        "type": "and",
        "fields": [<filter>, <filter>, ...]
    }
    </code>

The filters in fields can be any other filter defined on this page.

#### OR

The grammar for an OR filter is as follows:

    <code>"filter": {
        "type": "or",
        "fields": [<filter>, <filter>, ...]
    }
    </code>

The filters in fields can be any other filter defined on this page.

#### NOT

The grammar for a NOT filter is as follows:

    <code>"filter": {
        "type": "not",
        "field": <filter>
    }
    </code>

The filter specified at field can be any other filter defined on this page.

### JavaScript filter

The JavaScript filter matches a dimension against the specified JavaScript function predicate. The filter matches values for which the function returns true.

The function takes a single argument, the dimension value, and returns either true or false.

    <code>{
        "type" : "javascript",
        "dimension" : <dimension_string>,
        "function" : "function(value) { <...> }"
    }
    </code>

**Example**
The following matches any dimension values for the dimension `name` between `'bar'` and `'foo'`

    <code>{
        "type" : "javascript",
        "dimension" : "name",
        "function" : "function(x) { return(x >= 'bar' && x <= 'foo') }"
    }
    </code>
