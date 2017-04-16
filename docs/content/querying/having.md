---
layout: doc_page
---
# Filter groupBy Query Results
A having clause is a JSON object identifying which rows from a groupBy query should be returned, by specifying conditions on aggregated values.

It is essentially the equivalent of the HAVING clause in SQL.

Druid supports the following types of having clauses.

### Query filters

Query filter HavingSpecs allow all [Druid query filters](filters.html) to be used in the Having part of the query.

The grammar for a query filter HavingSpec is:

```json
{
    "type" : "filter",
    "filter" : <any Druid query filter>
}
```

For example, to use a selector filter:


```json
{
    "type" : "filter",
    "filter" : {
      "type": "selector",
      "dimension" : "<dimension>",
      "value" : "<dimension_value>"
    }
}
```

You can use "filter" HavingSpecs to filter on the timestamp of result rows by applying a filter to the "\_\_time"
column.

### Numeric filters

The simplest having clause is a numeric filter.
Numeric filters can be used as the base filters for more complex boolean expressions of filters.

Here's an example of a having-clause numeric filter:

```json
{
    "type": "greaterThan",
    "aggregation": "myAggMetric",
    "value": 100
}
```

#### Equal To

The equalTo filter will match rows with a specific aggregate value.
The grammar for an `equalTo` filter is as follows:

```json
{
    "type": "equalTo",
    "aggregation": "<aggregate_metric>",
    "value": <numeric_value>
}
```

This is the equivalent of `HAVING <aggregate> = <value>`.

#### Greater Than

The greaterThan filter will match rows with aggregate values greater than the given value.
The grammar for a `greaterThan` filter is as follows:

```json
{
    "type": "greaterThan",
    "aggregation": "<aggregate_metric>",
    "value": <numeric_value>
}
```

This is the equivalent of `HAVING <aggregate> > <value>`.

#### Less Than

The lessThan filter will match rows with aggregate values less than the specified value.
The grammar for a `greaterThan` filter is as follows:

```json
{
    "type": "lessThan",
    "aggregation": "<aggregate_metric>",
    "value": <numeric_value>
}
```

This is the equivalent of `HAVING <aggregate> < <value>`.



### Dimension Selector Filter

#### dimSelector

The dimSelector filter will match rows with dimension values equal to the specified value.
The grammar for a `dimSelector` filter is as follows:

```json
{
    "type": "dimSelector",
    "dimension": "<dimension>",
    "value": <dimension_value>
}
```


### Logical expression filters

#### AND

The grammar for an AND filter is as follows:

```json
{
    "type": "and",
    "havingSpecs": [<having clause>, <having clause>, ...]
}
```

The having clauses in `havingSpecs` can be any other having clause defined on this page.

#### OR

The grammar for an OR filter is as follows:

```json
{
    "type": "or",
    "havingSpecs": [<having clause>, <having clause>, ...]
}
```

The having clauses in `havingSpecs` can be any other having clause defined on this page.

#### NOT

The grammar for a NOT filter is as follows:

```json
{
    "type": "not",
    "havingSpec": <having clause>
}
```

The having clause specified at `havingSpec` can be any other having clause defined on this page.
