---
layout: doc_page
---
# Sort groupBy Query Results
The limitSpec field provides the functionality to sort and limit the set of results from a groupBy query. If you group by a single dimension and are ordering by a single metric, we highly recommend using [TopN Queries](../querying/topnquery.html) instead. The performance will be substantially better. Available options are:

### DefaultLimitSpec

The default limit spec takes a limit and the list of columns to do an orderBy operation over. The grammar is:

```json 
{
    "type"    : "default",
    "limit"   : <integer_value>,
    "columns" : [list of OrderByColumnSpec],
}
```

#### OrderByColumnSpec

OrderByColumnSpecs indicate how to do order by operations. Each order-by condition can be a `jsonString` or a map of the following form:

```json 
{
    "dimension" : "<Any dimension or metric name>",
    "direction" : <"ascending"|"descending">,
    "dimensionOrder" : <"lexicographic"(default)|"alphanumeric"|"strlen"|"numeric">
}
```

If only the dimension is provided (as a JSON string), the default order-by is ascending with lexicographic sorting.

See [Sorting Orders](./sorting-orders.html) for more information on the sorting orders specified by "dimensionOrder".
