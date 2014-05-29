---
layout: doc_page
---
# Sort groupBy Query Results
The limitSpec field provides the functionality to sort and limit the set of results from a groupBy query. If you group by a single dimension and are ordering by a single metric, we highly recommend using [TopN Queries](TopNQuery.html) instead. The performance will be substantially better. Available options are:

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

OrderByColumnSpecs indicate how to do order by operations. Each order by condition can be a `jsonString` or a map of the following form:

```json 
{
    "dimension" : <Any dimension or metric>,
    "direction" : "ASCENDING OR DESCENDING"
}
```
