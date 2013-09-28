---
layout: doc_page
---
The orderBy field provides the functionality to sort and limit the set of results from a groupBy query. Available options are:

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
