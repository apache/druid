---
layout: doc_page
---
The orderBy field provides the functionality to sort and limit the set of results from a groupBy query. Available options are:

### DefaultLimitSpec

The doc_page limit spec takes a limit and the list of columns to do an orderBy operation over. The grammar is:

    <code> 
    {
        "type"    : "doc_page",
        "limit"   : <integer_value>,
        "columns" : [list of OrderByColumnSpec],
    }
    </code>

#### OrderByColumnSpec

OrderByColumnSpecs indicate how to do order by operations. Each order by condition can be a <code>String</code> or a map of the following form:

    <code> 
    {
        "dimension"    : "<Any dimension or metric>",
        "direction"   : "ASCENDING OR DESCENDING"
    }
    </code>
