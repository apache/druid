---
layout: doc_page
---

# Virtual Columns

Virtual columns are queryable column "views" created from a set of columns during a query. 

A virtual column can potentially draw from multiple underlying columns, although a virtual column always presents itself as a single column.

Virtual columns can be used as dimensions or as inputs to aggregators.

Each Druid query can accept a list of virtual columns as a parameter. The following scan query is provided as an example:

```
{
 "queryType": "scan",
 "dataSource": "page_data",
 "columns":[],
 "virtualColumns": [
    {
      "type": "expression",
      "name": "fooPage",
      "expression": "concat('foo' + page)",
      "outputType": "STRING"
    },
    {
      "type": "expression",
      "name": "tripleWordCount",
      "expression": "wordCount * 3",
      "outputType": "LONG"
    }
  ],
 "intervals": [
   "2013-01-01/2019-01-02"
 ] 
}
```


## Virtual Column Types

### Expression virtual column

The expression virtual column has the following syntax:

```
{
  "type": "expression",
  "name": <name of the virtual column>,
  "expression": <row expression>,
  "outputType": <output value type of expression>
}
```

|property|description|required?|
|--------|-----------|---------|
|name|The name of the virtual column.|yes|
|expression|An [expression](../misc/math-expr.html) that takes a row as input and outputs a value for the virtual column.|yes|
|outputType|The expression's output will be coerced to this type. Can be LONG, FLOAT, DOUBLE, or STRING.|no, default is FLOAT|