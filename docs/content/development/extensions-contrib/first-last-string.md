---
layout: doc_page
---

# First/Last String Module

To use these aggregators, make sure you [include](../../operations/including-extensions.html) the extension in your config file:

```
druid.extensions.loadList=["druid-first-last-string"]
```

## First String aggregator

`firstString` computes the metric value with the minimum timestamp or `null` if no row exist

```json
{
  "type" : "firstString",
  "name" : <output_name>,
  "fieldName" : <metric_name>,
  "maxStringBytes" : <integer>
}
```

## Last String aggregator

```json
{
  "type" : "lastString",
  "name" : <output_name>,
  "fieldName" : <metric_name>,
  "maxStringBytes" : <integer>
}
```

`lastString` computes the metric value with the maximum timestamp or `null` if no row exist



Note: The default value of `maxStringBytes` is 1024.
