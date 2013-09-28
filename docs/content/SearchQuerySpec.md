---
layout: doc_page
---
Search query specs define how a "match" is defined between a search value and a dimension value. The available search query specs are:

InsensitiveContainsSearchQuerySpec
----------------------------------

If any part of a dimension value contains the value specified in this search query spec, regardless of case, a "match" occurs. The grammar is:

```json
{
  "type"  : "insensitive_contains",
  "value" : "some_value"
}
```

FragmentSearchQuerySpec
-----------------------

If any part of a dimension value contains any of the values specified in this search query spec, regardless of case, a "match" occurs. The grammar is:

```json
{ 
  "type" : "fragment",
  "values" : ["fragment1", "fragment2"]
}
```
