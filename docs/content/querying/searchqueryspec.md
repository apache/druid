---
layout: doc_page
---
# Refining Search Queries
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

If any part of a dimension value contains all of the values specified in this search query spec, regardless of case by default, a "match" occurs. The grammar is:

```json
{ 
  "type" : "fragment",
  "case_sensitive" : false,
  "values" : ["fragment1", "fragment2"]
}
```

ContainsSearchQuerySpec
----------------------------------

If any part of a dimension value contains the value specified in this search query spec, a "match" occurs. The grammar is:

```json
{
  "type"  : "contains",
  "case_sensitive" : true,
  "value" : "some_value"
}
```

RegexSearchQuerySpec
----------------------------------

If any part of a dimension value contains the pattern specified in this search query spec, a "match" occurs. The grammar is:

```json
{
  "type"  : "regex",
  "pattern" : "some_pattern"
}
```