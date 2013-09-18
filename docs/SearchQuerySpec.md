---
layout: default
---
Search query specs define how a “match” is defined between a search value and a dimension value. The available search query specs are:

InsensitiveContainsSearchQuerySpec
----------------------------------

If any part of a dimension value contains the value specified in this search query spec, regardless of case, a “match” occurs. The grammar is:

    <code>{
      "type"  : "insensitive_contains",
      "value" : "some_value"
    }
    </code>

FragmentSearchQuerySpec
-----------------------

If any part of a dimension value contains any of the values specified in this search query spec, regardless of case, a “match” occurs. The grammar is:

    <code>{ 
      "type" : "fragment",
      "values" : ["fragment1", "fragment2"]
    }
    </code>
