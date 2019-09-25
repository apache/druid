---
id: searchqueryspec
title: "Refining search queries"
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->


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
