---
id: limitspec
title: "Sorting and limiting (groupBy)"
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

> Apache Druid supports two query languages: [Druid SQL](sql.md) and [native queries](querying.md).
> This document describes the native
> language. For information about sorting in SQL, refer to the [SQL documentation](sql.md#order-by).

The limitSpec field provides the functionality to sort and limit the set of results from a groupBy query. If you group by a single dimension and are ordering by a single metric, we highly recommend using [TopN Queries](../querying/topnquery.md) instead. The performance will be substantially better. Available options are:

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

See [Sorting Orders](./sorting-orders.md) for more information on the sorting orders specified by "dimensionOrder".
