---
id: migr-three-value-logic
title: "Migration guide: three-value logic and null handling"
sidebar_label: Three-value logic
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

In Apache Druid 28.0.0, the default [null handling](../querying/sql-data-types.md#null-values) mode changed.
Now, to be compliant with the SQL standard, Druid stores segments in a SQL compatible null handling mode by default.

The SQL standard defines any comparison to null to be unknown.
Therefore, according to this three-value logic, `x <> 'some value'` only returns non-null values.

For Druid's columnar format, this means string columns always store the null value as id 0, the first position in the value dictionary and an associated entry in the bitmap value indexes used to filter null values.
Numeric columns also store a null value bitmap index to indicate the null valued rows, which is used to null check aggregations and for filter matching null values.

The default Druid configurations for SQL compatible null handling mode is as follows:

* `druid.generic.useDefaultValueForNull=false`
* `druid.expressions.useStrictBooleans=true`
* `druid.generic.useThreeValueLogicForNativeFilters=true` 

Note Druid has always applied three-value logic by default to expressions.
Therefore, queries such as `(x+y) <> ‘some value’` already exclude null values prior.

At query time, Druid treats data from segments written with the legacy two-value logic as follows:
- Empty strings, `''` are non-null values.
- 0 is a non-null value.

Follow the [Null handling tutorial](../tutorials/tutorial-sql-null.md) to learn how the default null handling works in Druid.

## Legacy null handling and two-value logic
Prior to Druid 28.0.0, Druid defaulted to a legacy mode which used default values instead of nulls.
In legacy mode, Druid segments created at ingestion time have the following characteristics:

- String columns can not distinguish an empty string, '', from null, so Druid treats them as an interchangeable value.
- Numeric columns can not represent null valued rows, and, therefore, store 0 instead of null.

In legacy mode, numeric columns do not have a null value bitmap, and so can have slightly decreased segment sizes.

The Druid configurations for the deprecated legacy mode are as follows:

* `druid.generic.useDefaultValueForNull=true`
* `druid.expressions.useStrictBooleans=false`
* `druid.generic.useThreeValueLogicForNativeFilters=true`

Note that these configurations are deprecated and scheduled for removal.

## Replicate Legacy null handling and two-value logic

If you want to reatain Druid's legacy behavior, which is not compliant with the SQL standard, update your queries.

The following indicate some strategies to include null values when querying for inequality:

- Modify inequality queries to include nulls. For example:
  `x <> 'some value'` becomes `(x <> 'some value' OR x IS NULL)`.
- Use COALESCE to replace nulls with a value:
  `x + 1` becomes ` COALESCE(numeric_value, 0)=1`
