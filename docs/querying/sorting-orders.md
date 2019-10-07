---
id: sorting-orders
title: "Sorting Orders"
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


These sorting orders are used by the [TopNMetricSpec](./topnmetricspec.md), [SearchQuery](./searchquery.md), GroupByQuery's [LimitSpec](./limitspec.md), and [BoundFilter](./filters.html#bound-filter).

## Lexicographic
Sorts values by converting Strings to their UTF-8 byte array representations and comparing lexicographically, byte-by-byte.

## Alphanumeric
Suitable for strings with both numeric and non-numeric content, e.g.: "file12 sorts after file2"

See https://github.com/amjjd/java-alphanum for more details on how this ordering sorts values.

This ordering is not suitable for numbers with decimal points or negative numbers.
* For example, "1.3" precedes "1.15" in this ordering because "15" has more significant digits than "3".
* Negative numbers are sorted after positive numbers (because numeric characters precede the "-" in the negative numbers).

## Numeric
Sorts values as numbers, supports integers and floating point values. Negative values are supported.

This sorting order will try to parse all string values as numbers. Unparseable values are treated as nulls, and nulls precede numbers.

When comparing two unparseable values (e.g., "hello" and "world"), this ordering will sort by comparing the unparsed strings lexicographically.

## Strlen
Sorts values by the their string lengths. When there is a tie, this comparator falls back to using the String compareTo method.

## Version
Sorts values as versions, e.g.: "10.0 sorts after 9.0", "1.0.0-SNAPSHOT sorts after 1.0.0".

See https://maven.apache.org/ref/3.6.0/maven-artifact/apidocs/org/apache/maven/artifact/versioning/ComparableVersion.html for more details on how this ordering sorts values.