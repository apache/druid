---
id: migration-guide
title: "Migration guides"
description: How to migrate from legacy features to get the most from Druid updates
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

In general, when we introduce new features and behaviors into Apache Druid, we make every effort to avoid breaking existing features when introducing new behaviors. However, sometimes there are either bugs or performance limitations with the old behaviors that are not possible to fix in a backward-compatible way. In these cases, we must introduce breaking changes for the future maintainability of Druid. 

The guides in this section outline breaking changes introduced in Druid 25 and later. Each guide provides instructions to migrate to new features.

<!--

## Migrate to arrays from multi-value dimensions

Druid now supports SQL-compliant array types. Whenever possible, you should use the array type over multi-value dimensions. See []()>.

## Migrate to `maxSubqueryBytes` from `maxSubqueryRows`

`maxSubqueryBytes` and `maxSubqueryRows` are guardrails to limit the amount of subquery data stored in the Java heap. `maxSubqueryBytes` is a better alternative to maxSubqueryRows because row-based limits  ignore the size of the individual rows. The values for `maxSubqueryRows` also doesn't take into account the size of the cluster, which is available with the `maxSubqueryBytes` automatic configuration. See []().
-->

## Migrate to front-coded dictionary encoding

Druid encodes string columns into dictionaries for better compression. Front-coded dictionary encoding reduces storage and improves performance by optimizing for strings that share similar beginning substrings. See [Migration guide: front-coded dictionaries](migr-front-coded-dict.md) for more information.