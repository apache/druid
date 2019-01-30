---
layout: doc_page
title: "Joins"
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

# Joins

Druid has limited support for joins through [query-time lookups](../querying/lookups.html). The common use case of 
query-time lookups is to replace one dimension value (e.g. a String ID) with another value (e.g. a human-readable String value). This is similar to a star-schema join.

Druid does not yet have full support for joins. Although Druid’s storage format would allow for the implementation 
of joins (there is no loss of fidelity for columns included as dimensions), full support for joins have not yet been implemented yet 
for the following reasons:

1. Scaling join queries has been, in our professional experience, 
a constant bottleneck of working with distributed databases.
2. The incremental gains in functionality are perceived to be 
of less value than the anticipated problems with managing 
highly concurrent, join-heavy workloads.

A join query is essentially the merging of two or more streams of data based on a shared set of keys. The primary  
high-level strategies for join queries we are aware of are a hash-based strategy or a 
sorted-merge strategy. The hash-based strategy requires that all but 
one data set be available as something that looks like a hash table, 
a lookup operation is then performed on this hash table for every 
row in the “primary” stream. The sorted-merge strategy assumes 
that each stream is sorted by the join key and thus allows for the incremental 
joining of the streams. Each of these strategies, however, 
requires the materialization of some number of the streams either in 
sorted order or in a hash table form.

When all sides of the join are significantly large tables (> 1 billion 
records), materializing the pre-join streams requires complex 
distributed memory management. The complexity of the memory 
management is only amplified by the fact that we are targeting highly 
concurrent, multi-tenant workloads.
