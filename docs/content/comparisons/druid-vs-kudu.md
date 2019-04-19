---
layout: doc_page
title: "Apache Druid (incubating) vs Kudu"
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

# Druid vs Kudu

Kudu's storage format enables single row updates, whereas updates to existing Druid segments requires recreating the segment, so theoretically  
the process for updating old values should be higher latency in Druid. However, the requirements in Kudu for maintaining extra head space to store 
updates as well as organizing data by id instead of time has the potential to introduce some extra latency and accessing 
of data that is not need to answer a query at query time. 

Druid summarizes/rollups up data at ingestion time, which in practice reduces the raw data that needs to be 
stored significantly (up to 40 times on average), and increases performance of scanning raw data significantly. 
Druid segments also contain bitmap indexes for fast filtering, which Kudu does not currently support. 
Druid's segment architecture is heavily geared towards fast aggregates and filters, and for OLAP workflows. Appends are very 
fast in Druid, whereas updates of older data is higher latency. This is by design as the data Druid is good for is typically event data, 
and does not need to be updated too frequently. Kudu supports arbitrary primary keys with uniqueness constraints, and 
efficient lookup by ranges of those keys. Kudu chooses not to include the execution engine, but supports sufficient 
operations so as to allow node-local processing from the execution engines. This means that Kudu can support multiple frameworks on the same data (eg MR, Spark, and SQL). 
Druid includes its own query layer that allows it to push down aggregations and computations directly to data processes for faster query processing.
