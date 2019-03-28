---
layout: doc_page
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

# Lookback Queries

## Overview
**Lookback Query** is an extension which provides support for computing a metric from multiple different time periods. 

This new query type, processed by the brokers, recursively processes component queries, joins results based on time and 
dimensions, and uses standard post-aggregators to combine values from component queries.

#### High level algorithm example

Consider the following example data:

**Underlying data (date,dimension):**

| Jan,A | Jan,B | Feb,A | Feb,B | Feb,C | Mar,B | Mar,C |
|:-------:|:-------:|:-------:|:-------:|:-------:|:-------:|:-------:|
|   7   |   9   |    1  |   5   |   6   |   3   |   5   |

**Cohort query results (date,dimension):**

| Jan,A | Jan,B | Feb,A | Feb,B | Feb,C |
|:-------:|:-------:|:-------:|:-------:|:-------:|
|   7   |   9   |    1  |   5   |   6   |

**Measurement results (date,dimension):**

| Feb,A | Feb,B | Feb,C | Mar,B | Mar,C |
|:-------:|:-------:|:-------:|:-------:|:-------:|
|   1   |   5   |    6  |   3   |   5   |

**Query results (date,dimension):**

| Feb,A | Feb,B | Feb,C | Mar,B | Mar,C |
|:-------:|:-------:|:-------:|:-------:|:-------:|
|  7,1  |  9,5  |null,6 |  5,3  |  6,5  |

#### Main enhancements provided by this extension:
1. Functionality: Extending druid query functionality and reducing the need for other programs to do this.
2. Performance: Improving performance of not sending a lot of data back to the requestor.
3. Generic approach: Can be used by multiple data types and types of lookback queries in many different environments.

## Operations
To use this extension, make sure to [load](../../operations/including-extensions.html) `druid-lookback-query` only to 
the Broker.

## Configuration
There are currently no configuration properties specific to Lookback Query.

## Limitations and drawbacks
* Overlapping periods between the primary and the lookback (cohort) periods result in Druid doing extra work since
each subquery is run independently and in parallel, the overlapping periods will be computed multiple times.

##Query spec:
* Most properties in the query spec derived from  [groupBy query](../../querying/groupbyquery.html) / [timeseries](../../querying/timeseriesquery.html), see documentation for these query types.

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "lookback"; this is the first thing Druid looks at to figure out how to interpret the query.|yes|
|dataSource|A String or Object defining the data source to query, very similar to a table in a relational database. See [DataSource](../../querying/datasource.html) for more information.|yes|
|postAggregations|Supports only aggregations as input; See [Post Aggregations](../../querying/post-aggregations.html)|no|
|context|An additional JSON Object which can be used to specify certain flags.|no|
|lookbackOffsets|A list of Periods to limit the lookback window.  For example: `P-1D`.|yes|
|lookbackPrefixes|A list of strings that prefixes the results, e.g. for a lookback of 1-D (`lookback_P-1D_`).|no|
|limitSpec|See [LimitSpec](../../querying/limitspec.html)|no|
|having|See [Having](../../querying/having.html)|no|

