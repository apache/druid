---
layout: doc_page
title: "Schema Changes"
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

# Schema Changes

Schemas for datasources can change at any time and Apache Druid (incubating) supports different schemas among segments.

## Replacing Segments

Druid uniquely 
identifies segments using the datasource, interval, version, and partition number. The partition number is only visible in the segment id if 
there are multiple segments created for some granularity of time. For example, if you have hourly segments, but you 
have more data in an hour than a single segment can hold, you can create multiple segments for the same hour. These segments will share 
the same datasource, interval, and version, but have linearly increasing partition numbers.

```
foo_2015-01-01/2015-01-02_v1_0
foo_2015-01-01/2015-01-02_v1_1
foo_2015-01-01/2015-01-02_v1_2
```

In the example segments above, the dataSource = foo, interval = 2015-01-01/2015-01-02, version = v1, partitionNum = 0. 
If at some later point in time, you reindex the data with a new schema, the newly created segments will have a higher version id.

```
foo_2015-01-01/2015-01-02_v2_0
foo_2015-01-01/2015-01-02_v2_1
foo_2015-01-01/2015-01-02_v2_2
```

Druid batch indexing (either Hadoop-based or IndexTask-based) guarantees atomic updates on an interval-by-interval basis. 
In our example, until all `v2` segments for `2015-01-01/2015-01-02` are loaded in a Druid cluster, queries exclusively use `v1` segments. 
Once all `v2` segments are loaded and queryable, all queries ignore `v1` segments and switch to the `v2` segments. 
Shortly afterwards, the `v1` segments are unloaded from the cluster.

Note that updates that span multiple segment intervals are only atomic within each interval. They are not atomic across the entire update. 
For example, you have segments such as the following:

```
foo_2015-01-01/2015-01-02_v1_0
foo_2015-01-02/2015-01-03_v1_1
foo_2015-01-03/2015-01-04_v1_2
```

`v2` segments will be loaded into the cluster as soon as they are built and replace `v1` segments for the period of time the 
segments overlap. Before v2 segments are completely loaded, your cluster may have a mixture of `v1` and `v2` segments.
 
```
foo_2015-01-01/2015-01-02_v1_0
foo_2015-01-02/2015-01-03_v2_1
foo_2015-01-03/2015-01-04_v1_2
``` 
 
In this case, queries may hit a mixture of `v1` and `v2` segments.

## Different Schemas Among Segments

Druid segments for the same datasource may have different schemas. If a string column (dimension) exists in one segment but not 
another, queries that involve both segments still work. Queries for the segment missing the dimension will behave as if the dimension has only null values. 
Similarly, if one segment has a numeric column (metric) but another does not, queries on the segment missing the 
metric will generally "do the right thing". Aggregations over this missing metric behave as if the metric were missing.
