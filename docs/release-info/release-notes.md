---
id: release-notes
title: "Release notes"
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

<!--Replace {{DRUIDVERSION}} with the correct Druid version.-->

Apache Druid 31.0.1 is a patch release that contains important fixes for topN queries using query granularity other than 'ALL' and for the new complex metric column compression feature introduced in Druid 31.0.0. It also contains fixes for the web console, the new projections feature, and a fix for a minor performance regression.

For information about new features in Druid 31, see the [Druid 31 release notes](https://druid.apache.org/docs/31.0.0/release-info/release-notes/).

## Bug fixes

* Fixes an issue with topN queries that use a query granularity other than 'ALL', which could cause some query correctness issues [#17565](https://github.com/apache/druid/pull/17565)
* Fixes an issue with complex metric compression that caused some data to be read incorrectly, resulting in segment data corruption or system instability due to out-of-memory exceptions. We recommend that you reingest data if you use compression for complex metric columns [#17422](https://github.com/apache/druid/pull/17422)
* Fixes an issue with projection segment merging [#17460](https://github.com/apache/druid/pull/17460)
* Fixes web console progress indicator [#17334](https://github.com/apache/druid/pull/17334)
* Fixes a minor performance regression with query processing [#17397](https://github.com/apache/druid/pull/17397)

## Dependency updates
Bumped the versions of the following dependencies:
* Jetty to 9.4.56.v20240826 [#17385](https://github.com/apache/druid/pull/17385)
* Apache Kafka to 3.9.0 [#17513](https://github.com/apache/druid/pull/17513)
