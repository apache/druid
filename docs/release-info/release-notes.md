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

Apache Druid 31.0.1 is a patch release that contains an important fix for the new complex metric column compression feature introduced in Druid 31.0.1. It also contains fixes for the web console, the new projections feature, and a fix for a minor performance regression.

## Bug fixes

* Fixes a byte order issue with compression complex metric feature [#17422](https://github.com/apache/druid/pull/17422)
* Fixes an issue with projection segment merging [#17460](https://github.com/apache/druid/pull/17460)
* Fixes web console progress indicator [#17334](https://github.com/apache/druid/pull/17334)
* Fixes a minor performance regression with query processing [#17397](https://github.com/apache/druid/pull/17397)