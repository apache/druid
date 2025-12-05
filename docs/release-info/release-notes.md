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

Apache Druid 35.0.1 is a patch release that contains important fixes for historical server segment dropping and the protobuf input format extension.

For information about new features in Druid 35, see the [Druid 35 release notes](https://druid.apache.org/docs/35.0.0/release-info/release-notes/).

## Bug fixes

* Fixed an issue where dropping segments didn't remove the memory mapping of segment files, leaving file descriptors of the deleted files open until the process exits [#18782](https://github.com/apache/druid/pull/18782)
* Fixed an issue where the URL path for the protobuf input format wasn't loading [#18770](https://github.com/apache/druid/pull/18770)

## Dependency updates
* Updated lz4-java to 1.8.1 [#18804](https://github.com/apache/druid/pull/18804)

