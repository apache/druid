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

* Fixes an issue with segment dropping that would not unmap the memory mapped segment files, leaving file descriptors open until the process exits [#18782](https://github.com/apache/druid/pull/18782)
* Fixes protobuf input format URL path loading [#18770](https://github.com/apache/druid/pull/18770)

