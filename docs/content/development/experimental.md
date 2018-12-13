---
layout: doc_page
title: "Experimental Features"
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

# Experimental Features

Experimental features are features we have developed but have not fully tested in a production environment. If you choose to try them out, there will likely be edge cases that we have not covered. We would love feedback on any of these features, whether they are bug reports, suggestions for improvement, or letting us know they work as intended.

<div class="note caution">
APIs for experimental features may change in backwards incompatible ways.
</div>

To enable experimental features, include their artifacts in the configuration runtime.properties file, e.g.,

```
druid.extensions.loadList=["druid-histogram"]
```

The configuration files for all the indexer and query nodes need to be updated with this.
