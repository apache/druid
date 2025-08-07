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

# Testcontainers for Apache Druid

This module contains Testcontainers implementation for running Apache Druid services.
The dependencies in the pom must be minimal so that this module can be used independently by libraries and codebases outside Druid to run Testcontainers.

## Usage

```
final DruidContainer container = new DruidContainer("overlord", "apache/druid:33.0.0");
container.start();
```

## Standard Druid Images

1. `apache/druid:33.0.0`
2. `apache/druid:32.0.1`
3. `apache/druid:31.0.2`

## Next steps

- Add Druid command for running a starter cluster in a single container
- Publish this extension as a community module for Testcontainers
- Contribute the code to the main Testcontainers repository
