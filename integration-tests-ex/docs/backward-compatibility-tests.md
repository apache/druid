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

#### Problem
The idea of this test group is to simulate issues that can arise during rolling upgrade/downgrade.

#### Implementation 
In this test group, the docker compose cluster is launched with services on different versions. 
The docker image for the previous version is built by downloading the previous version druid tar.
Currently, the case where Coordinator and Overlord is on the previous version is tested. 

#### Test coverage 
Existing
- MSQ ingestion
- Native ingestion 
- Stream ingestion
- Querying 

Pending
- Compaction 

#### Limitations 
* `druid-testing-tools` jar is not published. The image for the previous version still uses the 
extension from the current build. 
This could break in case of incompatible changes in this extension. 
In such a scenario the test should be disabled. However, this extension is primarily used to
test specific error scenarios and launch custom node role service (used in HighAvailability test group). 
