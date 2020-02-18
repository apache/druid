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

## Base Docker Image for Integration Tests

### Building Docker Image for Integration Tests

Run the following commands from <DRUIDS_REPO_DIR>/integration-tests/docker-base

To build docker image for Java JDK8:
- docker build -t druidbase:<NEW_TAG> -f jdk8/Dockerfile .

To build docker image for Java JDK11:
- docker build -t druidbase:<NEW_TAG> -f jdk11/Dockerfile .

