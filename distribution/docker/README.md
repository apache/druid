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

## Build

From the root of the repo, run `docker build -t apache/druid:tag -f distribution/docker/Dockerfile .`

## Run

Edit `environment` to suite. Run `docker-compose -f distribution/docker/docker-compose.yml up`

## Java 11 (experimental)

From the root of the repo, run `docker build -t apache/druid:tag -f distribution/docker/Dockerfile.java11 .` which will build Druid to run in a Java 11 environment.

## MySQL Database Connector

This image contains solely the postgres metadata storage connector. If you
need the mysql metadata storage connector, you can use Dockerfile.mysql to add
it to the base image above.

`docker build -t apache/druid:tag-mysql --build-arg DRUID_RELEASE=apache/druid:tag -f distribution/docker/Dockerfile.mysql .`

where `druid:tag` is the version to use as the base.
