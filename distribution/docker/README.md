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

From the root of the repo, run following command:

```bash
DOCKER_BUILDKIT=1 docker build -t apache/druid:tag -f distribution/docker/Dockerfile .
```

> NOTE:
> By default, the Druid image is built and run in Java 11 environment.
> If you want to run Druid in Java 8 environment, you need to add `--build-arg JDK_VERSION=8` to above command.

## Run

Edit `environment` file to suite. Run:

```bash
docker-compose -f distribution/docker/docker-compose.yml up
```

## MySQL Database Connector

This image contains solely the postgres metadata storage connector. If you
need the mysql metadata storage connector, you can use Dockerfile.mysql to add
it to the base image above.

```bash
docker build -t apache/druid:tag-mysql --build-arg DRUID_RELEASE=apache/druid:tag -f distribution/docker/Dockerfile.mysql .
```

where `druid:tag` is the version of Druid image to use as the base.
