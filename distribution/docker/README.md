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

### Building images on Apple M1/M2
To build images on Apple M1/M2, you need to follow the instructions in this section.

1. build Druid distribution from the root of the repo
   ```bash
   mvn clean package -DskipTests -Pdist
   ```
2. build target image
   ```
   DOCKER_BUILDKIT=1 docker build -t apache/druid:tag -f distribution/docker/Dockerfile --build-arg BUILD_FROM_SOURCE=false .
   ```

## Run

1. Edit `distribution/docker/docker-compose.yml` file to change the tag of Druid's images to the tag that's used in the 'Build' phase above.
2. Edit `environment` file to suite if necessary.
3. Run:
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
