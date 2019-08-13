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

From the root of the repo, run `docker build -t druid:tag -f distribution/docker/Dockerfile .`

## Run

Edit `environment` to suite. Run `docker-compose -f distribution/docker/docker-compose.yml up`

## MySQL Database Connector

This image contains solely the postgres metadata database connector. If you need
the mysql metadata storage connector, consider adding these lines before the `addgroup`
run-command.

```
RUN wget -O /opt/druid/extensions/mysql-metadata-storage/mysql-connector-java-5.1.38.jar http://central.maven.org/maven2/mysql/mysql-connector-java/5.1.38/mysql-connector-java-5.1.38.jar \
 && sha256sum --ignore-missing -c /src/distribution/docker/sha256sums.txt \
 && ln -s /opt/druid/extensions/mysql-metadata-storage/mysql-connector-java-5.1.38.jar /opt/druid/lib
```

Alternatively, `cd src/distribution/docker; docker build -t druid:mysql --build-arg DRUID_RELEASE=upstream -f Dockerfile.mysql .`

where `upstream` is the version to use as the base (e.g. druid:0.14.0 from Dockerhub)
