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

[![Slack](https://img.shields.io/badge/slack-%23druid-72eff8?logo=slack)](https://s.apache.org/slack-invite)
[![Build Status](https://api.travis-ci.com/apache/druid.svg?branch=master)](https://travis-ci.com/apache/druid)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/apache/druid.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/apache/druid/context:java)
[![Coverage Status](https://img.shields.io/codecov/c/gh/apache/druid)](https://codecov.io/gh/apache/druid)
[![Docker](https://img.shields.io/badge/container-docker-blue.svg)](https://hub.docker.com/r/apache/druid)
[![Helm](https://img.shields.io/badge/helm-druid-5F90AB?logo=helm)](https://artifacthub.io/packages/helm/helm-incubator/druid)
<!--- Following badges are disabled until they can be fixed: -->
<!--- [![Inspections Status](https://img.shields.io/teamcity/http/teamcity.jetbrains.com/s/OpenSourceProjects_Druid_Inspections.svg?label=TeamCity%20inspections)](https://teamcity.jetbrains.com/viewType.html?buildTypeId=OpenSourceProjects_Druid_Inspections) -->

---

[Website](https://druid.apache.org/) |
[Documentation](https://druid.apache.org/docs/latest/design/) |
[Developer Mailing List](https://lists.apache.org/list.html?dev@druid.apache.org) |
[User Mailing List](https://groups.google.com/forum/#!forum/druid-user) |
[Slack](https://s.apache.org/slack-invite) |
[Twitter](https://twitter.com/druidio) |
[Download](https://druid.apache.org/downloads.html)

---

## Apache Druid

Druid is a high performance real-time analytics database. Druid's main value add is to reduce time to insight and action.

Druid is designed for workflows where fast queries and ingest really matter. Druid excels at powering UIs, running operational (ad-hoc) queries, or handling high concurrency. Consider Druid as an open source alternative to data warehouses for a variety of use cases.

### Getting started

You can get started with Druid with our [local](https://druid.apache.org/docs/latest/tutorials/quickstart.html) or [Docker](http://druid.apache.org/docs/latest/tutorials/docker.html) quickstart.

Druid provides a rich set of APIs (via HTTP and [JDBC](https://druid.apache.org/docs/latest/querying/sql.html#jdbc)) for loading, managing, and querying your data.
You can also interact with Druid via the [built-in console](https://druid.apache.org/docs/latest/operations/druid-console.html) (shown below).

#### Load data

[![data loader Kafka](https://user-images.githubusercontent.com/177816/65819337-054eac80-e1d0-11e9-8842-97b92d8c6159.gif)](https://druid.apache.org/docs/latest/ingestion/index.html)

Load [streaming](https://druid.apache.org/docs/latest/ingestion/index.html#streaming) and [batch](https://druid.apache.org/docs/latest/ingestion/index.html#batch) data using a point-and-click wizard to guide you through ingestion setup. Monitor one off tasks and ingestion supervisors.

#### Manage the cluster

[![management](https://user-images.githubusercontent.com/177816/65819338-08499d00-e1d0-11e9-80fe-faee9e9468cb.gif)](https://druid.apache.org/docs/latest/ingestion/data-management.html)

Manage your cluster with ease. Get a view of your [datasources](https://druid.apache.org/docs/latest/design/architecture.html), [segments](https://druid.apache.org/docs/latest/design/segments.html), [ingestion tasks](https://druid.apache.org/docs/latest/ingestion/tasks.html), and [services](https://druid.apache.org/docs/latest/design/processes.html) from one convenient location. All powered by [SQL systems tables](https://druid.apache.org/docs/latest/querying/sql.html#metadata-tables), allowing you to see the underlying query for each view.

#### Issue queries

[![query view combo](https://user-images.githubusercontent.com/177816/65819341-0c75ba80-e1d0-11e9-9730-0f2d084defcc.gif)](https://druid.apache.org/docs/latest/querying/sql.html)

Use the built-in query workbench to prototype [DruidSQL](https://druid.apache.org/docs/latest/querying/sql.html) and [native](https://druid.apache.org/docs/latest/querying/querying.html) queries or connect one of the [many tools](https://druid.apache.org/libraries.html) that help you make the most out of Druid.

### Documentation

You can find the [documentation for the latest Druid release](https://druid.apache.org/docs/latest/) on
the [project website](https://druid.apache.org).

If you would like to contribute documentation, please do so under
`/docs` in this repository and submit a pull request.

### Community

Community support is available on the
[druid-user mailing list](https://groups.google.com/forum/#!forum/druid-user), which
is hosted at Google Groups.

Development discussions occur on [dev@druid.apache.org](https://lists.apache.org/list.html?dev@druid.apache.org), which
you can subscribe to by emailing [dev-subscribe@druid.apache.org](mailto:dev-subscribe@druid.apache.org).

Chat with Druid committers and users in real-time on the `#druid` channel in the Apache Slack team. Please use [this invitation link to join the ASF Slack](https://s.apache.org/slack-invite), and once joined, go into the `#druid` channel.

### Building from source

Please note that JDK 8 is required to build Druid.

For instructions on building Druid from source, see [docs/development/build.md](docs/development/build.md)

### Contributing

Please follow the [community guidelines](https://druid.apache.org/community/) for contributing.

For instructions on setting up IntelliJ [dev/intellij-setup.md](dev/intellij-setup.md)

### License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
