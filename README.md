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

[![Slack](https://img.shields.io/badge/slack-%23druid-72eff8?logo=slack)](https://druid.apache.org/community/join-slack)
[![Build Status](https://travis-ci.org/apache/incubator-druid.svg?branch=master)](https://travis-ci.org/apache/incubator-druid)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/apache/incubator-druid.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/apache/incubator-druid/context:java)
[![Coverage Status](https://img.shields.io/codecov/c/gh/apache/incubator-druid)](https://codecov.io/gh/apache/incubator-druid)
[![Docker](https://img.shields.io/badge/container-docker-blue.svg)](https://hub.docker.com/r/apache/incubator-druid)
<!--- Following badges are disabled until they can be fixed: -->
<!--- [![Inspections Status](https://img.shields.io/teamcity/http/teamcity.jetbrains.com/s/OpenSourceProjects_Druid_Inspections.svg?label=TeamCity%20inspections)](https://teamcity.jetbrains.com/viewType.html?buildTypeId=OpenSourceProjects_Druid_Inspections) -->

## Apache Druid (incubating)

Apache Druid (incubating) is a high performance analytics data store for event-driven data.

*Disclaimer: Apache Druid is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the Apache Incubator. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.*

### Getting started

You can get started with Druid with our [quickstart](https://druid.apache.org/docs/latest/tutorials/quickstart.html).

Druid provides a rich set of APIs (via HTTP and [JDBC](https://druid.apache.org/docs/latest/querying/sql.html#jdbc)) for loading, managing, and querying your data.
You can also interact with Druid via the [built-in console](https://druid.apache.org/docs/latest/operations/druid-console.html) (shown below).

#### Load data

[![data loader Kafka](https://user-images.githubusercontent.com/177816/65819337-054eac80-e1d0-11e9-8842-97b92d8c6159.gif)](https://druid.apache.org/docs/latest/ingestion/index.html)

Load [streaming](https://druid.apache.org/docs/latest/ingestion/index.html#streaming) and [batch](https://druid.apache.org/docs/latest/ingestion/index.html#batch) data using a point-and-click wizard to guide you through ingestion setup. Monitor one off tasks and ingestion supervisors.

#### Manage the cluster

[![management](https://user-images.githubusercontent.com/177816/65819338-08499d00-e1d0-11e9-80fe-faee9e9468cb.gif)](https://druid.apache.org/docs/latest/ingestion/data-management.html)

Manage your cluster with ease. Get a view of your datasources, [segments](https://druid.apache.org/docs/latest/design/segments.html), [ingestion tasks](https://druid.apache.org/docs/latest/ingestion/tasks.html), and [servers]() from one convenient location. All powered by [SQL systems tables](https://druid.apache.org/docs/latest/querying/sql.html#metadata-tables) allowing you to see the underlying query for each view.

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

### License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

