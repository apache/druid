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

[![Coverage Status](https://img.shields.io/codecov/c/gh/apache/druid?logo=codecov)](https://codecov.io/gh/apache/druid)
[![Docker](https://img.shields.io/badge/container-docker-blue.svg?logo=docker)](https://hub.docker.com/r/apache/druid)
[![Helm](https://img.shields.io/badge/helm-druid-5F90AB?logo=helm)](https://github.com/asdf2014/druid-helm)
<!--- Following badges are disabled until they can be fixed: -->
<!--- [![Inspections Status](https://img.shields.io/teamcity/http/teamcity.jetbrains.com/s/OpenSourceProjects_Druid_Inspections.svg?label=TeamCity%20inspections)](https://teamcity.jetbrains.com/viewType.html?buildTypeId=OpenSourceProjects_Druid_Inspections) -->

| Workflow                             | Status                                                       |
| :----------------------------------- | :----------------------------------------------------------- |
| ⚙️ CodeQL Config                      | [![codeql-config](https://img.shields.io/github/actions/workflow/status/apache/druid/codeql-config.yml?branch=master&logo=github-actions&style=flat-square)](https://github.com/apache/druid/actions/workflows/codeql-config.yml) |
| 🔍 CodeQL                             | [![codeql](https://img.shields.io/github/actions/workflow/status/apache/druid/codeql.yml?branch=master&logo=github-actions&style=flat-square)](https://github.com/apache/druid/actions/workflows/codeql.yml) |
| 🕒 Cron Job ITS                       | [![cron-job-its](https://img.shields.io/github/actions/workflow/status/apache/druid/cron-job-its.yml?branch=master&logo=github-actions&style=flat-square)](https://github.com/apache/druid/actions/workflows/cron-job-its.yml) |
| 🏷️ Labeler                            | [![labeler](https://img.shields.io/github/actions/workflow/status/apache/druid/labeler.yml?branch=master&logo=github-actions&style=flat-square)](https://github.com/apache/druid/actions/workflows/labeler.yml) |
| ♻️ Reusable Revised ITS               | [![reusable-revised-its](https://img.shields.io/github/actions/workflow/status/apache/druid/reusable-revised-its.yml?branch=master&logo=github-actions&style=flat-square)](https://github.com/apache/druid/actions/workflows/reusable-revised-its.yml) |
| ♻️ Reusable Standard ITS              | [![reusable-standard-its](https://img.shields.io/github/actions/workflow/status/apache/druid/reusable-standard-its.yml?branch=master&logo=github-actions&style=flat-square)](https://github.com/apache/druid/actions/workflows/reusable-standard-its.yml) |
| ♻️ Reusable Unit Tests                | [![reusable-unit-tests](https://img.shields.io/github/actions/workflow/status/apache/druid/reusable-unit-tests.yml?branch=master&logo=github-actions&style=flat-square)](https://github.com/apache/druid/actions/workflows/reusable-unit-tests.yml) |
| 🔄 Revised ITS                        | [![revised-its](https://img.shields.io/github/actions/workflow/status/apache/druid/revised-its.yml?branch=master&logo=github-actions&style=flat-square)](https://github.com/apache/druid/actions/workflows/revised-its.yml) |
| 🔧 Standard ITS                       | [![standard-its](https://img.shields.io/github/actions/workflow/status/apache/druid/standard-its.yml?branch=master&logo=github-actions&style=flat-square)](https://github.com/apache/druid/actions/workflows/standard-its.yml) |
| 🛠️ Static Checks                      | [![static-checks](https://img.shields.io/github/actions/workflow/status/apache/druid/static-checks.yml?branch=master&logo=github-actions&style=flat-square)](https://github.com/apache/druid/actions/workflows/static-checks.yml) |
| 🧪 Unit and Integration Tests Unified | [![unit-and-integration-tests-unified](https://img.shields.io/github/actions/workflow/status/apache/druid/unit-and-integration-tests-unified.yml?branch=master&logo=github-actions&style=flat-square)](https://github.com/apache/druid/actions/workflows/unit-and-integration-tests-unified.yml) |
| 🔬 Unit Tests                         | [![unit-tests](https://img.shields.io/github/actions/workflow/status/apache/druid/unit-tests.yml?branch=master&logo=github-actions&style=flat-square)](https://github.com/apache/druid/actions/workflows/unit-tests.yml) |

---

[![Website](https://img.shields.io/badge/Website-druid.apache.org-blue?style=flat-square&logo=apache-druid)](https://druid.apache.org/)
[![Twitter](https://img.shields.io/badge/Twitter-%40druidio-blue?style=flat-square&logo=twitter)](https://twitter.com/druidio)
[![Download](https://img.shields.io/badge/Download-Downloads_Page-blue?style=flat-square&logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCA0NDggNTEyIj4KICA8cGF0aCBkPSJNNDQxLjkgMTY3LjNsLTE5LjgtMTkuOGMtNC43LTQuNy0xMi4zLTQuNy0xNyAwbC0xODIuMSAxODAuNy0xODEuMS0xODAuN2MtNC43LTQuNy0xMi4zLTQuNy0xNyAwbC0xOS44IDE5LjhjLTQuNyA0LjctNC43IDEyLjMgMCAxN2wyMDkuNCAyMDkuNGM0LjcgNC43IDEyLjMgNC43IDE3IDBsMjA5LjQtMjA5LjRjNC43LTQuNyA0LjctMTIuMyAwLTE3eiIvPgo8L3N2Zz4K)](https://druid.apache.org/downloads.html)
[![Get Started](https://img.shields.io/badge/Get_Started-Getting_Started-blue?style=flat-square&logo=quicklook)](#getting-started)
[![Documentation](https://img.shields.io/badge/Documentation-Design_Docs-blue?style=flat-square&logo=read-the-docs)](https://druid.apache.org/docs/latest/design/)
[![Community](https://img.shields.io/badge/Community-Join_Us-blue?style=flat-square&logo=slack)](#community)
[![Build](https://img.shields.io/badge/Build-Building_From_Source-blue?style=flat-square&logo=github-actions)](#building-from-source)
[![Contribute](https://img.shields.io/badge/Contribute-How_to_Contribute-blue?style=flat-square&logo=github)](#contributing)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue?style=flat-square&logo=apache)](#license)

---

## Apache Druid

Druid is a high performance real-time analytics database. Druid's main value add is to reduce time to insight and action.

Druid is designed for workflows where fast queries and ingest really matter. Druid excels at powering UIs, running operational (ad-hoc) queries, or handling high concurrency. Consider Druid as an open source alternative to data warehouses for a variety of use cases. The [design documentation](https://druid.apache.org/docs/latest/design/architecture.html) explains the key concepts.

### Getting started

You can get started with Druid with our [local](https://druid.apache.org/docs/latest/tutorials/quickstart.html) or [Docker](http://druid.apache.org/docs/latest/tutorials/docker.html) quickstart.

Druid provides a rich set of APIs (via HTTP and [JDBC](https://druid.apache.org/docs/latest/querying/sql.html#jdbc)) for loading, managing, and querying your data.
You can also interact with Druid via the built-in [web console](https://druid.apache.org/docs/latest/operations/web-console.html) (shown below).

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

See the [latest documentation](https://druid.apache.org/docs/latest/) for the documentation for the current official release. If you need information on a previous release, you can browse [previous releases documentation](https://druid.apache.org/docs/).

Make documentation and tutorials updates in [`/docs`](https://github.com/apache/druid/tree/master/docs) using [Markdown](https://www.markdownguide.org/) or extended Markdown [(MDX)](https://mdxjs.com/). Then, open a pull request.

To build the site locally, you need Node 16.14 or higher and to install Docusaurus 2 with `npm|yarn install`  in the `website` directory. Then you can run `npm|yarn start` to launch a local build of the docs.

If you're looking to update non-doc pages like Use Cases, those files are in the [`druid-website-src`](https://github.com/apache/druid-website-src/tree/master) repo.

### Community

Visit the official project [community](https://druid.apache.org/community/) page to read about getting involved in contributing to Apache Druid, and how we help one another use and operate Druid.

* Druid users can find help in the [`druid-user`](https://groups.google.com/forum/#!forum/druid-user) mailing list on Google Groups, and have more technical conversations in `#troubleshooting` on Slack.
* Druid development discussions take place in the [`druid-dev`](https://lists.apache.org/list.html?dev@druid.apache.org) mailing list ([dev@druid.apache.org](https://lists.apache.org/list.html?dev@druid.apache.org)).  Subscribe by emailing [dev-subscribe@druid.apache.org](mailto:dev-subscribe@druid.apache.org).  For live conversations, join the `#dev` channel on Slack.

Check out the official [community](https://druid.apache.org/community/) page for details of how to join the community Slack channels.

Find articles written by community members and a calendar of upcoming events on the [project site](https://druid.apache.org/) - contribute your own events and articles by submitting a PR in the [`apache/druid-website-src`](https://github.com/apache/druid-website-src/tree/master/_data) repository.

### Building from source

Please note that JDK 8 or JDK 11 is required to build Druid.

See the latest [build guide](https://druid.apache.org/docs/latest/development/build.html) for instructions on building Apache Druid from source.

### Contributing

Please follow the [community guidelines](https://druid.apache.org/community/) for contributing.

For instructions on setting up IntelliJ [dev/intellij-setup.md](dev/intellij-setup.md)

### License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
