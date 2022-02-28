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

# Revised Integration Tests

This directory builds a Docker image for Druid, then uses that image, along
with test configuration to run tests. This version greatly evolves the
integration tests from the earlier form. See the last section for details.

## Contents

* [Goals](#Goals)
* [Quickstart](docs/quickstart.md)
* [Maven configuration](docs/maven.md)
* [Travis integration](docs/travis.md)
* [Docker image](docs/docker.md)
* [Druid configuration](docs/druid-config.md)
* [Docker Compose configuration](docs/compose.md)
* [Test configuration](docs/test-config.md)
* [Test structure](docs/tests.md)
* [Test runtime semantics](docs/runtime.md)
* [Dependencies](docs/dependencies.md)
* [Debugging](docs/debugging.md)

Background information

* [Next steps](docs/next-steps.md)
* [Test conversion](docs/conversion.md) - How to convert existing tests.
* [History](docs/history.md) - Comparison of prior integration tests.

## Goals

The goal of the present version is to simplify development.

* Speed up the Druid test image build by avoiding download of
  dependencies. (Instead, any such dependencies are managed by
  Maven and reside in the local build cache.)
* Use official images for dependencies to avoid the need to
  download, install, and manage those dependencies.
* Ensure that it is easy to manually build the image, launch
  a cluster, and run a test against the cluster.
* Convert tests to JUnit so that they will easily run in your
  favorite IDE, just like other Druid tests.
* Use the actual Druid build from `distribution` so we know
  what is tested.
* Leverage, don't fight, Maven.
* Run the integration tests easily on a typical development machine.

By meeting these goals, you can quickly:

* Build the Druid distribution.
* Build the Druid image. (< 1 minute)
* Launch the cluster for the particular test. (a few seconds)
* Run the test any number of times in your debugger.
* Clean up the test artifacts.

The result is that the fastest path to develop a Druid patch or
feature is:

* Create a normal unit test and run it to verify your code.
* Create an integration test that double-checks the code in
  a live cluster.

The result should also speed up Travis builds because a single
Maven run can produce the Druid artifacts and run all integration tests.

We can have the option to run the integration tests using different
settings: one with the MariaDB connector, say, another with MySQL.
Each of these might be a separate Travis run, since the tests run
with different configurations (as given by a Maven profile). But,
we won't have to run a separte Travis job for each integration test
group.
