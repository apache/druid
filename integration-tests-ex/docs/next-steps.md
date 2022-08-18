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

# Future Work

The present version establishes the new IT framework. Work completed to
date includes:

* Restructure the Docker images to use the Druid produced from the
  Maven build. Use "official" images for dependencies.
* Restructure the Docker compose files.
* Create the cluster configuration mechanisms.
* Convert one "test group" to a sub-module: "high-availability".
* Create the `pom.xml`, scripts and other knick-knacks needed to tie
  everything together.
* Create the initial test without using security settings to aid
  debugging.

However, *much* work remains:

* Convert remaining tests.
* Decide when we need full security. Convert the many certificate
  setup scripts.
* Support cluster types other than Docker.

## Open Tasks

The following detail items are open:

* Disable a test if the `disabled` type is set in the test configuration
  file. Apply it to disable the HA tests for all but Docker.
* Handle missing config files: generate a "dummy" that is disabled.
* When launching a container build or test run from Maven, write
  environment variables to a `target/env.sh` file so that the user
  doesn't have to find them manually to run the helper scripts.
* There is some redundancy in each test group project. Figure out
  solutions:
  * The `cluster.sh` script
  * Boilerplate in the `pom.xml` file.
* Move test data from `/resources` to `/shared/data`. Build up the
  data directory from multiple sources during cluster launch.
* Sort out which data and spec files are actually used. Remove those
  which are not used. Sort the files by test-specific and shared
  across tests by moving them into different directories.

## Later Tasks

The "public" and "integration test" versions of the Docker images have diverged significantly,
which makes it harder to "test what we ship." Differences include:

* Different base image
* Different ways to set up dependencies.
* Different paths within the container.
* Different launch scripts.
* The test images place Druid in `/usr/local`, the public images in `/opt`.

The tests do want to do things beyond what the "public" image does. However, this should
not require a fork of the builds. To address this issue:

* Extend this project to create a base common to the "public" and integration test images.
* Extend the integration test image to build on top of the public image.
