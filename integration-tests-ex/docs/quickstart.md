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

# Quickstart

If you just need to know how to build, run and use the tests, this
is the place. You can refer to the detailed material later as you
add new tests or work to improve the tests.

## Example Test

When first learning the framework, you can try thing out using the
`HighAvailability` test. Of the tests converted thus far, it is the
one that runs for the shortest time (on the order of a minute or two.)

## Working with Individual Tests

To work with tests for development and debugging, you can break the
above all-in-one step into a number of sub-steps.

* [Build Druid](#Build Druid).
* [Build the Docker image](#Build the Docker Image).
* [Start a cluster](#Start a Cluster).
* [Run a test from the command line](#Run a Test from the Command Line).
* [Run a test from an IDE](#Run a Test from an IDE).
* [Stop the cluster](#Stop the Cluster).
* [Clean up](#Clean Up).

## Build Druid

The integration tests start with a Druid distribution in `distribution/target`,
which you can build using your preferred Maven command line. Simplest:

```
cd $DRUID_DEV
./it.sh dist
```

Or, in detail:

For example:
To make the text a bit simpler, define a variable for the standard settings:

```bash
export MAVEN_IGNORE=-P skip-static-checks,skip-tests -Dmaven.javadoc.skip=true
```

```bash
mvn clean package -P dist $MAVEN_IGNORE -T1.0C
```

Hint: put this into a script somewhere, such as a `~/bin` directory and
add that to your `PATH`. A good name is `bdru` (Build DRUid).
Here is the full script:

```bash
#! /bin/bash

mvn clean package -P dist,skip-static-checks,skip-tests \
    -Dmaven.javadoc.skip=true -T1.0C $*
```

Now you can run the any Druid build with the above script. To resume a
build:

```bash
> bdru -rf :foo
```

## Build the Docker Image

You must rebuild the Docker image whenever you rebuild the Druid distribution,
since the image includes the distribution. You also will want to rebuild the
image if you change the `it-image` project which contains the build scripts.

```bash
./it.sh image
```

In detail, and assuming `DRUID_DEV` points to your Druid build directory,
to build the image (only):

```bash
cd $DRUID_DEV/integration-tests-ex/image
mvn install -P test-image $MAVEN_IGNORE
```

The above has you `cd` into the project to avoid the need to disable all the
unwanted bits of the Maven build.

See [this page](docker.md) for more information.

## Run an IT from the Command Line

```bash
./it.sh test <category>
```

Or, in detail:

```bash
mvn verify -P docker-tests,IT-<category> -pl :druid-it-cases \
    -P skip-static-checks,skip-tests -Dmaven.javadoc.skip=true -DskipUTs=true
```

Where `<category>` is one of the test categories.

Or

```bash
cd $DRUID_DEV/integration-tests-ex/cases
mvn verify -P skip-static-checks,docker-tests,IT-<category> \
    -Dmaven.javadoc.skip=true -DskipUTs=true \
    -pl :druid-it-cases
```

If the test fails, find the Druid logs in `target/shared/logs` within the
test group project.

## Start a Cluster

The previous generation of tests were organized into TestNG groups. This
revision moves those groups into Maven modules. Each group has a distinct
cluster configuration. (In fact, it is the cluster configuration which defines
the group: we combine all tests with the same configuration into the same module.)
So, to start a cluster, you have to pick a group to run. See
[this list](maven.md#Modules) for the list of groups.

```bash
./it.sh up <category>
```

Or, in detail:

```bash
cd $DRUID_DEV/integration-tests-ex/cases
./cluster.sh <category> up
```

You can use Docker Desktop to monitor the cluster. Give things about 30 seconds
or a minute: if something is going to fail, it will happen during starup and you'll
see that one or more containers exited unexpectedly.

Remember to first shut down any Druid cluster you may already be running on
your machine.

See [this page](docker.md) for more information.

## Run a Test from an IDE

To run an IT in your IDE:

* Find the IT to run.
* Run it as a JUnit test.

The tests are specifically designed to require no command-line setup: you can
just run them directly.

## Stop the Cluster

Once you are done with your cluster, you can stop it as follows:


```bash
./it.sh down <category>
```

Or, in detail:

```bash
cd $DRUID_DEV/integration-tests-ex/<group>
./cluster.sh <category> down
```

## Clean Up

You can remove the Docker image when you no longer need it:

```bash
cd $DRUID_DEV
mvn clean -P test-image
```

It is usually fine to skip this step: the next image build will replace
the current one anyway.
