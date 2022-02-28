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

# Maven Structure

The integration tests are built and run as part of Druid's Maven script.
The tests run against the Maven-created Druid build, and so
appear in the root `pom.xml` file *after* the `distribution`
project which builds the Druid tarball. Thus, when you run Maven, we
first build the Druid modules, then assemble them into the tarball, then
build the Docker test image based on that tarball, and finally run the
integration tests using the test image. The result is that a single pass
through Maven will build Druid and run the tests.

## Run the Projects

Use the following command to run the ITs, assuming `DRUID_DEV` points
to your Druid development directory:


```bash
cd $DRUID_DEV
mvn clean install -P dist,test-image,docker-tests \
    -P skip-static-checks -Ddruid.console.skip=true \
    -Dmaven.javadoc.skip=true -DskipUTs=true
```

The various pieces are:

* `clean`: Remove any existing artifacts, and the existing Docker image.
* `install`: Build the Druid code and write it to the local Maven repo.
* `-P dist`: Create the Druid distribution tarball by pulling jars from
  the local Maven repo.
* `-P test-image`: Build the Docker images by grabbing the Druid tarball
  and pulling additional dependencies into the local repo, then stage them
  for Docker.
* `-P docker-tests`: Run the new ITs.
* Everything else: ignore parts of the build not needed for the ITs, such
  as static checks, unit tests, the console, Javadoc, etc.

Once you've done the above once, you can do just the specific part you want
to repeat during development. See below for details.

### Skipping Tests

The revised integration tests use the [Maven failsafe plugin]
(https://maven.apache.org/surefire/maven-failsafe-plugin/) which shared
code with the [Maven Surefire plugin](
https://maven.apache.org/surefire/maven-surefire-plugin/)
used to run unit tests. One shared item is the `skipTests` flag.
Via a bit of [Maven config creativity](
https://stackoverflow.com/questions/6612344/prevent-unit-tests-but-allow-integration-tests-in-maven)
we define extra properties to control the two kinds of tests:

* `-DskipUTs=true` to skip Surefire (unit) tests.
* `-P docker-tests` to enable Failsafe (integration) tests.
* `-DskipTests=true` to skip all tests.

## Modules

The key modules in the above flow include:

* `distribution`: Builds the Druid tarball which the ITs exercise.

The IT process is a collection of sub-modules:

* `it-tools`: Testing extensions added to the Docker image.
* `it-image`: Builds the Druid test Docker image.
* `it-base`: Test code used by all tests.
* `<group>`: The remaining projects represent test groups: sets of
  tests which use the same Cluster configuration. A new group is created
  when test need a novel cluster configuration.

At present, the following test groups are fully or partly converted:

* `it-high-availability`: Fully converted (there is only one test file.)
* `it-batch-indexer`: Partially converted.

The "it-" prefix ensures that the test-related projects are grouped
together in your IDE.

### Two-level Module Structure

It turns out that, for obscure reasons, we must use a "flat" module
structure under the root Druid `pom.xml` even though the modules
themselves are in folders within `docker-tests`. That is, we cannot
create a `docker-tests` Maven module to hold the IT modules. The
reason has to do with the fact that Maven has no good way to
obtain the directory that contains the root `pom.xml` file. Yet,
we need this directory to find configuration files for some of the
static checking tools. Though there is a [directory plugin](
https://github.com/jdcasey/directory-maven-plugin) that *looks* like
it should work, we then run into a another issue: if we invoke a
goal directly from the `mvn` command line: Maven happily ignores the
`validate` and `initialize` phases where we'd set the directory path.
By using a two-level module structure, we can punt and just always
assume that `${project.parent.basedir}` points to the root Druid
directory. More than you wanted to know, but now you know why there
is no `docker-tests` module as there should be.

As a result, you may have to hunt in your IDE to find the non-project
files in the `docker-tests` directory. Look in the root `druid`
project, in the `docker-tests` folder.

## `org.apache.druid.testing2`

The `org.apache.druid.testing2` is temporary: it holds code from the
`integration-tests` `org.apache.druid.testing` package adapted to work
in the revised environment. Some classes have the same name in both
places. The goal is to merge the `testing2` package back into
`testing` at some future point when the tests are all upgraded.

## Profiles

Integration test artifacts are build only if you specifically request them
using a profile.

* `-P test-image` builds the test Docker image.
* `-P docker-tests` runs the integration tests.

The two profiles allow you to build the test image once during debugging,
and reuse it across multiple test runs. (See [Debugging](debugging.md).)

When run in Travis, the tests will run one after another in Maven project
order.

## Dependencies

The Docker image inclues three third-party dependencies not included in the
Druid build:

* MySQL connector
* MariaDB connector
* Kafka Protobuf provider

We use dependency rules in the `test-image/pom.xml` file to cause Maven to download
these dependencies into the Maven cache, then we use the
`maven-dependency-plugin` to copy those dependencies into a Docker directory,
and we use Docker to copy the files into the image. This approach avoids the need
to pull the dependency from a remote repository into the image directly, and thus
both speeds up the build, and is kinder to the upstream repositories.

If you add additional dependencies, please follow the above process. See the
`pom.xml` files for examples.

## Environment Variables

Maven communicates with Docker and Docker Compose via environment variables
set in the `exec-maven-plugin` of various `pom.xml` files. The environment
variables then flow into either the Docker build script (`Dockerfile`) or the
various Docker Compose scripts (`docker-compose.yaml`). It can be tedious to follow
this flow. A quick outline:

* Maven, via `exec-maven-plugin`, sets environment variables typically from Maven's
  own variables for things like versions.
* `exec-maven-plugin` invokes a script to do the needes shell fiddling. The environment
  variables are visible to the script and implicitly passed to whatever the script
  calls.
* When building, the script passes the environment variables to Docker as build
  arguments.
* `Dockerfile` typically passes the build arguments to the image as environment
  variables as the same name.
* When running, the script passes environment variables implicitly to Docker Compose,
  which uses them in the various service configurations and/or environment variable
  settings passed to each container.

If you find you need a new parameter in either the Docker build or the Docker Compose
configuration:

* First ensure that there is a Maven setting that holds the desired parameter.
* Wire it through the relevant `exec-maven-plugin` sections.
* Pass it to Docker or Docker Compose as above.

The easiest way to test is to insert (or enable, or view) the environment in the
image:

```shell
env
```

The output will typically go into the Docker output or the Docker Compose logs.

## Shortcuts


Since Druid's `pom.xml` file is quite large, Maven can be a bit slow when
all you want to do is to build the Docker image. To speed things up a bit,
you can build just the docker image. Assuming `DRUID_DEV` points to your development
area:

```bash
cd $DRUID_DEV
mvn install -P test-image
```

Here "install" means to install the image in the local Docker image repository.

Even with the above, Druid's Maven configuration has many plugins which Maven
must grind through on each
build if you are impatient, you can actually remove Maven from the build path for
the image and tests by using a script to do what `exec-maven-plugin` does. You just
have to be sure to set the needed environment variables to the proper values.
Once you do that, creating an image, or launching a cluster, is quite fast.
See [the Docker section](docker.md) for details.
