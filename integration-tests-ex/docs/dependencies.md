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

# Dependencies

The Docker tests have a number of dependencies which are important to understand
when making changes or debugging problems.

## Third-Party Libraries

As described in the [Docker](docker.md) section, the Docker image contains Druid
plus three external dependencies:

* The MySQL client library
* The MariaDB client library
* The Kafka protobuf provider

These libraries are not shipped with Druid itself. Instead, we add them to the
image as follows:

* Dependencies are listed in the `test-image/pom.xml` file.
* Maven fetches the dependencides from an upstream repo and places them
  into the local Maven cache.
* The `test-image/pom.xml` file uses the `maven-dependency-plugin`
  to copy these dependencies from the local repo into the
  `target/docker` directory.
* The `Dockerfile` copies the dependencies into the `/usr/local/druid/lib`
  directory after `build-image.sh` has unpacked the Druid distribution
  into `/usr/local/druid`.

The key benefit is that the dependencies are downloaded once and are
served from the local repo afterwards.

## Third-Party Servers

As described in the [Docker](docker.md) section, we use third-party
"official" images for three of our external server dependencies:

* [MySQL](https://hub.docker.com/_/mysql). This image is configured
  to create the Druid database and user upon startup.
* [ZooKeeper](https://hub.docker.com/_/zookeeper).
* [Kafka](https://hub.docker.com/r/bitnami/kafka/). There is no
  "official" image so we use the one from Bitnami.

See `compose/dependencies.yaml` for the Docker Compose configuration
for each of these services.

Other integration tests use additional servers such as Hadoop.
We will want to track down official images for those as well.

## Guice and Lifecycle

Nothing will consume more of your time than fighting with Druid's
Guice and Lifecycle mechanisms. These mechanisms are designed to do
exactly one thing: configure the Druid server. They are a nightmare
to use in other configurations such as unit or integration tests.

### Guice Modules

Druid has *many* Guice modules. There is no documentation to explain
which components are available from which modules, or their dependencies.
So, if one needs component X, one has to hunt through the source to
find the module that provides X. (Or, one has to "just know.") There
is no trick other than putting in the time to do the research, watching
things fail, and trying harder.

In addition, modules have implicit dependencies: to use module Y you
must also include module Z. Again, there is no documentation, you have
to know or figure it out.

The modules are designed to work only in the server, so they assume
the entire server is avaialble. Once we have a way that the modules
work in the server, we don't mess with it. But, in tests, we want
to use a subset because tests are clients, not a server. So, we end
up fighting to reuse a system that was designed for exactly one use
case: the server. The result is either a huge amount of time fiddling
to get things right or (as in the original integration tests), we just
include everything and pretend we are a server.

There is no obvious solution, it is just a massive time sink at
present.

### Druid Modules

Many of the modules we want to use in integration test are
`DruidModule`s. These go beyond the usual Guice modules to provide
extra functionality, some of which is vital in tests:

* The modules have depenencies injected from the "startup injector."
* The modules provide Jackson modules needed to serialized JSON.

The `Initialization` class provides the mechanisms needed to work
with `DruidModule`s, but only when creating a server: that same class
has a strong opinion about which modules to include based on the
assumption that the process is a server (or a Druid tool which acts
like a server.)

The code here refactored `Initialization` a bit to allow us to
use the functionality without being forced to accept all the default
server modules. The upside is that we don't end up having to fake the
tests to look like servers. The downside is the issue above: we have to
deal with the dependency nightmare.

### Lifecycle Race Conditions

Druid uses the `Lifecycle` class to manage object initialization. The
Lifecycle expects instances to be registered before the lifecycle
starts, after which it is impossible to register new instances.

The lifecycle works out startup order based on Guice injection
dependencies. Thus, if a constructor is `X(Y y, Z y)` Guice knows
to create an `Y` and `Z` before creating `X`. `Lifecycle` leverages
this knowledge to start `Y` and `Z` before starting `X`.

This works only if, during module creation, something has a
dependency on `X`. Else, if `X` is a `LazySingleton` it won't be
created until it is first used. But, by then, the `Lifecycle` will have
started and you'll get the dreaded "It doesn't work that way" error.

### Guice and Lifecycle in Tests

In the server, this works fine: there is exactly one usage of each
singleton, and the various modules have appearently been tweaked to
ensure every lifecycle-aware object is referenced (thus created,
this registerd in the lifecycle) by some other module.

In tests, however, this system breaks down. Maven runs a series of
tests (via `failsafe`), each of which has any number of test methods.
The test driver is free to create any number of test class instances.

When using the `Lifecycle` mechanism in tests, we would prefer to
set up the injector, and run the lifecycle, once per test class. This
is easy to do with the JUnit `@BeforeClass` annotation. But, when we
try this, the livecycle race condition issue slams us hard.

Tests want to reference certain components, such as `DruidNodeDiscoveryProvider`
which require `CuratorFramework` which is provided by a module that
registers a component with the lifecycle. Because of the lazy singleton
pattern, `DruidNodeDiscoveryProvider` (and hence its dependenencies)
are created when first referenced, which occurs when JUnit instantiates
the test class, which happens after the Guice/Lifecycle setup in
`@BeforeClass`. And, we get our "It doesn't work that way" error.

We can then try to move Guice/Lifecycle creation into the test class
constuctor, but then we'll watch as JUnit creates multiple instances
and we end up running initialization over and over. Further, it seems
there are race conditions when we do that (haven't figure out the
details), and we get strange errors. Further, we end up thrashing
the very complex initializaiton logic (which is a great stress test,
but we need to it only once, not on every test.)

A hacky compromise is to add a caching layer: do the initialization in
the constructor, so we can inject the member variables, which creates
references, which causes the comonents to be created, which causes them
to register with the `Lifecycle` at the proper time. In the second
constructor call, we reuse the injector created in the first call.
Since we simply reuse the same singletons, we should not run into
Livecycle race conditions. The `@AfterClass` JUnit annotation is pressed
into service to shut down the lifecycle after all tests run.

## Testing Tools And the Custom Node Role

The Druid extension `druid-testing-tools` (Maven project
`extensions-core/testing-tools` provides an extension to be loaded
into the Druid image along with the Druid distribution and third-party
libraries.

The `integration-tests` provides additional components (such as the
custom node role) that must be placed in the image, but uses an
entirely different mechanism.

There is no documentation to explain why we do `testing-tools` one
way, the custom node role a different way. Is there a reason other than
the items were created by different people at different times who chose
to use different approaches?

In an ideal world, `testing-tools` would contain the custom node role:
there would be a single way to provide test-only extensions. However,
since we must maintain backward compatibility with `integration-tests`,
and that module is a nightmare to modify, we must use a short-term
compromise.

For now, we punt: we make a copy of `druid-testing-tools`, add the
`integraton-tools` custom node role, and call it `testing-tools-ex`.
See [`testing-tools/README`](../testing-tools/README.md) for the
details.

## Integration Tests and `base-test`

The `integration-tests` project contains the set of existing TestNG-based
tests as well as a large number of utilities used by the tests.
The revised framework adds its own utilities.

The utilities speicfic to the new tests resides in the `base-test`
sub-project. We include the `integration-test` project to reusse its
utilities.

This does create a potential conflict: as we convert tests, the tests
here will have the same name as tests in the `integration-test`
package, which causes duplicate class names on the class path: never
a good thing.

The ideal solution would be to move the test utilities to a new
sub-project within `integration-tests` and have both the new and old test
projects include the resulting jar.

For now, we use a "shadow" approach, we use the `org.apache.druid.testsEx`
package name for new tests so names do not conflict with the
`org.apache.druid.tests` name used in `integration-tests`. Eventually,
if all tests are renamed, we can rename the `testsEx` package back
to `tests`.

In a few cases, the utilitiy classes make asumptions about the test
setup which does not match the new setup. In this case, we make a copy
of the class and apply needed changes. At present, only one class has this
issue:

* `DruidClusterAdminClient` - interfaces with Docker using hard-coded
  container names.

The old versions are in `org.apache.druid.testing.utils` in
`integration-tests`, the new versions in `org.apache.druid.testing2.utils`
in this project.
