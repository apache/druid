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

# Test Structure

The structure of these integration tests is heavily influenced by the existing
integration test structure. In that previous structure:

* Each test group ran as separate Maven build.
* Each would build an image, start a cluster, run the test, and shut down the cluster.
* Tests were created using [TestNG](https://testng.org/doc/), a long-obsolete
  test framework.
* A `IntegrationTestingConfig` is created from system properties (passed in from
  Maven via `-D<key>=<value>` options).
* A TestNG test runner uses a part of the Druid Guice configuration to inject
  test objects into the tests.
* The test then runs.

To minimize test changes, we try to keep much of the "interface" while changing
the "implementation". Basically:

* The same Docker image is used for all tests.
* Each test defines its own test cluster using Docker Compose.
* Tests are grouped into categories, represented by [JUnit categories](
  https://junit.org/junit4/javadoc/4.12/org/junit/experimental/categories/Categories.html).
* Maven runs one selected category, starting and stopping the test-specific cluster
  for each.
* A cluster-specific directory contains the `docker-compose.yaml` file that defines
  that cluster. Each of these files imports from common definitions.
* Each test is annotated with the `DruidTestRunner` to handle initialization, and
  JUnit `Category` to group the test into a category.
* Categories can share cluster configuration to reduce redundant definitions.
* A `docker.yaml` file defines the test configuration and creates the
  `IntegrationTestingConfig` object.
* Tests run as JUnit tests.

The remainder of this section describes the test internals.

## Test Name

Due to the way the [Failsafe](
https://maven.apache.org/surefire/maven-failsafe-plugin/integration-test-mojo.html)
Maven plugin works, it will look for ITs with
names of the form "IT*.java". This is the preferred form for Druid ITs. That is,
name your test "ITSomething", not "SomethingTest" or "IntegTestSomething", etc.
Many tests are called "ITSomethingTest", but this is a bit repetitious and redundant
since "IT" stands for "Integration Test".

## Cluster Configuration

A test must have a [cluster configuration](compose.md) to define the cluster.
There is a many-to-one relationship between test categories and test clusters.

## Test Configuration

See [Test Configuration](test-config.md) for details on the `docker.yaml` file
that you create for each test module to tell the tests about the cluster you
have defined.

Test configuration allows inheritance so, as in Docker Compose, we define
standard bits in one place, just providing test-specific information in each
tests `docker.yaml` file.

The test code assumes that the test configuration file is in
`src/test/resources/cluster/<category>/docker.yaml`, where `<category>` is
the test category. The test runner loads the configuration file into
(or, specifically that it is on the class path at `/yaml/docker.yaml`)
a `ClusterConfig` instance.

The `ClusterConfig` instance provides the backward-compatible
`IntegrationTestingConfig` instance tha that most existing test cases use.
New tests may want to work with `ClusterConfig` directly as the older interface
is a bit of a muddle in several areas.

## Test Category

Each test is associated with a cluster definition. Maven starts the required
cluster, runs a group of tests, and shuts down the cluster. We use the JUnit
`Category` to identify the category for each test:

```java
@RunWith(DruidTestRunner.class)
@Category(BatchIndex.class)
public class ITIndexerTest extends AbstractITBatchIndexTest
{
  ...
```

The category is a trivial class that exists just to provide the category name.
It can also hold annotations, which will use in a moment. When adding tests, use
and existing category, or define a new one if you want your tests to run in
parallel with other categories.

The `test-cases` module contains all integration tests. However,
Maven can run only one category per Maven run. You specify the category using a
profile of the same name, but with "IT-" prefixed. Thus the Maven profile for the
above `BatchIndex` category is `IT-BatchIndex`.

Test categories may share the same cluster definition. We mark this by adding an
annotation to the category (_not_ test) class. The test class itself:

```java
@RunWith(DruidTestRunner.class)
@Category(InputFormat.class)
public class ITLocalInputSourceAllInputFormatTest extends AbstractLocalInputSourceParallelIndexTest
{
   ...
```

The test category class:

```java
@Cluster(BatchIndex.class)
public class InputFormat
{
}
```

This says that the test above is in the `InputFormat` category, and tests in that
category use the same cluster definition as the `BatchIndex` category. Specifically,
to look for the cluster definition in the `BatchIndex` folders.

### Defined Categories

At present, the following test categories are fully or partly converted:

| Category | Test NG Group | Description |
| -------- | ------------- | ----------- |
| HighAvailability | high-availability | Cluster failover tests |
| BatchIndex | batch-index | Batch indexing tsets |
| InputFormat | input-format | Input format tests |

The new names correspond to class names. The Test NG names were strings.

## Test Runner

The ITs are JUnit test, but use a special test runner to handle configuration.
Test configuration is complex. The easiest way to configure, once the configuration
files are set, is to use the `DruidTestRunner` class:

```java
@RunWith(DruidTestRunner.class)
@Category(MyCategory.class)
public class MyTest
{
  @Inject
  private SomeObject myObject;
  ...

  @Test
  public void myTest()
  {
    ...
```

The test runner loads the configuration files, configures Guice, starts the
Druid lifecycle, and injects the requested values into the class each time
a test method runs. For simple tests, this is all you need.

The test runner validates that the test has a category, and handles the
above mapping from category to cluster definition.

### Parameterization

The `DruidTestRunner` extends `JUnitParamsRunner` to allow parameterized tests.
This class stays discretely out of the way if you don't care about parameters.
To use parameters, see the `CalciteJoinQueryTest` class for an example.

## Initialization

The JUnit-based integration tests are designed to be as simple as possible
to debug. Each test class uses annotations and configuration files to provide
all the information needed to run a test. Once the customer is started
(using `cluster.sh` as described [here](quickstart.md)), each test can
be run from the command line or IDE with no additional command-line parameters.
To do that, we use a `docker.yaml` configuration file that defines all needed
parameters, etc.

A test needs both configuration and a Guice setup. The `DruidTestRunner` ,
along with a number ofm support classes,  mostly hide the details from the tests.
However, you should know what's being done so you can debug.

* JUnit uses the annotation to notice that we've provided a custom
  test runner. (When converting tests, remember to add the required
  annotation.)
* JUnit calls the test class constructor one or more times per test class.
* On the first creation of the test class, `DruidTestRunner` creates an
  instance of the `Initializer` class, via its `Builder` to
  load test configuration, create the Guice injector,
  inject dependencies into the class instanance, and
  start the Druid lifecycle.
* JUnit calls one of the test methods in the class.
* On the second creation of the test class in the same JVM, `DruidTestRunner`
  reuses the existing injector to inject dependencies into the test,
  which avoids the large setup overhead.
* During the first configuration, `DruidTestRunner` causes initialization
  to check the health of each service prior to starting the tests.
* The test is now configured just as it would be from TestNG, and is ready to run.
* `DruidTestRunner` ends the lifecycle after the last test within this class runs.

See [this explanation](dependencies.md) for the gory details.

`DruidTestRunner` loads the basic set of Druid modules to run the basic client
code. Tests may wish to load additional modules specific to that test.

## Custom Configuration

There are times when a test needs additional Guice modules beyond what the
`Initializer` provides. In such cases, you can add a method to customize
configuration.

### Guice Modules

If your test requires additional Guice modules, add them as follows:

```java
@Configure
public static void configure(Initializer.Builder builder)
{
	builder.modules(
		new MyExtraModule(),
		new AnotherModule()
    );
}
```

### Properties

Druid makes heavy use of properties to configure objects via the 'JsonConfigProvider`
mechanism. Integration tests don't read the usual `runtime.properties` files: there
is no such file to read. Instead, properties are set in the test configuration
file. There are times, however, when it makes more sense to hard-code a property
value. This is done in the `@Configure` method:

```java
   builder.property(key, value);
```

You can also bind a property to an environment variable. This value is used when
the environment variable is set. You should also bind a default value:

```java
  builder.property("druid.my.property", 42);
  builder.propertyEnvVarBinding("druid.my.property", "ULTIMATE_ANSWER");
```

A property can also be passed in as either a system property or an environment
variable of the "Docker property environment variable form":

```bash
druid_property_a=foo
./it.sh Category test
```

Or, directly on the command line:

```text
-Ddruid_property_b=bar
```

Property precedence is:

* Properties set in code, as above.
* Properties from the configuration file.
* Properties bound to environment variables, and the environment variable is set.
* Properties from the command line.

The test properties can also be seen as default values for properties provided
in config files or via the command line.

## Resolving Lifecycle Issues

If your test get the dreaded "it doesn't work that way" message, it means that
an injected property in your test is asking Guice to instantiate a lifecycle-managed
class after the lifecycle itself was started. This typically happens if the class
in question is bound via the polymorphic `PolyBind` mechanism which doesn't support
"eager singletons". (If the class in question is not created via `PolyBind`, change
its Guice binding to include `.asEagerSingleton()` rather than `.as(LazySingleton.class)`.
See [this reference](https://github.com/google/guice/wiki/Scopes#eager-singletons).

A quick workaround is to tell the initializer to create an instance before the
lifecycle starts. The easy way to do that is simply to inject the object into a
field in your class. Otherwise, give the builder a hint:

```java
  builder.eagerInstance(ThePeskyComponent.class);
```

## Test Operation

When working with tests, it is helpful to know a bit more about the "magic"
behind `DruidTestRunner`.

Druid's code is designed to run in a server, not a client. Yet, the tests are
clients. This means that tests want to run code in a way that it was not
intended to be run. The existing ITs have mostly figured out how to make that
happen, but result is not very clean. This is an opportunity for improvement.

Druid introduced a set of "injector builders" to organize Guice initialization
a bit. The builders normally build the full server Guice setup. For the ITs,
the builders also allow us to pick and choose which modules to use to define
a client. The `Initializer` class in `it-base` uses the injector builders to
define the "client" modules needed to run tests.

Druid uses the `Lifecycle` class to start and stop services. For this to work,
the managed instance must be created *before* the lifecycle starts. There are
a few items that are lazy singletons. When run in the server, they work fine.
But, when run in tests, we run into a race condition: we want to start the
lifecycle once before the tests start, the inject dependencies into each test
class instance as tests run. But, those injections create the insteance we want
the lifecycle to manage, resulting in a muddle. This is why the `DruidTestRunner`
has that odd "first test. vs. subsequent test" logic.

The prior ITs would start running tests immediately. But, it can take up to a
minute or more for a Druid cluster to stabilize as all the services start
running simultaneously. The previous ITs would use a generic retry up to 240
times to work around the fact that any given test could fail due to the cluster
not being ready. This version does that startup check as part if `DruidTestRunner`.
By the time the tests run, the cluster is up and has reported itself healthy.
That is, your tests can assume a healthy cluster. If a test fails: it indicates
an actual error or race condition.

Specifically, if tests still randomly fail, those tests are telling you something: something
in Druid itself is non-deterministic (such as the delay to see changes to the DB, etc.),
or the tests are making invalid assumptions such as assuming an ordering when there
is none, using a time delay to try to synchronize actions when there should be
some specific synchronization, etc. This means that, in general, you should avoid
the use of the generic retry facility: if you have to retry to get your tests to
work, then the Druid user has to also retry. Unless we document the need to retry
in the API documentation, then having to retry should be considered a bug to be fixed
(perhaps by documenting the need to retry, perhaps by fixing a bug, perhaps by adding
a synchronization API.)

Another benefit of the startup check is that the startup and health-check costs are
paid once per test class. This allows you to structure your
tests as a large number of small tests rather than a few big tests.

## `ClusterConfig` and `ResolvedClusterConfig`

The `ClusterConfig` class is the Java representation of the
[test configuration](test-config.md). The instance is available from the
`Initializer` and by Guice injection.

It is a Jackson-serialized class that handles the "raw" form of
configuration.

The `ClusterConfig.resolve()` method expands includes, applies defaults,
validates values, and returns a `ResolvedClusterConfig` instance used
by tests. `ResolvedClusterConfig` is available via Guice injection.
In most cases, however, you'll use it indirecty via the various clients
described below. Each of those uses `IntegrationTestingConfig` class, an
instance of which is created to read from `ResolvedClusterConfig`.

Remember that each host has two names and two ports:

* The external (or "proxy") host and port, as seen by the machine running
  the tests.
* The internal host and port, as seen by the service itself running
  in the Docker cluster.

The various [config files](test-config.md) provide configurations for
the Docker, K8s and local cluster cases. This means that `resolveProxyHost()`
will resolve to the proxy for Docker, but the actual host for a local cluster.

The original test setup was designed before Druid introduced the router.
A good future improvement is to modify the code to use the router to do the
routing rather than doing it "by hand" in the tests. This means that each
test would use the router port and router API for things like the Overlord
and Coordinator. Then, configuration need only specify the router, not the
other services.

It is also possible to use Router APIs to obtain the server list dynamically
rather than hard-coding the services and ports. If we find cases where tests
must use the APIs directly, then we could either extend the Router API or
implement client-side service lookup.

## `ClusterClient`

The integration tests make many REST calls to the Druid cluster. The tests
contain much copy/paste code to make these calls. The `ClusterClient` class
is intended to gather up these calls so we have a single implementation
rather than many copies. Add methods as needed for additional APIs.

The cluster client is "test aware": it uses the information in
`ClusterConfig` to know how to send the requested API. The methods handle
JSON deserialization, so tests can focus simply on making a call and
checking the results.

## `org.apache.druid.testing.clients`

This package in `integration-tests` has clients for most other parts of
Druid. For example, `CoordinatorResourceTestClient` is a
client for Coordinator calls. These clients are also aware of the test
configuration, by way of the `IntegrationTestingConfig` class, an
instance of which is created to read from `ResolvedClusterConfig`.
