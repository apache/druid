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

Integration Testing
===================

To run integration tests, you have to specify the druid cluster the
tests should use.

Druid comes with the mvn profile integration-tests
for setting up druid running in docker containers, and using that
cluster to run the integration tests.

To use a druid cluster that is already running, use the
mvn profile int-tests-config-file, which uses a configuration file
describing the cluster.

Integration Testing Using Docker
-------------------

Before starting, if you don't already have docker on your machine, install it as described on
[Docker installation instructions](https://docs.docker.com/install/). Ensure that you
have at least 4GiB of memory allocated to the docker engine. (You can verify it
under Preferences > Resources > Advanced.)

Also set the `DOCKER_IP`
environment variable to localhost on your system, as follows:

```bash
export DOCKER_IP=127.0.0.1
```

Optionally, you can also set `APACHE_ARCHIVE_MIRROR_HOST` to override `https://archive.apache.org` host. This host is used to download archives such as hadoop and kafka during building docker images:

```bash
export APACHE_ARCHIVE_MIRROR_HOST=https://example.com/remote-generic-repo
```

## Running tests against auto brought up Docker containers

This section describes how to start integration tests against Docker containers which will be brought up automatically by following commands.
If you want to build Docker images and run tests separately, see the next section.

To run all tests from a test group using Docker and Maven run the following command:

```bash
mvn verify -P integration-tests -Dgroups=<test_group>
```

The list of test groups can be found at
`integration-tests/src/test/java/org/apache/druid/tests/TestNGGroup.java`.

### Run a single test

To run only a single test using Maven:

```bash
mvn verify -P integration-tests -Dgroups=<test_group> -Dit.test=<test_name>
```

Parameters:

* Test Group: Required, as certain test tasks for setup and cleanup are based on the test group. You can find
the test group for a given test as an annotation in the respective test class. A list of test groups can be found at
`integration-tests/src/test/java/org/apache/druid/tests/TestNGGroup.java`. The annotation uses a string
constant defined in `TestNGGroup.java`, be sure to use the constant value, not name. For example,
if your test has the the annotation: `@Test(groups = TestNGGroup.BATCH_INDEX)` then use the argument
`-Dgroups=batch-index`.

* Test Name: Use the fully-qualified class name. For example, `org.apache.druid.tests.BATCH_INDEX`.

* Add `-pl :druid-integration-tests` when running integration tests for the second time or later without changing
the code of core modules in between to skip up-to-date checks for the whole module dependency tree.

* Integration tests can also be run with either Java 8 or Java 11 by adding `-Djvm.runtime=#` to the `mvn` command, where `#`
can either be 8 or 11.

* Druid's configuration (using Docker) can be overridden by providing `-Doverride.config.path=<PATH_TO_FILE>`.
The file must contain one property per line, the key must start with `druid_` and the format should be snake case.
Note that when bringing up Docker containers through Maven and `-Doverride.config.path` is provided, additional
Druid routers for security group integration test (permissive tls, no client auth tls, custom check tls) will not be started.

### Debugging test runs

The integration test process is fragile and can fail for many reasons when run on your machine.
Here are some suggestions.

#### Workround for failed builds

Sometimes the command above may fail for reasons unrelated to the changes you wish to test.
In such cases, a workaround is to build the code first, then use the next section to run
individual tests. To build:

```bash
mvn clean package  -P integration-tests -Pskip-static-checks -Pskip-tests -Dmaven.javadoc.skip=true -T1.0C -nsu
```

#### Keep the local Maven cache fresh

As you work with issues, you may be tempted to reuse already-built jars. That only works for about 24 hours,
after which Maven will helpfully start downloading snapshot jars from an upstream repository.
This is, unfortunately, a feature of the build scripts. The `-nsu` option above tries to force
Maven to only look locally for snapshot jars.

## Running tests against mannually brought up Docker containers

1. Build docker images.

   From root module run maven command, run the following command:
   ```bash
   mvn clean install -pl integration-tests -P integration-tests -Ddocker.run.skip=true -Dmaven.test.skip=true -Ddocker.build.hadoop=true
   ```

   > **NOTE**: `-Ddocker.build.hadoop=true` is optional if you don't run tests against Hadoop.

2. Choose a docker-compose file to start containers.

   There are a few different Docker compose yamls located in "docker" folder that could be used to start containers for different tests.

   - To start basic Druid cluster (skip this if running Druid cluster with override configs):
     ```bash
     docker-compose -f integration-tests/docker/docker-compose.yml up
     ```

   - To start Druid cluster with override configs
     ```bash
     OVERRIDE_ENV=<PATH_TO_ENV> docker-compose -f docker-compose.yml up
     ```

   - To start tests against Hadoop
     ```bash
     docker-compose -f docker-compose.druid-hadoop.yml up
     ```

   - To start tests againt security group
     ```bash
     docker-compose -f docker-compose.yml -f docker-compose.security.yml up
     ```

3. Run tests.

   Execute the following command from root module, where `<test_name>` is the class name of a test, such as ITIndexerTest.
   ```bash
   mvn verify -P integration-tests -pl integration-tests -Ddocker.build.skip=true -Ddocker.run.skip=true -Dit.test=<test_name>
   ```

### Running tests from IntelliJ

Before running tests from IntelliJ, ensure you have a Druid cluster running.
Modify the tests run configurations to be the following Vm options:

```
-Duser.timezone=UTC
-Dfile.encoding=UTF-8
-Ddruid.test.config.dockerIp=localhost
-Ddruid.zk.service.host=localhost
-Ddruid.client.https.trustStorePath=client_tls/truststore.jks
-Ddruid.client.https.trustStorePassword=druid123
-Ddruid.client.https.keyStorePath=client_tls/client.jks
-Ddruid.client.https.certAlias=druid
-Ddruid.client.https.keyManagerPassword=druid123
-Ddruid.client.https.keyStorePassword=druid123
```

Run tests from the test configuration often found in the top right corner of the IntelliJ IDE.
The values shown above are for the default docker compose cluster. For other clusters the values will need to be changed.

## Docker Compose files

- docker-compose.base.yml

  Base file that defines all containers for integration testing

- docker-compose.yml

  Defines a Druid cluster with default configuration that is used for running integration tests.

  ```bash
  docker-compose -f docker-compose.yml up
  # DRUID_INTEGRATION_TEST_GROUP - an environment variable that specifies the integration test group to run.
  DRUID_INTEGRATION_TEST_GROUP=batch-index docker-compose -f docker-compose.yml up
  ```

  You can change the default configuration using a custom configuration file. The settings in the file will override
  the default settings if they conflict. They will be appended to the default configuration otherwise.

  ```bash
  # OVERRIDE_ENV - an environment variable that specifies the custom configuration file path.
  OVERRIDE_ENV=./environment-configs/test-groups/prepopulated-data DRUID_INTEGRATION_TEST_GROUP=query docker-compose -f docker-compose.yml up
  ```

- docker-compose.security.yml

  Defines three additional Druid router services with permissive tls, no client auth tls, and custom check tls respectively.
  This is meant to be used together with docker-compose.yml and is only needed for the "security" group integration test.

  ```bash
  docker-compose -f docker-compose.yml -f docker-compose.security.yml up
  ```

- docker-compose.druid-hadoop.yml

  For starting Apache Hadoop 2.8.5 cluster with the same setup as the Druid tutorial.

  ```bash
  docker-compose -f docker-compose.druid-hadoop.yml up
  ```

## Tips & tricks for debugging and developing integration tests

### Useful mvn command flags

| Flag | Description |
|:---|---|
| -Ddocker.build.skip=true   | Skip building the containers. <br/><br/>If you do not apply any change to Druid then you skip rebuilding the containers. This can save ~4 minutes. You need to build druid containers only once, after you can skip docker build step.  |
| -Ddocker.run.skip=true     | Skip starting docker containers.<br/><br/> This can save ~3 minutes by skipping building and bringing up the docker containers (Druid, Kafka, Hadoop, MYSQL, zookeeper, etc).<br/> Please make sure that you actually do have these containers already running if using this flag.<br/><br/> Additionally, please make sure that the running containers are in the same state that the setup script (run_cluster.sh) would have brought it up in. |
| -Ddocker.build.hadoop=true | Build the hadoop image when either running integration tests or when building the integration test docker images without running the tests. |
| -Dstart.hadoop.docker=true | Start hadoop container when you need to run IT tests that utilize local hadoop docker. |

### Debugging Druid while running tests

For your convenience, Druid processes running inside Docker have been debugging enabled at following debugging ports:

| Process | Remote Debugging Port |
| --- | :---: |
| Router with permissive tls | 5001 |
| Router with no client auth tls | 5002 |
| Router with custom check tls | 5003 |
| Router| 5004 |
| Broker| 5005 |
| Coordinator | 5006 |
| Historical | 5007 |
| Middlemanager | 5008 |
| Overlord | 5009 |
| Peons (Workers on Middlemanager) |  Ephemeral port assigned by debugger (check task log for port assigned to each task) |

You can use remote debugger(such as via IntelliJ IDEA's Remote Configuration) to debug the corresponding Druid process at above port.

Running Tests Using A Quickstart Cluster
-------------------

When writing integration tests, it can be helpful to test against a quickstart
cluster so that you can set up remote debugging with in your developer
environment. This section walks you through setting up the integration tests
so that it can run against a [quickstart cluster](../docs/tutorials/index.md) running on your development
machine.

> **NOTE**: Not all features run by default on a quickstart cluster, so it may not make sense to run the entire test suite against this configuration.
>
> Quickstart does not run with ssl, so to trick the integration tests we specify the `*_tls_url` in the config to be the same as the http url.

Make sure you have at least 6GiB of memory available before you run the tests.

The tests rely on files in the test/resources folder to exist under the path /resources,
so create a symlink to make them available:

```bash
ln -s ${DRUID_HOME}/integration-tests/src/test/resources /resources
```

Set the cluster config file environment variable to the quickstart config:
```bash
export CONFIG_FILE=${DRUID_HOME}/integration-tests/quickstart-it.json
```

The test group `quickstart-compatible` has tests that have been verified to work against the quickstart cluster.
There may be more tests that work, if you find that they do, please mark it as quickstart-compatible
(TestNGGroup#QUICKSTART_COMPATIBLE) and open a PR.
If you find some integration tests do not work, look at the docker files to see what setup they do. You may need to
do similar steps to get the test to work.

Then run the tests using a command similar to:
```bash
mvn verify -P int-tests-config-file -Dit.test=<test_name>
# Run all integration tests that have been verified to work against a quickstart cluster.
mvn verify -P int-tests-config-file -Dgroups=quickstart-compatible
```

Running Tests Using A Configuration File for Any Cluster
-------------------

Make sure that you have at least 6GiB of memory available before you run the tests.

To run tests on any druid cluster that is already running, create a configuration file:

    {
       "broker_host": "<broker_ip>",
       "broker_port": "<broker_port>",
       "router_host": "<router_ip>",
       "router_port": "<router_port>",
       "indexer_host": "<indexer_ip>",
       "indexer_port": "<indexer_port>",
       "coordinator_host": "<coordinator_ip>",
       "coordinator_port": "<coordinator_port>",
       "middlemanager_host": "<middle_manager_ip>",
       "zookeeper_hosts": "<comma-separated list of zookeeper_ip:zookeeper_port>",
       "cloud_bucket": "<(optional) cloud_bucket for test data if running cloud integration test>",
       "cloud_path": "<(optional) cloud_path for test data if running cloud integration test>"
    }

Set the environment variable `CONFIG_FILE` to the name of the configuration file:
```
export CONFIG_FILE=<config file name>
```

To run all tests from a test group using mvn run the following command:
(list of test groups can be found at integration-tests/src/test/java/org/apache/druid/tests/TestNGGroup.java)
```bash
mvn verify -P int-tests-config-file -Dgroups=<test_group>
```

To run only a single test using mvn run the following command:
```bash
mvn verify -P int-tests-config-file -Dit.test=<test_name>
```

Running a Test That Uses Cloud
-------------------
The integration test that indexes from Cloud or uses Cloud as deep storage is not run as part
of the integration test run discussed above. Running these tests requires the user to provide
their own Cloud.

Currently, the integration test supports Amazon Kinesis, Google Cloud Storage, Amazon S3, and Microsoft Azure.
These can be run by providing "kinesis-index", "kinesis-data-format", "gcs-deep-storage", "s3-deep-storage", or "azure-deep-storage"
to -Dgroups for Amazon Kinesis, Google Cloud Storage, Amazon S3, and Microsoft Azure respectively. Note that only
one group should be run per mvn command.

For all the Cloud Integration tests, the following will also need to be provided:
1) Provide -Doverride.config.path=<PATH_TO_FILE> with your Cloud credentials/configs set. See
integration-tests/docker/environment-configs/override-examples/ directory for env vars to provide for each Cloud.

For Amazon Kinesis, the following will also need to be provided:
1) Provide -Ddruid.test.config.streamEndpoint=<STREAM_ENDPOINT> with the endpoint of your stream set.
For example, kinesis.us-east-1.amazonaws.com

For Google Cloud Storage, Amazon S3, and Microsoft Azure, the following will also need to be provided:
1) Set the bucket and path for your test data. This can be done by setting -Ddruid.test.config.cloudBucket and
-Ddruid.test.config.cloudPath in the mvn command or setting "cloud_bucket" and "cloud_path" in the config file.
2) Copy wikipedia_index_data1.json, wikipedia_index_data2.json, and wikipedia_index_data3.json
located in integration-tests/src/test/resources/data/batch_index/json to your Cloud storage at the location set in step 1.

For Google Cloud Storage, in addition to the above, you will also have to:
1) Provide -Dresource.file.dir.path=<PATH_TO_FOLDER> with folder that contains GOOGLE_APPLICATION_CREDENTIALS file

For example, to run integration test for Google Cloud Storage:
```bash
mvn verify -P integration-tests -Dgroups=gcs-deep-storage -Doverride.config.path=<PATH_TO_FILE> \
           -Dresource.file.dir.path=<PATH_TO_FOLDER> -Ddruid.test.config.cloudBucket=test-bucket \
           -Ddruid.test.config.cloudPath=test-data-folder/
```


Running a Test That Uses Hadoop
-------------------

The integration test that indexes from hadoop is not run as part
of the integration test run discussed above.  This is because druid
test clusters might not, in general, have access to hadoop.
This also applies to integration test that uses Hadoop HDFS as an inputSource or as a deep storage.
To run integration test that uses Hadoop, you will have to run a Hadoop cluster. This can be done in two ways:
1) Run Druid Docker test clusters with Hadoop container by passing -Dstart.hadoop.docker=true to the mvn command.
   If you have not already built the hadoop image, you will also need to add -Ddocker.build.hadoop=true to the mvn command.
2) Run your own Druid + Hadoop cluster and specified Hadoop configs in the configuration file (CONFIG_FILE).

Currently, hdfs-deep-storage and other <cloud>-deep-storage integration test groups can only be run with
Druid Docker test clusters by passing -Dstart.hadoop.docker=true to start Hadoop container.
You will also have to provide -Doverride.config.path=<PATH_TO_FILE> with your Druid's Hadoop configs set.
See integration-tests/docker/environment-configs/override-examples/hdfs directory for example.
Note that if the integration test you are running also uses other cloud extension (S3, Azure, GCS), additional
credentials/configs may need to be set in the same file as your Druid's Hadoop configs set.

If you are running ITHadoopIndexTest with your own Druid + Hadoop cluster, please follow the below steps:
- Copy wikipedia_index_data1.json, wikipedia_index_data2.json, and wikipedia_index_data3.json
  located in integration-tests/src/test/resources/data/batch_index/json to your HDFS at /batch_index/json/
- Copy batch_hadoop.data located in integration-tests/src/test/resources/data/batch_index/hadoop_tsv to your HDFS
  at /batch_index/hadoop_tsv/
If using the Docker-based Hadoop container, the steps above are automatically done by the integration tests.

When running the Hadoop tests, you must set `-Dextra.datasource.name.suffix=''`, due to https://github.com/apache/druid/issues/9788.

Option 1: Run the test using mvn (using the bundled Docker-based Hadoop cluster and building docker images at runtime):
```bash
mvn verify -P integration-tests -Dit.test=ITHadoopIndexTest -Dstart.hadoop.docker=true -Ddocker.build.hadoop=true \
           -Doverride.config.path=docker/environment-configs/override-examples/hdfs -Dextra.datasource.name.suffix=''
```

Option 2: Run the test using mvn (using the bundled Docker-based hadoop cluster and not building images at runtime):
```bash
mvn verify -P integration-tests -Dit.test=ITHadoopIndexTest -Dstart.hadoop.docker=true -Ddocker.build.skip=true \
           -Doverride.config.path=docker/environment-configs/override-examples/hdfs -Dextra.datasource.name.suffix=''
```

Option 3: Run the test using mvn (using the bundled Docker-based hadoop cluster and when you have already started all containers)
```bash
mvn verify -P integration-tests -Dit.test=ITHadoopIndexTest -Ddocker.run.skip=true -Ddocker.build.skip=true \
           -Doverride.config.path=docker/environment-configs/override-examples/hdfs -Dextra.datasource.name.suffix=''
```

Option 4: Run the test using mvn (using config file for existing Hadoop cluster):
```bash
mvn verify -P int-tests-config-file -Dit.test=ITHadoopIndexTest -Dextra.datasource.name.suffix=''
```

In some test environments, the machine where the tests need to be executed
cannot access the outside internet, so mvn cannot be run.  In that case,
do the following instead of running the tests using mvn:

### Compile druid and the integration tests

On a machine that can do mvn builds:

```bash
cd druid
mvn clean package
cd integration_tests
mvn dependency:copy-dependencies package
```

### Put the compiled test code into your test cluster

Copy the integration-tests directory to the test cluster.

### Set CLASSPATH

```bash
TDIR=<directory containing integration-tests>/target
VER=<version of druid you built>
export CLASSPATH=$TDIR/dependency/*:$TDIR/druid-integration-tests-$VER.jar:$TDIR/druid-integration-tests-$VER-tests.jar
```

### Run the test

```bash
java -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Ddruid.test.config.type=configFile \
     -Ddruid.test.config.configFile=<pathname of configuration file> org.testng.TestNG \
     -testrunfactory org.testng.DruidTestRunnerFactory -testclass org.apache.druid.tests.hadoop.ITHadoopIndexTest
```

Writing a New Test
-------------------

## What should we cover in integration tests

For every end-user functionality provided by druid we should have an integration-test verifying the correctness.

## Rules to be followed while writing a new integration test

### Every Integration Test must follow these rules:

1) Name of the test must start with a prefix "IT"
2) A test should be independent of other tests
3) Tests are to be written in TestNG style ([http://testng.org/doc/documentation-main.html#methods](http://testng.org/doc/documentation-main.html#methods))
4) If a test loads some data it is the responsibility of the test to clean up the data from the cluster

### How to use Guice Dependency Injection in a test

A test can access different helper and utility classes provided by test-framework in order to access Coordinator,Broker etc..
To mark a test be able to use Guice Dependency Injection -
Annotate the test class with the below annotation

```java
@Guice(moduleFactory = DruidTestModuleFactory.class)
```
This will tell the test framework that the test class needs to be constructed using guice.

### Helper Classes provided

1) IntegrationTestingConfig - configuration of the test
2) CoordinatorResourceTestClient - httpclient for coordinator endpoints
3) OverlordResourceTestClient - httpclient for indexer endpoints
4) QueryResourceTestClient - httpclient for broker endpoints

### Static Utility classes

1) RetryUtil - provides methods to retry an operation until it succeeds for configurable no. of times
2) FromFileTestQueryHelper - reads queries with expected results from file and executes them and verifies the results using ResultVerifier

Refer ITIndexerTest as an example on how to use dependency Injection

### Running test methods in parallel

By default, test methods in a test class will be run in sequential order one at a time. Test methods for a given test
class can be set to run in parallel (multiple test methods of each class running at the same time) by excluding
the given class/package from the "AllSerializedTests" test tag section and including it in the "AllParallelizedTests"
test tag section in integration-tests/src/test/resources/testng.xml. TestNG uses two parameters, i.e.,
`thread-count` and `data-provider-thread-count`, for parallel test execution, which are both set to 2 for Druid integration tests.

For test using parallel execution with data provider, you will also need to set `@DataProvider(parallel = true)`
on your data provider method in your test class. Note that for test using parallel execution with data provider, the test
class does not need to be in the "AllParallelizedTests" test tag section and if it is in the "AllParallelizedTests"
test tag section it will actually be run with `thread-count` times `data-provider-thread-count` threads.
You may want to modify those values for faster execution.
See https://testng.org/doc/documentation-main.html#parallel-running and https://testng.org/doc/documentation-main.html#parameters-dataproviders for details.

Please be mindful when adding tests to the "AllParallelizedTests" test tag that the tests can run in parallel with
other tests from the same class at the same time. i.e. test does not modify/restart/stop the druid cluster or other dependency containers,
test does not use excessive memory starving other concurent task, test does not modify and/or use other task,
supervisor, datasource it did not create.

### Limitation of Druid cluster in Travis environment

By default, integration tests are run in Travis environment on commits made to open PR. These integration test jobs are
required to pass for a PR to be elligible to be merged. Here are known issues and limitations to the Druid docker cluster
running in Travis machine that may cause the tests to fail:
- Number of concurrent running tasks. Although the default Druid cluster config sets the maximum number of tasks (druid.worker.capacity) to 10,
the actual maximum can be lowered depending on the type of the tasks. For example, running 2 range partitioning compaction tasks with 2 subtasks each
(for a total of 6 tasks) concurrently can cause the cluster to intermittently fail. This can cause the Travis job to become stuck until it timeouts (50 minutes)
and/or terminates after 10 mins of not receiving new output.
