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
have at least 4GB of memory allocated to the docker engine. (You can verify it 
under Preferences > Advanced.)

Also set the `DOCKER_IP`
environment variable to localhost on your system, as follows:

```
export DOCKER_IP=127.0.0.1
```

## Running tests

To run all tests from a test group using docker and mvn run the following command: 
(list of test groups can be found at integration-tests/src/test/java/org/apache/druid/tests/TestNGGroup.java)
```
  mvn verify -P integration-tests -Dgroups=<test_group>
```

To run only a single test using mvn run the following command:
```
  mvn verify -P integration-tests -Dit.test=<test_name>
```

Add `-rf :druid-integration-tests` when running integration tests for the second time or later without changing
the code of core modules in between to skip up-to-date checks for the whole module dependency tree.

Integration tests can also be run with either Java 8 or Java 11 by adding -Djvm.runtime=# to mvn command, where #
can either be 8 or 11.

Druid's configuration (using Docker) can be overrided by providing -Doverride.config.path=<PATH_TO_FILE>. 
The file must contain one property per line, the key must start with `druid_` and the format should be snake case.
Note that when bringing up docker containers through mvn and -Doverride.config.path is provided, additional
Druid routers for security group integration test (permissive tls, no client auth tls, custom check tls) will not be started.   

## Docker compose

Docker compose yamls located in "docker" folder

docker-compose.base.yml - Base file that defines all containers for integration test

docker-compose.yml - Defines Druid cluster with default configuration that is used for running integration tests in Travis CI.
    
    docker-compose -f docker-compose.yml up
    // DRUID_INTEGRATION_TEST_GROUP - this variable is used in Druid docker container for "security" and "query" test group. Use next docker-compose if you want to run security/query tests.
    DRUID_INTEGRATION_TEST_GROUP=security docker-compose -f docker-compose.yml up

docker-compose.override-env.yml - Defines Druid cluster with default configuration plus any additional and/or overriden configurations from override-env file.

    // OVERRIDE_ENV - variable that must contains path to Druid configuration file 
    OVERRIDE_ENV=./environment-configs/override-examples/s3 docker-compose -f docker-compose.override-env.yml up
    
docker-compose.security.yml - Defines three additional Druid router services with permissive tls, no client auth tls, and custom check tls respectively. 
This is meant to be use together with docker-compose.yml or docker-compose.override-env.yml and is only needed for the "security" group integration test. 

    docker-compose -f docker-compose.yml -f docker-compose.security.yml up 
    
docker-compose.druid-hadoop.yml - for starting Apache Hadoop 2.8.5 cluster with the same setup as the Druid tutorial

    docker-compose -f docker-compose.druid-hadoop.yml up

## Manual bringing up docker containers and running tests

1. Build druid-cluster, druid-hadoop docker images. From root module run maven command:
```
mvn clean install -pl integration-tests -P integration-tests -Ddocker.run.skip=true -Dmaven.test.skip=true
```

2. Run druid cluster by docker-compose:

```
- Basic Druid cluster (skip this if running Druid cluster with override configs):
docker-compose -f integration-tests/docker/docker-compose.yml up
- Druid cluster with override configs (skip this if running Basic Druid cluster):
OVERRIDE_ENV=<PATH_TO_ENV> docker-compose -f ${DOCKERDIR}/docker-compose.override-env.yml up
- Druid hadoop (if needed):
docker-compose -f ${DOCKERDIR}/docker-compose.druid-hadoop.yml up
- Druid routers for security group integration test (if needed):
 docker-compose -f ${DOCKERDIR}/docker-compose.security.yml up
```

3. Run maven command to execute tests with -Ddocker.build.skip=true -Ddocker.run.skip=true

## Tips & tricks for debugging and developing integration tests

### Useful mvn command flags

- -Ddocker.build.skip=true to skip build druid containers. 
If you do not apply any change to druid then you can do not rebuild druid. 
This can save ~4 minutes to build druid cluster and druid hadoop.
You need to build druid containers only once, after you can skip docker build step. 
- -Ddocker.run.skip=true to skip starting docker containers. This can save ~3 minutes by skipping building and bringing 
up the docker containers (Druid, Kafka, Hadoop, MYSQL, zookeeper, etc). Please make sure that you actually do have
these containers already running if using this flag. Additionally, please make sure that the running containers
are in the same state that the setup script (run_cluster.sh) would have brought it up in. 

### Debugging Druid while running tests

For your convenience, Druid processes running inside Docker have debugging enabled and the following ports have 
been made available to attach your remote debugger (such as via IntelliJ IDEA's Remote Configuration):

- Overlord process at port 5009
- Middlemanager process at port 5008
- Historical process at port 5007
- Coordinator process at port 5006
- Broker process at port 5005
- Router process at port 5004
- Router with custom check tls process at port 5003
- Router with no client auth tls process at port 5002
- Router with permissive tls process at port 5001

Running Tests Using A Quickstart Cluster
-------------------

When writing integration tests, it can be helpful to test against a quickstart
cluster so that you can set up remote debugging with in your developer
environment. This section walks you through setting up the integration tests
so that it can run against a [quickstart cluster](../docs/tutorials/index.md#getting-started) running on your development
machine.

> NOTE: Not all features run by default on a quickstart cluster, so it may not make sense to run the entire test suite against this configuration.

> NOTE: Quickstart does not run with ssl, so to trick the integration tests we specify the `*_tls_url` in the config to be the same as the http url.

Make sure you have at least 6GB of memory available before you run the tests.

The tests rely on files in the test/resources folder to exist under the path /resources,
so create a symlink to make them available:

```
  ln -s ${DRUID_HOME}/integration-tests/src/test/resources /resources
```

Set the cluster config file environment variable to the quickstart config:
```
  export CONFIG_FILE=${DRUID_HOME}/integration-tests/quickstart-it.json
```

The test group `quickstart-compatible` has tests that have been verified to work against the quickstart cluster.
There may be more tests that work, if you find that they do, please mark it as quickstart-compatible
(TestNGGroup#QUICKSTART_COMPATIBLE) and open a PR.
If you find some integration tests do not work, look at the docker files to see what setup they do. You may need to
do similar steps to get the test to work.

Then run the tests using a command similar to:
```
  mvn verify -P int-tests-config-file -Dit.test=<test_name>
  # Run all integration tests that have been verified to work against a quickstart cluster.
  mvn verify -P int-tests-config-file -Dgroups=quickstart-compatible
```

Running Tests Using A Configuration File for Any Cluster
-------------------

Make sure that you have at least 6GB of memory available before you run the tests.

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
       "cloud_path": "<(optional) cloud_path for test data if running cloud integration test>",
    }

Set the environment variable `CONFIG_FILE` to the name of the configuration file:
```
export CONFIG_FILE=<config file name>
```

To run all tests from a test group using mvn run the following command: 
(list of test groups can be found at integration-tests/src/test/java/org/apache/druid/tests/TestNGGroup.java)
```
  mvn verify -P int-tests-config-file -Dgroups=<test_group>
```

To run only a single test using mvn run the following command:
```
  mvn verify -P int-tests-config-file -Dit.test=<test_name>
```

Running a Test That Uses Cloud
-------------------
The integration test that indexes from Cloud or uses Cloud as deep storage is not run as part
of the integration test run discussed above. Running these tests requires the user to provide
their own Cloud. 

Currently, the integration test supports Amazon Kinesis, Google Cloud Storage, Amazon S3, and Microsoft Azure.
These can be run by providing "kinesis-index", "gcs-deep-storage", "s3-deep-storage", or "azure-deep-storage" 
to -Dgroups for Amazon Kinesis, Google Cloud Storage, Amazon S3, and Microsoft Azure respectively. Note that only
one group should be run per mvn command.

For all of the Cloud Integration tests, the following will also need to be provided:
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
```
  mvn verify -P integration-tests -Dgroups=gcs-deep-storage -Doverride.config.path=<PATH_TO_FILE> -Dresource.file.dir.path=<PATH_TO_FOLDER> -Ddruid.test.config.cloudBucket=test-bucket -Ddruid.test.config.cloudPath=test-data-folder/
```

 
Running a Test That Uses Hadoop
-------------------

The integration test that indexes from hadoop is not run as part
of the integration test run discussed above.  This is because druid
test clusters might not, in general, have access to hadoop.
This also applies to integration test that uses Hadoop HDFS as an inputSource or as a deep storage. 
To run integration test that uses Hadoop, you will have to run a Hadoop cluster. This can be done in two ways:
1) Run Druid Docker test clusters with Hadoop container by passing -Dstart.hadoop.docker=true to the mvn command. 
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

Run the test using mvn (using the bundled Docker-based Hadoop cluster):
```
  mvn verify -P integration-tests -Dit.test=ITHadoopIndexTest -Dstart.hadoop.docker=true -Doverride.config.path=docker/environment-configs/override-examples/hdfs -Dextra.datasource.name.suffix=''
```

Run the test using mvn (using config file for existing Hadoop cluster):
```
  mvn verify -P int-tests-config-file -Dit.test=ITHadoopIndexTest -Dextra.datasource.name.suffix=''
```

In some test environments, the machine where the tests need to be executed
cannot access the outside internet, so mvn cannot be run.  In that case,
do the following instead of running the tests using mvn:

### Compile druid and the integration tests

On a machine that can do mvn builds:

```
cd druid 
mvn clean package
cd integration_tests 
mvn dependency:copy-dependencies package
```

### Put the compiled test code into your test cluster

Copy the integration-tests directory to the test cluster.

### Set CLASSPATH

```
TDIR=<directory containing integration-tests>/target
VER=<version of druid you built>
export CLASSPATH=$TDIR/dependency/*:$TDIR/druid-integration-tests-$VER.jar:$TDIR/druid-integration-tests-$VER-tests.jar
```

### Run the test

```
java -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Ddruid.test.config.type=configFile -Ddruid.test.config.configFile=<pathname of configuration file> org.testng.TestNG -testrunfactory org.testng.DruidTestRunnerFactory -testclass org.apache.druid.tests.hadoop.ITHadoopIndexTest
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

```
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
