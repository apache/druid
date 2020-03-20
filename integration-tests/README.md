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
The file must contain one property per line, the key must start with druid_ and the format should be snake case. 

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

Set the environment variable CONFIG_FILE to the name of the configuration file:
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

Currently, the integration test supports Google Cloud Storage, Amazon S3, and Microsoft Azure.
These can be run by providing "gcs-deep-storage", "s3-deep-storage", or "azure-deep-storage" 
to -Dgroups for Google Cloud Storage, Amazon S3, and Microsoft Azure respectively. Note that only
one group should be run per mvn command.

In addition to specifying the -Dgroups to mvn command, the following will need to be provided:
1) Set the bucket and path for your test data. This can be done by setting -Ddruid.test.config.cloudBucket and 
-Ddruid.test.config.cloudPath in the mvn command or setting "cloud_bucket" and "cloud_path" in the config file.
2) Copy wikipedia_index_data1.json, wikipedia_index_data2.json, and wikipedia_index_data3.json 
located in integration-tests/src/test/resources/data/batch_index to your Cloud storage at the location set in step 1.
3) Provide -Doverride.config.path=<PATH_TO_FILE> with your Cloud credentials/configs set. See
integration-tests/docker/environment-configs/override-examples/ directory for env vars to provide for each Cloud storage.

For running Google Cloud Storage, in addition to the above, you will also have to:
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
1) Run your own Druid + Haddop cluster and specified Hadoop configs in the configuration file (CONFIG_FILE).
2) Run Druid Docker test clusters with Hadoop container by passing -Dstart.hadoop.docker=true to the mvn command. 

Currently, hdfs-deep-storage and other <cloud>-deep-storage integration test groups can only be run with 
Druid Docker test clusters by passing -Dstart.hadoop.docker=true to start Hadoop container.
You will also have to provide -Doverride.config.path=<PATH_TO_FILE> with your Druid's Hadoop configs set. 
See integration-tests/docker/environment-configs/override-examples/hdfs directory for example.
Note that if the integration test you are running also uses other cloud extension (S3, Azure, GCS), additional
credentials/configs may need to be set in the same file as your Druid's Hadoop configs set. 

Currently, ITHadoopIndexTest can only be run with your own Druid + Haddop cluster by following the below steps:
Create a directory called batchHadoop1 in the hadoop file system
(anywhere you want) and put batch_hadoop.data (integration-tests/src/test/resources/hadoop/batch_hadoop.data) 
into that directory (as its only file).

Add this keyword to the configuration file (see above):

```
    "hadoopTestDir": "<name_of_dir_containing_batchHadoop1>"
```

Run the test using mvn:

```
  mvn verify -P int-tests-config-file -Dit.test=ITHadoopIndexTest
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
