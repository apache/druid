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

## Installing Docker

Please refer to instructions at [https://github.com/druid-io/docker-druid/blob/master/docker-install.md](https://github.com/druid-io/docker-druid/blob/master/docker-install.md).

## Creating the Docker VM

Create a new VM for integration tests with at least 6GB of memory.

```
docker-machine create --driver virtualbox --virtualbox-memory 6000 integration
```

Set the docker environment:

```
eval "$(docker-machine env integration)"
export DOCKER_IP=$(docker-machine ip integration)
```

## Running tests

To run all the tests using docker and mvn run the following command:
```
  mvn verify -P integration-tests
```

To run only a single test using mvn run the following command:
```
  mvn verify -P integration-tests -Dit.test=<test_name>
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
    }

Set the environment variable CONFIG_FILE to the name of the configuration file:
```
export CONFIG_FILE=<config file name>
```

To run all the tests using mvn run the following command: 
```
  mvn verify -P int-tests-config-file
```

To run only a single test using mvn run the following command:
```
  mvn verify -P int-tests-config-file -Dit.test=<test_name>
```

Running a Test That Uses Hadoop
-------------------

The integration test that indexes from hadoop is not run as part
of the integration test run discussed above.  This is because druid
test clusters might not, in general, have access to hadoop.
That's the case (for now, at least) when using the docker cluster set 
up by the integration-tests profile, so the hadoop test
has to be run using a cluster specified in a configuration file.

The data file is 
integration-tests/src/test/resources/hadoop/batch_hadoop.data.
Create a directory called batchHadoop1 in the hadoop file system
(anywhere you want) and put batch_hadoop.data into that directory
(as its only file).

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
java -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Ddruid.test.config.type=configFile -Ddruid.test.config.configFile=<pathname of configuration file> org.testng.TestNG -testrunfactory org.testng.DruidTestRunnerFactory -testclass io.druid.tests.hadoop.ITHadoopIndexTest
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

TODOS
-----------------------
1) Remove the patch for TestNG after resolution of Surefire-622
