Integration Testing
=========================

## Installing Docker and Running

Please refer to instructions at [https://github.com/druid-io/docker-druid/blob/master/docker-install.md](https://github.com/druid-io/docker-druid/blob/master/docker-install.md)

Instead of running
```
boot2docker init
```

run instead
```
boot2docker init -m 6000
```

Make sure that you have at least 6GB of memory available before you run the tests.

Set the docker ip via:
```
export DOCKER_IP=$(boot2docker ip 2>/dev/null)
```

Verify that docker is running by issuing the following command:

```
docker info
```

Running Integration tests
=========================

## Running tests using mvn

To run all the tests using mvn run the following command -
'''''
  mvn verify -P integration-tests
'''''

To run only a single test using mvn run following command -
'''''
  mvn verify -P integration-tests -Dit.test=<test_name>
'''''


Writing a New Test
===============

## What should we cover in integration tests

For every end-user functionality provided by druid we should have an integration-test verifying the correctness.

## Rules to be followed while writing a new integration test

### Every Integration Test must follow these rules

1) Name of the test must start with a prefix "IT"
2) A test should be independent of other tests
3) Tests are to be written in TestNG style ([http://testng.org/doc/documentation-main.html#methods](http://testng.org/doc/documentation-main.html#methods))
4) If a test loads some data it is the responsibility of the test to clean up the data from the cluster

### How to use Guice Dependency Injection in a test

A test can access different helper and utility classes provided by test-framework in order to access Coordinator,Broker etc..
To mark a test be able to use Guice Dependency Injection -
Annotate the test class with the below annotation

 '''''''
 @Guice(moduleFactory = DruidTestModuleFactory.class)
 '''''''
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
=======================
1) Remove the patch for TestNG after resolution of Surefire-622
