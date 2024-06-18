
# Quidem IT

Enables to write sql level tests easily.
Can be used to write tests against existing test backends (ComponentSupplier) - by doing so the testcases can be moved closer to the excercised codes.

## Usage

### Running these checks

* CI execution happens by a standard JUnit test `QTest` in this module
* the `dev/quidem` script can be used to run these tests (after the project is built)

### Launching a test generating Broker

* make sure to build the project first; one way to do that is:
  ```
  mvn install -pl quidem-it/ -am -DskipTests -Pskip-static-checks
  ```
* launch the broker instance with:
  ```
  mvn exec:exec -pl quidem-it -Dquidem.url=
  ```








### usage:

 *

