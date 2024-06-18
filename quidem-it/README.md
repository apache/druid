
# Quidem IT

Enables to write sql level tests easily.
Can be used to write tests against existing test backends (ComponentSupplier) - by doing so the testcases can be moved closer to the excercised codes.

## Usage

### Running these tests

* CI execution happens by a standard JUnit test `QTest` in this module
* the `dev/quidem` script can be used to run these tests (after the project is built)

### Launching a test generating broker

* make sure to build the project first; one way to do that is:
  ```
  mvn install -pl quidem-it/ -am -DskipTests -Pskip-static-checks
  ```
* launch the broker instance with:
  ```
  mvn exec:exec -pl quidem-it
  ```
  * the broker will be running at http://localhost:12345
  * the used test configuration backend can configured by supplying `quidem.uri`
    ```
    mvn exec:exec -pl quidem-it -Dquidem.uri=druidtest:///?componentSupplier=ThetaSketchComponentSupplier
    ``` 
  * new record files can be started by calling http://localhost:12345/quidem/start
* after finished with the usage of the broker ; a new `record-123.iq` file will be in the project's worktree - it will contain all the executed statements
  * running `dev/quidem -Dquidem.overwrite` adds the resultsets to the `iq` file

