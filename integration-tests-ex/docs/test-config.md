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

## Test Configuration

Tests typically need to understand how the cluster is structured.
To create a test, you must supply at least three key components:

* A `cluster/<category>/docker-compose.yaml` file that launches the desired cluster.
  (The folder name `<category>` becomes the application name in Docker.)
* A `src/test/resources/cluster/<category>/docker.yaml` file that describes the cluster
  for tests. This file can also include Metastore SQL statements needed to
  populate the metastore.
* The test itself, as a JUnit test that uses the `Initializer` class to
  configure the tests to match the cluster.

This section explains the test configuration file which defines the test
cluster.

## Cluster Types

The integration tests can run in a variety of cluster types, depending
on the details of the test:

* Docker Compose: the normal configuration that all tests support.
* Micro Quickstart: allows for a manual cluster setup, if, say, you
  want to run services in your IDE. Supported by a subset of tests.
* Kubernetes: (Details needed.)

Each cluster type has its own quirks. The job of the tests's cluster configuration
file is to communicate those quirks to the test.

Docker and Kubernetes use proxies to communicate to the cluster. Thus, the host
known to the tests is different than the hosts known within the cluster. Ports
may also are mapped differently "outside" than "inside."

Clusters outside of Docker don't provide a good way to start and stop
services, so tests that want to do that (to, say, test high availability)
can't run except in a Docker cluster.

### Specify the Cluster Type

To reflect this, tests provide named configuration files. The configuration
itself is passed in via the environment:

```bash
export TEST_CONFIG=quickstart
```

```bash
java ... -DtestConfig=quickstart
```

The system property taskes precedence over the environment variable.
If neither are set, `docker` is the default. The configuration file
itself is assumed to be a resource named `/yaml/<config>.yaml`.

As a debug aide, a test can specify and ad-hoc file in the file system
to load for one-off special cases. See `Initialization.Builder` for
details.

## Cluster Configuration Files

Cluster configuration is specified in a file for ease of debugging. Since
configuration is in a file (resource), and not in environment variables or
system properties, you
should need no special launch setup in your IDE to run a test that uses
the standard Docker Compose cluster for that test.

The configuration file has the same name as the cluster type and resides on
the class path at `/yaml/<type>.yaml` and in the source tree at
`<test module>/src/test/resources/yaml/<type>.yaml`. The standard names are:

* `docker.yaml`: the default and required for all tests. Describes a Docker
  Compose based test.
* `k8s.yaml`: a test cluster running in Kubernetes. (Details needed.)
* `local.yaml`: a local cluser such as Micro Quickstart cluster. (Details needed.)
* `<other>.yaml`: custom cluster configuration.

Configuration files support include files. Most of the boiler-plate configuration
should appear in commmon files. As a result, you should only need to specify
test-specific differences in your `docker.yaml` file, with all else obtained
from the included files.

## Configuration File Syntax

The configuration is a [YAML](https://yaml.org/spec/1.2.2/) file that
has a few top-level properties and an entry for each service in your
cluster.

### `type`

```yaml
type: docker|k8s|local|disabled
```

The type explains the infrastructure that runs the cluster:

* `docker`: a cluster launched in Docker, typically via Docker Compose.
  A proxy host is needed. (See below.)
* `k8s`: a cluster run in Kubernets. (Details needed). A proxy host
  is needed.
* `local`: a cluster running as processes on a network directly reachable
  by the tests. Example: a micro-quickstart cluster running locally.
* `disabled`: the configuration is not supported by the test.

The `disabled` type is handy for tests that require Docker: you can say that
the test is not available when the cluster is local.

If the test tries to load a cluster name that does not exist, a "dummy"
configuration is loaded instead with the type set to `disabled`.

The type is separate from the cluster name (as explained earlier): there
may be multiple names for the same type. For example, you might have two
or three local cluster setups you wish to test.

### `include`

```yaml:
include:
  - <file>
```

Allows including any number of other files. Similar to inheritance for
Docker Compose. The inheritance rules are:

* Properties set later in the list take precedence over properties set in
  files earlier in the list.
* Properties set in the file take precedence over properties set in
  included files.
* Includes can nest to any level.

Merging occurs as follows:

* Top level scalars: newer values replace older values.
* Services: newer values replace all older settings for that service.
* Metastore init: newer values add more queries to any list defined
  by an earlier file.
* Properties: newer values replace values defined by earlier files.

The files are assumed to be resources (on the class path) and require
the full path name. Example: `/cluster/Commmon/base.yaml`

### `proxyHost`

```yaml
proxyHost: <host name>
```

When tests run in either Docker or Kubernetes, the test communicate with
a proxy, which forwards requests to the cluster hosts and ports. In
Docker, the proxy host is the machine that runs Docker. In Kubernetes,
the proxy host is the host running the Kubernetes proxy service.

There is no proxy host for clusters running directly on a machine.

If the proxy host is omitted for Docker, `localhost` is assumed.

### `datasourceSuffix`

```yaml
datasourceSuffix: <suffix>
```

Suffix to append to data source names in indexer tests. The default
is the empty string.

### `zk`

```yaml
zk:
  <service object>
```

Specifies the ZooKeeper instances.

#### `startTimeoutSecs`

```yaml
startTimeoutSecs: <secs>
```

Specifies the amount of time to wait for ZK to become available when using the
test client. Optional.

### `metastore`

```yaml
metastore:
  <service object>
```

Describes the Druid "metadata storage" (metastore) typically
hosted in the offical MySql container. See `MetastoreConfig` for
configuration options.

#### `driver`

```yaml
driver: <full class name>
```

The Driver to use to work with the metastore. The driver must be
available on the tests's class path.

#### `connectURI`

```yaml:
connectURI: <url>
```

The JDBC connetion URL. Example:

```text
jdbc:mysql://<host>:<port>/druid
```

The config system supports two special fields: `<host>` and `<port>`.
A string of form `<host>` will be replaced by the resolved host name
(proxy host for Docker) and `<port>` with the resolved port number.

#### `user`

```yaml
user: <user name>
```

The MySQL user name.


#### `password`

```yaml
user: <password>
```

The MySQL password.

#### `properties`

```yaml
properties:
  <key>: <value>
```

Optional map of additional key/value pairs to pass to the JDBC driver.

### `kafka`

```yaml
zk:
  <service object>
```

Describes the optional Kafka service.

### `druid`

```yaml
druid:
  <service>:
    <service object>
```

Describes the set of Druid services using the `ServiceConfig` object.
Each service is keyed by the standard service name: the same name used
by the Druid `server` option.

When using inheritance, overrides replace entire services: it is not possible
to override individual instances of the service. That is, an include file might
define `coordinator`, but a test-specific file might override this with a
definition of two Coordinators.

### `properties`

```yaml
properties:
  <key>: <value>
```

Optional set of properties to use to configuration the Druid components loaded
by tests. This is the test-specific form of the standard Druid `common.runtime.properties`
and `runtime.properties` files.  Because the test runs as a client, the server
files are not available, and might not even make sense. (The client is not
a "service", for example.) Technically, the properties listed here are added to
Guice as the one and only `Properties` object.

Typically most components work using the default values. Tests are free to change
any of these values for a given test scenario. At present, the properties are
the same for all tests within a Maven module, but we could extend the
`ClusterConfig.Builder` class to allow test-specific settings if needed.

The "JSON configuration" mechanism wants all properties to be strings. YAML
will deserialize number-like properties as numbers. To avoid confusion, all
properties are converted to strings before being passed to Druid.

When using inheritance, later properties override earlier properties.

### `metastoreInit`

```yaml
metastoreInit:
  - sql: |
      <sql query>
```

A set of MySQL statements to be run against the
metadata storage before the test starts. Queries run in the
order specified. Ensure each is idempotent to
allow running tests multiple times against the same database.

To be kind to readers, please format the statements across multiple lines.
The code will compress out extra spaces before submitting the query so
that JSON payloads are as compact as possible.

The `sql` keyword is the only one supported at present. The idea is that
there may need to be context for some queries in some tests. (To be
enhanced as query conversion proceeds.)

When using inheritance, the set of queries is the union of all queries
in all configuration files. Base statements appear first, then included
statements.

### `metastoreInitDelaySec`

``yaml
metastoreInitDelaySec: <sec>
```

The default value is 6 seconds.

The metastore init section issues queries to the MySQL DB read by the
Coordinator. For performance, the Coordinator *does not* directly query
the database: instead, it queries an in-memory cache. This leads to the
following behavior:

* The Coordinator starts, checks the DB, and records the poll time.
* The test starts and updates the DB.
* The test runs and issues a query that needs the DB contents.
* The Coordinator checks that its poll timeout has not yet occurred
  and returns the (empty) contents of the cache.
* The test checks the empty contents against the expected contents,
  notices the results differ, and fails the test.

To work around this, we must change _two_ settings. First, change
the following Druid configuration for the Coordinator:

```yaml
      - druid_manager_segments_pollDuration=PT5S
```

Second, change the `metastoreInitDelaySec` to be a bit longer:

```yaml
metastoreInitDelaySec: 6
```

The result is that the test will sit idle for 6 seconds, but that is better
than random failures.

**Note:** a better fix would be for the Coordinator to have an API that causes
it to flush its cache. Since some tests run two coordinators, the message must be
sent to both. An even better fix would be fore the Coordinator to detect such
changes itself somehow.

### Service Object

Generic object to describe Docker Compose services.

#### `service`

```yaml
service: <service name>
```

Name of the service as known to Docker Compose. Defaults to be
the same as the service name used in this configuration file.

#### `instances`

```yaml
instances:
  - <service-instance>
```

Describes the instances of the service as `ServiceInstance` objects.
Each service requires at least one instance. If more than one, then
each instance must define a `tag` that is a suffix that distinguishes
the instances.

### Service Instance Object

The service sections all allow multiple instances of each service. Service
instances define each instance of a service and provide a number of properties:

#### `tag`

When a service has more than one instance, the instances must have unique
names. The name is made up of the a base name (see below) with the tag
appended. Thus, if the service is `cooordinator` and the tag is `one`,
then the instance name is `coordinator-one`.

The tag is required when there is more than one instance of a service,
and is optional if there is only one instance. The tag corresponds to the
`DRUID_INSTANCE` environment variable passed into the container.

#### `container`

```yaml
container: <container name>
```

Name of the Docker container. If omitted, defaults to:

* `<service name>-<tag>` if a `tag` is provided (see below.)
* The name of the service (if there is only one instance).

#### `host`

```yaml
host: <host name or IP>
```

The host name or IP address on which the instance runs. This is
the host name known to the _cluster_: the name inside a Docker overlay network.
Has the same defaults as `container`.

#### `port`

```yaml
port: <port>
```

The port number of the service on the container as seen by other
services running within Docker. Required.

(TODO: If TLS is enabled, this is the TLS port.)

#### `proxyPort`

```yaml
proxyPort: <port>
```

The port number for the service as exposed on the proxy host.
Defaults to the same as `port`. You must specify a value if
you run multiple instances of the same service.
