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

# gRPC Query Extension for Druid

This extension provides a gRPC API for SQL queries.

Druid uses REST as its RPC protocol. Druid has a large variety of REST operations
including query, ingest jobs, monitoring, configuration and many more. Although
REST is a universally supported RPC format, is not the only one in use. This
extension allows gRPC-based clients to issue SQL queries.

Druid is optimized for high-concurrency, low-complexity queries that return a
small result set (a few thousand rows at most). The small-query focus allows
Druid to offer a simple, stateless request/response REST API. This gRPC API
follows that Druid pattern: it is optimized for simple queries and follows
Druid's request/response model. APIs such as JDBC can handle larger results
because they stateful: a client can request pages of results using multiple
API calls. This API does not support paging: the entire result set is returned
in the response, resulting in a API which is fast for small queries, and not
suitable for larger result sets.

## Use Cases

The gRPC query extension can be used in two ways, depending on the selected
result format.

### CSV or JSON Response Format

The simplest way to use the gRPC extension is to send a query request that
uses CSV or JSON as the return format. The client simply pulls the results
from the response and does something useful with them. For the CSV format,
headers can be created from the column metadata in the response message.

### Protobuf Response Format

Some applications want to use Protobuf as the result format. In this case,
the extension encodes Protobuf-encoded rows as the binary payload of the query
response. This works for an application which uses a fixed set of queries, each
of which is carefully designed to power one application, say a dashboard. The
(simplified) message flow is:

```text
+-----------+   query ->  +-------+
| Dashboard | -- gRPC --> | Druid |
+-----------+  <- data    +-------+
```

In practice, there may be multiple proxy layers: one on the application side, and
the Router on the Druid side.

The dashboard displays a fixed set of reports and charts. Each of those sends a
well-defined query specified as part of the application. The returned data is thus
both well-known and fixed for each query. The set of queries is fixed by the contents
of the dashboard. That is, this is not an ad-hoc query use case.

Because the queries are locked down, and are part of the application, the set of valid
result sets is also well known and locked down. Given this well-controlled use case, it
is possible to use a pre-defined Protobuf message to represent the results of each distinct
query. (Protobuf is a compiled format: the solution works only because the set of messages
are well known. It would not work for the ad-hoc case in which each query has a different
result set schema.)
---
To be very clear: the application has a fixed set of queries to be sent to Druid via gRPC.
For each query, there is a fixed Protobuf response format defined by the application.
No other queries, aside from this well-known set, will be sent to the gRPC endpoint using
the Protobuf response format. If the set of queries is not well-defined, use the CSV
or JSON response format instead.

## Installation

The gRPC query extension is a "contrib" extension and is not installed by default when
you install Druid. Instead, you must install it manually.

In development, you can build Druid with all the "contrib" extensions. When building
Druid, include the `-P bundle-contrib-exts` in place of the `-P dist` option.

In production, follow the [Druid documentation](https://druid.apache.org/docs/latest/development/extensions.html).
Use the `pull-deps` command to install the `org.apache.druid.extensions.contrib:grpc-query`
extension.

To enable the extension, add the following to the load list in
`_commmon/common.runtime.properties`:

```text
druid.extensions.loadList=[..., "grpc-query"]
```

Adding the extension to the load list automatically enables the extension.

Then, enable and configure the extension in `broker/runtime.properties`:

```text
druid.grpcQuery.port=50051
```

The default port is 50051 (preliminary).

## Usage

See the `src/main/proto/query.proto` file in the `grpc-shaded` project for the request and
response message formats. The request message format closely follows the REST JSON message
format. The response is optimized for gRPC: it contains an error (if the request fails),
or the result schema and result data as a binary payload. You can query the gRPC endpoint
with any gRPC client.

## Implementation Notes

The extension is made up of two projects. Druid uses a different version of Guava than
does rRPC. To work around this, the `grpc-shade` project creates a shaded jar that includes
both gRPC and Guava. Since the rRPC compiler generates references to Guava, it turns out we
must generate Java from the `.proto` files _before_ shading, so the `.proto` files, and
the generated Java code, also reside in the `grpc-shade` project. One unfortunate aspect
of shading is that your IDE will not be able to find the source attachments for the rRPC
classes. The result is very tedious debugging. A workaround is to manually attach the
original gRPC source jars for each gRPC source file.

This project contains several components:

* Guice modules and associated server initialization code.
* Netty-based gRPC server.
* A "driver" that performs the actual query and generates the results.

## Debugging

Debugging of the gRPC extension requires extra care.

### Debugging in an IDE

To debug the gRPC extension in a production-like environment, you'll want to debug the
extension in a running Broker. The easiest way to do this is:

* Build your branch. Use the `-P bundle-contrib-exts` flag in place of `-P dist`, as described
  above.
* Create an install from the distribution produced above.
* Use the `single-server/micro-quickstart` config for debugging.
* Configure the installation using the steps above.
* Modify the Supervisor config for your config to comment out the line that launches
  the broker. Use the hash (`#`) character to comment out the line.
* In your IDE, define a launch configuration for the Broker.
  * The launch command is `server broker`
  * Add the following JVM arguments:
  
```text
--add-exports java.base/jdk.internal.perf=ALL-UNNAMED
--add-exports jdk.management/com.sun.management.internal=ALL-UNNAMED
```

  * Define `grpc-query` as a project dependency. (This is for Eclipse; IntelliJ may differ.)
  * Configure the class path to include the common and Broker properties files.
  * Explicitly add the `extension/grpc-query/druid-shaded-grpc-<version>-SNAPSHOT.jar` in
    the class path.
* Launch the micro-quickstart cluster.
* Launch the Broker in your IDE.

### gRPC Logging

Debugging of the gRPC stack is difficult since the shaded jar loses source attachments.

Logging helps. gRPC logging is not enabled via Druid's logging system. Intead, [create
the following `logging.properties` file](https://stackoverflow.com/questions/50243717/grpc-logger-level):

```text
handlers=java.util.logging.ConsoleHandler
io.grpc.level=FINE
java.util.logging.ConsoleHandler.level=FINE
java.util.logging.ConsoleHandler.formatter=java.util.logging.SimpleFormatter
```

Then, pass the following on the command line:

```text
-Djava.util.logging.config.file=logging.properties
```

Adjust the path to the file depending on where you put the file.

## Acknowledgements

This is not the first project to have created a gRPC API for Druid. Others include:

* [[Proposal] define a RPC protocol for querying data, support apache Arrow as data
  exchange interface](https://github.com/apache/druid/issues/3891)
* [gRPC Druid extension PoC](https://github.com/ndolgov/gruid)
* [Druid gRPC-json server extension](https://github.com/apache/druid/pull/6798)

Full credit goes to those who have gone this way before.
