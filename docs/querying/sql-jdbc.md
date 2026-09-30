---
id: sql-jdbc
title: "Druid JDBC driver"
sidebar_label: "JDBC driver"
---

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

<!-- docs/tutorials/tutorial-jdbc.md redirects here -->

The Druid JDBC driver provides connectivity to Apache Druid using the
[SQL HTTP API](../api-reference/sql-api.md) at `/druid/v2/sql/`. You can use it from Java code, or from any
BI or SQL tool that accepts a third-party JDBC driver.

## Install

The driver is published to Maven Central as a single JAR with its dependencies bundled in:

```xml
<dependency>
  <groupId>org.apache.druid</groupId>
  <artifactId>druid-jdbc-driver</artifactId>
  <version>${druid.version}</version>
</dependency>
```

The driver requires a minimum of Java 17. It is not part of the Druid distribution tarball.

For a third-party tool, download the JAR from
[Maven Central](https://repo1.maven.org/maven2/org/apache/druid/druid-jdbc-driver/) and register it with the tool.
If the tool requires a driver class, use `org.apache.druid.jdbc.DruidJdbcDriver`.

## Connection URL

The driver accepts URLs of the following form:

```
jdbc:druid:https://host:port/druid/v2/sql/?param=value&param=value
```

| Component | Description |
|---|---|
| Prefix | `jdbc:druid:` followed by an HTTP or HTTPS URL. |
| Scheme | `http` or `https`. Required. |
| Host | Hostname or IP address of a Druid Router or Broker. Required. |
| Port | Port of the Router or Broker. Defaults to 80 for `http` and 443 for `https`. |
| Path | Path of the SQL endpoint, normally `/druid/v2/sql/`. |
| Parameters | Optional [driver parameters](#driver-parameters) and [query context parameters](#query-context-parameters), in standard URL query string form. URL-encode any values that contain reserved characters such as `&`, `=`, or `#`. |

Credentials in the `user:password@host` form are rejected. Pass them as [driver parameters](#driver-parameters)
instead.

Some examples:

```
# Router on the default quickstart port.
jdbc:druid:http://localhost:8888/druid/v2/sql/

# HTTPS on the default port (443).
jdbc:druid:https://druid.example.com/druid/v2/sql/

# Basic authentication.
jdbc:druid:http://localhost:8888/druid/v2/sql/?authentication=basic&user=admin&password=secret

# Setting query context parameters.
jdbc:druid:http://localhost:8888/druid/v2/sql/?timeout=60000&sqlTimeZone=Etc/UTC
```

## Connection parameters

You can supply connection parameters either as a query string in the JDBC URL or connection properties. When writing
Java code, you can specify the connection properties using `Properties` passed to `DriverManager.getConnection`.
When the same name appears in both, the URL takes precedence.

### Driver parameters

The driver interprets certain connection parameters itself. All other connection parameters are sent to Druid as
[query context parameters](#query-context-parameters).

| Parameter | Description | Default |
|---|---|---|
| `authentication` | Authentication scheme: `basic` or `basicRaw`. See [Authentication](#authentication). Values are case-sensitive. | `basic` if `user` or `password` is set; otherwise no credentials are sent |
| `user` | Username. Only used when `authentication` is `basic`. | None |
| `password` | Password, or the credential token when `authentication` is `basicRaw`. | None |
| `verifyTls` | Whether to validate the server's TLS certificate chain on HTTPS connections. | `true` |

### Query context parameters

Any parameter that isn't in the table above is passed to Druid as a [query context parameter](sql-query-context.md)
on every query issued by the connection. For example, `?timeout=60000&sqlTimeZone=America/Los_Angeles` sets a 60
second server-side query timeout and makes Druid compute time values in the `America/Los_Angeles` time zone.

Query context parameters can also be supplied using [`SET` statements](#set-statements), which take precedence
over properties supplied in the JDBC URL or connection properties.

## Authentication

When configured with authentication, the driver sends credentials with every request as an HTTP `Authorization` header.
It sends no header at all when `authentication`, `user`, and `password` are all unset.

`authentication=basic` performs standard HTTP basic authentication: the driver joins `user` and `password` with a
colon and base64-encodes the result. Use this with Druid's
[basic security](../development/extensions-core/druid-basic-security.md) extension. This is also the default scheme
when you set `user` or `password` without setting `authentication`.

`authentication=basicRaw` sends `Authorization: Basic <password>`, using the value of `password` verbatim as the
already-encoded credential, and ignores `user`. `password` is required. Use this when you already hold an encoded
credential or a token issued by a custom authenticator.

Whenever possible, supply credentials using connection properties rather than the JDBC URL. This helps prevent them from
being logged or stored in configuration files.

## SET statements

The driver accepts `SET` statements, which set [query context parameters](sql-query-context.md) for all subsequent
queries on the same connection. `SET` statements are handled entirely within the driver and generate no requests to
Druid.

```sql
SET timeout = 60000;
SET sqlTimeZone = 'America/Los_Angeles';
SET engine = 'msq-dart';
```

Values use SQL literal syntax: single quotes for strings, unquoted numbers, and `true`, `false`, `null`, or
`unknown` in any capitalization.

You can send several `SET` statements in one JDBC call, optionally followed by one regular query, separated by
semicolons. Only one non-`SET` statement is allowed per call, and it must come last, such as:

```sql
SET timeout = 60000;
SET sqlTimeZone = 'Etc/UTC';
SELECT COUNT(*) FROM wikipedia
```

Setting a parameter to `NULL` undoes any earlier `SET` of that parameter on the same connection.
The value then falls back to whatever the JDBC URL or connection properties supplied, if anything.

Prepared statements cannot be used for `SET`.

## Driver behavior

### Connections exist only in the driver

Druid does not maintain per-connection server-side state. A JDBC `Connection` is a client-side object that holds a
URL, credentials, an HTTP client, and any context parameters set by `SET` statements. Each query is an independent
HTTP request.

This has a few practical consequences:

- You do not need connection stickiness or a sticky load balancer.
- A Broker restart does not invalidate a connection. In-flight queries fail, but later queries succeed.
- Idle connections consume no server resources. They are not free on the client side, though: each `Connection` owns
  an HTTP client, and therefore its own socket pool and threads. Reuse connections rather than opening one per query.

`DriverManager.getConnection` does issue one `SELECT 1` query to check that the endpoint is reachable and the
credentials work, so an unreachable host or bad credentials fails at connection time rather than at first query.
`Connection.isValid` runs the same query.

### Prepared statements are prepared at execution

`Connection.prepareStatement` performs no network I/O. The driver sends the SQL and the bound parameter values
together when you execute the statement. Druid plans the query on each execution.

As a result, errors you might expect from `prepareStatement`, such as syntax errors or unknown table and column
names, surface when the statement is first executed. Re-executing a `PreparedStatement` with new parameter values
is not cheaper than executing a fresh `Statement`.

### Statements can run concurrently

Because each query is a separate HTTP request, statements created from one connection are independent and can
execute concurrently from different threads.

A single `Statement` holds at most one open `ResultSet`. Executing a statement again closes the `ResultSet` from
its previous execution, so read one result set to completion before re-executing the statement that produced it.

`Statement.close`, `Statement.isClosed`, `Statement.cancel`, `ResultSet.close`, and `ResultSet.isClosed` are safe
to call from any thread. All other methods on a statement or result set must be called from one thread at a time.

### Streaming results

Rows are parsed from the HTTP response as you call `ResultSet.next`, so a large result set doesn't have to fit in
memory. `Statement.setFetchSize` is accepted and ignored, and `getFetchSize` reports `0`; there is no batched
fetching.

Because a `ResultSet` holds an open HTTP response, it is important to close it when you're done.
Closing a `Statement` closes its current `ResultSet`, and closing a `Connection` closes all of its statements.

### Timeouts and cancellation

`Statement.setQueryTimeout(seconds)` sets the Druid `timeout` context parameter, and `Statement.setMaxRows(n)` sets
`sqlOuterLimit`. These calls override values that you set explicitly in the JDBC URL or with a `SET` statement.
Following JDBC, `0` means "no limit", so it removes any value from those other sources rather than restoring it.

Both of these are server-side limits. The only client-side limit is `Connection.setNetworkTimeout`, which bounds how
long the driver waits for a response. It has no default and no connection parameter, so it must be set on a
`Connection` you already hold. The connect timeout is fixed at 10 seconds and cancellation requests time out after
5 seconds.

`Statement.cancel` cancels the query on the server by issuing an HTTP `DELETE` for the query's `sqlQueryId`. The
driver generates a fresh `sqlQueryId` per query if necessary.

### Metadata

`DatabaseMetaData` methods that describe schemas, tables, and columns run queries against the
[`INFORMATION_SCHEMA` tables](sql-metadata-tables.md). You can also query those tables directly. Methods that
describe features Druid does not have, such as stored procedures, primary and foreign keys, and indexes, return
empty result sets.

`getDatabaseProductVersion`, `getDatabaseMajorVersion`, and `getDatabaseMinorVersion` query the
[`sys.servers` table](sql-metadata-tables.md#servers-table), which may require `sys` permissions. If that query
is not authorized, an unknown version is reported.

### Unsupported features

The driver rejects the following with `SQLFeatureNotSupportedException`:

- Writes. `executeUpdate`, `addBatch`, and `executeBatch` are rejected, and `ResultSet` update methods throw.
- Transactions. Auto-commit is always on, the only supported isolation level is `TRANSACTION_NONE`, and savepoints
  are rejected.
- Scrollable and updatable result sets. Result sets are always `TYPE_FORWARD_ONLY` and `CONCUR_READ_ONLY`, so
  `previous`, `absolute`, `first`, and `last` throw.
- Stored procedures, `Blob`, `Clob`, `Ref`, `RowId`, `SQLXML`, and the stream-valued getters and setters.

## Example

To run a query and read the results, you can use code like the following. Note that in production usage, reusing
the `Connection` is encouraged, because each `Connection` has its own HTTP client.

```java
final String url = "jdbc:druid:http://localhost:8888/druid/v2/sql/";

try (
    Connection connection = DriverManager.getConnection(url);
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        "SELECT page, COUNT(*) AS edits FROM wikipedia GROUP BY 1 ORDER BY 2 DESC LIMIT 10")
) {
  while (resultSet.next()) {
    System.out.println(resultSet.getString("page") + ": " + resultSet.getLong("edits"));
  }
}
```
