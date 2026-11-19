/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.jdbc.http;

import com.fasterxml.jackson.core.io.JsonEOFException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.druid.jdbc.DruidConnectionUrl;
import org.apache.druid.jdbc.DruidSQLState;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpHeaders;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class DruidHttpClientTest
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testCloseHttpClient() throws SQLException
  {
    final DruidHttpClientImpl httpClient = newHttpClient();

    Assertions.assertEquals("http://localhost:8888/druid/v2/sql/", httpClient.getUrl());
    Assertions.assertFalse(httpClient.isClosed());

    httpClient.close();
    Assertions.assertTrue(httpClient.isClosed());

    // Closing again is harmless.
    httpClient.close();
    Assertions.assertTrue(httpClient.isClosed());
  }

  @Test
  public void testRunQueryAfterClose() throws SQLException
  {
    final DruidHttpClientImpl httpClient = newHttpClient();
    httpClient.close();

    final SqlRequest request = SqlRequest.of("SELECT 1", null, null);

    final SQLException exception = Assertions.assertThrows(
        SQLException.class,
        () -> httpClient.runQuery(request)
    );

    Assertions.assertEquals(DruidSQLState.ConnectionDoesNotExist.getSqlState(), exception.getSQLState());
    Assertions.assertEquals("HTTP client is closed", exception.getMessage());
  }

  @Test
  public void testRunQueryWithNetworkError() throws SQLException
  {
    // An unresolvable host forces a network error.
    final DruidConnectionUrl connectionUrl = DruidConnectionUrl.parse(
        "jdbc:druid:http://invalid-host-name-12345:8888/druid/v2/sql/",
        null
    );

    try (final DruidHttpClientImpl httpClient = new DruidHttpClientImpl(connectionUrl, OBJECT_MAPPER)) {
      final SqlRequest request = SqlRequest.of("SELECT 1", null, null);

      final SQLException exception = Assertions.assertThrows(
          SQLException.class,
          () -> httpClient.runQuery(request)
      );

      Assertions.assertEquals(DruidSQLState.ConnectionUnableToConnect.getSqlState(), exception.getSQLState());
      MatcherAssert.assertThat(
          exception.getMessage(), Matchers.anyOf(
              Matchers.containsString("Failed to connect"),
              Matchers.containsString("Failed to execute")
          )
      );
    }
  }

  @Test
  public void testBasicRawAuthenticationRequiresPassword() throws SQLException
  {
    final Properties props = new Properties();
    props.setProperty("authentication", "basicRaw");

    final DruidConnectionUrl connectionUrl = DruidConnectionUrl.parse(
        "jdbc:druid:http://localhost:8888/druid/v2/sql/",
        props
    );
    try (final DruidHttpClientImpl httpClient = new DruidHttpClientImpl(connectionUrl, OBJECT_MAPPER)) {
      final SqlRequest request = SqlRequest.of("SELECT 1", null, null);

      final SQLException exception = Assertions.assertThrows(
          SQLException.class,
          () -> httpClient.runQuery(request)
      );

      MatcherAssert.assertThat(
          exception.getMessage(),
          Matchers.containsString("Password is required for basicRaw authentication")
      );
    }
  }

  /**
   * Exercises the trust-all SSL context that verifyTls=false selects.
   */
  @Test
  public void testHttpsWithVerifyTlsDisabledBuildsClient() throws SQLException
  {
    final Properties props = new Properties();
    props.setProperty("verifyTls", "false");

    final DruidConnectionUrl connectionUrl = DruidConnectionUrl.parse(
        "jdbc:druid:https://localhost:8443/druid/v2/sql/",
        props
    );
    try (final DruidHttpClient httpClient = new DruidHttpClientImpl(connectionUrl, OBJECT_MAPPER)) {
      Assertions.assertFalse(httpClient.isClosed());
    }
  }

  /**
   * Parsing fails after the response stream has been handed over, so that stream must not leak.
   */
  @Test
  public void testRunQueryClosesStreamOnMalformedResponse() throws Exception
  {
    final HttpServer server = startSqlServer(
        exchange -> {
          final byte[] body = "this is not valid json".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        }
    );

    try (Closeable ignored = () -> server.stop(0);
         DruidHttpClientImpl httpClient = newHttpClient(server.getAddress().getPort())) {
      final SqlRequest request = SqlRequest.of("SELECT 1", null, null);

      final SQLException exception = Assertions.assertThrows(
          SQLException.class,
          () -> httpClient.runQuery(request)
      );

      MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("Failed to create streaming iterator"));
    }
  }

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "{\"error\":\"Query failed\", \"errorMessage\":\"Invalid query syntax\"} | Invalid query syntax",
      "{\"error\":\"Connection timeout\"}                                     | Connection timeout",
      "{\"some\":\"other fields\"}                                            | body: {\"some\":\"other fields\"}"
  })
  public void testToSQLException(final String responseJson, final String expectedMessage) throws SQLException
  {
    try (final DruidHttpClientImpl httpClient = newHttpClient()) {
      final SQLException exception = httpClient.toSQLException(500, jsonHeaders(), responseJson);
      MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString(expectedMessage));
    }
  }

  @Test
  public void testRunQueryWithTruncatedResponse() throws Exception
  {
    final HttpServer server = startSqlServer(
        exchange -> {
          // Headers, two rows, and then nothing: no closing bracket for the outer array.
          final byte[] body =
              "[[\"name\",\"age\"],[\"STRING\",\"LONG\"],[\"VARCHAR\",\"INTEGER\"],[\"Alice\",25],[\"Bob\",30]"
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, 0); // 0 means chunked, as Druid uses
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        }
    );

    try (Closeable ignored = () -> server.stop(0);
         DruidHttpClientImpl httpClient = newHttpClient(server.getAddress().getPort())) {
      final SqlRequest request = SqlRequest.of("SELECT name, age FROM tbl", Map.of("sqlQueryId", "abc-123"), null);

      try (final QueryResultsIterator iterator = httpClient.runQuery(request)) {
        // The rows that did arrive are still readable.
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("Alice", iterator.next()[0]);
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("Bob", iterator.next()[0]);

        final SQLException exception = Assertions.assertThrows(SQLException.class, iterator::hasNext);
        MatcherAssert.assertThat(
            exception.getMessage(),
            Matchers.containsString("Truncated response after[2] rows")
        );

        Assertions.assertInstanceOf(JsonEOFException.class, exception.getCause());
      }
    }
  }

  /**
   * Only 401 and 403 map to a SQLSTATE; every other status is left without one.
   */
  @ParameterizedTest
  @CsvSource({"401, true", "403, true", "404, false", "500, false"})
  public void testToSQLExceptionSqlState(final int statusCode, final boolean expectAuthSqlState)
      throws SQLException
  {
    final String responseJson = "{\"error\":\"Denied\"}";

    try (final DruidHttpClientImpl httpClient = newHttpClient()) {
      final SQLException exception = httpClient.toSQLException(statusCode, jsonHeaders(), responseJson);
      Assertions.assertEquals(
          expectAuthSqlState ? DruidSQLState.InvalidAuthorizationSpecification.getSqlState() : null,
          exception.getSQLState()
      );
      MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("HTTP " + statusCode + " error"));
    }
  }

  /**
   * Starts a local HTTP server that answers the SQL endpoint with {@code handler}. The caller must stop it.
   */
  private static HttpServer startSqlServer(final HttpHandler handler) throws IOException
  {
    final HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/druid/v2/sql/", handler);
    server.start();
    return server;
  }

  private static DruidHttpClientImpl newHttpClient() throws SQLException
  {
    return newHttpClient(8888);
  }

  private static DruidHttpClientImpl newHttpClient(final int port) throws SQLException
  {
    final DruidConnectionUrl connectionUrl = DruidConnectionUrl.parse(
        "jdbc:druid:http://localhost:" + port + "/druid/v2/sql/",
        null
    );
    return new DruidHttpClientImpl(connectionUrl, OBJECT_MAPPER);
  }

  private static HttpHeaders jsonHeaders()
  {
    return HttpHeaders.of(Map.of("Content-Type", List.of("application/json")), (k, v) -> true);
  }
}
