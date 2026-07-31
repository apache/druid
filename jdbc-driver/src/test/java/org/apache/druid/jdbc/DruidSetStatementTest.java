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

package org.apache.druid.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jdbc.http.DruidHttpClient;
import org.apache.druid.jdbc.http.SqlRequest;
import org.apache.druid.jdbc.http.TestQueryResultsIterator;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DruidSetStatementTest
{
  private DruidHttpClient mockHttpClient;
  private DruidConnection connection;
  private DruidStatement statement;

  @BeforeEach
  public void setUp() throws SQLException
  {
    mockHttpClient = mock(DruidHttpClient.class);

    connection = new DruidConnection(
        DruidConnectionUrl.parse("jdbc:druid:http://localhost:8888/druid/v2/sql/?urlParam=urlValue", null),
        mockHttpClient,
        new ObjectMapper()
    );
    statement = new DruidStatement(connection);

    // These tests assert on the connection context rather than on results, but a query must still come back
    // with results, since DruidHttpClient#runQuery throws rather than returning null. Each call gets its own
    // iterator, as it would from the server.
    when(mockHttpClient.runQuery(any(SqlRequest.class)))
        .thenAnswer(invocation -> TestQueryResultsIterator.empty(List.of()));
  }

  /**
   * The value's SQL literal form determines its Java type.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setStatements")
  public void testSetValue(final String sql, final String key, final Object expectedValue) throws SQLException
  {
    Assertions.assertFalse(statement.execute(sql));
    Assertions.assertEquals(expectedValue, connection.getSetStatementQueryContext().get(key));
  }

  private static Stream<Arguments> setStatements()
  {
    return Stream.of(
        Arguments.of("SET engine = 'native'", "engine", "native"),
        Arguments.of("SET engine = 'msq'", "engine", "msq"),
        Arguments.of("SET maxRows = 1000", "maxRows", 1000L),
        Arguments.of("SET useApproximateCountDistinct = true", "useApproximateCountDistinct", true),
        Arguments.of("SET priority = 3.14", "priority", 3.14),
        Arguments.of("SET   complexParam   =   'value with spaces'   ", "complexParam", "value with spaces"),
        Arguments.of("SET message = 'It''s a test'", "message", "It's a test"),
        Arguments.of("SET threshold = 1e5", "threshold", 1e5),
        Arguments.of("SET epsilon = 2.5E-3", "epsilon", 2.5E-3),
        Arguments.of("SET bigValue = 3E10", "bigValue", 3E10),
        Arguments.of("SET value = 1.5e+2", "value", 1.5e+2)
    );
  }

  @Test
  public void testSetNullRemovesTheKey() throws SQLException
  {
    Assertions.assertFalse(statement.execute("SET timeout = 60000"));
    Assertions.assertEquals(60000L, connection.getSetStatementQueryContext().get("timeout"));

    Assertions.assertFalse(statement.execute("SET timeout = null"));
    Assertions.assertFalse(connection.getSetStatementQueryContext().containsKey("timeout"));
    Assertions.assertFalse(connection.getQueryContext().containsKey("timeout"));

    // Setting a key that was never set is a no-op rather than an error.
    Assertions.assertFalse(statement.execute("SET neverSet = NULL"));
    Assertions.assertFalse(connection.getSetStatementQueryContext().containsKey("neverSet"));

    // The key can be set again afterwards.
    Assertions.assertFalse(statement.execute("SET timeout = 30000"));
    Assertions.assertEquals(30000L, connection.getSetStatementQueryContext().get("timeout"));
  }

  /**
   * Removing a SET override leaves the value the JDBC URL supplied.
   */
  @Test
  public void testSetNullRevealsTheUrlValue() throws SQLException
  {
    Assertions.assertEquals("urlValue", connection.getQueryContext().get("urlParam"));

    Assertions.assertFalse(statement.execute("SET urlParam = 'overridden'"));
    Assertions.assertEquals("overridden", connection.getQueryContext().get("urlParam"));

    Assertions.assertFalse(statement.execute("SET urlParam = null"));
    Assertions.assertEquals("urlValue", connection.getQueryContext().get("urlParam"));
  }

  @Test
  public void testInvalidSetValue()
  {
    assertSetFails("SET engine = native", "SET value invalid: native");
    assertSetFails("SET engine = \"native\"", "SET value invalid: \"native\"");
    assertSetFails("SET timeout = ", "SET value missing");
  }

  /**
   * A statement that opens with SET but is malformed is rejected here, not passed off to the server.
   */
  @Test
  public void testMalformedSetIsRejected() throws SQLException
  {
    for (final String sql : new String[]{"SET", "SET timeout", "SET = value", "SET timeout 60000"}) {
      assertSetFails(sql, "SET syntax invalid: " + sql);
    }

    Assertions.assertTrue(connection.getSetStatementQueryContext().isEmpty());
    verify(mockHttpClient, never()).runQuery(any(SqlRequest.class));
  }

  private void assertSetFails(final String sql, final String expectedMessage)
  {
    final SQLException exception = Assertions.assertThrows(SQLException.class, () -> statement.execute(sql));
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString(expectedMessage));
  }

  @Test
  public void testSetPrefixedIdentifierIsNotASetStatement() throws SQLException
  {
    Assertions.assertTrue(statement.execute("SELECT * FROM settings"));
    Assertions.assertTrue(connection.getSetStatementQueryContext().isEmpty());
  }

  /**
   * A SET may precede a regular statement, which then runs with those settings in effect.
   */
  @Test
  public void testSetMayPrecedeAnotherStatement() throws SQLException
  {
    final String[] statements = {
        "SET timeout = 60000; SELECT COUNT(*) FROM datasource",
        "SET engine = 'native'; INSERT INTO test VALUES (1, 'test')",
        "SET timeout = 60000 /* this is a SET */; /* comment */ SELECT COUNT(*) FROM test /* not a SET */"
    };

    for (final String sql : statements) {
      Assertions.assertTrue(statement.execute(sql));
      Assertions.assertFalse(connection.getSetStatementQueryContext().isEmpty());
    }
  }

  /**
   * At most one regular statement may run at a time, so nothing may follow one, not even a SET.
   */
  @Test
  public void testCannotExecuteMoreThanOneRegularStatement()
  {
    for (final String sql : new String[]{"SELECT 1; SET timeout = 60000", "SELECT 1; SELECT 2"}) {
      final SQLException exception = Assertions.assertThrows(SQLException.class, () -> statement.execute(sql));
      MatcherAssert.assertThat(
          exception.getMessage(),
          Matchers.containsString("Cannot execute more than one regular (non-SET) statement")
      );
    }
  }

  @Test
  public void testCommentHandling() throws SQLException
  {
    Assertions.assertFalse(statement.execute("SET timeout = 30000 -- timeout for queries"));
    Assertions.assertFalse(statement.execute("SET /* comment */ maxRows /* another */ = /* yet another */ 1000"));
    Assertions.assertFalse(statement.execute("SET priority = 5 -- SET invalid_param = unquoted"));
    Assertions.assertFalse(statement.execute(
        "-- This is an entire line comment\n"
        + "SET engine = 'msq'\n"
        + "-- SET this_is_in_comment = 'ignored'"
    ));

    final Map<String, Object> connectionContext = connection.getSetStatementQueryContext();
    Assertions.assertEquals(30000L, connectionContext.get("timeout"));
    Assertions.assertEquals(1000L, connectionContext.get("maxRows"));
    Assertions.assertEquals(5L, connectionContext.get("priority"));
    Assertions.assertEquals("msq", connectionContext.get("engine"));
    Assertions.assertNull(connectionContext.get("invalid_param"));
    Assertions.assertNull(connectionContext.get("this_is_in_comment"));
  }

  /**
   * A query's context is the URL's parameters plus everything SET, with SET winning on a conflict.
   */
  @Test
  public void testCombinedQueryContext() throws SQLException
  {
    statement.execute("SET timeout = 60000");
    statement.execute("SET urlParam = 'overridden'");
    statement.executeQuery("SELECT 1");

    final ArgumentCaptor<SqlRequest> requestCaptor = ArgumentCaptor.forClass(SqlRequest.class);
    verify(mockHttpClient).runQuery(requestCaptor.capture());

    final Map<String, Object> queryContext = requestCaptor.getValue().context();
    Assertions.assertEquals(60000L, queryContext.get("timeout"));
    Assertions.assertEquals("overridden", queryContext.get("urlParam"));
    Assertions.assertTrue(queryContext.containsKey("sqlQueryId"));
  }

  @Test
  public void testCaseInsensitiveSetKeyword() throws SQLException
  {
    Assertions.assertFalse(statement.execute("set timeout = 1000"));
    Assertions.assertFalse(statement.execute("Set maxRows = 500"));
    Assertions.assertFalse(statement.execute("SET useCache = false"));

    final Map<String, Object> connectionContext = connection.getSetStatementQueryContext();
    Assertions.assertEquals(1000L, connectionContext.get("timeout"));
    Assertions.assertEquals(500L, connectionContext.get("maxRows"));
    Assertions.assertEquals(false, connectionContext.get("useCache"));
  }

  @Test
  public void testMultipleSetStatements() throws SQLException
  {
    Assertions.assertFalse(statement.execute(
        "SET timeout = 60000; ; SET maxRows = 1000 /* with a comment */; ; SET engine = 'native';"
    ));

    final Map<String, Object> connectionContext = connection.getSetStatementQueryContext();
    Assertions.assertEquals(60000L, connectionContext.get("timeout"));
    Assertions.assertEquals(1000L, connectionContext.get("maxRows"));
    Assertions.assertEquals("native", connectionContext.get("engine"));
  }
}
