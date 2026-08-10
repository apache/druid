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
import org.apache.druid.jdbc.http.ColumnMetadata;
import org.apache.druid.jdbc.http.DruidHttpClient;
import org.apache.druid.jdbc.http.SqlRequest;
import org.apache.druid.jdbc.http.TestQueryResultsIterator;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Array;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class DruidConnectionTest
{
  private static final String DEFAULT_URL = "jdbc:druid:http://localhost:8888/druid/v2/sql/";
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  @Mock
  private DruidHttpClient mockHttpClient;

  @BeforeEach
  public void setUp() throws SQLException
  {
    lenient().when(mockHttpClient.runQuery(any())).thenAnswer(
        invocation -> new TestQueryResultsIterator(
            List.of(new ColumnMetadata("EXPR$0", JDBCType.INTEGER)),
            List.<Object[]>of(new Object[]{1})
        )
    );
  }

  private DruidConnection newConnection() throws SQLException
  {
    return new DruidConnection(
        DruidConnectionUrl.parse(DEFAULT_URL, new Properties()),
        mockHttpClient,
        JSON_MAPPER
    );
  }

  /**
   * The most recent query sent to the server.
   */
  private SqlRequest lastRequest() throws SQLException
  {
    final ArgumentCaptor<SqlRequest> captor = ArgumentCaptor.forClass(SqlRequest.class);
    verify(mockHttpClient, atLeastOnce()).runQuery(captor.capture());
    return captor.getValue();
  }

  @Test
  public void testConnectionClose() throws SQLException
  {
    final Connection connection = newConnection();

    Assertions.assertFalse(connection.isClosed());
    connection.close();
    Assertions.assertTrue(connection.isClosed());
  }

  @Test
  public void testUnchangeableConnectionSettings() throws SQLException
  {
    try (final Connection connection = newConnection()) {
      connection.setAutoCommit(true);
      Assertions.assertTrue(connection.getAutoCommit());
      MatcherAssert.assertThat(
          Assertions.assertThrows(SQLException.class, () -> connection.setAutoCommit(false)).getMessage(),
          Matchers.containsString("Auto-commit cannot be disabled.")
      );

      connection.setReadOnly(true);
      Assertions.assertTrue(connection.isReadOnly());
      connection.setReadOnly(false);
      Assertions.assertTrue(connection.isReadOnly());

      connection.setTransactionIsolation(Connection.TRANSACTION_NONE);
      Assertions.assertEquals(Connection.TRANSACTION_NONE, connection.getTransactionIsolation());
      MatcherAssert.assertThat(
          Assertions.assertThrows(
              SQLException.class,
              () -> connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
          ).getMessage(),
          Matchers.containsString("Only TRANSACTION_NONE is supported")
      );

      connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
      Assertions.assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, connection.getHoldability());
      MatcherAssert.assertThat(
          Assertions.assertThrows(
              SQLException.class,
              () -> connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT)
          ).getMessage(),
          Matchers.containsString("Only CLOSE_CURSORS_AT_COMMIT is supported")
      );
    }
  }

  @Test
  public void testIsValid() throws SQLException
  {
    final Connection connection = newConnection();

    Assertions.assertThrows(SQLException.class, () -> connection.isValid(-1));

    // isValid probes the server with a query, so its timeout goes into that query's context.
    Assertions.assertTrue(connection.isValid(0));
    Assertions.assertFalse(lastRequest().context().containsKey("timeout"));

    Assertions.assertTrue(connection.isValid(5));
    Assertions.assertEquals(5000L, lastRequest().context().get("timeout"));

    connection.close();
    Assertions.assertFalse(connection.isValid(0));
  }

  @Test
  public void testOperationsOnClosedConnection() throws SQLException
  {
    final Connection connection = newConnection();
    connection.close();

    // Only statement creation checks closed state; the other accessors are no-ops in Druid's model.
    Assertions.assertThrows(SQLException.class, () -> connection.createStatement());
    Assertions.assertThrows(SQLException.class, () -> connection.prepareStatement("SELECT 1"));
  }

  @Test
  public void testCreateArrayOf() throws SQLException
  {
    try (final Connection connection = newConnection()) {
      final Array integers = connection.createArrayOf("INTEGER", new Object[]{1, 2, 3});
      Assertions.assertEquals("INTEGER", integers.getBaseTypeName());
      Assertions.assertEquals(Types.INTEGER, integers.getBaseType());
      Assertions.assertArrayEquals(new Object[]{1, 2, 3}, (Object[]) integers.getArray());

      final Array strings = connection.createArrayOf("VARCHAR", new Object[]{"a", "b"});
      Assertions.assertEquals("VARCHAR", strings.getBaseTypeName());
      Assertions.assertEquals(Types.VARCHAR, strings.getBaseType());
      Assertions.assertArrayEquals(new Object[]{"a", "b"}, (Object[]) strings.getArray());
    }
  }

  @Test
  public void testWrapperInterface() throws SQLException
  {
    try (final Connection connection = newConnection()) {
      Assertions.assertTrue(connection.isWrapperFor(Connection.class));
      Assertions.assertTrue(connection.isWrapperFor(DruidConnection.class));
      Assertions.assertFalse(connection.isWrapperFor(String.class));

      Assertions.assertSame(connection, connection.unwrap(Connection.class));
      Assertions.assertSame(connection, connection.unwrap(DruidConnection.class));

      Assertions.assertThrows(SQLException.class, () -> connection.unwrap(String.class));
    }
  }

  @Test
  public void testCloseConnectionClosesOpenStatements() throws SQLException
  {
    final DruidConnection connection = newConnection();

    final Statement statement1 = connection.createStatement();
    final ResultSet resultSet1 = statement1.executeQuery("SELECT 1");
    final Statement statement2 = connection.createStatement();
    final ResultSet resultSet2 = statement2.executeQuery("SELECT 1");
    final PreparedStatement preparedStatement = connection.prepareStatement("SELECT 1");
    final ResultSet preparedResultSet = preparedStatement.executeQuery();

    connection.close();

    Assertions.assertTrue(connection.isClosed());
    Assertions.assertTrue(statement1.isClosed());
    Assertions.assertTrue(resultSet1.isClosed());
    Assertions.assertTrue(statement2.isClosed());
    Assertions.assertTrue(resultSet2.isClosed());
    Assertions.assertTrue(preparedStatement.isClosed());
    Assertions.assertTrue(preparedResultSet.isClosed());
  }

  @Test
  public void testManualOnStatementCloseDeregisters() throws SQLException
  {
    final DruidConnection connection = newConnection();

    final Statement statement = connection.createStatement();
    final ResultSet resultSet = statement.executeQuery("SELECT 1");

    statement.close();
    Assertions.assertTrue(statement.isClosed());
    Assertions.assertTrue(resultSet.isClosed());

    connection.close();
    Assertions.assertTrue(connection.isClosed());
  }

}
