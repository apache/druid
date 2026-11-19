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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * What a SET statement itself accepts is covered by DruidSetStatementTest.
 */
public class DruidSetStatementPreparedStatementTest
{
  private DruidHttpClient mockHttpClient;
  private DruidConnection connection;

  @BeforeEach
  public void setUp() throws SQLException
  {
    mockHttpClient = mock(DruidHttpClient.class);

    connection = new DruidConnection(
        DruidConnectionUrl.parse("jdbc:druid:http://localhost:8888/druid/v2/sql/?urlParam=urlValue", null),
        mockHttpClient,
        new ObjectMapper()
    );

    stubQueryResults();
  }

  private void stubQueryResults() throws SQLException
  {
    when(mockHttpClient.runQuery(any(SqlRequest.class)))
        .thenAnswer(invocation -> TestQueryResultsIterator.empty(List.of()));
  }

  private Map<String, Object> executeAndCaptureContext(final String sql, final Object parameter) throws SQLException
  {
    final PreparedStatement preparedStatement = connection.prepareStatement(sql);
    preparedStatement.setObject(1, parameter);

    // The rows are irrelevant here: what matters is the request that executing produced.
    preparedStatement.executeQuery().close();

    final ArgumentCaptor<SqlRequest> requestCaptor = ArgumentCaptor.forClass(SqlRequest.class);
    verify(mockHttpClient).runQuery(requestCaptor.capture());
    return requestCaptor.getValue().context();
  }

  @Test
  public void testSetViaStatementPropagatedToPreparedStatement() throws SQLException
  {
    final Statement statement = connection.createStatement();
    statement.execute("SET engine = 'native'");
    statement.execute("SET timeout = 60000; SET priority = 3");
    statement.execute("SET urlParam = 'overridden'");

    final Map<String, Object> queryContext =
        executeAndCaptureContext("SELECT * FROM test WHERE id = ?", 42);

    Assertions.assertEquals("native", queryContext.get("engine"));
    Assertions.assertEquals(60000L, queryContext.get("timeout"));
    Assertions.assertEquals(3L, queryContext.get("priority"));
    Assertions.assertEquals("overridden", queryContext.get("urlParam"));

    // The driver adds a sqlQueryId of its own so the query can be cancelled.
    Assertions.assertNotNull(queryContext.get("sqlQueryId"));
  }

  @Test
  public void testSetSqlQueryIdPropagatedToPreparedStatement() throws SQLException
  {
    connection.createStatement().execute("SET sqlQueryId = 'my-query-id'");

    final Map<String, Object> queryContext =
        executeAndCaptureContext("SELECT * FROM test WHERE id = ?", 1);

    Assertions.assertEquals("my-query-id", queryContext.get("sqlQueryId"));
  }

  @Test
  public void testContextIsReadAtExecutionTime() throws SQLException
  {
    final PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM test WHERE id = ?");
    final Statement statement = connection.createStatement();

    // A later SET is picked up by the statement that was prepared before it ran.
    statement.execute("SET engine = 'msq'");
    preparedStatement.setInt(1, 1);
    preparedStatement.executeQuery().close();

    final ArgumentCaptor<SqlRequest> requestCaptor = ArgumentCaptor.forClass(SqlRequest.class);
    verify(mockHttpClient).runQuery(requestCaptor.capture());
    Assertions.assertEquals("msq", requestCaptor.getValue().context().get("engine"));
  }

  /**
   * SET is handled per-connection by the driver, so it cannot be prepared or routed through execute(String).
   */
  @Test
  public void testSetCannotBeRoutedThroughAPreparedStatement() throws SQLException
  {
    final PreparedStatement setStatement = connection.prepareStatement("SET engine = 'native'");
    final SQLException exception = Assertions.assertThrows(SQLException.class, setStatement::execute);
    Assertions.assertTrue(exception.getMessage().contains("Cannot prepare SET statements"));

    final PreparedStatement queryStatement = connection.prepareStatement("SELECT * FROM test WHERE id = ?");
    Assertions.assertThrows(SQLException.class, () -> queryStatement.execute("SET engine = 'native'"));

    Assertions.assertFalse(connection.getSetStatementQueryContext().containsKey("engine"));
    verify(mockHttpClient, never()).runQuery(any(SqlRequest.class));
  }
}
