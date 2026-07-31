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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.BatchUpdateException;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DruidStatementTest
{
  private static final List<ColumnMetadata> COLUMNS = List.of(new ColumnMetadata("value", JDBCType.INTEGER));

  @Mock
  private DruidHttpClient mockHttpClient;

  private DruidStatement statement;
  private DruidConnection connection;

  @BeforeEach
  public void setUp() throws SQLException
  {
    connection = new DruidConnection(
        DruidConnectionUrl.parse("jdbc:druid:http://localhost:8888/druid/v2/sql/", null),
        mockHttpClient,
        new ObjectMapper()
    );
    statement = new DruidStatement(connection);
  }

  @AfterEach
  public void tearDown() throws SQLException
  {
    if (statement != null && !statement.isClosed()) {
      statement.close();
    }
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  /**
   * Answers every query with a fresh single-row iterator, as the server would.
   */
  private void stubOneRow(final Object value) throws SQLException
  {
    when(mockHttpClient.runQuery(any(SqlRequest.class))).thenAnswer(
        invocation -> new TestQueryResultsIterator(COLUMNS, Collections.singletonList(new Object[]{value}))
    );
  }

  private Map<String, Object> executedQueryContext() throws SQLException
  {
    final ArgumentCaptor<SqlRequest> captor = ArgumentCaptor.forClass(SqlRequest.class);
    verify(mockHttpClient).runQuery(captor.capture());
    return captor.getValue().context();
  }

  /**
   * A close that lands while the request is in flight must not leave the result set, and its response stream,
   * unreachable by any caller.
   */
  @Test
  public void testCloseDuringExecutionClosesTheResultSet() throws SQLException
  {
    final List<TestQueryResultsIterator> served = new ArrayList<>();
    when(mockHttpClient.runQuery(any(SqlRequest.class))).thenAnswer(
        invocation -> {
          statement.close();
          final TestQueryResultsIterator results =
              new TestQueryResultsIterator(COLUMNS, Collections.singletonList(new Object[]{1}));
          served.add(results);
          return results;
        }
    );

    Assertions.assertThrows(SQLException.class, () -> statement.executeQuery("SELECT 1 as value"));
    Assertions.assertTrue(served.get(0).isClosed());
  }

  @Test
  public void testStatementClose() throws SQLException
  {
    Assertions.assertFalse(statement.isClosed());
    statement.close();
    Assertions.assertTrue(statement.isClosed());

    // Closing again is harmless.
    statement.close();
    Assertions.assertTrue(statement.isClosed());
  }

  @Test
  public void testOperationsAfterClose() throws SQLException
  {
    statement.close();

    final SQLException exception =
        Assertions.assertThrows(SQLException.class, () -> statement.executeQuery("SELECT 1"));
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("Statement is closed"));
  }

  @Test
  public void testConnectionClosedBehavior() throws SQLException
  {
    connection.close();

    final SQLException exception =
        Assertions.assertThrows(SQLException.class, () -> statement.executeQuery("SELECT 1"));
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("Connection is closed"));
  }

  @Test
  public void testSuccessfulQueryExecution() throws SQLException
  {
    stubOneRow(1);

    final ResultSet resultSet = statement.executeQuery("SELECT 1 as value");
    Assertions.assertSame(resultSet, statement.getResultSet());
    Assertions.assertTrue(resultSet.next());
    Assertions.assertEquals(1, resultSet.getInt(1));
  }

  @Test
  public void testQueryWithoutResultSetRejected()
  {
    for (final String query : new String[]{null, "", "   "}) {
      final SQLException exception = Assertions.assertThrows(SQLException.class, () -> statement.executeQuery(query));
      MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("Query did not return a result set"));
    }
  }

  @Test
  public void testExecuteOverloadsDelegate() throws SQLException
  {
    stubOneRow(1);

    Assertions.assertTrue(statement.execute("SELECT 1 as value"));
    Assertions.assertNotNull(statement.getResultSet());
    Assertions.assertTrue(statement.execute("SELECT 1 as value", Statement.NO_GENERATED_KEYS));
    Assertions.assertTrue(statement.execute("SELECT 1 as value", new int[]{1}));
    Assertions.assertTrue(statement.execute("SELECT 1 as value", new String[]{"value"}));
  }

  @Test
  public void testStatementProperties() throws SQLException
  {
    statement.setMaxRows(100);
    Assertions.assertEquals(100, statement.getMaxRows());

    statement.setQueryTimeout(30);
    Assertions.assertEquals(30, statement.getQueryTimeout());

    MatcherAssert.assertThat(
        Assertions.assertThrows(SQLException.class, () -> statement.setMaxRows(-1)).getMessage(),
        Matchers.containsString("cannot be negative")
    );
    MatcherAssert.assertThat(
        Assertions.assertThrows(SQLException.class, () -> statement.setQueryTimeout(-1)).getMessage(),
        Matchers.containsString("cannot be negative")
    );
    MatcherAssert.assertThat(
        Assertions.assertThrows(SQLException.class, () -> statement.setFetchSize(-1)).getMessage(),
        Matchers.containsString("cannot be negative")
    );

    // setFetchSize is a no-op for nonnegative values, since rows are fetched as a stream rather than in batches.
    statement.setFetchSize(50);
  }

  @Test
  public void testFetchDirection() throws SQLException
  {
    statement.setFetchDirection(ResultSet.FETCH_FORWARD);
    Assertions.assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection());

    final SQLException exception =
        Assertions.assertThrows(SQLException.class, () -> statement.setFetchDirection(ResultSet.FETCH_REVERSE));
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("FETCH_FORWARD"));
  }

  @Test
  public void testExecuteUpdateRejected() throws SQLException
  {
    final SQLException updateException =
        Assertions.assertThrows(SQLException.class, () -> statement.executeUpdate("UPDATE test SET x = 1"));
    MatcherAssert.assertThat(updateException.getMessage(), Matchers.containsString("executeUpdate not supported"));

    // Cancel does not throw when no query is running.
    statement.cancel();
  }

  /**
   * Batches are rejected upfront rather than at execution time, whether or not anything was added.
   */
  @Test
  public void testExecuteBatchThrowsBatchUpdateException() throws SQLException
  {
    final BatchUpdateException emptyBatchException =
        Assertions.assertThrows(BatchUpdateException.class, () -> statement.executeBatch());
    Assertions.assertEquals(0, emptyBatchException.getUpdateCounts().length);

    statement.addBatch("SELECT 1");
    statement.addBatch("SELECT 2");

    final BatchUpdateException exception =
        Assertions.assertThrows(BatchUpdateException.class, () -> statement.executeBatch());
    Assertions.assertEquals(0, exception.getUpdateCounts().length);
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("does not support batched updates"));

    statement.clearBatch();
    verify(mockHttpClient, never()).runQuery(any(SqlRequest.class));
  }

  @Test
  public void testWrapperInterface() throws SQLException
  {
    Assertions.assertTrue(statement.isWrapperFor(DruidStatement.class));
    Assertions.assertTrue(statement.isWrapperFor(Statement.class));
    Assertions.assertFalse(statement.isWrapperFor(String.class));

    Assertions.assertSame(statement, statement.unwrap(DruidStatement.class));
    Assertions.assertSame(statement, statement.unwrap(Statement.class));

    final SQLException exception = Assertions.assertThrows(SQLException.class, () -> statement.unwrap(String.class));
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("Cannot unwrap"));
  }

  @Test
  public void testHttpClientError() throws SQLException
  {
    when(mockHttpClient.runQuery(any(SqlRequest.class)))
        .thenThrow(new DruidJdbcException("Druid query failed: Table not found"));

    final SQLException exception =
        Assertions.assertThrows(SQLException.class, () -> statement.executeQuery("SELECT * FROM nonexistent_table"));
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("Druid query failed"));
  }

  @Test
  public void testConcurrentResultSets() throws SQLException
  {
    stubOneRow(1);

    final ResultSet rs1 = statement.executeQuery("SELECT 1 as value");

    // Executing again closes the previous result set and makes the new one current.
    final ResultSet rs2 = statement.executeQuery("SELECT 2 as value");
    Assertions.assertTrue(rs1.isClosed());
    Assertions.assertFalse(rs2.isClosed());
    Assertions.assertSame(rs2, statement.getResultSet());
  }

  @Test
  public void testCancelDuringQuery() throws SQLException
  {
    stubOneRow(1);

    statement.executeQuery("SELECT 1 as value");
    statement.cancel();

    verify(mockHttpClient).cancelQuery(anyString());
  }

  @Test
  public void testCancelFailure() throws SQLException
  {
    stubOneRow(1);
    doThrow(new DruidJdbcException("Cancellation failed")).when(mockHttpClient).cancelQuery(anyString());

    statement.executeQuery("SELECT 1 as value");

    final SQLException exception = Assertions.assertThrows(SQLException.class, () -> statement.cancel());
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("Failed to cancel query"));
  }

  @Test
  public void testQueryTimeoutAppliedToContext() throws SQLException
  {
    stubOneRow(1);

    statement.setQueryTimeout(5);
    statement.executeQuery("SELECT 1 as value");

    Assertions.assertEquals(5000L, executedQueryContext().get("timeout"));
  }

  /**
   * Per JDBC, zero means "no limit", so it clears a value that came from the URL or a SET statement.
   */
  @Test
  public void testQueryTimeoutZeroClearsContextTimeout() throws SQLException
  {
    statement.execute("SET timeout = 30000");
    stubOneRow(1);

    statement.setQueryTimeout(0);
    statement.executeQuery("SELECT 1 as value");

    Assertions.assertFalse(executedQueryContext().containsKey("timeout"));
  }

  @Test
  public void testQueryTimeoutOverridesContextTimeout() throws SQLException
  {
    statement.execute("SET timeout = 30000");
    stubOneRow(1);

    statement.setQueryTimeout(5);
    statement.executeQuery("SELECT 1 as value");

    Assertions.assertEquals(5000L, executedQueryContext().get("timeout"));
  }

  @Test
  public void testMaxRowsAppliedToContext() throws SQLException
  {
    stubOneRow(1);

    statement.setMaxRows(100);
    statement.executeQuery("SELECT 1 as value");

    Assertions.assertEquals(100, executedQueryContext().get("sqlOuterLimit"));
  }

  @Test
  public void testMaxRowsZeroClearsContextSqlOuterLimit() throws SQLException
  {
    statement.execute("SET sqlOuterLimit = 50");
    stubOneRow(1);

    statement.setMaxRows(0);
    statement.executeQuery("SELECT 1 as value");

    Assertions.assertFalse(executedQueryContext().containsKey("sqlOuterLimit"));
  }

  @Test
  public void testMaxRowsOverridesContextSqlOuterLimit() throws SQLException
  {
    statement.execute("SET sqlOuterLimit = 50");
    stubOneRow(1);

    statement.setMaxRows(200);
    statement.executeQuery("SELECT 1 as value");

    Assertions.assertEquals(200, executedQueryContext().get("sqlOuterLimit"));
  }

  @Test
  public void testCloseOnCompletionClosesStatementWhenOnResultSetClosed() throws SQLException
  {
    stubOneRow(1);

    statement.closeOnCompletion();
    Assertions.assertTrue(statement.isCloseOnCompletion());

    final ResultSet rs = statement.executeQuery("SELECT 1 as value");
    Assertions.assertFalse(statement.isClosed());

    rs.close();
    Assertions.assertTrue(statement.isClosed());
  }

  @Test
  public void testWithoutCloseOnCompletionStatementRemainsOpenAfterOnResultSetClosed() throws SQLException
  {
    stubOneRow(1);

    Assertions.assertFalse(statement.isCloseOnCompletion());

    final ResultSet rs = statement.executeQuery("SELECT 1 as value");
    rs.close();
    Assertions.assertFalse(statement.isClosed());
  }

  /**
   * Close-on-completion applies only to result sets the application closes, not ones the driver closes.
   */
  @Test
  public void testCloseOnCompletionDoesNotCloseStatementWhenReExecuted() throws SQLException
  {
    when(mockHttpClient.runQuery(any(SqlRequest.class))).thenReturn(
        new TestQueryResultsIterator(COLUMNS, Collections.singletonList(new Object[]{1})),
        new TestQueryResultsIterator(COLUMNS, Collections.singletonList(new Object[]{2}))
    );

    statement.closeOnCompletion();

    final ResultSet firstRs = statement.executeQuery("SELECT 1 as value");
    final ResultSet secondRs = statement.executeQuery("SELECT 2 as value");
    Assertions.assertTrue(firstRs.isClosed());
    Assertions.assertFalse(statement.isClosed());

    Assertions.assertTrue(secondRs.next());
    Assertions.assertEquals(2, secondRs.getInt(1));

    secondRs.close();
    Assertions.assertTrue(statement.isClosed());
  }
}
