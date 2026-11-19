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
import org.junit.jupiter.api.function.Executable;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.BatchUpdateException;
import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// Binding of parameter values is covered by DruidParameterBindingTest.
@ExtendWith(MockitoExtension.class)
public class DruidPreparedStatementTest
{
  @Mock
  private DruidHttpClient mockHttpClient;

  private DruidConnection connection;
  private DruidPreparedStatement preparedStatement;

  @BeforeEach
  public void setUp() throws SQLException
  {
    connection = new DruidConnection(
        DruidConnectionUrl.parse("jdbc:druid:http://localhost:8888/druid/v2/sql/", null),
        mockHttpClient,
        new ObjectMapper()
    );
    preparedStatement = new DruidPreparedStatement(connection, "SELECT * FROM test WHERE id = ?");

    // Most of these tests assert on the request that was sent rather than on results, but a query must still
    // come back with results, since DruidHttpClient#runQuery throws rather than returning null. Each call gets
    // its own iterator, as it would from the server.
    lenient().when(mockHttpClient.runQuery(any(SqlRequest.class)))
             .thenAnswer(invocation -> TestQueryResultsIterator.empty(List.of()));
  }

  /**
   * Answers the next query with the given columns and no rows, and returns the iterator it will answer with.
   */
  private TestQueryResultsIterator stubQueryResultColumns(final List<ColumnMetadata> columns) throws SQLException
  {
    final TestQueryResultsIterator results = TestQueryResultsIterator.empty(columns);
    when(mockHttpClient.runQuery(any(SqlRequest.class))).thenReturn(results);
    return results;
  }

  @Test
  public void testClearParameters() throws SQLException
  {
    preparedStatement.setString(1, "test");

    preparedStatement.clearParameters();

    final SQLException exception = Assertions.assertThrows(SQLException.class, () -> preparedStatement.executeQuery());
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("Parameter[1] must be bound"));
  }

  @Test
  public void testMissingParameter() throws SQLException
  {
    final DruidPreparedStatement twoParamStatement = new DruidPreparedStatement(
        connection,
        "SELECT * FROM test WHERE id = ? AND name = ?"
    );

    twoParamStatement.setString(1, "test");

    final SQLException exception = Assertions.assertThrows(SQLException.class, () -> twoParamStatement.executeQuery());
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("Parameter[2] must be bound"));
  }

  @Test
  @SuppressWarnings("UseOfIndexZeroInJDBCResultSet")
  public void testParameterIndexBelowOneRejected()
  {
    final SQLException exception =
        Assertions.assertThrows(SQLException.class, () -> preparedStatement.setString(0, "test"));
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("Parameter index must be >= 1"));
  }

  @Test
  public void testParameterIndexAboveExpectedCountRejected() throws SQLException
  {
    assertIndexRejected(() -> preparedStatement.setString(2, "x"), "2", "1");
    assertIndexRejected(() -> preparedStatement.setInt(999, 42), "999", "1");
    assertIndexRejected(() -> preparedStatement.setObject(5, "value"), "5", "1");

    final DruidPreparedStatement noParamStatement =
        new DruidPreparedStatement(connection, "SELECT COUNT(*) FROM test");
    assertIndexRejected(() -> noParamStatement.setString(1, "x"), "1", "0");

    // The boundary: index = the placeholder count is accepted, one past it is not.
    final DruidPreparedStatement twoParamStatement =
        new DruidPreparedStatement(connection, "SELECT * FROM test WHERE a = ? AND b = ?");
    twoParamStatement.setString(2, "ok");
    assertIndexRejected(() -> twoParamStatement.setString(3, "bad"), "3", "2");
  }

  private static void assertIndexRejected(
      final Executable call,
      final String expectedIndex,
      final String expectedCount
  )
  {
    final SQLException exception = Assertions.assertThrows(SQLException.class, call);
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString(expectedIndex));
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString(expectedCount));
  }

  @Test
  public void testGetMetaData() throws Exception
  {
    final TestQueryResultsIterator queryResults = stubQueryResultColumns(Arrays.asList(
        new ColumnMetadata("id", JDBCType.BIGINT),
        new ColumnMetadata("name", JDBCType.VARCHAR),
        new ColumnMetadata("created_at", JDBCType.TIMESTAMP)
    ));

    preparedStatement.setString(1, "test");

    final ResultSetMetaData metaData1 = preparedStatement.getMetaData();
    final ResultSetMetaData metaData2 = preparedStatement.getMetaData();

    final ArgumentCaptor<SqlRequest> requestCaptor = ArgumentCaptor.forClass(SqlRequest.class);
    verify(mockHttpClient, times(1)).runQuery(requestCaptor.capture());
    Assertions.assertEquals(0, requestCaptor.getValue().context().get("sqlOuterLimit"));

    Assertions.assertSame(metaData1, metaData2);
    Assertions.assertEquals(3, metaData1.getColumnCount());
    Assertions.assertTrue(queryResults.isClosed());
  }

  @Test
  public void testGetMetaDataWithNoParameters() throws SQLException
  {
    final DruidPreparedStatement noParamStatement =
        new DruidPreparedStatement(connection, "SELECT COUNT(*) FROM test");
    stubQueryResultColumns(List.of(new ColumnMetadata("EXPR$0", JDBCType.BIGINT)));

    Assertions.assertEquals(1, noParamStatement.getMetaData().getColumnCount());
  }

  @Test
  public void testGetMetaDataAfterStatementClosed() throws SQLException
  {
    preparedStatement.close();

    final SQLException exception = Assertions.assertThrows(SQLException.class, () -> preparedStatement.getMetaData());
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("Statement is closed"));
  }

  @Test
  public void testExecuteUpdate()
  {
    final SQLException exception = Assertions.assertThrows(SQLException.class, () -> preparedStatement.executeUpdate());
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("Update operations are not supported"));
  }

  @Test
  public void testExecuteReturnsTrue() throws SQLException
  {
    preparedStatement.setString(1, "test");
    Assertions.assertTrue(preparedStatement.execute());
    Assertions.assertNotNull(preparedStatement.getResultSet());
  }

  @Test
  public void testExecuteBatchThrowsBatchUpdateException() throws SQLException
  {
    final BatchUpdateException exception = Assertions.assertThrows(
        BatchUpdateException.class,
        () -> preparedStatement.executeBatch()
    );
    Assertions.assertEquals(0, exception.getUpdateCounts().length);
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("does not support batched updates"));

    verify(mockHttpClient, never()).runQuery(any(SqlRequest.class));
  }

  @Test
  public void testStringArgStatementMethodsRejected() throws SQLException
  {
    Assertions.assertThrows(SQLException.class, () -> preparedStatement.executeQuery("SELECT 1"));
    Assertions.assertThrows(SQLException.class, () -> preparedStatement.execute("SELECT 1"));
    Assertions.assertThrows(SQLException.class, () -> preparedStatement.executeUpdate("SELECT 1"));
    Assertions.assertThrows(SQLException.class, () -> preparedStatement.addBatch("SELECT 1"));
    Assertions.assertThrows(
        SQLException.class,
        () -> preparedStatement.executeUpdate("SELECT 1", Statement.NO_GENERATED_KEYS)
    );
    Assertions.assertThrows(SQLException.class, () -> preparedStatement.executeUpdate("SELECT 1", new int[]{1}));
    Assertions.assertThrows(SQLException.class, () -> preparedStatement.executeUpdate("SELECT 1", new String[]{"c"}));
    Assertions.assertThrows(
        SQLException.class,
        () -> preparedStatement.execute("SELECT 1", Statement.NO_GENERATED_KEYS)
    );
    Assertions.assertThrows(SQLException.class, () -> preparedStatement.execute("SELECT 1", new int[]{1}));
    Assertions.assertThrows(SQLException.class, () -> preparedStatement.execute("SELECT 1", new String[]{"c"}));

    verify(mockHttpClient, never()).runQuery(any(SqlRequest.class));
  }

  @Test
  public void testConnectionClosedBeforeExecution() throws SQLException
  {
    connection.close();

    final SQLException exception = Assertions.assertThrows(SQLException.class, () -> preparedStatement.executeQuery());
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("Connection is closed"));
  }

  @Test
  public void testStatementClosedBeforeExecution() throws SQLException
  {
    preparedStatement.close();

    final SQLException exception = Assertions.assertThrows(SQLException.class, () -> preparedStatement.executeQuery());
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("Statement is closed"));
  }
}
