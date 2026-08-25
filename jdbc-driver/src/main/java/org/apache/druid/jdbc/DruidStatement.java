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

import org.apache.druid.jdbc.http.DruidHttpClient;
import org.apache.druid.jdbc.http.QueryResultsIterator;
import org.apache.druid.jdbc.http.SqlParameter;
import org.apache.druid.jdbc.http.SqlRequest;
import org.apache.druid.jdbc.sql.SetStatement;
import org.apache.druid.jdbc.sql.SqlScanner;

import javax.annotation.Nullable;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Our implementation of JDBC {@link Statement}. Executes queries using Druid's SQL endpoint.
 *
 * <p>SET statements are intercepted in the driver so they may apply to future statements in the same connection.
 * This is necessary because JDBC connections are a purely driver-side concept: the server is connectionless.
 *
 * <p><b>Thread safety:</b> {@link #close()}, {@link #isClosed()}, and {@link #cancel()} are safe to
 * call from any thread. All other methods are not.
 */
public class DruidStatement implements Statement
{
  private final DruidConnection connection;
  private final DruidHttpClient httpClient;

  /**
   * The sqlQueryId of the currently-outstanding query. Used by {@link #cancel()}.
   */
  private final AtomicReference<String> currentSqlQueryId = new AtomicReference<>();

  /**
   * Whether this statement is closed. Set by {@link #close()}.
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Row limit set by {@link #setMaxRows(int)}, or null if unset. Applied to query context by
   * {@link #applyMaxRows(Map)}.
   */
  @Nullable
  private Integer maxRows;

  /**
   * Timeout set by {@link #setQueryTimeout(int)}, or null if unset. Applied to query context by
   * {@link #applyQueryTimeout(Map)}.
   */
  @Nullable
  private Integer queryTimeoutSeconds;

  /**
   * Flag set by {@link #closeOnCompletion()}. If set, this statement is closed when the associated result set
   * is closed.
   */
  private volatile boolean closeOnCompletion;

  /**
   * Reference to the currently associated result set.
   */
  private volatile ResultSet currentResultSet;

  public DruidStatement(final DruidConnection connection)
  {
    this.connection = connection;
    this.httpClient = connection.getHttpClient();
  }

  @Override
  public ResultSet executeQuery(final String sql) throws SQLException
  {
    if (execute(sql)) {
      return currentResultSet;
    } else {
      throw new DruidJdbcException("Query did not return a result set");
    }
  }

  @Override
  public int executeUpdate(final String sql) throws SQLException
  {
    throwIfClosed();
    throw new DruidJdbcFeatureNotSupportedException("executeUpdate not supported");
  }

  @Override
  public void close() throws SQLException
  {
    if (closed.compareAndSet(false, true)) {
      try {
        closeCurrentResultSet();
      }
      finally {
        connection.onStatementClosed(this);
      }
    }
  }

  @Override
  public int getMaxFieldSize() throws SQLException
  {
    throwIfClosed();
    return 0; // No limit
  }

  @Override
  public void setMaxFieldSize(final int max) throws SQLException
  {
    throwIfClosed();

    // Validated, but then ignored.
    if (max < 0) {
      throw new DruidJdbcException("maxFieldSize cannot be negative");
    }
  }

  @Override
  public int getMaxRows() throws SQLException
  {
    throwIfClosed();
    return maxRows != null ? maxRows : 0;
  }

  @Override
  public void setMaxRows(final int max) throws SQLException
  {
    throwIfClosed();
    if (max < 0) {
      throw new DruidJdbcException("maxRows cannot be negative");
    }
    this.maxRows = max;
  }

  @Override
  public void setEscapeProcessing(final boolean enable)
  {
    // Driver does not implement escape processing, so there's nothing to disable. Druid SQL implements some
    // escape processing server side, like {fn ...} and {ts ...}, which are not controlled here.
  }

  @Override
  public int getQueryTimeout() throws SQLException
  {
    throwIfClosed();
    return queryTimeoutSeconds != null ? queryTimeoutSeconds : 0;
  }

  @Override
  public void setQueryTimeout(final int seconds) throws SQLException
  {
    throwIfClosed();
    if (seconds < 0) {
      throw new DruidJdbcException("queryTimeout cannot be negative");
    }
    this.queryTimeoutSeconds = seconds;
  }

  @Override
  public void cancel() throws SQLException
  {
    throwIfClosed();

    final String sqlQueryId = currentSqlQueryId.getAndSet(null);
    if (sqlQueryId != null) {
      try {
        httpClient.cancelQuery(sqlQueryId);
      }
      catch (SQLException e) {
        throw new DruidJdbcException(e, "Failed to cancel query[%s]", sqlQueryId);
      }
    }
  }

  @Override
  @Nullable
  public SQLWarning getWarnings() throws SQLException
  {
    throwIfClosed();
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException
  {
    throwIfClosed();
  }

  @Override
  public void setCursorName(final String name) throws SQLException
  {
    throwIfClosed();
    throw new DruidJdbcFeatureNotSupportedException("Named cursors not supported");
  }

  @Override
  public boolean execute(final String sql) throws SQLException
  {
    throwIfClosed();

    final SqlScanner sqlScanner = SqlScanner.scan(sql);
    closeCurrentResultSet();

    // Handle SET statements in the driver.
    for (final SetStatement setStatement : sqlScanner.getSetStatements()) {
      connection.addSetStatementQueryContext(setStatement.key(), setStatement.value());
    }

    final String sqlStatement = sqlScanner.getSqlStatement();
    if (sqlStatement == null) {
      return false;
    }

    executeSql(sqlStatement, List.of());
    return true;
  }

  /**
   * Runs a query, installs the result as the current result set, and returns it. Shared by {@link #execute(String)}
   * and {@link DruidPreparedStatement#executeQuery()}.
   */
  protected ResultSet executeSql(final String sql, final List<SqlParameter> parameters) throws SQLException
  {
    try {
      closeCurrentResultSet();

      final Map<String, Object> queryContext = newQueryContext();
      currentSqlQueryId.set((String) queryContext.get("sqlQueryId"));

      final QueryResultsIterator results = httpClient.runQuery(SqlRequest.of(sql, queryContext, parameters));
      final ResultSet resultSet = new DruidResultSet(results, this, connection.getJsonMapper());

      currentResultSet = resultSet;
      if (closed.get()) {
        resultSet.close();
        throw new DruidJdbcException("Statement is closed");
      }

      return resultSet;
    }
    catch (Throwable e) {
      currentSqlQueryId.set(null);

      if (e instanceof SQLException) {
        throw e;
      } else {
        throw new DruidJdbcException(e, "Failed to execute query: %s", e);
      }
    }
  }

  @Override
  public ResultSet getResultSet() throws SQLException
  {
    throwIfClosed();
    return currentResultSet;
  }

  @Override
  public int getUpdateCount() throws SQLException
  {
    throwIfClosed();
    return -1; // No update count for read-only operations
  }

  @Override
  public boolean getMoreResults() throws SQLException
  {
    throwIfClosed();
    closeCurrentResultSet();
    return false; // Only one result set per query
  }

  @Override
  public int getFetchDirection() throws SQLException
  {
    throwIfClosed();
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchDirection(final int direction) throws SQLException
  {
    throwIfClosed();
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new DruidJdbcFeatureNotSupportedException("Only FETCH_FORWARD is supported");
    }
  }

  @Override
  public int getFetchSize() throws SQLException
  {
    throwIfClosed();

    // We don't use fetch size, so report zero (see setFetchSize).
    return 0;
  }

  @Override
  public void setFetchSize(final int rows) throws SQLException
  {
    throwIfClosed();

    // No-op, other than validating that "rows" is nonnegative. We fetch rows as a stream, not in batches.
    if (rows < 0) {
      throw new DruidJdbcException("fetchSize cannot be negative");
    }
  }

  @Override
  public int getResultSetConcurrency() throws SQLException
  {
    throwIfClosed();
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public int getResultSetType() throws SQLException
  {
    throwIfClosed();
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public void addBatch(final String sql) throws SQLException
  {
    // Nothing to do, executeBatch() always throws.
  }

  @Override
  public void clearBatch()
  {
    // Nothing to do, executeBatch() always throws.
  }

  @Override
  public int[] executeBatch() throws SQLException
  {
    throwIfClosed();

    // Per interface docs, batch statements that produce ResultSets must raise BatchUpdateException.
    // This driver currently only accepts SELECT statements, so we reject batches upfront.
    clearBatch();
    throw new BatchUpdateException(
        "Druid does not support batched updates",
        DruidSQLState.FeatureUnsupported.getSqlState(),
        0,
        new int[0]
    );
  }

  @Override
  public Connection getConnection() throws SQLException
  {
    throwIfClosed();
    return connection;
  }

  @Override
  public boolean getMoreResults(final int current) throws SQLException
  {
    throwIfClosed();

    if (current == Statement.CLOSE_CURRENT_RESULT || current == Statement.CLOSE_ALL_RESULTS) {
      closeCurrentResultSet();
    }

    // Only one result set per query
    return false;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException
  {
    throwIfClosed();
    throw new DruidJdbcFeatureNotSupportedException("Generated keys not supported");
  }

  @Override
  public int executeUpdate(final String sql, final int autoGeneratedKeys) throws SQLException
  {
    return executeUpdate(sql);
  }

  @Override
  public int executeUpdate(final String sql, final int[] columnIndexes) throws SQLException
  {
    return executeUpdate(sql);
  }

  @Override
  public int executeUpdate(final String sql, final String[] columnNames) throws SQLException
  {
    return executeUpdate(sql);
  }

  @Override
  public boolean execute(final String sql, final int autoGeneratedKeys) throws SQLException
  {
    return execute(sql);
  }

  @Override
  public boolean execute(final String sql, final int[] columnIndexes) throws SQLException
  {
    return execute(sql);
  }

  @Override
  public boolean execute(final String sql, final String[] columnNames) throws SQLException
  {
    return execute(sql);
  }

  @Override
  public int getResultSetHoldability() throws SQLException
  {
    throwIfClosed();
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public boolean isClosed() throws SQLException
  {
    return closed.get();
  }

  @Override
  public boolean isPoolable() throws SQLException
  {
    throwIfClosed();
    return false;
  }

  @Override
  public void setPoolable(final boolean poolable) throws SQLException
  {
    throwIfClosed();
  }

  @Override
  public void closeOnCompletion() throws SQLException
  {
    throwIfClosed();
    this.closeOnCompletion = true;
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException
  {
    throwIfClosed();
    return closeOnCompletion;
  }

  /**
   * Callback invoked by a {@link DruidResultSet} when it is closed. Clears the current result set state and,
   * if {@link #closeOnCompletion} is set, closes this statement as required by the JDBC contract.
   */
  void onResultSetClosed(final ResultSet resultSet) throws SQLException
  {
    //noinspection ObjectEquality
    if (resultSet == currentResultSet) {
      currentResultSet = null;
      currentSqlQueryId.set(null);
    }
    if (closeOnCompletion && !closed.get()) {
      close();
    }
  }

  @Override
  public <T> T unwrap(final Class<T> iface) throws SQLException
  {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new DruidJdbcException("Cannot unwrap to class[%s]", iface.getName());
  }

  @Override
  public boolean isWrapperFor(final Class<?> iface)
  {
    return iface.isAssignableFrom(getClass());
  }

  protected void throwIfClosed() throws SQLException
  {
    if (closed.get()) {
      throw new DruidJdbcException("Statement is closed");
    }
    if (connection.isClosed()) {
      throw new DruidJdbcException("Connection is closed");
    }
  }

  private void closeCurrentResultSet() throws SQLException
  {
    final boolean wasCloseOnCompletion = closeOnCompletion;

    if (currentResultSet != null && !currentResultSet.isClosed()) {
      // Clear closeOnCompletion if set, since this method is used for driver-initiated result set closes.
      // Close-on-completion only applies to user-initiated result set closes.
      closeOnCompletion = false;
      try {
        currentResultSet.close();
      }
      finally {
        // Restore closeOnCompletion, if it was set.
        closeOnCompletion = wasCloseOnCompletion;
        currentResultSet = null;
      }
    }
    // Clear the query ID when result set is closed
    currentSqlQueryId.set(null);
  }

  /**
   * Builds the query context for a query issued by this statement: the connection's context (from the JDBC URL,
   * connection properties, and {@code SET} statements), plus the per-statement settings from
   * {@link #setQueryTimeout(int)} and {@link #setMaxRows(int)}, plus a resolved {@code sqlQueryId}.
   *
   * <p>This does not make the query cancelable; only {@link #executeSql} does that, by saving the resolved
   * {@code sqlQueryId} as {@link #currentSqlQueryId}.
   */
  protected Map<String, Object> newQueryContext()
  {
    final Map<String, Object> queryContext = new HashMap<>(connection.getQueryContext());
    applyQueryTimeout(queryContext);
    applyMaxRows(queryContext);
    computeSqlQueryId(queryContext);
    return queryContext;
  }

  /**
   * Applies {@link #queryTimeoutSeconds} to the query context as the "timeout" key (in milliseconds). Per JDBC,
   * an explicit zero means "no limit", and therefore removes any value supplied by the URL or a SET statement.
   */
  private void applyQueryTimeout(final Map<String, Object> queryContext)
  {
    if (queryTimeoutSeconds != null) {
      if (queryTimeoutSeconds > 0) {
        queryContext.put("timeout", (long) queryTimeoutSeconds * 1000);
      } else {
        queryContext.remove("timeout");
      }
    }
  }

  /**
   * Applies {@link #maxRows} to the query context using "sqlOuterLimit". Zero is handled as in
   * {@link #applyQueryTimeout(Map)}.
   */
  private void applyMaxRows(final Map<String, Object> queryContext)
  {
    if (maxRows != null) {
      if (maxRows > 0) {
        queryContext.put("sqlOuterLimit", maxRows);
      } else {
        queryContext.remove("sqlOuterLimit");
      }
    }
  }

  protected DruidHttpClient getHttpClient()
  {
    return httpClient;
  }

  /**
   * Sets sqlQueryId to a random UUID if it is not already set in the context.
   */
  private static void computeSqlQueryId(final Map<String, Object> queryContext)
  {
    queryContext.compute(
        "sqlQueryId",
        (ignored, existingId) -> {
          if (existingId instanceof String) {
            return existingId;
          } else {
            return UUID.randomUUID().toString();
          }
        }
    );
  }
}
