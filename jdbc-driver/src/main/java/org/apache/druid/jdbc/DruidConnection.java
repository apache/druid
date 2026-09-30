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
import org.apache.druid.jdbc.http.QueryResultsIterator;
import org.apache.druid.jdbc.http.SqlRequest;

import javax.annotation.Nullable;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Our implementation of JDBC {@link Connection}. Thread-safe.
 */
public class DruidConnection implements Connection
{
  private final DruidConnectionUrl connectionUrl;
  private final DruidHttpClient httpClient;
  private final ObjectMapper jsonMapper;
  private final ConcurrentHashMap<String, Object> setStatementQueryContext = new ConcurrentHashMap<>();

  // Guarded by "this".
  private final Set<DruidStatement> openStatements = new HashSet<>();
  private volatile boolean closed; // Must acquire "this" when writing.

  // Guarded by "metaDataLock".
  private DruidDatabaseMetaData metaData;
  private final Object metaDataLock = new Object();

  public DruidConnection(
      final DruidConnectionUrl connectionUrl,
      final DruidHttpClient httpClient,
      final ObjectMapper jsonMapper
  )
  {
    this.connectionUrl = connectionUrl;
    this.httpClient = httpClient;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public Statement createStatement() throws SQLException
  {
    final DruidStatement statement = new DruidStatement(this);

    synchronized (this) {
      throwIfClosed();
      openStatements.add(statement);
    }

    return statement;
  }

  @Override
  public PreparedStatement prepareStatement(final String sql) throws SQLException
  {
    final DruidPreparedStatement statement = new DruidPreparedStatement(this, sql);

    synchronized (this) {
      throwIfClosed();
      openStatements.add(statement);
    }

    return statement;
  }

  @Override
  public CallableStatement prepareCall(final String sql) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("CallableStatement not supported");
  }

  @Override
  public String nativeSQL(final String sql)
  {
    return sql;
  }

  @Override
  public boolean getAutoCommit()
  {
    return true;
  }

  @Override
  public void setAutoCommit(final boolean autoCommit) throws SQLException
  {
    if (!autoCommit) {
      throw new DruidJdbcFeatureNotSupportedException("Auto-commit cannot be disabled.");
    }
  }

  @Override
  public void commit()
  {
    // Do nothing: we are always in autocommit mode, so there cannot be an active transaction.
  }

  @Override
  public void rollback()
  {
    // Do nothing: we are always in autocommit mode, so there cannot be an active transaction.
  }

  @Override
  public DatabaseMetaData getMetaData()
  {
    synchronized (metaDataLock) {
      if (metaData == null) {
        metaData = new DruidDatabaseMetaData(this);
      }
      return metaData;
    }
  }

  @Override
  public boolean isReadOnly()
  {
    return true;
  }

  @Override
  public void setReadOnly(final boolean readOnly)
  {
    // JDBC defines this as a hint, which we choose to ignore.
  }

  @Override
  @Nullable
  public String getCatalog()
  {
    // Druid doesn't have catalogs (only schemas and tables).
    return null;
  }

  @Override
  public void setCatalog(final String catalog)
  {
    // Druid doesn't have catalogs (only schemas and tables), ignore.
  }

  @Override
  public int getTransactionIsolation()
  {
    // Transactions are not supported, return TRANSACTION_NONE.
    return TRANSACTION_NONE;
  }

  @Override
  public void setTransactionIsolation(final int level) throws SQLException
  {
    if (level != Connection.TRANSACTION_NONE) {
      throw new DruidJdbcFeatureNotSupportedException("Only TRANSACTION_NONE is supported.");
    }
  }

  @Override
  @Nullable
  public SQLWarning getWarnings()
  {
    // Druid's SQL protocol does not include warnings.
    return null;
  }

  @Override
  public void clearWarnings()
  {
    // Druid's SQL protocol does not include warnings, nothing to do.
  }

  @Override
  public Statement createStatement(final int resultSetType, final int resultSetConcurrency) throws SQLException
  {
    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
      throw new DruidJdbcFeatureNotSupportedException("Only TYPE_FORWARD_ONLY result sets are supported");
    }
    if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new DruidJdbcFeatureNotSupportedException("Only CONCUR_READ_ONLY result sets are supported");
    }
    return createStatement();
  }

  @Override
  public PreparedStatement prepareStatement(
      final String sql,
      final int resultSetType,
      final int resultSetConcurrency
  ) throws SQLException
  {
    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
      throw new DruidJdbcFeatureNotSupportedException("Only TYPE_FORWARD_ONLY result sets are supported");
    }
    if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new DruidJdbcFeatureNotSupportedException("Only CONCUR_READ_ONLY result sets are supported");
    }
    return prepareStatement(sql);
  }

  @Override
  public CallableStatement prepareCall(final String sql, final int resultSetType, final int resultSetConcurrency)
      throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("CallableStatement not supported");
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("Type maps not supported");
  }

  @Override
  public void setTypeMap(final Map<String, Class<?>> map) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("Type maps not supported");
  }

  @Override
  public int getHoldability()
  {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public void setHoldability(final int holdability) throws SQLException
  {
    if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
      throw new DruidJdbcFeatureNotSupportedException("Only CLOSE_CURSORS_AT_COMMIT is supported");
    }
  }

  @Override
  public Savepoint setSavepoint() throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("Savepoints not supported");
  }

  @Override
  public Savepoint setSavepoint(final String name) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("Savepoints not supported");
  }

  @Override
  public void rollback(final Savepoint savepoint) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("Savepoints not supported");
  }

  @Override
  public void releaseSavepoint(final Savepoint savepoint) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("Savepoints not supported");
  }

  @Override
  public Statement createStatement(
      final int resultSetType,
      final int resultSetConcurrency,
      final int resultSetHoldability
  ) throws SQLException
  {
    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
      throw new DruidJdbcFeatureNotSupportedException("Only TYPE_FORWARD_ONLY result sets are supported");
    }
    if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new DruidJdbcFeatureNotSupportedException("Only CONCUR_READ_ONLY result sets are supported");
    }
    if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
      throw new DruidJdbcFeatureNotSupportedException("Only CLOSE_CURSORS_AT_COMMIT holdability is supported");
    }
    return createStatement();
  }

  @Override
  public PreparedStatement prepareStatement(
      final String sql,
      final int resultSetType,
      final int resultSetConcurrency,
      final int resultSetHoldability
  ) throws SQLException
  {
    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
      throw new DruidJdbcFeatureNotSupportedException("Only TYPE_FORWARD_ONLY result sets are supported");
    }
    if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new DruidJdbcFeatureNotSupportedException("Only CONCUR_READ_ONLY result sets are supported");
    }
    if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
      throw new DruidJdbcFeatureNotSupportedException("Only CLOSE_CURSORS_AT_COMMIT holdability is supported");
    }
    return prepareStatement(sql);
  }

  @Override
  public CallableStatement prepareCall(
      final String sql,
      final int resultSetType,
      final int resultSetConcurrency,
      final int resultSetHoldability
  ) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("CallableStatement not supported");
  }

  @Override
  public PreparedStatement prepareStatement(final String sql, final int autoGeneratedKeys) throws SQLException
  {
    if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
      throw new DruidJdbcFeatureNotSupportedException("Auto-generated keys not supported");
    }
    return prepareStatement(sql);
  }

  @Override
  public PreparedStatement prepareStatement(final String sql, final int[] columnIndexes) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("Auto-generated keys not supported");
  }

  @Override
  public PreparedStatement prepareStatement(final String sql, final String[] columnNames) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("Auto-generated keys not supported");
  }

  @Override
  public Clob createClob() throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("Clob creation not supported");
  }

  @Override
  public Blob createBlob() throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("Blob creation not supported");
  }

  @Override
  public NClob createNClob() throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("NClob creation not supported");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("SQLXML creation not supported");
  }

  @Override
  public boolean isValid(final int timeout) throws SQLException
  {
    if (timeout < 0) {
      throw new DruidJdbcException("Timeout value must not be negative");
    }

    if (closed) {
      return false;
    }

    try {
      runValidationQuery(timeout);
      return true;
    }
    catch (Exception e) {
      return false;
    }
  }

  @Override
  public void setClientInfo(final String name, final String value) throws SQLClientInfoException
  {
    throw new SQLClientInfoException();
  }

  @Override
  @Nullable
  public String getClientInfo(final String name)
  {
    return null;
  }

  @Override
  public Properties getClientInfo()
  {
    return new Properties();
  }

  @Override
  public void setClientInfo(final Properties properties) throws SQLClientInfoException
  {
    throw new SQLClientInfoException();
  }

  @Override
  public Array createArrayOf(final String typeName, final Object[] elements)
  {
    final int baseType = switch (typeName.toUpperCase(Locale.ENGLISH)) {
      case "INTEGER", "INT" -> Types.INTEGER;
      case "BIGINT", "LONG" -> Types.BIGINT;
      case "DOUBLE" -> Types.DOUBLE;
      case "FLOAT" -> Types.FLOAT;
      case "REAL" -> Types.REAL;
      case "BOOLEAN" -> Types.BOOLEAN;
      default -> Types.VARCHAR;
    };
    return new DruidArray(baseType, typeName, elements);
  }

  @Override
  public Struct createStruct(final String typeName, final Object[] attributes) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("Struct creation not supported");
  }

  @Override
  @Nullable
  public String getSchema()
  {
    return null;
  }

  @Override
  public void setSchema(final String schema)
  {
    // Cannot set schema at the driver level, it must be specified in SQL. JDBC says drivers that do not support
    // this call should silently ignore it.
  }

  @Override
  public void abort(final Executor executor) throws SQLException
  {
    close();
  }

  @Override
  public void setNetworkTimeout(final Executor executor, final int milliseconds) throws SQLException
  {
    throwIfClosed();
    if (milliseconds < 0) {
      throw new DruidJdbcException("networkTimeout cannot be negative");
    }
    httpClient.setNetworkTimeoutMillis(milliseconds);
  }

  @Override
  public int getNetworkTimeout()
  {
    return httpClient.getNetworkTimeoutMillis();
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

  public DruidConnectionUrl getConnectionUrl()
  {
    return connectionUrl;
  }

  public DruidHttpClient getHttpClient()
  {
    return httpClient;
  }

  /**
   * Returns the full query context, taking into account both {@link DruidConnectionUrl#getQueryContext()} (from the
   * JDBC URL and properties) and {@link #setStatementQueryContext} (from {@code SET} statements issued in this
   * connection).
   */
  public Map<String, Object> getQueryContext()
  {
    final Map<String, Object> combined = new HashMap<>(connectionUrl.getQueryContext());
    combined.putAll(setStatementQueryContext);
    return combined;
  }

  /**
   * Called when a {@code SET} statement is executed. The driver intercepts these rather than sending
   * them to the server, so they can apply to all subsequent statements issued by the connection.
   */
  public void addSetStatementQueryContext(final String key, @Nullable final Object value)
  {
    if (value == null) {
      setStatementQueryContext.remove(key);
    } else {
      setStatementQueryContext.put(key, value);
    }
  }

  /**
   * Runs a {@code SELECT 1} validation query, blocking until it completes.
   */
  public void runValidationQuery(final int timeoutSeconds) throws SQLException
  {
    final Map<String, Object> context = getQueryContext();
    if (timeoutSeconds > 0) {
      context.put("timeout", (long) timeoutSeconds * 1000);
    }

    //noinspection EmptyTryBlock
    try (final QueryResultsIterator ignored = httpClient.runQuery(SqlRequest.of("SELECT 1", context, null))) {
      // Do nothing
    }
    catch (Exception e) {
      throw new DruidJdbcException(e, "Connection validation failed: %s", e);
    }
  }

  @Override
  public boolean isClosed() throws SQLException
  {
    return closed;
  }

  @Override
  public synchronized void close() throws SQLException
  {
    if (closed) {
      return;
    }

    closed = true;

    // Close all open statements. Use a snapshot to avoid ConcurrentModificationException: DruidStatement.close()
    // calls statementClosed() which removes from openStatements.
    Throwable e = null;
    for (final DruidStatement statement : new ArrayList<>(openStatements)) {
      try {
        statement.close();
      }
      catch (SQLException e2) {
        if (e == null) {
          e = e2;
        } else {
          e.addSuppressed(e2);
        }
      }
    }

    if (httpClient != null) {
      try {
        httpClient.close();
      }
      catch (Throwable e2) {
        if (e == null) {
          e = e2;
        } else {
          e.addSuppressed(e2);
        }
      }
    }

    if (e instanceof SQLException) {
      throw (SQLException) e;
    } else if (e != null) {
      throw new DruidJdbcException(e, "%s", e);
    }
  }

  ObjectMapper getJsonMapper()
  {
    return jsonMapper;
  }

  private void throwIfClosed() throws SQLException
  {
    if (closed) {
      throw new DruidJdbcException("Connection is closed");
    }
  }

  /**
   * Returns all connection context parameters that have been set by
   * {@link #addSetStatementQueryContext(String, Object)}.
   */
  Map<String, Object> getSetStatementQueryContext()
  {
    return new HashMap<>(setStatementQueryContext);
  }

  /**
   * Callback invoked by {@link DruidStatement#close()} to deregister the statement from this connection's
   * set of open statements.
   */
  synchronized void onStatementClosed(final DruidStatement statement)
  {
    openStatements.remove(statement);
  }
}
