/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.avatica;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerFactory;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DruidMeta extends MetaImpl
{
  private static final Logger log = new Logger(DruidMeta.class);

  private final PlannerFactory plannerFactory;
  private final ScheduledExecutorService exec;
  private final AvaticaServerConfig config;

  // Used to track statements for a connection. Connection id -> statement id -> statement.
  // Not concurrent; synchronize on it when reading or writing.
  private final Map<String, DruidConnection> connections = new HashMap<>();

  // Used to generate statement ids.
  private final AtomicInteger statementCounter = new AtomicInteger();

  @Inject
  public DruidMeta(final PlannerFactory plannerFactory, final AvaticaServerConfig config)
  {
    super(null);
    this.plannerFactory = Preconditions.checkNotNull(plannerFactory, "plannerFactory");
    this.config = config;
    this.exec = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder()
            .setNameFormat(String.format("DruidMeta@%s-ScheduledExecutor", Integer.toHexString(hashCode())))
            .setDaemon(true)
            .build()
    );
  }

  @Override
  public void openConnection(final ConnectionHandle ch, final Map<String, String> info)
  {
    getDruidConnection(ch.id, true);
  }

  @Override
  public void closeConnection(final ConnectionHandle ch)
  {
    final List<DruidStatement> statements = new ArrayList<>();

    synchronized (connections) {
      final DruidConnection connection = connections.remove(ch.id);
      if (connection != null) {
        connection.sync(null);
        statements.addAll(connection.statements().values());
        log.debug("Connection[%s] closed, closing %,d statements.", ch.id, statements.size());
      }
    }

    final Closer closer = Closer.create();
    for (final DruidStatement statement : statements) {
      closer.register(statement);
    }
    try {
      closer.close();
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public ConnectionProperties connectionSync(final ConnectionHandle ch, final ConnectionProperties connProps)
  {
    // getDruidConnection re-syncs it.
    getDruidConnection(ch.id);
    return connProps;
  }

  @Override
  public StatementHandle createStatement(final ConnectionHandle ch)
  {
    synchronized (connections) {
      final DruidConnection connection = getDruidConnection(ch.id);
      final StatementHandle statement = new StatementHandle(ch.id, statementCounter.incrementAndGet(), null);

      if (connection.statements().containsKey(statement.id)) {
        // Will only happen if statementCounter rolls over before old statements are cleaned up. If this
        // ever happens then something fishy is going on, because we shouldn't have billions of statements.
        throw new ISE("Uh oh, too many statements");
      }

      if (connection.statements().size() >= config.getMaxStatementsPerConnection()) {
        throw new ISE("Too many open statements, limit is[%,d]", config.getMaxStatementsPerConnection());
      }

      connection.statements().put(statement.id, new DruidStatement(ch.id, statement.id));
      log.debug("Connection[%s] opened statement[%s].", ch.id, statement.id);
      return statement;
    }
  }

  @Override
  public StatementHandle prepare(
      final ConnectionHandle ch,
      final String sql,
      final long maxRowCount
  )
  {
    final StatementHandle statement = createStatement(ch);
    final DruidStatement druidStatement = getDruidStatement(statement);
    statement.signature = druidStatement.prepare(plannerFactory, sql, maxRowCount).getSignature();
    return statement;
  }

  @Deprecated
  @Override
  public ExecuteResult prepareAndExecute(
      final StatementHandle h,
      final String sql,
      final long maxRowCount,
      final PrepareCallback callback
  ) throws NoSuchStatementException
  {
    // Avatica doesn't call this.
    throw new UnsupportedOperationException("Deprecated");
  }

  @Override
  public ExecuteResult prepareAndExecute(
      final StatementHandle statement,
      final String sql,
      final long maxRowCount,
      final int maxRowsInFirstFrame,
      final PrepareCallback callback
  ) throws NoSuchStatementException
  {
    // Ignore "callback", this class is designed for use with LocalService which doesn't use it.
    final DruidStatement druidStatement = getDruidStatement(statement);
    final Signature signature = druidStatement.prepare(plannerFactory, sql, maxRowCount).getSignature();
    final Frame firstFrame = druidStatement.execute().nextFrame(DruidStatement.START_OFFSET, maxRowsInFirstFrame);

    return new ExecuteResult(
        ImmutableList.of(
            MetaResultSet.create(
                statement.connectionId,
                statement.id,
                false,
                signature,
                firstFrame
            )
        )
    );
  }

  @Override
  public ExecuteBatchResult prepareAndExecuteBatch(
      final StatementHandle statement,
      final List<String> sqlCommands
  ) throws NoSuchStatementException
  {
    // Batch statements are used for bulk updates, but we don't support updates.
    throw new UnsupportedOperationException("Batch statements not supported");
  }

  @Override
  public ExecuteBatchResult executeBatch(
      final StatementHandle statement,
      final List<List<TypedValue>> parameterValues
  ) throws NoSuchStatementException
  {
    // Batch statements are used for bulk updates, but we don't support updates.
    throw new UnsupportedOperationException("Batch statements not supported");
  }

  @Override
  public Frame fetch(
      final StatementHandle statement,
      final long offset,
      final int fetchMaxRowCount
  ) throws NoSuchStatementException, MissingResultsException
  {
    return getDruidStatement(statement).nextFrame(offset, fetchMaxRowCount);
  }

  @Deprecated
  @Override
  public ExecuteResult execute(
      final StatementHandle statement,
      final List<TypedValue> parameterValues,
      final long maxRowCount
  ) throws NoSuchStatementException
  {
    // Avatica doesn't call this.
    throw new UnsupportedOperationException("Deprecated");
  }

  @Override
  public ExecuteResult execute(
      final StatementHandle statement,
      final List<TypedValue> parameterValues,
      final int maxRowsInFirstFrame
  ) throws NoSuchStatementException
  {
    Preconditions.checkArgument(parameterValues.isEmpty(), "Expected parameterValues to be empty");

    final DruidStatement druidStatement = getDruidStatement(statement);
    final Signature signature = druidStatement.getSignature();
    final Frame firstFrame = druidStatement.execute().nextFrame(DruidStatement.START_OFFSET, maxRowsInFirstFrame);

    return new ExecuteResult(
        ImmutableList.of(
            MetaResultSet.create(
                statement.connectionId,
                statement.id,
                false,
                signature,
                firstFrame
            )
        )
    );
  }

  @Override
  public Iterable<Object> createIterable(
      final StatementHandle statement,
      final QueryState state,
      final Signature signature,
      final List<TypedValue> parameterValues,
      final Frame firstFrame
  )
  {
    // Avatica calls this but ignores the return value.
    return null;
  }

  @Override
  public void closeStatement(final StatementHandle h)
  {
    closeDruidStatement(getDruidStatement(h));
  }

  @Override
  public boolean syncResults(
      final StatementHandle sh,
      final QueryState state,
      final long offset
  ) throws NoSuchStatementException
  {
    final DruidStatement druidStatement = getDruidStatement(sh);
    final boolean isDone = druidStatement.isDone();
    final long currentOffset = druidStatement.getCurrentOffset();
    if (currentOffset != offset) {
      throw new ISE("Requested offset[%,d] does not match currentOffset[%,d]", offset, currentOffset);
    }
    return !isDone;
  }

  @Override
  public void commit(final ConnectionHandle ch)
  {
    // We don't support writes, just ignore commits.
  }

  @Override
  public void rollback(final ConnectionHandle ch)
  {
    // We don't support writes, just ignore rollbacks.
  }

  @Override
  public Map<DatabaseProperty, Object> getDatabaseProperties(final ConnectionHandle ch)
  {
    return ImmutableMap.of();
  }

  @Override
  public MetaResultSet getCatalogs(final ConnectionHandle ch)
  {
    final String sql = "SELECT\n"
                       + "  DISTINCT CATALOG_NAME AS TABLE_CAT\n"
                       + "FROM\n"
                       + "  INFORMATION_SCHEMA.SCHEMATA\n"
                       + "ORDER BY\n"
                       + "  TABLE_CAT\n";

    return sqlResultSet(ch, sql);
  }

  @Override
  public MetaResultSet getSchemas(
      final ConnectionHandle ch,
      final String catalog,
      final Pat schemaPattern
  )
  {
    final List<String> whereBuilder = new ArrayList<>();
    if (catalog != null) {
      whereBuilder.add("SCHEMATA.CATALOG_NAME = " + Calcites.escapeStringLiteral(catalog));
    }

    if (schemaPattern.s != null) {
      whereBuilder.add("SCHEMATA.SCHEMA_NAME LIKE " + Calcites.escapeStringLiteral(schemaPattern.s));
    }

    final String where = whereBuilder.isEmpty() ? "" : "WHERE " + Joiner.on(" AND ").join(whereBuilder);
    final String sql = "SELECT\n"
                       + "  SCHEMA_NAME AS TABLE_SCHEM,\n"
                       + "  CATALOG_NAME AS TABLE_CATALOG\n"
                       + "FROM\n"
                       + "  INFORMATION_SCHEMA.SCHEMATA\n"
                       + where + "\n"
                       + "ORDER BY\n"
                       + "  TABLE_CATALOG, TABLE_SCHEM\n";

    return sqlResultSet(ch, sql);
  }

  @Override
  public MetaResultSet getTables(
      final ConnectionHandle ch,
      final String catalog,
      final Pat schemaPattern,
      final Pat tableNamePattern,
      final List<String> typeList
  )
  {
    final List<String> whereBuilder = new ArrayList<>();
    if (catalog != null) {
      whereBuilder.add("TABLES.TABLE_CATALOG = " + Calcites.escapeStringLiteral(catalog));
    }

    if (schemaPattern.s != null) {
      whereBuilder.add("TABLES.TABLE_SCHEMA LIKE " + Calcites.escapeStringLiteral(schemaPattern.s));
    }

    if (tableNamePattern.s != null) {
      whereBuilder.add("TABLES.TABLE_NAME LIKE " + Calcites.escapeStringLiteral(tableNamePattern.s));
    }

    if (typeList != null) {
      final List<String> escapedTypes = new ArrayList<>();
      for (String type : typeList) {
        escapedTypes.add(Calcites.escapeStringLiteral(type));
      }
      whereBuilder.add("TABLES.TABLE_TYPE IN (" + Joiner.on(", ").join(escapedTypes) + ")");
    }

    final String where = whereBuilder.isEmpty() ? "" : "WHERE " + Joiner.on(" AND ").join(whereBuilder);
    final String sql = "SELECT\n"
                       + "  TABLE_CATALOG AS TABLE_CAT,\n"
                       + "  TABLE_SCHEMA AS TABLE_SCHEM,\n"
                       + "  TABLE_NAME AS TABLE_NAME,\n"
                       + "  TABLE_TYPE AS TABLE_TYPE,\n"
                       + "  CAST(NULL AS VARCHAR) AS REMARKS,\n"
                       + "  CAST(NULL AS VARCHAR) AS TYPE_CAT,\n"
                       + "  CAST(NULL AS VARCHAR) AS TYPE_SCHEM,\n"
                       + "  CAST(NULL AS VARCHAR) AS TYPE_NAME,\n"
                       + "  CAST(NULL AS VARCHAR) AS SELF_REFERENCING_COL_NAME,\n"
                       + "  CAST(NULL AS VARCHAR) AS REF_GENERATION\n"
                       + "FROM\n"
                       + "  INFORMATION_SCHEMA.TABLES\n"
                       + where + "\n"
                       + "ORDER BY\n"
                       + "  TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME\n";

    return sqlResultSet(ch, sql);
  }

  @Override
  public MetaResultSet getColumns(
      final ConnectionHandle ch,
      final String catalog,
      final Pat schemaPattern,
      final Pat tableNamePattern,
      final Pat columnNamePattern
  )
  {
    final List<String> whereBuilder = new ArrayList<>();
    if (catalog != null) {
      whereBuilder.add("COLUMNS.TABLE_CATALOG = " + Calcites.escapeStringLiteral(catalog));
    }

    if (schemaPattern.s != null) {
      whereBuilder.add("COLUMNS.TABLE_SCHEMA LIKE " + Calcites.escapeStringLiteral(schemaPattern.s));
    }

    if (tableNamePattern.s != null) {
      whereBuilder.add("COLUMNS.TABLE_NAME LIKE " + Calcites.escapeStringLiteral(tableNamePattern.s));
    }

    if (columnNamePattern.s != null) {
      whereBuilder.add("COLUMNS.COLUMN_NAME LIKE " + Calcites.escapeStringLiteral(columnNamePattern.s));
    }

    final String where = whereBuilder.isEmpty() ? "" : "WHERE " + Joiner.on(" AND ").join(whereBuilder);
    final String sql = "SELECT\n"
                       + "  TABLE_CATALOG AS TABLE_CAT,\n"
                       + "  TABLE_SCHEMA AS TABLE_SCHEM,\n"
                       + "  TABLE_NAME AS TABLE_NAME,\n"
                       + "  COLUMN_NAME AS COLUMN_NAME,\n"
                       + "  CAST(JDBC_TYPE AS INTEGER) AS DATA_TYPE,\n"
                       + "  DATA_TYPE AS TYPE_NAME,\n"
                       + "  -1 AS COLUMN_SIZE,\n"
                       + "  -1 AS BUFFER_LENGTH,\n"
                       + "  -1 AS DECIMAL_DIGITS,\n"
                       + "  -1 AS NUM_PREC_RADIX,\n"
                       + "  CASE IS_NULLABLE WHEN 'YES' THEN 1 ELSE 0 END AS NULLABLE,\n"
                       + "  CAST(NULL AS VARCHAR) AS REMARKS,\n"
                       + "  COLUMN_DEFAULT AS COLUMN_DEF,\n"
                       + "  -1 AS SQL_DATA_TYPE,\n"
                       + "  -1 AS SQL_DATETIME_SUB,\n"
                       + "  -1 AS CHAR_OCTET_LENGTH,\n"
                       + "  CAST(ORDINAL_POSITION AS INTEGER) AS ORDINAL_POSITION,\n"
                       + "  IS_NULLABLE AS IS_NULLABLE,\n"
                       + "  CAST(NULL AS VARCHAR) AS SCOPE_CATALOG,\n"
                       + "  CAST(NULL AS VARCHAR) AS SCOPE_SCHEMA,\n"
                       + "  CAST(NULL AS VARCHAR) AS SCOPE_TABLE,\n"
                       + "  -1 AS SOURCE_DATA_TYPE,\n"
                       + "  'NO' AS IS_AUTOINCREMENT,\n"
                       + "  'NO' AS IS_GENERATEDCOLUMN\n"
                       + "FROM\n"
                       + "  INFORMATION_SCHEMA.COLUMNS\n"
                       + where + "\n"
                       + "ORDER BY\n"
                       + "  TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION\n";

    return sqlResultSet(ch, sql);
  }

  @Override
  public MetaResultSet getTableTypes(final ConnectionHandle ch)
  {
    final String sql = "SELECT\n"
                       + "  DISTINCT TABLE_TYPE AS TABLE_TYPE\n"
                       + "FROM\n"
                       + "  INFORMATION_SCHEMA.TABLES\n"
                       + "ORDER BY\n"
                       + "  TABLE_TYPE\n";

    return sqlResultSet(ch, sql);
  }

  private DruidConnection getDruidConnection(final String connectionId)
  {
    return getDruidConnection(connectionId, false);
  }

  private DruidConnection getDruidConnection(final String connectionId, final boolean createIfNotExists)
  {
    DruidConnection connection;

    synchronized (connections) {
      connection = connections.get(connectionId);

      if (connection == null && createIfNotExists) {
        if (connections.size() >= config.getMaxConnections()) {
          throw new ISE("Too many connections, limit is[%,d]", config.getMaxConnections());
        }
        connection = new DruidConnection();
        connections.put(connectionId, connection);
        log.debug("Connection[%s] opened.", connectionId);
      }

      if (connection == null) {
        throw new ISE("Connection[%s] not open", connectionId);
      }
    }

    final DruidConnection finalConnection = connection;

    return finalConnection.sync(
        exec.schedule(
            new Runnable()
            {
              @Override
              public void run()
              {
                final List<DruidStatement> statements = new ArrayList<>();

                synchronized (connections) {
                  if (connections.remove(connectionId) == finalConnection) {
                    statements.addAll(finalConnection.statements().values());
                    log.debug("Connection[%s] timed out, closing %,d statements.", connectionId, statements.size());
                  }
                }

                final Closer closer = Closer.create();
                for (final DruidStatement statement : statements) {
                  closer.register(statement);
                }
                try {
                  closer.close();
                }
                catch (IOException e) {
                  throw Throwables.propagate(e);
                }
              }
            },
            new Interval(new DateTime(), config.getConnectionIdleTimeout()).toDurationMillis(),
            TimeUnit.MILLISECONDS
        )
    );
  }

  private DruidStatement getDruidStatement(final StatementHandle statement)
  {
    synchronized (connections) {
      final DruidConnection connection = getDruidConnection(statement.connectionId);
      final DruidStatement druidStatement = connection.statements().get(statement.id);
      Preconditions.checkState(druidStatement != null, "Statement[%s] does not exist", statement.id);
      return druidStatement;
    }
  }

  private void closeDruidStatement(final DruidStatement statement)
  {
    synchronized (connections) {
      final DruidConnection connection = getDruidConnection(statement.getConnectionId());
      if (connection.statements().get(statement.getStatementId()) == statement) {
        connection.statements().remove(statement.getStatementId());
      } else {
        // "statement" is not actually in the set of open statements for this connection
        throw new ISE("Statement[%s] not open", statement.getStatementId());
      }
    }

    log.debug("Connection[%s] closed statement[%s].", statement.getConnectionId(), statement.getStatementId());
    statement.close();
  }

  private MetaResultSet sqlResultSet(final ConnectionHandle ch, final String sql)
  {
    final StatementHandle statement = createStatement(ch);
    try {
      final ExecuteResult result = prepareAndExecute(statement, sql, -1, -1, null);
      final MetaResultSet metaResultSet = Iterables.getOnlyElement(result.resultSets);
      if (!metaResultSet.firstFrame.done) {
        throw new ISE("Expected all results to be in a single frame!");
      }
      return metaResultSet;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    finally {
      closeStatement(statement);
    }
  }
}
