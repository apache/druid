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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerFactory;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchConnectionException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DruidMeta extends MetaImpl
{
  private static final Logger log = new Logger(DruidMeta.class);
  private static final Set<String> SKIP_PROPERTIES = ImmutableSet.of("user", "password");

  private final PlannerFactory plannerFactory;
  private final ScheduledExecutorService exec;
  private final AvaticaServerConfig config;

  // Used to track logical connections.
  private final Map<String, DruidConnection> connections = new ConcurrentHashMap<>();

  // Number of connections reserved in "connections". May be higher than the actual number of connections at times,
  // such as when we're reserving space to open a new one.
  private final AtomicInteger connectionCount = new AtomicInteger();

  @Inject
  public DruidMeta(final PlannerFactory plannerFactory, final AvaticaServerConfig config)
  {
    super(null);
    this.plannerFactory = Preconditions.checkNotNull(plannerFactory, "plannerFactory");
    this.config = config;
    this.exec = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder()
            .setNameFormat(StringUtils.format("DruidMeta@%s-ScheduledExecutor", Integer.toHexString(hashCode())))
            .setDaemon(true)
            .build()
    );
  }

  @Override
  public void openConnection(final ConnectionHandle ch, final Map<String, String> info)
  {
    // Build connection context.
    final ImmutableMap.Builder<String, Object> context = ImmutableMap.builder();
    for (Map.Entry<String, String> entry : info.entrySet()) {
      if (!SKIP_PROPERTIES.contains(entry.getKey())) {
        context.put(entry);
      }
    }
    openDruidConnection(ch.id, context.build());
  }

  @Override
  public void closeConnection(final ConnectionHandle ch)
  {
    final DruidConnection druidConnection = connections.remove(ch.id);
    if (druidConnection != null) {
      connectionCount.decrementAndGet();
      druidConnection.close();
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
    final DruidStatement druidStatement = getDruidConnection(ch.id).createStatement();
    return new StatementHandle(ch.id, druidStatement.getStatementId(), null);
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
    final Frame firstFrame = druidStatement.execute()
                                           .nextFrame(
                                               DruidStatement.START_OFFSET,
                                               getEffectiveMaxRowsPerFrame(maxRowsInFirstFrame)
                                           );

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
    return getDruidStatement(statement).nextFrame(offset, getEffectiveMaxRowsPerFrame(fetchMaxRowCount));
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
    final Frame firstFrame = druidStatement.execute()
                                           .nextFrame(
                                               DruidStatement.START_OFFSET,
                                               getEffectiveMaxRowsPerFrame(maxRowsInFirstFrame)
                                           );

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
    // connections.get, not getDruidConnection, since we want to silently ignore nonexistent statements
    final DruidConnection druidConnection = connections.get(h.connectionId);
    if (druidConnection != null) {
      final DruidStatement druidStatement = druidConnection.getStatement(h.id);
      if (druidStatement != null) {
        druidStatement.close();
      }
    }
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

  @VisibleForTesting
  void closeAllConnections()
  {
    for (String connectionId : ImmutableSet.copyOf(connections.keySet())) {
      closeConnection(new ConnectionHandle(connectionId));
    }
  }

  private DruidConnection openDruidConnection(final String connectionId, final Map<String, Object> context)
  {
    if (connectionCount.incrementAndGet() > config.getMaxConnections()) {
      // O(connections) but we don't expect this to happen often (it's a last-ditch effort to clear out
      // abandoned connections) or to have too many connections.
      final Iterator<Map.Entry<String, DruidConnection>> entryIterator = connections.entrySet().iterator();
      while (entryIterator.hasNext()) {
        final Map.Entry<String, DruidConnection> entry = entryIterator.next();
        if (entry.getValue().closeIfEmpty()) {
          entryIterator.remove();

          // Removed a connection, decrement the counter.
          connectionCount.decrementAndGet();
          break;
        }
      }

      if (connectionCount.get() > config.getMaxConnections()) {
        // We aren't going to make a connection after all.
        connectionCount.decrementAndGet();
        throw new ISE("Too many connections, limit is[%,d]", config.getMaxConnections());
      }
    }

    final DruidConnection putResult = connections.putIfAbsent(
        connectionId,
        new DruidConnection(connectionId, config.getMaxStatementsPerConnection(), context)
    );

    if (putResult != null) {
      // Didn't actually insert the connection.
      connectionCount.decrementAndGet();
      throw new ISE("Connection[%s] already open.", connectionId);
    }

    log.debug("Connection[%s] opened.", connectionId);

    // Call getDruidConnection to start the timeout timer.
    return getDruidConnection(connectionId);
  }

  /**
   * Get a connection, or throw an exception if it doesn't exist. Also refreshes the timeout timer.
   *
   * @param connectionId connection id
   *
   * @return the connection
   *
   * @throws NoSuchConnectionException if the connection id doesn't exist
   */
  @Nonnull
  private DruidConnection getDruidConnection(final String connectionId)
  {
    final DruidConnection connection = connections.get(connectionId);

    if (connection == null) {
      throw new NoSuchConnectionException(connectionId);
    }

    return connection.sync(
        exec.schedule(
            () -> {
              log.debug("Connection[%s] timed out.", connectionId);
              closeConnection(new ConnectionHandle(connectionId));
            },
            new Interval(new DateTime(), config.getConnectionIdleTimeout()).toDurationMillis(),
            TimeUnit.MILLISECONDS
        )
    );
  }

  @Nonnull
  private DruidStatement getDruidStatement(final StatementHandle statement)
  {
    final DruidConnection connection = getDruidConnection(statement.connectionId);
    final DruidStatement druidStatement = connection.getStatement(statement.id);
    Preconditions.checkState(druidStatement != null, "Statement[%s] does not exist", statement.id);
    return druidStatement;
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

  private int getEffectiveMaxRowsPerFrame(int clientMaxRowsPerFrame)
  {
    // no configured row limit, use the client provided limit
    if (config.getMaxRowsPerFrame() < 0) {
      return clientMaxRowsPerFrame;
    }
    // client provided no row limit, use the configured row limit
    if (clientMaxRowsPerFrame < 0) {
      return config.getMaxRowsPerFrame();
    }
    return Math.min(clientMaxRowsPerFrame, config.getMaxRowsPerFrame());
  }
}
