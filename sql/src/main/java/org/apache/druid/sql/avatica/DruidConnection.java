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

package org.apache.druid.sql.avatica;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryContext;
import org.apache.druid.sql.PreparedStatement;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.SqlStatementFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Connection tracking for {@link DruidMeta}. Thread-safe.
 */
public class DruidConnection
{
  private static final Logger LOG = new Logger(DruidConnection.class);

  private final String connectionId;
  private final int maxStatements;
  private final ImmutableMap<String, Object> userSecret;
  private final QueryContext context;
  private final AtomicInteger statementCounter = new AtomicInteger();
  private final AtomicReference<Future<?>> timeoutFuture = new AtomicReference<>();

  // Typically synchronized by connectionLock, except in one case: the onClose function passed
  // into DruidStatements contained by the map.
  @GuardedBy("connectionLock")
  private final ConcurrentMap<Integer, AbstractDruidJdbcStatement> statements = new ConcurrentHashMap<>();
  private final Object connectionLock = new Object();

  @GuardedBy("connectionLock")
  private boolean open = true;

  public DruidConnection(
      final String connectionId,
      final int maxStatements,
      final Map<String, Object> userSecret,
      final QueryContext context
  )
  {
    this.connectionId = Preconditions.checkNotNull(connectionId);
    this.maxStatements = maxStatements;
    this.userSecret = ImmutableMap.copyOf(userSecret);
    this.context = context;
  }

  public String getConnectionId()
  {
    return connectionId;
  }

  public DruidJdbcStatement createStatement(SqlStatementFactory sqlStatementFactory)
  {
    final int statementId = statementCounter.incrementAndGet();

    synchronized (connectionLock) {
      if (statements.containsKey(statementId)) {
        // Will only happen if statementCounter rolls over before old statements are cleaned up. If this
        // ever happens then something fishy is going on, because we shouldn't have billions of statements.
        throw DruidMeta.logFailure(new ISE("Uh oh, too many statements"));
      }

      if (statements.size() >= maxStatements) {
        throw DruidMeta.logFailure(new ISE("Too many open statements, limit is [%,d]", maxStatements));
      }

      @SuppressWarnings("GuardedBy")
      final DruidJdbcStatement statement = new DruidJdbcStatement(
          connectionId,
          statementId,
          context,
          sqlStatementFactory
      );

      statements.put(statementId, statement);
      LOG.debug("Connection [%s] opened statement [%s].", connectionId, statementId);
      return statement;
    }
  }

  public DruidJdbcPreparedStatement createPreparedStatement(
      SqlStatementFactory sqlStatementFactory,
      SqlQueryPlus sqlRequest,
      final long maxRowCount)
  {
    final int statementId = statementCounter.incrementAndGet();

    synchronized (connectionLock) {
      if (statements.containsKey(statementId)) {
        // Will only happen if statementCounter rolls over before old statements are cleaned up. If this
        // ever happens then something fishy is going on, because we shouldn't have billions of statements.
        throw DruidMeta.logFailure(new ISE("Uh oh, too many statements"));
      }

      if (statements.size() >= maxStatements) {
        throw DruidMeta.logFailure(new ISE("Too many open statements, limit is [%,d]", maxStatements));
      }

      @SuppressWarnings("GuardedBy")
      final PreparedStatement statement = sqlStatementFactory.preparedStatement(
          sqlRequest.withContext(context)
      );
      final DruidJdbcPreparedStatement jdbcStmt = new DruidJdbcPreparedStatement(
          connectionId,
          statementId,
          statement,
          maxRowCount
      );

      statements.put(statementId, jdbcStmt);
      LOG.debug("Connection [%s] opened prepared statement [%s].", connectionId, statementId);
      return jdbcStmt;
    }
  }

  public AbstractDruidJdbcStatement getStatement(final int statementId)
  {
    synchronized (connectionLock) {
      return statements.get(statementId);
    }
  }

  public void closeStatement(int statementId)
  {
    AbstractDruidJdbcStatement stmt;
    synchronized (connectionLock) {
      stmt = statements.remove(statementId);
    }
    if (stmt != null) {
      stmt.close();
      LOG.debug("Connection [%s] closed statement [%s].", connectionId, statementId);
    }
  }

  /**
   * Closes this connection if it has no statements.
   *
   * @return true if closed
   */
  public boolean closeIfEmpty()
  {
    synchronized (connectionLock) {
      if (statements.isEmpty()) {
        close();
        return true;
      } else {
        return false;
      }
    }
  }

  public void close()
  {
    synchronized (connectionLock) {
      // Copy statements before iterating because statement.close() modifies it.
      for (AbstractDruidJdbcStatement statement : ImmutableList.copyOf(statements.values())) {
        try {
          statement.close();
        }
        catch (Exception e) {
          LOG.warn("Connection [%s] failed to close statement [%s]!", connectionId, statement.getStatementId());
        }
      }

      LOG.debug("Connection [%s] closed.", connectionId);
      open = false;
    }
  }

  public DruidConnection sync(final Future<?> newTimeoutFuture)
  {
    final Future<?> oldFuture = timeoutFuture.getAndSet(newTimeoutFuture);
    if (oldFuture != null) {
      oldFuture.cancel(false);
    }
    return this;
  }

  public Map<String, Object> userSecret()
  {
    return userSecret;
  }
}
