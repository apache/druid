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
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.sql.SqlLifecycleFactory;

import java.util.Map;
import java.util.Set;
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
  private static final Logger log = new Logger(DruidConnection.class);
  private static final Set<String> SENSITIVE_CONTEXT_FIELDS = Sets.newHashSet(
      "user", "password"
  );

  private final String connectionId;
  private final int maxStatements;
  private final ImmutableMap<String, Object> context;
  private final AtomicInteger statementCounter = new AtomicInteger();
  private final AtomicReference<Future<?>> timeoutFuture = new AtomicReference<>();

  // Typically synchronized by connectionLock, except in one case: the onClose function passed
  // into DruidStatements contained by the map.
  @GuardedBy("connectionLock")
  private final ConcurrentMap<Integer, DruidStatement> statements;
  private final Object connectionLock = new Object();

  @GuardedBy("connectionLock")
  private boolean open = true;

  public DruidConnection(final String connectionId, final int maxStatements, final Map<String, Object> context)
  {
    this.connectionId = Preconditions.checkNotNull(connectionId);
    this.maxStatements = maxStatements;
    this.context = ImmutableMap.copyOf(context);
    this.statements = new ConcurrentHashMap<>();
  }

  public DruidStatement createStatement(SqlLifecycleFactory sqlLifecycleFactory)
  {
    final int statementId = statementCounter.incrementAndGet();

    synchronized (connectionLock) {
      if (statements.containsKey(statementId)) {
        // Will only happen if statementCounter rolls over before old statements are cleaned up. If this
        // ever happens then something fishy is going on, because we shouldn't have billions of statements.
        throw new ISE("Uh oh, too many statements");
      }

      if (statements.size() >= maxStatements) {
        throw new ISE("Too many open statements, limit is[%,d]", maxStatements);
      }

      // remove sensitive fields from the context, only the connection's context needs to have authentication
      // credentials
      Map<String, Object> sanitizedContext = Maps.filterEntries(
          context,
          e -> !SENSITIVE_CONTEXT_FIELDS.contains(e.getKey())
      );

      @SuppressWarnings("GuardedBy")
      final DruidStatement statement = new DruidStatement(
          connectionId,
          statementId,
          ImmutableSortedMap.copyOf(sanitizedContext),
          sqlLifecycleFactory.factorize(),
          () -> {
            // onClose function for the statement
            log.debug("Connection[%s] closed statement[%s].", connectionId, statementId);
            // statements will be accessed unsynchronized to avoid deadlock
            statements.remove(statementId);
          }
      );

      statements.put(statementId, statement);
      log.debug("Connection[%s] opened statement[%s].", connectionId, statementId);
      return statement;
    }
  }

  public DruidStatement getStatement(final int statementId)
  {
    synchronized (connectionLock) {
      return statements.get(statementId);
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
      for (DruidStatement statement : ImmutableList.copyOf(statements.values())) {
        try {
          statement.close();
        }
        catch (Exception e) {
          log.warn("Connection[%s] failed to close statement[%s]!", connectionId, statement.getStatementId());
        }
      }

      log.debug("Connection[%s] closed.", connectionId);
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

  public Map<String, Object> context()
  {
    return context;
  }
}
