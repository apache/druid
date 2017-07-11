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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;

import javax.annotation.concurrent.GuardedBy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Connection tracking for {@link DruidMeta}. Thread-safe.
 */
public class DruidConnection
{
  private static final Logger log = new Logger(DruidConnection.class);

  private final String connectionId;
  private final int maxStatements;
  private final ImmutableMap<String, Object> context;
  private final AtomicInteger statementCounter = new AtomicInteger();
  private final AtomicReference<Future<?>> timeoutFuture = new AtomicReference<>();

  @GuardedBy("statements")
  private final Map<Integer, DruidStatement> statements;

  @GuardedBy("statements")
  private boolean open = true;

  public DruidConnection(final String connectionId, final int maxStatements, final Map<String, Object> context)
  {
    this.connectionId = Preconditions.checkNotNull(connectionId);
    this.maxStatements = maxStatements;
    this.context = ImmutableMap.copyOf(context);
    this.statements = new HashMap<>();
  }

  public DruidStatement createStatement()
  {
    final int statementId = statementCounter.incrementAndGet();

    synchronized (statements) {
      if (statements.containsKey(statementId)) {
        // Will only happen if statementCounter rolls over before old statements are cleaned up. If this
        // ever happens then something fishy is going on, because we shouldn't have billions of statements.
        throw new ISE("Uh oh, too many statements");
      }

      if (statements.size() >= maxStatements) {
        throw new ISE("Too many open statements, limit is[%,d]", maxStatements);
      }

      final DruidStatement statement = new DruidStatement(connectionId, statementId, context, () -> {
        // onClose function for the statement
        synchronized (statements) {
          log.debug("Connection[%s] closed statement[%s].", connectionId, statementId);
          statements.remove(statementId);
        }
      });

      statements.put(statementId, statement);
      log.debug("Connection[%s] opened statement[%s].", connectionId, statementId);
      return statement;
    }
  }

  public DruidStatement getStatement(final int statementId)
  {
    synchronized (statements) {
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
    synchronized (statements) {
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
    synchronized (statements) {
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
}
