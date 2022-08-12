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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.calcite.planner.PrepareResult;
import org.apache.druid.sql.calcite.run.SqlEngine;

import java.util.List;

/**
 * The Druid implementation of the server-side representation of the
 * JDBC {@code PreparedStatement} class. A prepared statement can be prepared
 * once with a query, then executed any number of times, typically with
 * parameter values. No parameters are provided during prepare, though we do
 * learn the parameter definitions passed back to the client and used by
 * Avatica for serialization. Each execution produces a
 * {@link DruidJdbcResultSet}. Only one execution is active at a time.
 */
public class DruidJdbcPreparedStatement extends AbstractDruidJdbcStatement
{
  private final SqlLifecycle sqlStatement;
  private final SqlQueryPlus queryPlus;
  private final SqlEngine engine;
  private final SqlLifecycleFactory lifecycleFactory;
  private final long maxRowCount;
  private Meta.Signature signature;
  private State state = State.NEW;

  public DruidJdbcPreparedStatement(
      final DruidConnection connection,
      final int statementId,
      final SqlQueryPlus queryPlus,
      final SqlEngine engine,
      final SqlLifecycleFactory lifecycleFactory,
      final long maxRowCount
  )
  {
    super(connection, statementId);
    this.engine = engine;
    this.lifecycleFactory = lifecycleFactory;
    this.queryPlus = queryPlus;
    this.maxRowCount = maxRowCount;
    this.sqlStatement = lifecycleFactory.factorize(engine);
    sqlStatement.initialize(queryPlus.sql(), connection.makeContext());
  }

  public synchronized void prepare()
  {
    try {
      ensure(State.NEW);
      sqlStatement.validateAndAuthorize(queryPlus.authResult());
      PrepareResult prepareResult = sqlStatement.prepare();
      signature = createSignature(
          prepareResult,
          queryPlus.sql()
      );
      state = State.PREPARED;
    }
    catch (ForbiddenException e) {
      // Can't finalize statement in this case. Call will fail with an
      // assertion error.
      DruidMeta.logFailure(e);
      state = State.CLOSED;
      throw e;
    }
    catch (RuntimeException e) {
      failed(e);
      throw e;
    }
    catch (Throwable t) {
      failed(t);
      throw new RuntimeException(t);
    }
  }

  @Override
  public synchronized Meta.Signature getSignature()
  {
    ensure(State.PREPARED);
    return signature;
  }

  public synchronized void execute(List<TypedValue> parameters)
  {
    ensure(State.PREPARED);
    closeResultSet();
    try {
      SqlLifecycle directStmt = lifecycleFactory.factorize(engine);
      directStmt.initialize(queryPlus.sql(), connection.makeContext());
      directStmt.setParameters(parameters);
      resultSet = new DruidJdbcResultSet(this, queryPlus, directStmt, maxRowCount);
      resultSet.execute();
    }
    // Failure to execute does not close the prepared statement.
    catch (RuntimeException e) {
      failed(e);
      throw e;
    }
    catch (Throwable t) {
      failed(t);
      throw new RuntimeException(t);
    }
  }

  @GuardedBy("this")
  private void ensure(final State... desiredStates)
  {
    for (State desiredState : desiredStates) {
      if (state == desiredState) {
        return;
      }
    }
    throw new ISE("Invalid action for state [%s]", state);
  }

  private void failed(Throwable t)
  {
    super.close();
    sqlStatement.finalizeStateAndEmitLogsAndMetrics(t, null, -1);
    state = State.CLOSED;
  }

  @Override
  public synchronized void close()
  {
    if (state != State.CLOSED) {
      super.close();
      sqlStatement.finalizeStateAndEmitLogsAndMetrics(null, null, -1);
    }
    state = State.CLOSED;
  }

  enum State
  {
    NEW,
    PREPARED,
    CLOSED
  }
}
