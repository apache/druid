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
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.PreparedStatement;
import org.apache.druid.sql.avatica.DruidJdbcResultSet.ResultFetcherFactory;
import org.apache.druid.sql.calcite.planner.PrepareResult;

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
  private final PreparedStatement sqlStatement;
  private final long maxRowCount;
  private Meta.Signature signature;
  private State state = State.NEW;

  public DruidJdbcPreparedStatement(
      final String connectionId,
      final int statementId,
      final PreparedStatement stmt,
      final long maxRowCount,
      final ResultFetcherFactory fetcherFactory
  )
  {
    super(connectionId, statementId, fetcherFactory);
    this.sqlStatement = stmt;
    this.maxRowCount = maxRowCount;
  }

  public synchronized void prepare()
  {
    try {
      ensure(State.NEW);
      PrepareResult prepareResult = sqlStatement.prepare();
      signature = createSignature(
          prepareResult,
          sqlStatement.query().sql()
      );
      state = State.PREPARED;
    }
    // Preserve the type of forbidden and runtime exceptions.
    catch (ForbiddenException e) {
      close();
      throw e;
    }
    catch (RuntimeException e) {
      close();
      throw e;
    }
    // Wrap everything else
    catch (Throwable t) {
      close();
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
      DirectStatement directStmt = sqlStatement.execute(parameters);
      resultSet = new DruidJdbcResultSet(this, directStmt, maxRowCount, fetcherFactory);
      resultSet.execute();
    }
    // Failure to execute does not close the prepared statement.
    catch (RuntimeException e) {
      resultSet = null;
      throw e;
    }
    catch (Throwable t) {
      resultSet = null;
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

  @Override
  public synchronized void close()
  {
    if (state != State.CLOSED) {
      super.close();
      sqlStatement.close();
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
