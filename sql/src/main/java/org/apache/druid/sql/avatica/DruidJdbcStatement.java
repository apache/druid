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
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.tools.RelConversionException;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.calcite.run.SqlEngine;

/**
 * Represents Druid's version of the JDBC {@code Statement} class:
 * can be executed multiple times, one after another, producing a
 * {@link DruidJdbcResultSet} for each execution.
 */
public class DruidJdbcStatement extends AbstractDruidJdbcStatement
{
  private final SqlEngine engine;
  private final SqlLifecycleFactory lifecycleFactory;
  protected boolean closed;

  public DruidJdbcStatement(
      final DruidConnection connection,
      final int statementId,
      final SqlEngine engine,
      final SqlLifecycleFactory lifecycleFactory
  )
  {
    super(connection, statementId);
    this.engine = engine;
    this.lifecycleFactory = Preconditions.checkNotNull(lifecycleFactory, "lifecycleFactory");
  }

  public synchronized void execute(SqlQueryPlus sqlRequest, long maxRowCount) throws RelConversionException
  {
    closeResultSet();
    SqlLifecycle stmt = lifecycleFactory.factorize(engine);
    stmt.initialize(sqlRequest.sql(), connection.makeContext());
    try {
      stmt.validateAndAuthorize(sqlRequest.authResult());
      resultSet = new DruidJdbcResultSet(this, sqlRequest, stmt, Long.MAX_VALUE);
      resultSet.execute();
    }
    catch (ForbiddenException e) {
      // Can't finalize statement in in this case. Call will fail with an
      // assertion error.
      resultSet = null;
      DruidMeta.logFailure(e);
      throw e;
    }
    catch (Throwable t) {
      stmt.finalizeStateAndEmitLogsAndMetrics(t, null, -1);
      resultSet = null;
      throw t;
    }
  }

  @Override
  public Meta.Signature getSignature()
  {
    return requireResultSet().getSignature();
  }
}
