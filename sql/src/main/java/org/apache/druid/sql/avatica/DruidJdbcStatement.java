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
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.avatica.DruidJdbcResultSet.ResultFetcherFactory;

import java.util.Map;

/**
 * Represents Druid's version of the JDBC {@code Statement} class:
 * can be executed multiple times, one after another, producing a
 * {@link DruidJdbcResultSet} for each execution. Thread safe, but
 * only when accessed sequentially by different request threads:
 * not designed for concurrent access as JDBC does not support
 * concurrent actions on the same statement.
 */
public class DruidJdbcStatement extends AbstractDruidJdbcStatement
{
  private final SqlStatementFactory lifecycleFactory;
  protected final Map<String, Object> queryContext;

  public DruidJdbcStatement(
      final String connectionId,
      final int statementId,
      final Map<String, Object> queryContext,
      final SqlStatementFactory lifecycleFactory,
      final ResultFetcherFactory fetcherFactory
  )
  {
    super(connectionId, statementId, fetcherFactory);
    this.queryContext = queryContext;
    this.lifecycleFactory = Preconditions.checkNotNull(lifecycleFactory, "lifecycleFactory");
  }

  public synchronized void execute(SqlQueryPlus queryPlus, long maxRowCount)
  {
    closeResultSet();
    queryPlus = queryPlus.withContext(queryContext);
    DirectStatement stmt = lifecycleFactory.directStatement(queryPlus);
    resultSet = new DruidJdbcResultSet(this, stmt, Long.MAX_VALUE, fetcherFactory);
    try {
      resultSet.execute();
    }
    catch (Throwable t) {
      closeResultSet();
      throw t;
    }
  }

  @Override
  public Meta.Signature getSignature()
  {
    return requireResultSet().getSignature();
  }
}
