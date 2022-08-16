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

package org.apache.druid.sql;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.http.SqlQuery;

import javax.servlet.http.HttpServletRequest;

/**
 * Factory factories: when design patterns go too far.
 *
 * Almost everything we need to create a {@link SqlStatementFactory} is injectable, except for the {@link SqlEngine}.
 * So this class exists to produce {@link SqlStatementFactory} once the engine for a query is known.
 */
@LazySingleton
public class SqlStatementFactoryFactory
{
  protected final SqlToolbox lifecycleToolbox;

  @Inject
  public SqlStatementFactoryFactory(
      final PlannerFactory plannerFactory,
      final ServiceEmitter emitter,
      final RequestLogger requestLogger,
      final QueryScheduler queryScheduler,
      final AuthConfig authConfig,
      final Supplier<DefaultQueryConfig> defaultQueryConfig,
      final SqlLifecycleManager sqlLifecycleManager
  )
  {
    this.lifecycleToolbox = new SqlToolbox(
        null,
        plannerFactory,
        emitter,
        requestLogger,
        queryScheduler,
        authConfig,
        defaultQueryConfig.get(),
        sqlLifecycleManager
    );
  }

  public SqlStatementFactory factorize(final SqlEngine engine)
  {
    return new FactoryImpl(lifecycleToolbox.withEngine(engine));
  }

  private static class FactoryImpl implements SqlStatementFactory
  {
    private final SqlToolbox lifecycleToolbox;

    public FactoryImpl(SqlToolbox lifecycleToolbox)
    {
      this.lifecycleToolbox = lifecycleToolbox;
    }

    @Override
    public HttpStatement httpStatement(
        final SqlQuery sqlQuery,
        final HttpServletRequest req
    )
    {
      return new HttpStatement(lifecycleToolbox, sqlQuery, req);
    }

    @Override
    public DirectStatement directStatement(final SqlQueryPlus sqlRequest)
    {
      return new DirectStatement(lifecycleToolbox, sqlRequest);
    }

    @Override
    public PreparedStatement preparedStatement(final SqlQueryPlus sqlRequest)
    {
      return new PreparedStatement(lifecycleToolbox, sqlRequest);
    }
  }
}
