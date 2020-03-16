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

package org.apache.druid.sql.calcite.planner;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.server.security.AuthenticationResult;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Like {@link PlannerConfig}, but that has static configuration and this class contains dynamic, per-query
 * configuration.
 */
public class PlannerContext
{
  // query context keys
  public static final String CTX_SQL_QUERY_ID = "sqlQueryId";
  public static final String CTX_SQL_CURRENT_TIMESTAMP = "sqlCurrentTimestamp";
  public static final String CTX_SQL_TIME_ZONE = "sqlTimeZone";

  // This context parameter is an undocumented parameter, used internally, to allow the web console to
  // apply a limit without having to rewrite the SQL query.
  public static final String CTX_SQL_OUTER_LIMIT = "sqlOuterLimit";

  // DataContext keys
  public static final String DATA_CTX_AUTHENTICATION_RESULT = "authenticationResult";

  private final DruidOperatorTable operatorTable;
  private final ExprMacroTable macroTable;
  private final PlannerConfig plannerConfig;
  private final DateTime localNow;
  private final Map<String, Object> queryContext;
  private final List<TypedValue> parameters;
  private final AuthenticationResult authenticationResult;
  private final String sqlQueryId;
  private final List<String> nativeQueryIds = new CopyOnWriteArrayList<>();


  private PlannerContext(
      final DruidOperatorTable operatorTable,
      final ExprMacroTable macroTable,
      final PlannerConfig plannerConfig,
      final DateTime localNow,
      final Map<String, Object> queryContext,
      final List<TypedValue> parameters,
      final AuthenticationResult authenticationResult
  )
  {
    this.operatorTable = operatorTable;
    this.macroTable = macroTable;
    this.plannerConfig = Preconditions.checkNotNull(plannerConfig, "plannerConfig");
    this.queryContext = queryContext != null ? new HashMap<>(queryContext) : new HashMap<>();
    this.parameters = Preconditions.checkNotNull(parameters);
    this.localNow = Preconditions.checkNotNull(localNow, "localNow");
    this.authenticationResult = Preconditions.checkNotNull(authenticationResult, "authenticationResult");

    String sqlQueryId = (String) this.queryContext.get(CTX_SQL_QUERY_ID);
    // special handling for DruidViewMacro, normal client will allocate sqlid in SqlLifecyle
    if (Strings.isNullOrEmpty(sqlQueryId)) {
      sqlQueryId = UUID.randomUUID().toString();
    }
    this.sqlQueryId = sqlQueryId;
  }

  public static PlannerContext create(
      final DruidOperatorTable operatorTable,
      final ExprMacroTable macroTable,
      final PlannerConfig plannerConfig,
      final Map<String, Object> queryContext,
      final List<TypedValue> parameters,
      final AuthenticationResult authenticationResult
  )
  {
    final DateTime utcNow;
    final DateTimeZone timeZone;

    if (queryContext != null) {
      final Object tsParam = queryContext.get(CTX_SQL_CURRENT_TIMESTAMP);
      final Object tzParam = queryContext.get(CTX_SQL_TIME_ZONE);

      if (tsParam != null) {
        utcNow = new DateTime(tsParam, DateTimeZone.UTC);
      } else {
        utcNow = new DateTime(DateTimeZone.UTC);
      }

      if (tzParam != null) {
        timeZone = DateTimes.inferTzFromString(String.valueOf(tzParam));
      } else {
        timeZone = plannerConfig.getSqlTimeZone();
      }
    } else {
      utcNow = new DateTime(DateTimeZone.UTC);
      timeZone = plannerConfig.getSqlTimeZone();
    }

    return new PlannerContext(
        operatorTable,
        macroTable,
        plannerConfig.withOverrides(queryContext),
        utcNow.withZone(timeZone),
        queryContext,
        parameters,
        authenticationResult
    );
  }

  public DruidOperatorTable getOperatorTable()
  {
    return operatorTable;
  }

  public ExprMacroTable getExprMacroTable()
  {
    return macroTable;
  }

  public PlannerConfig getPlannerConfig()
  {
    return plannerConfig;
  }

  public DateTime getLocalNow()
  {
    return localNow;
  }

  public DateTimeZone getTimeZone()
  {
    return localNow.getZone();
  }

  public Map<String, Object> getQueryContext()
  {
    return queryContext;
  }

  public List<TypedValue> getParameters()
  {
    return parameters;
  }

  public AuthenticationResult getAuthenticationResult()
  {
    return authenticationResult;
  }

  public String getSqlQueryId()
  {
    return sqlQueryId;
  }

  public List<String> getNativeQueryIds()
  {
    return nativeQueryIds;
  }

  public void addNativeQueryId(String queryId)
  {
    this.nativeQueryIds.add(queryId);
  }

  public DataContext createDataContext(final JavaTypeFactory typeFactory, List<TypedValue> parameters)
  {
    class DruidDataContext implements DataContext
    {
      private final Map<String, Object> base_context = ImmutableMap.of(
          DataContext.Variable.UTC_TIMESTAMP.camelName, localNow.getMillis(),
          DataContext.Variable.CURRENT_TIMESTAMP.camelName, localNow.getMillis(),
          DataContext.Variable.LOCAL_TIMESTAMP.camelName, new Interval(
              new DateTime("1970-01-01T00:00:00.000", localNow.getZone()),
              localNow
          ).toDurationMillis(),
          DataContext.Variable.TIME_ZONE.camelName, localNow.getZone().toTimeZone().clone(),
          DATA_CTX_AUTHENTICATION_RESULT, authenticationResult
      );
      private final Map<String, Object> context;

      DruidDataContext()
      {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.putAll(base_context);
        int i = 0;
        for (TypedValue parameter : parameters) {
          builder.put("?" + i, parameter.value);
          i++;
        }
        context = builder.build();
      }

      @Override
      public SchemaPlus getRootSchema()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public JavaTypeFactory getTypeFactory()
      {
        return typeFactory;
      }

      @Override
      public QueryProvider getQueryProvider()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public Object get(final String name)
      {
        return context.get(name);
      }
    }

    return new DruidDataContext();
  }
}
