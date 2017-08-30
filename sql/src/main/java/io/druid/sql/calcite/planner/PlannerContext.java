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

package io.druid.sql.calcite.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.druid.math.expr.ExprMacroTable;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import java.util.Map;

/**
 * Like {@link PlannerConfig}, but that has static configuration and this class contains dynamic, per-query
 * configuration.
 */
public class PlannerContext
{
  public static final String CTX_SQL_CURRENT_TIMESTAMP = "sqlCurrentTimestamp";
  public static final String CTX_SQL_TIME_ZONE = "sqlTimeZone";

  private final DruidOperatorTable operatorTable;
  private final ExprMacroTable macroTable;
  private final PlannerConfig plannerConfig;
  private final DateTime localNow;
  private final long queryStartTimeMillis;
  private final Map<String, Object> queryContext;

  private PlannerContext(
      final DruidOperatorTable operatorTable,
      final ExprMacroTable macroTable,
      final PlannerConfig plannerConfig,
      final DateTime localNow,
      final Map<String, Object> queryContext
  )
  {
    this.operatorTable = operatorTable;
    this.macroTable = macroTable;
    this.plannerConfig = Preconditions.checkNotNull(plannerConfig, "plannerConfig");
    this.queryContext = queryContext != null ? ImmutableMap.copyOf(queryContext) : ImmutableMap.<String, Object>of();
    this.localNow = Preconditions.checkNotNull(localNow, "localNow");
    this.queryStartTimeMillis = System.currentTimeMillis();
  }

  public static PlannerContext create(
      final DruidOperatorTable operatorTable,
      final ExprMacroTable macroTable,
      final PlannerConfig plannerConfig,
      final Map<String, Object> queryContext
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
        timeZone = DateTimeZone.forID(String.valueOf(tzParam));
      } else {
        timeZone = DateTimeZone.UTC;
      }
    } else {
      utcNow = new DateTime(DateTimeZone.UTC);
      timeZone = DateTimeZone.UTC;
    }

    return new PlannerContext(
        operatorTable,
        macroTable,
        plannerConfig.withOverrides(queryContext),
        utcNow.withZone(timeZone),
        queryContext
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

  public long getQueryStartTimeMillis()
  {
    return queryStartTimeMillis;
  }

  public DataContext createDataContext(final JavaTypeFactory typeFactory)
  {
    class DruidDataContext implements DataContext
    {
      private final Map<String, Object> context = ImmutableMap.<String, Object>of(
          DataContext.Variable.UTC_TIMESTAMP.camelName, localNow.getMillis(),
          DataContext.Variable.CURRENT_TIMESTAMP.camelName, localNow.getMillis(),
          DataContext.Variable.LOCAL_TIMESTAMP.camelName, new Interval(
              new DateTime("1970-01-01T00:00:00.000", localNow.getZone()),
              localNow
          ).toDurationMillis(),
          DataContext.Variable.TIME_ZONE.camelName, localNow.getZone().toTimeZone()
      );

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
