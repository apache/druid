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

package org.apache.druid.msq.test;

import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.quidem.DruidQTestInfo;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest.CalciteTestConfig;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.List;
import java.util.Map;

public class DecoupledDartExtension implements BeforeEachCallback
{
  private BaseCalciteQueryTest baseTest;

  public DecoupledDartExtension(BaseCalciteQueryTest baseTest)
  {
    this.baseTest = baseTest;
  }

  @Override
  public void beforeEach(ExtensionContext context)
  {
  }

  private static final Map<String, Object> CONTEXT_OVERRIDES = QueryContext.builder()
      .putAll(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
      .put(QueryContextParameters.NATIVE_QUERY_SQL_PLANNING_MODE, QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED)
      .put(QueryContextParameters.PREPLANNED, true)
      .put(QueryContextParameters.DEBUG, true)
      .toMap();

  public QueryTestBuilder testBuilder()
  {
    CalciteTestConfig testConfig = baseTest.new CalciteTestConfig(CONTEXT_OVERRIDES, true)
    {
      @Override
      public SqlTestFramework.PlannerFixture plannerFixture(PlannerConfig plannerConfig, AuthConfig authConfig)
      {
        plannerConfig = plannerConfig.withOverrides(CONTEXT_OVERRIDES);
        return baseTest.queryFramework().plannerFixture(plannerConfig, authConfig);
      }

      @Override
      public DruidQTestInfo getQTestInfo()
      {
        return null;
      }
    };

    QueryTestBuilder builder = new QueryTestBuilder(testConfig)
    {
      @Override
      public QueryTestBuilder expectedQueries(List<Query<?>> expectedQueries)
      {
        // ignore all queries specified
        return this;
      }
    };

    return builder.skipVectorize(true);
  }
}
