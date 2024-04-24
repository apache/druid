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

package org.apache.druid.sql.calcite;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest.CalciteTestConfig;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.junit.jupiter.api.extension.Extension;

public class DecoupledExtension implements Extension
{
  private BaseCalciteQueryTest baseTest;

  public DecoupledExtension(BaseCalciteQueryTest baseTest)
  {
    this.baseTest = baseTest;
  }

  private static final ImmutableMap<String, Object> CONTEXT_OVERRIDES = ImmutableMap.<String, Object>builder()
      .putAll(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
      .put(PlannerConfig.CTX_NATIVE_QUERY_SQL_PLANNING_MODE, PlannerConfig.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED)
      .put(QueryContexts.ENABLE_DEBUG, true)
      .build();

  public QueryTestBuilder testBuilder()
  {
    DecoupledTestConfig decTestConfig = BaseCalciteQueryTest.queryFrameworkRule
        .getAnnotation(DecoupledTestConfig.class);

    CalciteTestConfig testConfig = baseTest.new CalciteTestConfig(CONTEXT_OVERRIDES)
    {
      @Override
      public SqlTestFramework.PlannerFixture plannerFixture(PlannerConfig plannerConfig, AuthConfig authConfig)
      {
        plannerConfig = plannerConfig.withOverrides(CONTEXT_OVERRIDES);

        return baseTest.queryFramework().plannerFixture(plannerConfig, authConfig);
      }
    };

    QueryTestBuilder builder = new QueryTestBuilder(testConfig)
        .cannotVectorize(baseTest.cannotVectorize)
        .skipVectorize(baseTest.skipVectorize);

    if (decTestConfig != null && decTestConfig.nativeQueryIgnore().isPresent()) {
      builder.verifyNativeQueries(x -> false);
    }

    return builder;
  }
}
