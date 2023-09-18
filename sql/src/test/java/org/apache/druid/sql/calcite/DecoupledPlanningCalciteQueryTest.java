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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.junit.AssumptionViolatedException;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.regex.Matcher;

import static org.junit.Assert.assertThrows;

public class DecoupledPlanningCalciteQueryTest extends CalciteQueryTest
{

  @Rule(order = 0)
  public DecoupledIgnoreProcessor decoupledIgnoreProcessor = new DecoupledIgnoreProcessor();

  public static class DecoupledIgnoreProcessor implements TestRule
  {
    public Statement apply(Statement base, Description description)
    {
      DecoupledIgnore annotation = description.getAnnotation(DecoupledIgnore.class);
      if (annotation == null) {
        return base;
      }
      return new Statement()
      {
        @Override
        public void evaluate() throws Throwable
        {
          Throwable e = assertThrows(
              "Expected that this testcase will fail - it might got fixed?",
              annotation.mode().throwableClass,
              () -> {
                base.evaluate();
              });

          String trace = Throwables.getStackTraceAsString(e);
          Matcher m = annotation.mode().getPattern().matcher(trace);

          if (!m.find()) {
            throw new AssertionError("Exception stactrace doesn't match regex: " + annotation.mode().regex, e);
          }
          throw new AssumptionViolatedException("Test is not-yet supported in Decoupled mode");
        }
      };
    }
  }

  private static final ImmutableMap<String, Object> CONTEXT_OVERRIDES = ImmutableMap.of(
      PlannerConfig.CTX_NATIVE_QUERY_SQL_PLANNING_MODE, PlannerConfig.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED,
      QueryContexts.ENABLE_DEBUG, true);

  @Override
  protected QueryTestBuilder testBuilder()
  {
    return new QueryTestBuilder(
        new CalciteTestConfig(CONTEXT_OVERRIDES)
        {
          @Override
          public SqlTestFramework.PlannerFixture plannerFixture(PlannerConfig plannerConfig, AuthConfig authConfig)
          {
            plannerConfig = plannerConfig.withOverrides(CONTEXT_OVERRIDES);
            return queryFramework().plannerFixture(DecoupledPlanningCalciteQueryTest.this, plannerConfig, authConfig);
          }
        })
            .cannotVectorize(cannotVectorize)
            .skipVectorize(skipVectorize);
  }
}
