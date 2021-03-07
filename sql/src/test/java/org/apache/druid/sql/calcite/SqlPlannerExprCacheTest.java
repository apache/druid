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
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.tools.RelConversionException;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class SqlPlannerExprCacheTest extends BaseCalciteQueryTest
{
  private static final PlannerConfig CACHE_DISABLING_CONFIG = new PlannerConfig();
  private static final PlannerConfig CACHE_ENABLING_CONFIG = new PlannerConfig().withOverrides(
      ImmutableMap.of(PlannerConfig.CTX_KEY_USE_PARSED_EXPR_CACHE, true)
  );
  private static final String DEFAULT_SQL =
      "SELECT CAST(__time AS BIGINT), m1, ANY_VALUE(dim3, 100) FROM foo\n"
      + "WHERE (CAST(TIME_FLOOR(__time, 'PT1H') AS BIGINT), m1) IN\n"
      + "   (\n"
      + "     SELECT CAST(TIME_FLOOR(__time, 'PT1H') AS BIGINT) + 0 AS t1, MIN(m1) AS t2\n"
      + "     FROM foo\n"
      + "     WHERE dim3 = 'b' AND __time BETWEEN '1994-04-29 00:00:00' AND '2020-01-11 00:00:00'\n"
      + "     GROUP BY 1\n"
      + "   )\n"
      + "GROUP BY 1, 2\n";

  @Test
  public void testEnableCacheViaConfig() throws RelConversionException
  {
    final PlannerContext context = plan(CACHE_ENABLING_CONFIG, DEFAULT_SQL, ImmutableMap.of());
    Assert.assertEquals(
        ImmutableSet.of(
            "100",
            "\"dim3\"",
            "timestamp_parse('2020-01-11 00:00:00',null,'UTC')",
            "timestamp_floor(\"__time\",'PT1H',null,'UTC')",
            "(timestamp_floor(\"__time\",'PT1H',null,'UTC') + 0)",
            "\"__time\"",
            "timestamp_parse('1994-04-29 00:00:00',null,'UTC')"
        ),
        context.getCachingExprParser().getExprCache().keySet()
    );
  }

  @Test
  public void testEnableCacheViaQueryContext() throws RelConversionException
  {
    final PlannerContext context = plan(
        CACHE_DISABLING_CONFIG,
        DEFAULT_SQL,
        ImmutableMap.of(PlannerConfig.CTX_KEY_USE_PARSED_EXPR_CACHE, true)
    );
    Assert.assertEquals(
        ImmutableSet.of(
            "100",
            "\"dim3\"",
            "timestamp_parse('2020-01-11 00:00:00',null,'UTC')",
            "timestamp_floor(\"__time\",'PT1H',null,'UTC')",
            "(timestamp_floor(\"__time\",'PT1H',null,'UTC') + 0)",
            "\"__time\"",
            "timestamp_parse('1994-04-29 00:00:00',null,'UTC')"
        ),
        context.getCachingExprParser().getExprCache().keySet()
    );
  }

  @Test
  public void testDisableCacheViaConfig() throws RelConversionException
  {
    final PlannerContext context = plan(CACHE_DISABLING_CONFIG, DEFAULT_SQL, ImmutableMap.of());
    Assert.assertTrue(context.getCachingExprParser().getExprCache().isEmpty());
  }

  @Test
  public void testDisableCacheViaQueryContext() throws RelConversionException
  {
    final PlannerContext context = plan(
        CACHE_ENABLING_CONFIG,
        DEFAULT_SQL,
        ImmutableMap.of(PlannerConfig.CTX_KEY_USE_PARSED_EXPR_CACHE, false)
    );
    Assert.assertTrue(context.getCachingExprParser().getExprCache().isEmpty());
  }

  private PlannerContext plan(PlannerConfig plannerConfig, String sql, Map<String, Object> queryContext)
      throws RelConversionException
  {
    final SqlLifecycle lifecycle = getSqlLifecycleFactory(
        plannerConfig,
        CalciteTests.createOperatorTable(),
        CalciteTests.createExprMacroTable(),
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper()
    ).factorize();
    lifecycle.initialize(sql, queryContext);
    lifecycle.validateAndAuthorize(CalciteTests.REGULAR_USER_AUTH_RESULT);
    return lifecycle.plan();
  }
}
