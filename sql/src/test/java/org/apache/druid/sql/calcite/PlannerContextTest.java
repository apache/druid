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
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.NoOpPlannerHook;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerToolbox;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchema;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.NamedDruidSchema;
import org.apache.druid.sql.calcite.schema.NamedViewSchema;
import org.apache.druid.sql.calcite.schema.ViewSchema;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class PlannerContextTest
{

  private static final PlannerToolbox PLANNER_TOOLBOX = new PlannerToolbox(
      CalciteTests.createOperatorTable(),
      CalciteTests.createExprMacroTable(),
      CalciteTests.getJsonMapper(),
      new PlannerConfig(),
      new DruidSchemaCatalog(
          EasyMock.createMock(SchemaPlus.class),
          ImmutableMap.of(
              "druid", new NamedDruidSchema(EasyMock.createMock(DruidSchema.class), "druid"),
              NamedViewSchema.NAME, new NamedViewSchema(EasyMock.createMock(ViewSchema.class))
          )
      ),
      CalciteTests.createJoinableFactoryWrapper(),
      CatalogResolver.NULL_RESOLVER,
      "druid",
      new CalciteRulesManager(ImmutableSet.of()),
      CalciteTests.TEST_AUTHORIZER_MAPPER,
      AuthConfig.newBuilder().build()
  );

  private static final NativeSqlEngine ENGINE = CalciteTests.createMockSqlEngine(
      EasyMock.createMock(QuerySegmentWalker.class),
      EasyMock.createMock(QueryRunnerFactoryConglomerate.class)
  );

  private static final String SQL = "SELECT 1";

  @Test
  public void testCreate()
  {
    PlannerContext plannerContext = PlannerContext.create(PLANNER_TOOLBOX,
                                                          SQL, // The actual query isn't important for this test
                                                          ENGINE,
                                                          Collections.emptyMap(),
                                                          null);

    Assert.assertEquals(PLANNER_TOOLBOX, plannerContext.getPlannerToolbox());
    Assert.assertEquals(SQL, plannerContext.getSql());
    Assert.assertEquals(ENGINE, plannerContext.getEngine());
    Assert.assertEquals(NoOpPlannerHook.INSTANCE, plannerContext.getPlannerHook());
    Assert.assertTrue(plannerContext.queryContextMap().isEmpty());

    // Validate that the map is mutable, even though an immutable map was passed during creation.
    plannerContext.queryContextMap().put("test", "value");
  }
}
