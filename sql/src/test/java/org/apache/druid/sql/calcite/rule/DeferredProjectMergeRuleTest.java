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

package org.apache.druid.sql.calcite.rule;

import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.druid.query.QueryContext;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class DeferredProjectMergeRuleTest extends CalciteTestBase
{
  private static final int CUSTOM_BLOAT = 1200;

  @Test
  public void testCustomBloat()
  {
    PlannerContext mockContext = Mockito.mock(PlannerContext.class);
    QueryContext mockQueryContext = Mockito.mock(QueryContext.class);

    Mockito.when(mockContext.queryContext()).thenReturn(mockQueryContext);
    Mockito.when(mockQueryContext.getInt(CalciteRulesManager.BLOAT_PROPERTY)).thenReturn(CUSTOM_BLOAT);

    final DeferredProjectMergeRule deferredRule = new DeferredProjectMergeRule(mockContext);
    final ProjectMergeRule delegateRule = deferredRule.getDelegateRule();

    Assertions.assertEquals(CUSTOM_BLOAT, delegateRule.config.bloat());
  }
}
