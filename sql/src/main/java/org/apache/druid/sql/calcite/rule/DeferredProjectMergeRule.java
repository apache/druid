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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.PlannerContext;

/**
 * Defers creation of {@link ProjectMergeRule} to runtime.
 *
 * It is necessary to defer decisions based on query context until rule runtime, since context from SET statements
 * is not available at rule construction time. In this case, the decision is around
 * {@link CalciteRulesManager#BLOAT_PROPERTY}.
 */
public class DeferredProjectMergeRule extends RelOptRule
{
  private final PlannerContext plannerContext;
  private ProjectMergeRule delegateRule;

  public DeferredProjectMergeRule(PlannerContext plannerContext)
  {
    super(operand(Project.class, operand(Project.class, any())));
    this.plannerContext = plannerContext;
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    return getDelegateRule().matches(call);
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    getDelegateRule().onMatch(call);
  }

  public ProjectMergeRule getDelegateRule()
  {
    if (delegateRule == null) {
      final int bloat = getBloatProperty();
      delegateRule = ProjectMergeRule.Config.DEFAULT.withBloat(bloat).toRule();
    }
    return delegateRule;
  }

  private int getBloatProperty()
  {
    final Integer bloat = plannerContext.queryContext().getInt(CalciteRulesManager.BLOAT_PROPERTY);
    return (bloat != null) ? bloat : CalciteRulesManager.DEFAULT_BLOAT;
  }
}
