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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.function.Predicate;

/**
 * A wrapper rule that conditionally executes a delegate rule based on runtime evaluation of {@link PlannerContext}.
 *
 * It is necessary to defer decisions based on query context until rule runtime, since context from SET statements
 * is not available at rule construction time.
 */
public class ContextuallyConditionalRule extends RelOptRule
{
  private final RelOptRule delegateRule;
  private final PlannerContext plannerContext;
  private final Predicate<PlannerContext> condition;

  @Nullable
  private Boolean doRun;

  /**
   * Creates a conditional rule that wraps the given delegate rule with a runtime condition.
   */
  public ContextuallyConditionalRule(
      final RelOptRule delegateRule,
      final PlannerContext plannerContext,
      final Predicate<PlannerContext> condition
  )
  {
    super(delegateRule.getOperand(), delegateRule.relBuilderFactory, delegateRule.toString());
    this.delegateRule = delegateRule;
    this.plannerContext = plannerContext;
    this.condition = condition;
  }

  @Override
  public boolean matches(final RelOptRuleCall call)
  {
    if (computeDoRun()) {
      return delegateRule.matches(call);
    } else {
      return false;
    }
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    // No need to check doRun, since "matches" controls conditional enabling.
    delegateRule.onMatch(call);
  }

  @Override
  public Convention getOutConvention()
  {
    return delegateRule.getOutConvention();
  }

  @Override
  public RelTrait getOutTrait()
  {
    return delegateRule.getOutTrait();
  }

  private boolean computeDoRun()
  {
    if (doRun == null) {
      doRun = condition.test(plannerContext);
    }

    return doRun;
  }
}
