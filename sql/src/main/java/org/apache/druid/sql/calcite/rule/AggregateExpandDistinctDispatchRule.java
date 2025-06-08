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
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.EngineFeature;

import javax.annotation.Nullable;

/**
 * Rule that dispatches at runtime to either {@link CoreRules#AGGREGATE_EXPAND_DISTINCT_AGGREGATES}
 * or {@link CoreRules#AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN}.
 *
 * It is necessary to defer decisions based on query context until rule runtime, since context from SET statements
 * is not available at rule construction time.
 */
public class AggregateExpandDistinctDispatchRule extends RelOptRule
{
  private final PlannerContext plannerContext;

  @Nullable
  private RelOptRule rule;

  public AggregateExpandDistinctDispatchRule(final PlannerContext plannerContext)
  {
    super(
        CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES.getOperand(),
        CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES.relBuilderFactory,
        CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES.toString()
    );

    this.plannerContext = plannerContext;
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    return computeRule().matches(call);
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    computeRule().onMatch(call);
  }

  private RelOptRule computeRule()
  {
    if (rule == null) {
      if (plannerContext.getPlannerConfig().isUseGroupingSetForExactDistinct()
          && plannerContext.featureAvailable(EngineFeature.GROUPING_SETS)) {
        rule = CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES;
      } else {
        rule = CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN;
      }
    }

    return rule;
  }
}
