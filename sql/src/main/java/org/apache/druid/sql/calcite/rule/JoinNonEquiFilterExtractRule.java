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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.AbstractJoinExtractFilterRule;
import org.apache.calcite.rel.rules.JoinExtractFilterRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;

/**
 * This rule will extract non-equi join conditions from a join and pull them into filters post-join. This is done so
 * that native layer can still execute the join and the non-equi join conditions can be evaluated post-join.
 * <p>
 * The rule (the code and the comments) is borrowed from Apache Calcite's JoinExtractFilterRule and modified to only
 * extract non-equi join conditions. This should be revisited once we have de-coupled SQL planning in place.
 */
public class JoinNonEquiFilterExtractRule extends AbstractJoinExtractFilterRule
{

  public static JoinNonEquiFilterExtractRule INSTANCE = new JoinNonEquiFilterExtractRule(JoinExtractFilterRule.Config.DEFAULT);

  JoinNonEquiFilterExtractRule(JoinExtractFilterRule.Config config)
  {
    super(config);
  }

  @Deprecated // to be removed before Calcite 2.0
  public JoinNonEquiFilterExtractRule(
      Class<? extends Join> clazz,
      RelBuilderFactory relBuilderFactory
  )
  {
    this(JoinExtractFilterRule.Config.DEFAULT
             .withRelBuilderFactory(relBuilderFactory)
             .withOperandSupplier(b ->
                                      b.operand(clazz).anyInputs())
             .as(JoinExtractFilterRule.Config.class));
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Join join = call.rel(0);

    if (join.getJoinType() != JoinRelType.INNER) {
      return;
    }

    if (join.getCondition().isAlwaysTrue()) {
      return;
    }

    if (!join.getSystemFieldList().isEmpty()) {
      // FIXME Enable this rule for joins with system fields
      return;
    }

    RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    final RelBuilder builder = call.builder();
    JoinRules.ConditionAnalysis conditionAnalysis = JoinRules.analyzeCondition(
        join.getCondition(),
        join.getLeft().getRowType(),
        rexBuilder,
        null
    );
    List<RexNode> unsupportedSubConditions = conditionAnalysis.getUnsupportedSubConditions();
    RexNode postJoinFilter = RexUtil.composeConjunction(rexBuilder, unsupportedSubConditions, true);
    if (postJoinFilter == null) {
      return;
    }

    RexNode joinCond = conditionAnalysis.getConditionWithUnsupportedSubConditionsIgnored(rexBuilder);

    // NOTE jvs 14-Mar-2006:  See JoinCommuteRule for why we
    // preserve attribute semiJoinDone here.

    final RelNode cartesianJoin =
        join.copy(
            join.getTraitSet(),
            joinCond,
            join.getLeft(),
            join.getRight(),
            join.getJoinType(),
            join.isSemiJoinDone()
        );

    builder.push(cartesianJoin)
           .filter(postJoinFilter);
    call.transformTo(builder.build());
  }
}
