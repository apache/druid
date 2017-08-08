/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.rule;

import com.google.common.base.Predicate;
import io.druid.sql.calcite.aggregation.DimensionExpression;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.DruidSemiJoin;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule adapted from Calcite 1.11.0's SemiJoinRule.
 *
 * This rule identifies a JOIN where the right-hand side is being used like a filter. Requirements are:
 *
 * 1) Right-hand side is grouping on the join key
 * 2) No fields from the right-hand side are selected
 * 3) Join is INNER (right-hand side acting as filter) or LEFT (right-hand side can be ignored)
 *
 * This is used instead of Calcite's built in rule because that rule's un-doing of aggregation is unproductive (we'd
 * just want to add it back again). Also, this rule operates on DruidRels.
 */
public class DruidSemiJoinRule extends RelOptRule
{
  private static final Predicate<Join> IS_LEFT_OR_INNER =
      new Predicate<Join>()
      {
        @Override
        public boolean apply(Join join)
        {
          final JoinRelType joinType = join.getJoinType();
          return joinType == JoinRelType.LEFT || joinType == JoinRelType.INNER;
        }
      };

  private static final Predicate<DruidRel> IS_GROUP_BY =
      new Predicate<DruidRel>()
      {
        @Override
        public boolean apply(DruidRel druidRel)
        {
          return druidRel.getQueryBuilder().getGrouping() != null;
        }
      };

  private static final DruidSemiJoinRule INSTANCE = new DruidSemiJoinRule();

  private DruidSemiJoinRule()
  {
    super(
        operand(
            Project.class,
            operand(
                Join.class,
                null,
                IS_LEFT_OR_INNER,
                some(
                    operand(DruidRel.class, any()),
                    operand(DruidRel.class, null, IS_GROUP_BY, any())
                )
            )
        )
    );
  }

  public static DruidSemiJoinRule instance()
  {
    return INSTANCE;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Project project = call.rel(0);
    final Join join = call.rel(1);
    final DruidRel left = call.rel(2);
    final DruidRel right = call.rel(3);

    final ImmutableBitSet bits =
        RelOptUtil.InputFinder.bits(project.getProjects(), null);
    final ImmutableBitSet rightBits =
        ImmutableBitSet.range(
            left.getRowType().getFieldCount(),
            join.getRowType().getFieldCount()
        );

    if (bits.intersects(rightBits)) {
      return;
    }

    final JoinInfo joinInfo = join.analyzeCondition();
    final List<Integer> rightDimsOut = new ArrayList<>();
    for (DimensionExpression dimension : right.getQueryBuilder().getGrouping().getDimensions()) {
      rightDimsOut.add(right.getOutputRowSignature().getRowOrder().indexOf(dimension.getOutputName()));
    }

    if (!joinInfo.isEqui() || !joinInfo.rightSet().equals(ImmutableBitSet.of(rightDimsOut))) {
      // Rule requires that aggregate key to be the same as the join key.
      // By the way, neither a super-set nor a sub-set would work.
      return;
    }

    final RelBuilder relBuilder = call.builder();

    if (join.getJoinType() == JoinRelType.LEFT) {
      // Join can be eliminated since the right-hand side cannot have any effect (nothing is being selected,
      // and LEFT means even if there is no match, a left-hand row will still be included).
      relBuilder.push(left);
    } else {
      final DruidSemiJoin druidSemiJoin = DruidSemiJoin.from(
          left,
          right,
          joinInfo.leftKeys,
          joinInfo.rightKeys,
          left.getPlannerContext()
      );

      if (druidSemiJoin == null) {
        return;
      }

      // Check maxQueryCount.
      final PlannerConfig plannerConfig = left.getPlannerContext().getPlannerConfig();
      if (plannerConfig.getMaxQueryCount() > 0 && druidSemiJoin.getQueryCount() > plannerConfig.getMaxQueryCount()) {
        return;
      }

      relBuilder.push(druidSemiJoin);
    }

    call.transformTo(
        relBuilder
            .project(project.getProjects(), project.getRowType().getFieldNames())
            .build()
    );
  }
}
