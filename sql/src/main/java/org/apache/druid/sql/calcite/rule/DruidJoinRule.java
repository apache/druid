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

import com.google.common.annotations.VisibleForTesting;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidJoinQueryRel;
import org.apache.druid.sql.calcite.rel.DruidQueryRel;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;

import java.util.ArrayList;
import java.util.List;

public class DruidJoinRule extends RelOptRule
{

  private final boolean enableLeftScanDirect;
  private final PlannerContext plannerContext;

  private DruidJoinRule(final PlannerContext plannerContext)
  {
    super(
        operand(
            Join.class,
            operand(DruidRel.class, any()),
            operand(DruidRel.class, any())
        )
    );
    this.enableLeftScanDirect = plannerContext.queryContext().getEnableJoinLeftScanDirect();
    this.plannerContext = plannerContext;
  }

  public static DruidJoinRule instance(PlannerContext plannerContext)
  {
    return new DruidJoinRule(plannerContext);
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    final Join join = call.rel(0);
    final DruidRel<?> left = call.rel(1);
    final DruidRel<?> right = call.rel(2);

    // 1) Can handle the join condition as a native join.
    // 2) Left has a PartialDruidQuery (i.e., is a real query, not top-level UNION ALL).
    // 3) Right has a PartialDruidQuery (i.e., is a real query, not top-level UNION ALL).
    return canHandleCondition(join.getCondition(), join.getLeft().getRowType(), right, join.getCluster().getRexBuilder())
           && left.getPartialDruidQuery() != null
           && right.getPartialDruidQuery() != null;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Join join = call.rel(0);
    final DruidRel<?> left = call.rel(1);
    final DruidRel<?> right = call.rel(2);

    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();

    final DruidRel<?> newLeft;
    final DruidRel<?> newRight;
    final Filter leftFilter;
    final List<RexNode> newProjectExprs = new ArrayList<>();

    // Can't be final, because we're going to reassign it up to a couple of times.
    JoinRules.ConditionAnalysis conditionAnalysis = JoinRules.analyzeCondition(
        join.getCondition(),
        join.getLeft().getRowType(),
        rexBuilder,
        plannerContext
    );
    final boolean isLeftDirectAccessPossible = enableLeftScanDirect && (left instanceof DruidQueryRel);

    if (!plannerContext.getJoinAlgorithm().requiresSubquery()
        && left.getPartialDruidQuery().stage() == PartialDruidQuery.Stage.SELECT_PROJECT
        && (isLeftDirectAccessPossible || left.getPartialDruidQuery().getWhereFilter() == null)) {
      // Swap the left-side projection above the join, so the left side is a simple scan or mapping. This helps us
      // avoid subqueries.
      final RelNode leftScan = left.getPartialDruidQuery().getScan();
      final Project leftProject = left.getPartialDruidQuery().getSelectProject();
      leftFilter = left.getPartialDruidQuery().getWhereFilter();

      // Left-side projection expressions rewritten to be on top of the join.
      newProjectExprs.addAll(leftProject.getProjects());
      newLeft = left.withPartialQuery(PartialDruidQuery.create(leftScan));
      conditionAnalysis = conditionAnalysis.pushThroughLeftProject(leftProject);
    } else {
      // Leave left as-is. Write input refs that do nothing.
      for (int i = 0; i < left.getRowType().getFieldCount(); i++) {
        newProjectExprs.add(rexBuilder.makeInputRef(join.getRowType().getFieldList().get(i).getType(), i));
      }

      newLeft = left;
      leftFilter = null;
    }

    if (!plannerContext.getJoinAlgorithm().requiresSubquery()
        && right.getPartialDruidQuery().stage() == PartialDruidQuery.Stage.SELECT_PROJECT
        && right.getPartialDruidQuery().getWhereFilter() == null
        && !right.getPartialDruidQuery().getSelectProject().isMapping()
        && conditionAnalysis.onlyUsesMappingsFromRightProject(right.getPartialDruidQuery().getSelectProject())) {
      // Swap the right-side projection above the join, so the right side is a simple scan or mapping. This helps us
      // avoid subqueries.
      final RelNode rightScan = right.getPartialDruidQuery().getScan();
      final Project rightProject = right.getPartialDruidQuery().getSelectProject();

      // Right-side projection expressions rewritten to be on top of the join.
      for (final RexNode rexNode : RexUtil.shift(rightProject.getProjects(), newLeft.getRowType().getFieldCount())) {
        if (join.getJoinType().generatesNullsOnRight()) {
          newProjectExprs.add(makeNullableIfLiteral(rexNode, rexBuilder));
        } else {
          newProjectExprs.add(rexNode);
        }
      }

      newRight = right.withPartialQuery(PartialDruidQuery.create(rightScan));
      conditionAnalysis = conditionAnalysis.pushThroughRightProject(rightProject);
    } else {
      // Leave right as-is. Write input refs that do nothing.
      for (int i = 0; i < right.getRowType().getFieldCount(); i++) {
        newProjectExprs.add(
            rexBuilder.makeInputRef(
                join.getRowType().getFieldList().get(left.getRowType().getFieldCount() + i).getType(),
                newLeft.getRowType().getFieldCount() + i
            )
        );
      }

      newRight = right;
    }

    // Druid join written on top of the new left and right sides.
    final DruidJoinQueryRel druidJoin = DruidJoinQueryRel.create(
        join.copy(
            join.getTraitSet(),
            conditionAnalysis.getCondition(rexBuilder),
            newLeft,
            newRight,
            join.getJoinType(),
            join.isSemiJoinDone()
        ),
        leftFilter,
        left.getPlannerContext()
    );

    final RelBuilder relBuilder =
        call.builder()
            .push(druidJoin)
            .project(
                RexUtil.fixUp(
                    rexBuilder,
                    newProjectExprs,
                    RelOptUtil.getFieldTypeList(druidJoin.getRowType())
                )
            );

    call.transformTo(relBuilder.build());
  }

  private static RexNode makeNullableIfLiteral(final RexNode rexNode, final RexBuilder rexBuilder)
  {
    if (rexNode.isA(SqlKind.LITERAL)) {
      return rexBuilder.makeLiteral(
          RexLiteral.value(rexNode),
          rexBuilder.getTypeFactory().createTypeWithNullability(rexNode.getType(), true),
          true
      );
    } else {
      return rexNode;
    }
  }

  /**
   * Returns whether {@link JoinRules#analyzeCondition} would return something.
   */
  @VisibleForTesting
  public boolean canHandleCondition(final RexNode condition, final RelDataType leftRowType, DruidRel<?> right, final RexBuilder rexBuilder)
  {
    return JoinRules.validateJoinCondition(condition, leftRowType, right, rexBuilder, plannerContext);
  }
}
