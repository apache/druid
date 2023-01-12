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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidCorrelateUnnestRel;
import org.apache.druid.sql.calcite.rel.DruidQueryRel;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.DruidUnnestDatasourceRel;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;

import java.util.ArrayList;
import java.util.List;

public class DruidCorrelateUnnestRule extends RelOptRule
{
  private final PlannerContext plannerContext;
  private final boolean enableLeftScanDirect;

  public DruidCorrelateUnnestRule(final PlannerContext plannerContext)
  {
    super(
        operand(
            LogicalCorrelate.class,
            operand(DruidRel.class, any()),
            operand(DruidUnnestDatasourceRel.class, any())
        )
    );

    this.plannerContext = plannerContext;
    this.enableLeftScanDirect = plannerContext.queryContext().getEnableJoinLeftScanDirect();
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    final DruidRel<?> left = call.rel(1);
    final DruidRel<?> right = call.rel(2);

    return left.getPartialDruidQuery() != null
           && right.getPartialDruidQuery() != null;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Correlate correlate = call.rel(0);
    final DruidRel<?> left = call.rel(1);
    DruidUnnestDatasourceRel right = call.rel(2);


    final RexBuilder rexBuilder = correlate.getCluster().getRexBuilder();

    final Filter leftFilter;
    final DruidRel<?> newLeft;
    final List<RexNode> newProjectExprs = new ArrayList<>();

    final boolean isLeftDirectAccessPossible = enableLeftScanDirect && (left instanceof DruidQueryRel);

    if (left.getPartialDruidQuery().stage() == PartialDruidQuery.Stage.SELECT_PROJECT
        && (isLeftDirectAccessPossible || left.getPartialDruidQuery().getWhereFilter() == null)) {
      // Swap the left-side projection above the correlate, so the left side is a simple scan or mapping. This helps us
      // avoid subqueries.
      final RelNode leftScan = left.getPartialDruidQuery().getScan();
      final Project leftProject = left.getPartialDruidQuery().getSelectProject();
      leftFilter = left.getPartialDruidQuery().getWhereFilter();

      // Left-side projection expressions rewritten to be on top of the correlate.
      newProjectExprs.addAll(leftProject.getProjects());
      newLeft = left.withPartialQuery(PartialDruidQuery.create(leftScan));
    } else {
      // Leave left as-is. Write input refs that do nothing.
      for (int i = 0; i < left.getRowType().getFieldCount(); i++) {
        newProjectExprs.add(rexBuilder.makeInputRef(correlate.getRowType().getFieldList().get(i).getType(), i));
      }
      newLeft = left;
      leftFilter = null;
    }


    if (right.getPartialDruidQuery().stage() == PartialDruidQuery.Stage.SELECT_PROJECT) {
      for (final RexNode rexNode : RexUtil.shift(
          right.getPartialDruidQuery()
               .getSelectProject()
               .getProjects(),
          newLeft.getRowType().getFieldCount()
      )) {
        newProjectExprs.add(rexNode);
      }
    } else {
      // Leave right as-is. Write input refs that do nothing.
      for (int i = 0; i < right.getRowType().getFieldCount(); i++) {
        newProjectExprs.add(
            rexBuilder.makeInputRef(
                correlate.getRowType()
                         .getFieldList()
                         .get(left.getRowType().getFieldCount() + i)
                         .getType(),
                newLeft.getRowType().getFieldCount() + i
            )
        );
      }
    }

    Correlate corr = correlate.copy(
        correlate.getTraitSet(),
        newLeft,
        right,
        correlate.getCorrelationId(),
        correlate.getRequiredColumns(),
        correlate.getJoinType()
    );

    final DruidCorrelateUnnestRel druidCorr = DruidCorrelateUnnestRel.create(
        corr,
        leftFilter,
        plannerContext
    );


    final RelBuilder relBuilder =
        call.builder()
            .push(druidCorr)
            .project(RexUtil.fixUp(
                rexBuilder,
                newProjectExprs,
                RelOptUtil.getFieldTypeList(druidCorr.getRowType())
            ));

    call.transformTo(relBuilder.build());
  }
}
