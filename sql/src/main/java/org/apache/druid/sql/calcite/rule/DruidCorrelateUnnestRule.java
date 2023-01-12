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
            Correlate.class,
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
    final DruidRel<?> druidRel = call.rel(1);
    final DruidRel<?> uncollectRel = call.rel(2);

    return druidRel.getPartialDruidQuery() != null
           && uncollectRel.getPartialDruidQuery() != null;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Correlate correlate = call.rel(0);
    final DruidRel<?> druidRel = call.rel(1);
    DruidUnnestDatasourceRel druidUnnestDatasourceRel = call.rel(2);


    final RexBuilder rexBuilder = correlate.getCluster().getRexBuilder();

    final Filter druidRelFilter;
    final DruidRel<?> newDruidRelFilter;
    final List<RexNode> newProjectExprs = new ArrayList<>();

    final boolean isLeftDirectAccessPossible = enableLeftScanDirect && (druidRel instanceof DruidQueryRel);

    if (druidRel.getPartialDruidQuery().stage() == PartialDruidQuery.Stage.SELECT_PROJECT
        && (isLeftDirectAccessPossible || druidRel.getPartialDruidQuery().getWhereFilter() == null)) {
      // Swap the druidRel-side projection above the correlate, so the druidRel side is a simple scan or mapping. This helps us
      // avoid subqueries.
      final RelNode leftScan = druidRel.getPartialDruidQuery().getScan();
      final Project leftProject = druidRel.getPartialDruidQuery().getSelectProject();
      druidRelFilter = druidRel.getPartialDruidQuery().getWhereFilter();

      // Left-side projection expressions rewritten to be on top of the correlate.
      newProjectExprs.addAll(leftProject.getProjects());
      newDruidRelFilter = druidRel.withPartialQuery(PartialDruidQuery.create(leftScan));
    } else {
      // Leave druidRel as-is. Write input refs that do nothing.
      for (int i = 0; i < druidRel.getRowType().getFieldCount(); i++) {
        newProjectExprs.add(rexBuilder.makeInputRef(correlate.getRowType().getFieldList().get(i).getType(), i));
      }
      newDruidRelFilter = druidRel;
      druidRelFilter = null;
    }

    if (druidUnnestDatasourceRel.getPartialDruidQuery().stage() == PartialDruidQuery.Stage.SELECT_PROJECT) {
      for (final RexNode rexNode : RexUtil.shift(
          druidUnnestDatasourceRel.getPartialDruidQuery()
                                  .getSelectProject()
                                  .getProjects(),
          newDruidRelFilter.getRowType().getFieldCount()
      )) {
        newProjectExprs.add(rexNode);
      }
    } else {
      // Leave druidUnnestDatasourceRel as-is. Write input refs that do nothing.
      for (int i = 0; i < druidUnnestDatasourceRel.getRowType().getFieldCount(); i++) {
        newProjectExprs.add(
            rexBuilder.makeInputRef(
                correlate.getRowType()
                         .getFieldList()
                         .get(druidRel.getRowType().getFieldCount() + i)
                         .getType(),
                newDruidRelFilter.getRowType().getFieldCount() + i
            )
        );
      }
    }

    final DruidCorrelateUnnestRel druidCorr = DruidCorrelateUnnestRel.create(
        correlate.copy(
            correlate.getTraitSet(),
            newDruidRelFilter,
            druidUnnestDatasourceRel,
            correlate.getCorrelationId(),
            correlate.getRequiredColumns(),
            correlate.getJoinType()
        ),
        druidRelFilter,
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
