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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.RelBuilder;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidQueryRel;
import org.apache.druid.sql.calcite.rel.DruidUnnestDatasourceRel;

public class DruidUnnestDatasourceRule extends RelOptRule
{
  private final PlannerContext plannerContext;

  public DruidUnnestDatasourceRule(PlannerContext plannerContext)
  {
    super(
        operand(
            Uncollect.class,
            operand(LogicalProject.class, operand(DruidQueryRel.class, none()))
        )
    );
    this.plannerContext = plannerContext;
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    return true;
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    final Uncollect uncollectRel = call.rel(0);
    final LogicalProject logicalProject = call.rel(1);
    final DruidQueryRel druidQueryRel = call.rel(2);

    final RexBuilder rexBuilder = logicalProject.getCluster().getRexBuilder();

    final LogicalProject queryProject = LogicalProject.create(
        uncollectRel,
        ImmutableList.of(rexBuilder.makeInputRef(uncollectRel.getRowType().getFieldList().get(0).getType(), 0)),
        uncollectRel.getRowType()
    );

    DruidUnnestDatasourceRel unnestDatasourceRel = new DruidUnnestDatasourceRel(
        uncollectRel,
        druidQueryRel.withPartialQuery(druidQueryRel.getPartialDruidQuery().withSelectProject(queryProject)),
        logicalProject,
        plannerContext
    );
    // build this based on the druidUnnestRel

    final RelBuilder relBuilder =
        call.builder()
            .push(unnestDatasourceRel);

    call.transformTo(relBuilder.build());
  }
}
