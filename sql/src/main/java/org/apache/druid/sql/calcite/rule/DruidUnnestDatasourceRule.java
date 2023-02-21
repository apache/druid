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

/**
 * This class creates the rule to abide by for creating unnest (internally uncollect) in Calcite.
 * Typically, Calcite plans the *unnest* part of the query involving a table such as
 * SELECT * from numFoo, unnest(dim3)
 * or even a standalone unnest query such as
 * SELECT * from unnest(ARRAY[1,2,3]) in the following way:
 *   78:Uncollect(subset=[rel#79:Subset#3.NONE.[]])
 *     76:LogicalProject(subset=[rel#77:Subset#2.NONE.[]], EXPR$0=[MV_TO_ARRAY($cor0.dim3)])
 *       7:LogicalValues(subset=[rel#75:Subset#1.NONE.[0]], tuples=[[{ 0 }]])
 *
 * Calcite tackles plans bottom up. Therefore,
 * {@link DruidLogicalValuesRule} converts the LogicalValues part into a leaf level {@link DruidQueryRel}
 * thereby creating the following subtree in the call tree
 *
 * Uncollect
 *  \
 *  LogicalProject
 *   \
 *   DruidQueryRel
 *
 *
 *  This forms the premise of this rule. The goal is to transform the above-mentioned structure in the tree
 *  with a new rel {@link DruidUnnestDatasourceRel} which shall be created here.
 *
 */
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
        null,
        plannerContext
    );

    final RelBuilder relBuilder =
        call.builder()
            .push(unnestDatasourceRel);

    call.transformTo(relBuilder.build());
  }
}
