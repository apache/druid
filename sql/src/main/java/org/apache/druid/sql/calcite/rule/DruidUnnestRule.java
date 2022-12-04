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
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.tools.RelBuilder;
import org.apache.druid.query.DataSource;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.DruidUnnestRel;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;

public class DruidUnnestRule extends RelOptRule
{
  private final PlannerContext plannerContext;

  public DruidUnnestRule(final PlannerContext plannerContext)
  {
    /*
                      LogicalCorrelate
                     /      \
              DruidRel      Uncollect
                              \
                            LogicalProject
                                \
                              LogicalValues
    */
    super(
        operand(
            LogicalCorrelate.class,
            operand(DruidRel.class, any()),
            operand(Uncollect.class, operand(LogicalProject.class, operand(LogicalValues.class, any())))
        )
    );

    this.plannerContext = plannerContext;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    LogicalCorrelate logicalCorrelate = call.rel(0);
    DruidRel druidRel = call.rel(1);

    LogicalProject logicalProject = call.rel(3);

    RexCall rx = (RexCall) logicalProject.getChildExps().get(0);
    RexFieldAccess rf = (RexFieldAccess) rx.getOperands().get(0);
    String dimensionToUnnest = rf.getField().getName();

    DataSource base = druidRel.getDataSourceFromRel();


    //create an UnnestRel here and transform using that rel
    DruidUnnestRel druidUnnestRel = new DruidUnnestRel(
        logicalCorrelate.getCluster(),
        logicalCorrelate.getTraitSet(),
        logicalCorrelate,
        PartialDruidQuery.create(logicalCorrelate),
        base,
        dimensionToUnnest,
        plannerContext
    );

    // build this based on the druidUnnestRel
    final RelBuilder relBuilder =
        call.builder()
            .push(druidUnnestRel);

    call.transformTo(relBuilder.build());
  }
}
