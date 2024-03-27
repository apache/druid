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
import org.apache.calcite.rel.core.Join;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidJoinUnnestRel;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.DruidUnnestRel;

/**
 * Queries such as select * from mytest, unnest(mv_to_array(c2)) as u(c) where c in (select col from (values('A'),('C')) as t(col))
 * Creates a plan in Calcite as follows:
 * <p>
 * 971383:LogicalSort(fetch=[1001:BIGINT])
 *   971379:LogicalCorrelate(subset=[rel#971380:Subset#8.NONE.[]], correlation=[$cor0], joinType=[inner], requiredColumns=[{2}])
 *     971229:LogicalTableScan(subset=[rel#971366:Subset#0.NONE.[]], table=[[druid, mytest]])
 *     971377:LogicalProject(subset=[rel#971378:Subset#7.NONE.[]], c=[$0])
 *       971375:LogicalJoin(subset=[rel#971376:Subset#6.NONE.[]], condition=[=($0, $1)], joinType=[inner])
 *         971370:Uncollect(subset=[rel#971371:Subset#3.NONE.[]])
 *           971368:LogicalProject(subset=[rel#971369:Subset#2.NONE.[]], EXPR$0=[MV_TO_ARRAY($cor0.c2)])
 *             971230:LogicalValues(subset=[rel#971367:Subset#1.NONE.[0]], tuples=[[{ 0 }]])
 *         971364:LogicalValues(subset=[rel#971374:Subset#5.NONE.[0]], tuples=[[{ 'A' }, { 'C' }]])
 * <p>
 *  See that Calcite is trying to do a JOIN between Uncollect (or unnest) on the left with a DruidRel/Query on the right
 * <p>
 * This rule identifies this pattern of
 *     Join
 *    /     \
 * Unnest   DruidRel
 * <p>
 * and creates a rel which is to be consumed later by Correlate to transpose the join and Correlate
 *
 */
public class DruidJoinWithUnnestOnLeftRule extends RelOptRule
{
  private final PlannerContext plannerContext;

  public DruidJoinWithUnnestOnLeftRule(PlannerContext plannerContext1)
  {
    super(
        operand(
            Join.class,
            operand(DruidUnnestRel.class, any()),
            operand(DruidRel.class, any())
        )
    );
    this.plannerContext = plannerContext1;
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    final DruidRel<?> right = call.rel(2);
    return right.getPartialDruidQuery() != null;

  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Join join = call.rel(0);
    final DruidRel<?> left = call.rel(1);
    final DruidRel<?> right = call.rel(2);

    DruidJoinUnnestRel joinWithUnnest = DruidJoinUnnestRel.create(join, left, right, plannerContext);
    call.transformTo(joinWithUnnest);
  }
}
