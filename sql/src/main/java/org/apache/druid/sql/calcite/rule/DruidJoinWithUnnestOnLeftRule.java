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
    final Join join = call.rel(0);
    final DruidRel<?> left = call.rel(1);
    final DruidRel<?> right = call.rel(2);

    // 1) Can handle the join condition as a native join.
    // 2) Left has a PartialDruidQuery (i.e., is a real query, not top-level UNION ALL).
    // 3) Right has a PartialDruidQuery (i.e., is a real query, not top-level UNION ALL).
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
