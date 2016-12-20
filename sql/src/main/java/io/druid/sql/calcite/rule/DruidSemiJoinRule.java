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

import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.DruidSemiJoin;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.SemiJoin;

public class DruidSemiJoinRule extends RelOptRule
{
  private static final DruidSemiJoinRule INSTANCE = new DruidSemiJoinRule();

  private DruidSemiJoinRule()
  {
    super(
        operand(
            SemiJoin.class,
            operand(DruidRel.class, none()),
            operand(DruidRel.class, none())
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
    final SemiJoin semiJoin = call.rel(0);
    final DruidRel left = call.rel(1);
    final DruidRel right = call.rel(2);
    final DruidSemiJoin druidSemiJoin = DruidSemiJoin.from(semiJoin, semiJoin.getTraitSet(), left, right);
    if (druidSemiJoin != null) {
      call.transformTo(druidSemiJoin);
    }
  }
}
