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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.druid.common.config.NullHandling;

/**
 * Rule that converts `= null` into `= ''` in filter.
 */
public class DruidRewriteEqualNullRule extends RelOptRule
{
  private static final DruidRewriteEqualNullRule INSTANCE = new DruidRewriteEqualNullRule();

  private DruidRewriteEqualNullRule()
  {
    super(operand(Filter.class, any()));
  }

  public static DruidRewriteEqualNullRule instance()
  {
    return INSTANCE;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    if (!NullHandling.replaceWithDefault()) {
      return;
    }
    Filter oldFilter = call.rel(0);
    RexNode oldFilterCond = oldFilter.getCondition();
    
    if (RexUtil.findOperatorCall(
        SqlStdOperatorTable.EQUALS,
        oldFilterCond)
        == null) {
      // no longer contains equals
      return;
    }
    RewriteEqualNullRexShuttle rewriteShuttle =
        new RewriteEqualNullRexShuttle(
            oldFilter.getCluster().getRexBuilder());

    final RelBuilder relBuilder = call.builder();
    RexNode newfilter = oldFilterCond.accept(rewriteShuttle);
    if (newfilter.toString().equals(oldFilterCond.toString())) {
      return;
    }
    final RelNode newFilterRel = relBuilder
        .push(oldFilter.getInput())
        .filter(newfilter)
        .build();

    call.transformTo(newFilterRel);
    call.getPlanner().setImportance(oldFilter, 0.0);
  }

  /** Shuttle that convert `a = null` to `a = ''` */
  private class RewriteEqualNullRexShuttle extends RexShuttle
  {
    RexBuilder rexBuilder;

    RewriteEqualNullRexShuttle(RexBuilder rexBuilder) 
    {
      this.rexBuilder = rexBuilder;
    }

    // override RexShuttle
    public RexNode visitCall(RexCall call) 
    {
      RexNode newCall = super.visitCall(call);

      if (call.getOperator() == SqlStdOperatorTable.EQUALS) {
        RexCall tmpCall = (RexCall) newCall;
        RexNode op0 = tmpCall.operands.get(0);
        RexNode op1 = tmpCall.operands.get(1);

        RexLiteral emptyLit = rexBuilder.makeLiteral("");
        
        if (RexUtil.isNullLiteral(op1, true)) {
          newCall = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, op0, emptyLit);
        }
      }
      return newCall;
    }
  }
}
