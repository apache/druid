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
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Rewrites comparisions to avoid bug FIXME.
 *
 * Rewrites RexCall::VARCHAR = RexLiteral::CHAR to RexCall::VARCHAR =
 * RexLiteral::VARCHAR
 *
 * needed until CALCITE-6435 is fixed & released.
 */
public class FixIncorrectInExpansionTypes extends RelOptRule implements SubstitutionRule
{
  public FixIncorrectInExpansionTypes()
  {
    super(operand(RelNode.class, any()));
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final RelNode oldNode = call.rel(0);
    final RewriteShuttle shuttle = new RewriteShuttle(oldNode.getCluster().getRexBuilder());
    final RelNode newNode = oldNode.accept(shuttle);

    // noinspection ObjectEquality
    if (newNode != oldNode) {
      call.transformTo(newNode);
      call.getPlanner().prune(oldNode);
    }
  }

  private static class RewriteShuttle extends RexShuttle
  {
    private final RexBuilder rexBuilder;

    public RewriteShuttle(RexBuilder rexBuilder)
    {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call)
    {
      RexNode newNode = super.visitCall(call);
      if (newNode.getKind() == SqlKind.EQUALS || newNode.getKind() == SqlKind.NOT_EQUALS) {
        RexCall newCall = (RexCall) newNode;
        RexNode op0 = newCall.getOperands().get(0);
        RexNode op1 = newCall.getOperands().get(1);
        if (RexUtil.isLiteral(op1, false)) {

          if (op1.getType().getSqlTypeName() == SqlTypeName.CHAR
              && op0.getType().getSqlTypeName() == SqlTypeName.VARCHAR) {

            RexNode newLiteral = rexBuilder.ensureType(op0.getType(), op1, true);
            return rexBuilder.makeCall(
                newCall.getOperator(),
                op0,
                newLiteral
            );
          }
        }
      }
      return newNode;
    }
  }
}
