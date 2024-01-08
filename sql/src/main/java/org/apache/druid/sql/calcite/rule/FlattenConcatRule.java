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
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.Function;
import org.apache.druid.sql.calcite.expression.builtin.ConcatOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TextcatOperatorConversion;

import java.util.ArrayList;
import java.util.List;

/**
 * Flattens calls to CONCAT. Useful because otherwise [a || b || c] would get planned as [CONCAT(CONCAT(a, b), c)].
 */
public class FlattenConcatRule extends RelOptRule implements SubstitutionRule
{
  public FlattenConcatRule()
  {
    super(operand(RelNode.class, any()));
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final RelNode oldNode = call.rel(0);
    final FlattenConcatShuttle shuttle = new FlattenConcatShuttle(oldNode.getCluster().getRexBuilder());
    final RelNode newNode = oldNode.accept(shuttle);

    //noinspection ObjectEquality
    if (newNode != oldNode) {
      call.transformTo(newNode);
      call.getPlanner().prune(oldNode);
    }
  }

  private static class FlattenConcatShuttle extends RexShuttle
  {
    private final RexBuilder rexBuilder;

    public FlattenConcatShuttle(RexBuilder rexBuilder)
    {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call)
    {
      if (isNonTrivialStringConcat(call)) {
        final List<RexNode> newOperands = new ArrayList<>();
        for (final RexNode operand : call.getOperands()) {
          if (isNonTrivialStringConcat(operand)) {
            // Recursively flatten. We only flatten non-trivial CONCAT calls, because trivial ones (which do not
            // reference any inputs) are reduced to constants by ReduceExpressionsRule.
            final RexNode visitedOperand = visitCall((RexCall) operand);

            if (isStringConcat(visitedOperand)) {
              newOperands.addAll(((RexCall) visitedOperand).getOperands());
            } else {
              newOperands.add(visitedOperand);
            }
          } else if (RexUtil.isNullLiteral(operand, true) && NullHandling.sqlCompatible()) {
            return rexBuilder.makeNullLiteral(call.getType());
          } else {
            newOperands.add(operand);
          }
        }

        if (!newOperands.equals(call.getOperands())) {
          return rexBuilder.makeCall(ConcatOperatorConversion.SQL_FUNCTION, newOperands);
        } else {
          return call;
        }
      } else {
        return super.visitCall(call);
      }
    }
  }

  /**
   * Whether a rex is a string concatenation operator. All of these end up being converted to
   * {@link Function.ConcatFunc}.
   */
  static boolean isStringConcat(final RexNode rexNode)
  {
    if (SqlTypeFamily.STRING.contains(rexNode.getType()) && rexNode instanceof RexCall) {
      final SqlOperator operator = ((RexCall) rexNode).getOperator();
      return ConcatOperatorConversion.SQL_FUNCTION.equals(operator)
             || TextcatOperatorConversion.SQL_FUNCTION.equals(operator)
             || SqlStdOperatorTable.CONCAT.equals(operator);
    } else {
      return false;
    }
  }

  /**
   * Whether a rex is a string concatenation involving at least one an input field.
   */
  static boolean isNonTrivialStringConcat(final RexNode rexNode)
  {
    return isStringConcat(rexNode) && !RelOptUtil.InputFinder.bits(rexNode).isEmpty();
  }
}
