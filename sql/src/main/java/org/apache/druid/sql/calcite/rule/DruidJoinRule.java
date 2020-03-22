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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.druid.sql.calcite.rel.DruidJoinQueryRel;
import org.apache.druid.sql.calcite.rel.DruidRel;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class DruidJoinRule extends RelOptRule
{
  private static final DruidJoinRule INSTANCE = new DruidJoinRule();

  private DruidJoinRule()
  {
    super(
        operand(
            Join.class,
            operand(DruidRel.class, none()),
            operand(DruidRel.class, none())
        )
    );
  }

  public static DruidJoinRule instance()
  {
    return INSTANCE;
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    final Join join = call.rel(0);
    final DruidRel<?> right = call.rel(2);

    // 1) Condition must be handleable.
    // 2) Right cannot be a join; we want to generate left-heavy trees.
    return canHandleCondition(join.getCondition(), join.getLeft().getRowType())
           && !(right instanceof DruidJoinQueryRel);
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Join join = call.rel(0);
    final DruidRel<?> left = call.rel(1);
    final DruidRel<?> right = call.rel(2);

    // Preconditions were already verified in "matches".
    call.transformTo(DruidJoinQueryRel.create(join, left, right));
  }

  /**
   * Returns true if this condition is an AND of equality conditions of the form: f(LeftRel) = RightColumn.
   *
   * @see org.apache.druid.segment.join.JoinConditionAnalysis where "equiCondition" is the same concept.
   */
  @VisibleForTesting
  static boolean canHandleCondition(final RexNode condition, final RelDataType leftRowType)
  {
    final List<RexNode> subConditions = decomposeAnd(condition);

    for (RexNode subCondition : subConditions) {
      if (subCondition.isA(SqlKind.LITERAL)) {
        // Literals are always OK.
        continue;
      }

      if (!subCondition.isA(SqlKind.EQUALS)) {
        // If it's not EQUALS, it's not supported.
        return false;
      }

      final List<RexNode> operands = ((RexCall) subCondition).getOperands();
      Preconditions.checkState(operands.size() == 2, "Expected 2 operands, got[%,d]", operands.size());

      final int numLeftFields = leftRowType.getFieldList().size();

      final boolean rhsIsFieldOfRightRel =
          operands.get(1).isA(SqlKind.INPUT_REF)
          && ((RexInputRef) operands.get(1)).getIndex() >= numLeftFields;

      final boolean lhsIsExpressionOfLeftRel =
          RelOptUtil.InputFinder.bits(operands.get(0)).intersects(ImmutableBitSet.range(numLeftFields));

      if (!(lhsIsExpressionOfLeftRel && rhsIsFieldOfRightRel)) {
        // Cannot handle this condition.
        return false;
      }
    }

    return true;
  }

  @VisibleForTesting
  static List<RexNode> decomposeAnd(final RexNode condition)
  {
    final List<RexNode> retVal = new ArrayList<>();
    final Stack<RexNode> stack = new Stack<>();

    stack.push(condition);

    while (!stack.empty()) {
      final RexNode current = stack.pop();

      if (current.isA(SqlKind.AND)) {
        final List<RexNode> operands = ((RexCall) current).getOperands();

        // Add right-to-left, so when we unwind the stack, the operands are in the original order.
        for (int i = operands.size() - 1; i >= 0; i--) {
          stack.push(operands.get(i));
        }
      } else {
        retVal.add(current);
      }
    }

    return retVal;
  }
}
