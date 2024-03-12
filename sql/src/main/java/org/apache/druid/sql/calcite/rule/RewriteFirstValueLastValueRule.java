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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.math.BigDecimal;
import java.util.List;

/**
 * Rewrites exotic cases of FIRST_VALUE/LAST_VALUE to simpler plans.
 *
 * LAST_VALUE(x) OVER (ORDER BY Y)
 * implicitly means:
 * LAST_VALUE(x) OVER (ORDER BY Y RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
 * which is equiv to
 * LAST_VALUE(x) OVER (ORDER BY Y ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
 * since it will take the last value from the window; the value of the window will be:
 * X at the CURRENT ROW.
 *
 * This rule does this and a symmetric one for FIRST_VALUE.
 */
public class RewriteFirstValueLastValueRule extends RelOptRule implements SubstitutionRule
{
  public RewriteFirstValueLastValueRule()
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
    public RexNode visitOver(RexOver over)
    {
      SqlOperator operator = over.getOperator();
      RexWindow window = over.getWindow();
      RexWindowBound upperBound = window.getUpperBound();
      RexWindowBound lowerBound = window.getLowerBound();

      if (window.orderKeys.size() > 0) {
        if (operator.getKind() == SqlKind.LAST_VALUE && !upperBound.isUnbounded()) {
          if (upperBound.isCurrentRow()) {
            return rewriteToReferenceCurrentRow(over);
          }
        }
        if (operator.getKind() == SqlKind.FIRST_VALUE && !lowerBound.isUnbounded()) {
          if (lowerBound.isCurrentRow()) {
            return rewriteToReferenceCurrentRow(over);
          }
        }
      }
      return super.visitOver(over);
    }

    private RexNode rewriteToReferenceCurrentRow(RexOver over)
    {
      // could remove `last_value( x ) over ( .... order by y )`
      // best would be to: return over.getOperands().get(0);
      // however that make some queries too good
      return makeOver(
          over,
          over.getWindow(),
          SqlStdOperatorTable.LAG,
          ImmutableList.of(over.getOperands().get(0), rexBuilder.makeBigintLiteral(BigDecimal.ZERO))
      );
    }

    private RexNode makeOver(RexOver over, RexWindow window, SqlAggFunction aggFunction, List<RexNode> operands)
    {
      return rexBuilder.makeOver(
          over.type,
          aggFunction,
          operands,
          window.partitionKeys,
          window.orderKeys,
          window.getLowerBound(),
          window.getUpperBound(),
          window.isRows(),
          true,
          false,
          over.isDistinct(),
          over.ignoreNulls()
      );
    }
  }
}
