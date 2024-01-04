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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.druid.sql.calcite.aggregation.builtin.EarliestLatestAnySqlAggregator;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule that un-does the rewrite from {@link org.apache.calcite.sql.fun.SqlCoalesceFunction#rewriteCall}.
 *
 * Important because otherwise COALESCE turns into a gnarly CASE with duplicated expressions. We must un-do the
 * rewrite rather than disable {@link SqlValidator.Config#callRewrite()}, because we rely on validator rewrites
 * in other cases, such as {@link EarliestLatestAnySqlAggregator#EARLIEST} and
 * {@link EarliestLatestAnySqlAggregator#LATEST}.
 */
public class CaseToCoalesceRule extends RelOptRule implements SubstitutionRule
{
  public CaseToCoalesceRule()
  {
    super(operand(RelNode.class, any()));
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final RelNode oldNode = call.rel(0);
    final CaseToCoalesceShuttle shuttle = new CaseToCoalesceShuttle(oldNode.getCluster().getRexBuilder());
    final RelNode newNode = oldNode.accept(shuttle);

    //noinspection ObjectEquality
    if (newNode != oldNode) {
      call.transformTo(newNode);
      call.getPlanner().prune(oldNode);
    }
  }

  private static class CaseToCoalesceShuttle extends RexShuttle
  {
    private final RexBuilder rexBuilder;

    public CaseToCoalesceShuttle(RexBuilder rexBuilder)
    {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call)
    {
      if (call.getKind() == SqlKind.CASE) {
        final List<RexNode> caseArgs = call.getOperands();
        final List<RexNode> coalesceArgs = new ArrayList<>();

        for (int i = 0; i < caseArgs.size(); i += 2) {
          if (i == caseArgs.size() - 1) {
            // ELSE x
            if (coalesceArgs.isEmpty()) {
              return super.visitCall(call);
            } else {
              coalesceArgs.add(caseArgs.get(i));
            }
          } else if (isCoalesceWhenThen(rexBuilder.getTypeFactory(), caseArgs.get(i), caseArgs.get(i + 1))) {
            // WHEN x IS NOT NULL THEN x
            coalesceArgs.add(((RexCall) caseArgs.get(i)).getOperands().get(0));
          } else {
            return super.visitCall(call);
          }
        }

        return rexBuilder.makeCall(SqlStdOperatorTable.COALESCE, coalesceArgs);
      }

      return super.visitCall(call);
    }
  }

  /**
   * Returns whether "when" is like "then IS NOT NULL". Ignores nullability casts on "then".
   */
  private static boolean isCoalesceWhenThen(
      final RelDataTypeFactory typeFactory,
      final RexNode when,
      final RexNode then
  )
  {
    if (when.isA(SqlKind.IS_NOT_NULL)) {
      final RexNode whenIsNotNullArg =
          RexUtil.removeNullabilityCast(typeFactory, ((RexCall) when).getOperands().get(0));
      return whenIsNotNullArg.equals(RexUtil.removeNullabilityCast(typeFactory, then));
    } else {
      return false;
    }
  }
}
