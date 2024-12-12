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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.sql.calcite.planner.Calcites;

import java.util.ArrayList;
import java.util.List;

/**
 * Transform calls like f(COALESCE(x, y)) => (x IS NOT NULL AND f(x)) OR (x IS NULL AND f(y)), for boolean functions f.
 *
 * Only simple calls are transformed; see {@link #isSimpleCoalesce(RexNode)} for a definition. This is because the
 * main purpose of this decomposition is to make it more likely that we'll be able to use indexes when filtering.
 */
public class FilterDecomposeCoalesceRule extends RelOptRule implements SubstitutionRule
{
  public FilterDecomposeCoalesceRule()
  {
    super(operand(Filter.class, any()));
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Filter oldFilter = call.rel(0);
    final DecomposeCoalesceShuttle shuttle = new DecomposeCoalesceShuttle(oldFilter.getCluster().getRexBuilder());
    final RexNode newCondition = oldFilter.getCondition().accept(shuttle);

    //noinspection ObjectEquality
    if (newCondition != oldFilter.getCondition()) {
      call.transformTo(
          call.builder()
              .push(oldFilter.getInput())
              .filter(newCondition).build()
      );

      call.getPlanner().prune(oldFilter);
    }
  }

  /**
   * Shuttle that decomposes predicates on top of simple COALESCE calls. Implementation is similar to
   * {@link ReduceExpressionsRule.CaseShuttle}.
   */
  private static class DecomposeCoalesceShuttle extends RexShuttle
  {
    private final RexBuilder rexBuilder;

    public DecomposeCoalesceShuttle(RexBuilder rexBuilder)
    {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(final RexCall call)
    {
      RexCall retVal = call;
      while (true) {
        retVal = (RexCall) super.visitCall(retVal);
        final RexCall old = retVal;
        retVal = decomposePredicateOnCoalesce(retVal, rexBuilder);
        //noinspection ObjectEquality
        if (retVal == old) {
          return retVal;
        }
      }
    }
  }

  /**
   * Whether a rex is a 2-arg COALESCE where both arguments satisfy {@link #isSimpleCoalesceArg(RexNode)}.
   */
  private static boolean isSimpleCoalesce(final RexNode rexNode)
  {
    if (rexNode.getKind() == SqlKind.COALESCE) {
      final List<RexNode> operands = ((RexCall) rexNode).getOperands();
      return operands.size() == 2 && isSimpleCoalesceArg(operands.get(0)) && isSimpleCoalesceArg(operands.get(1));
    }

    return false;
  }

  /**
   * Whether an expression is a literal (allowing arrays), or an input reference. Ignores casts either way.
   */
  private static boolean isSimpleCoalesceArg(final RexNode arg)
  {
    final RexNode argNoCast = RexUtil.removeCast(arg);
    return Calcites.isLiteral(argNoCast, false, true) || argNoCast.isA(SqlKind.INPUT_REF);
  }

  /**
   * Convert f(COALESCE(x, y)) => (x IS NOT NULL AND f(x)) OR (x IS NULL AND f(y)).
   */
  private static RexCall decomposePredicateOnCoalesce(final RexCall call, final RexBuilder rexBuilder)
  {
    if (call.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
      return call;
    }

    switch (call.getKind()) {
      case AND:
      case CASE:
      case COALESCE:
      case OR:
        return call;
      default:
        break;
    }

    int coalesceOrdinal = -1;
    final List<RexNode> operands = call.getOperands();
    for (int i = 0; i < operands.size(); i++) {
      if (isSimpleCoalesce(operands.get(i))) {
        coalesceOrdinal = i;
        break;
      }
    }

    if (coalesceOrdinal < 0) {
      return call;
    }

    // Convert f(COALESCE(x, y)) => (x IS NOT NULL AND f(x)) OR (x IS NULL AND f(y)).
    final RexCall coalesceCall = (RexCall) operands.get(coalesceOrdinal);
    final RexNode x = coalesceCall.getOperands().get(0);
    final RexNode y = coalesceCall.getOperands().get(1);

    final List<RexNode> fxArgs = new ArrayList<>(call.getOperands());
    fxArgs.set(coalesceOrdinal, x);

    final List<RexNode> fyArgs = new ArrayList<>(call.getOperands());
    fyArgs.set(coalesceOrdinal, y);

    return (RexCall) RexUtil.composeDisjunction(
        rexBuilder,
        ImmutableList.of(
            RexUtil.composeConjunction(
                rexBuilder,
                ImmutableList.of(
                    rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, x),
                    call.clone(call.getType(), fxArgs)
                )
            ),
            RexUtil.composeConjunction(
                rexBuilder,
                ImmutableList.of(
                    rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, x),
                    call.clone(call.getType(), fyArgs)
                )
            )
        )
    );
  }
}
