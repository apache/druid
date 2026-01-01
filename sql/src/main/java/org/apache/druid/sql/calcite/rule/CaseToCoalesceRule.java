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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
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

            // Use x from the 'when' arg, potentially with a cast to the type of 'then', ignoring nullability.
            final RexNode whenIsNotNullArg = ((RexCall) caseArgs.get(i)).getOperands().get(0);
            final RexNode thenArg = caseArgs.get(i + 1);
            final boolean typesMatch = SqlTypeUtil.equalSansNullability(
                rexBuilder.getTypeFactory(),
                whenIsNotNullArg.getType(),
                thenArg.getType()
            );

            if (typesMatch) {
              coalesceArgs.add(whenIsNotNullArg);
            } else {
              coalesceArgs.add(
                  rexBuilder.makeCast(
                      rexBuilder.getTypeFactory()
                                .createTypeWithNullability(thenArg.getType(), whenIsNotNullArg.getType().isNullable()),
                      RexUtil.removeNullabilityCast(rexBuilder.getTypeFactory(), whenIsNotNullArg)
                  )
              );
            }
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
   * Returns whether "when" is like "then IS NOT NULL". Ignores irrelevant casts, as defined by
   * {@link #isIrrelevantCast(RelDataTypeFactory, RexNode, RelDataType)}.
   */
  private static boolean isCoalesceWhenThen(
      final RelDataTypeFactory typeFactory,
      final RexNode when,
      final RexNode then
  )
  {
    if (when.isA(SqlKind.IS_NOT_NULL)) {
      // Remove any casts that don't change the type name. (We don't do anything different during execution based on
      // features of the type other than its name, so they can be safely ignored.)
      final RexNode whenIsNotNullArg =
          removeIrrelevantCasts(typeFactory, ((RexCall) when).getOperands().get(0));
      return whenIsNotNullArg.equals(removeIrrelevantCasts(typeFactory, then));
    } else {
      return false;
    }
  }

  /**
   * Remove any irrelevant casts, as defined by {@link #isIrrelevantCast(RelDataTypeFactory, RexNode, RelDataType)}.
   */
  private static RexNode removeIrrelevantCasts(final RelDataTypeFactory typeFactory, final RexNode rexNode)
  {
    final RelDataType type = rexNode.getType();

    RexNode retVal = rexNode;
    while (isIrrelevantCast(typeFactory, retVal, type)) {
      retVal = ((RexCall) retVal).operands.get(0);
    }
    return retVal;
  }

  /**
   * Returns whether "rexNode" is a {@link SqlKind#CAST} that changes type in a way that is irrelevant to the
   * CASE-to-COALESCE analysis done by {@link #isCoalesceWhenThen}. This means ignorning nullability, and ignoring type
   * changes that don't affect runtime execution behavior.
   */
  private static boolean isIrrelevantCast(
      final RelDataTypeFactory typeFactory,
      final RexNode rexNode,
      final RelDataType castType
  )
  {
    if (!rexNode.isA(SqlKind.CAST)) {
      return false;
    }
    final RexNode argRexNode = ((RexCall) rexNode).getOperands().get(0);
    final SqlTypeName typeName = argRexNode.getType().getSqlTypeName();
    if (SqlTypeName.NUMERIC_TYPES.contains(typeName)
        || SqlTypeName.CHAR_TYPES.contains(typeName)
        || SqlTypeName.BOOLEAN_TYPES.contains(typeName)
        || SqlTypeName.DATETIME_TYPES.contains(typeName)
        || SqlTypeName.INTERVAL_TYPES.contains(typeName)) {
      // For these types, we have no difference in runtime behavior that is affected by anything about the type
      // other than its name.
      return typeName == castType.getSqlTypeName();
    } else {
      return SqlTypeUtil.equalSansNullability(typeFactory, argRexNode.getType(), castType);
    }
  }
}
