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
import com.google.common.collect.Iterables;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.sql.calcite.rel.DruidJoinQueryRel;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Stack;
import java.util.stream.Collectors;

public class DruidJoinRule extends RelOptRule
{
  private static final DruidJoinRule INSTANCE = new DruidJoinRule();

  private DruidJoinRule()
  {
    super(
        operand(
            Join.class,
            operand(DruidRel.class, any()),
            operand(DruidRel.class, any())
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
    return canHandleCondition(join.getCondition(), join.getLeft().getRowType());
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Join join = call.rel(0);
    final DruidRel<?> left = call.rel(1);
    final DruidRel<?> right = call.rel(2);

    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();

    final DruidRel<?> newLeft;
    final DruidRel<?> newRight;
    final List<RexNode> newProjectExprs = new ArrayList<>();

    // Already verified to be present in "matches", so just call "get".
    // Can't be final, because we're going to reassign it up to a couple of times.
    ConditionAnalysis conditionAnalysis = analyzeCondition(join.getCondition(), join.getLeft().getRowType()).get();

    if (left.getPartialDruidQuery().stage() == PartialDruidQuery.Stage.SELECT_PROJECT
        && left.getPartialDruidQuery().getWhereFilter() == null) {
      // Swap the left-side projection above the join, so the left side is a simple scan or mapping. This helps us
      // avoid subqueries.
      final RelNode leftScan = left.getPartialDruidQuery().getScan();
      final Project leftProject = left.getPartialDruidQuery().getSelectProject();

      // Left-side projection expressions rewritten to be on top of the join.
      newProjectExprs.addAll(leftProject.getProjects());
      newLeft = left.withPartialQuery(PartialDruidQuery.create(leftScan));
      conditionAnalysis = conditionAnalysis.pushThroughLeftProject(leftProject);
    } else {
      // Leave left as-is. Write input refs that do nothing.
      for (int i = 0; i < left.getRowType().getFieldCount(); i++) {
        newProjectExprs.add(rexBuilder.makeInputRef(join.getRowType().getFieldList().get(i).getType(), i));
      }

      newLeft = left;
    }

    if (right.getPartialDruidQuery().stage() == PartialDruidQuery.Stage.SELECT_PROJECT
        && right.getPartialDruidQuery().getWhereFilter() == null
        && !right.getPartialDruidQuery().getSelectProject().isMapping()
        && conditionAnalysis.onlyUsesMappingsFromRightProject(right.getPartialDruidQuery().getSelectProject())) {
      // Swap the right-side projection above the join, so the right side is a simple scan or mapping. This helps us
      // avoid subqueries.
      final RelNode rightScan = right.getPartialDruidQuery().getScan();
      final Project rightProject = right.getPartialDruidQuery().getSelectProject();

      // Right-side projection expressions rewritten to be on top of the join.
      Iterables.addAll(
          newProjectExprs,
          RexUtil.shift(rightProject.getProjects(), newLeft.getRowType().getFieldCount())
      );
      newRight = right.withPartialQuery(PartialDruidQuery.create(rightScan));
      conditionAnalysis = conditionAnalysis.pushThroughRightProject(rightProject);
    } else {
      // Leave right as-is. Write input refs that do nothing.
      for (int i = 0; i < right.getRowType().getFieldCount(); i++) {
        newProjectExprs.add(
            rexBuilder.makeInputRef(
                join.getRowType().getFieldList().get(left.getRowType().getFieldCount() + i).getType(),
                newLeft.getRowType().getFieldCount() + i
            )
        );
      }

      newRight = right;
    }

    // Druid join written on top of the new left and right sides.
    final DruidJoinQueryRel druidJoin = DruidJoinQueryRel.create(
        join.copy(
            join.getTraitSet(),
            conditionAnalysis.getCondition(rexBuilder),
            newLeft,
            newRight,
            join.getJoinType(),
            join.isSemiJoinDone()
        ),
        left.getQueryMaker()
    );

    final RelBuilder relBuilder =
        call.builder()
            .push(druidJoin)
            .project(
                RexUtil.fixUp(
                    rexBuilder,
                    newProjectExprs,
                    RelOptUtil.getFieldTypeList(druidJoin.getRowType())
                )
            );

    call.transformTo(relBuilder.build());
  }

  /**
   * Returns whether {@link #analyzeCondition} would return something.
   */
  @VisibleForTesting
  static boolean canHandleCondition(final RexNode condition, final RelDataType leftRowType)
  {
    return analyzeCondition(condition, leftRowType).isPresent();
  }

  /**
   * If this condition is an AND of some combination of (1) literals; (2) equality conditions of the form
   * {@code f(LeftRel) = RightColumn}, then return a {@link ConditionAnalysis}.
   */
  private static Optional<ConditionAnalysis> analyzeCondition(final RexNode condition, final RelDataType leftRowType)
  {
    final List<RexNode> subConditions = decomposeAnd(condition);
    final List<Pair<RexNode, RexInputRef>> equalitySubConditions = new ArrayList<>();
    final List<RexLiteral> literalSubConditions = new ArrayList<>();
    final int numLeftFields = leftRowType.getFieldCount();

    for (RexNode subCondition : subConditions) {
      if (RexUtil.isLiteral(subCondition, true)) {
        if (subCondition.isA(SqlKind.CAST)) {
          // This is CAST(literal) which is always OK.
          // We know that this is CAST(literal) as it passed the check from RexUtil.isLiteral
          RexCall call = (RexCall) subCondition;
          // We have to verify the types of the cast here, because if the underlying literal and the cast output type
          // are different, then skipping the cast might change the meaning of the subcondition.
          if (call.getType().getSqlTypeName().equals(call.getOperands().get(0).getType().getSqlTypeName())) {
            // If the types are the same, unwrap the cast and use the underlying literal.
            literalSubConditions.add((RexLiteral) call.getOperands().get(0));
          } else {
            // If the types are not the same, return Optional.empty() indicating the condition is not supported.
            return Optional.empty();
          }
        } else {
          // Literals are always OK.
          literalSubConditions.add((RexLiteral) subCondition);
        }
        continue;
      }

      if (!subCondition.isA(SqlKind.EQUALS)) {
        // If it's not EQUALS, it's not supported.
        return Optional.empty();
      }

      final List<RexNode> operands = ((RexCall) subCondition).getOperands();
      Preconditions.checkState(operands.size() == 2, "Expected 2 operands, got[%,d]", operands.size());

      if (isLeftExpression(operands.get(0), numLeftFields) && isRightInputRef(operands.get(1), numLeftFields)) {
        equalitySubConditions.add(Pair.of(operands.get(0), (RexInputRef) operands.get(1)));
      } else if (isRightInputRef(operands.get(0), numLeftFields)
                 && isLeftExpression(operands.get(1), numLeftFields)) {
        equalitySubConditions.add(Pair.of(operands.get(1), (RexInputRef) operands.get(0)));
      } else {
        // Cannot handle this condition.
        return Optional.empty();
      }
    }

    return Optional.of(new ConditionAnalysis(numLeftFields, equalitySubConditions, literalSubConditions));
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

  private static boolean isLeftExpression(final RexNode rexNode, final int numLeftFields)
  {
    return ImmutableBitSet.range(numLeftFields).contains(RelOptUtil.InputFinder.bits(rexNode));
  }

  private static boolean isRightInputRef(final RexNode rexNode, final int numLeftFields)
  {
    return rexNode.isA(SqlKind.INPUT_REF) && ((RexInputRef) rexNode).getIndex() >= numLeftFields;
  }

  @VisibleForTesting
  static class ConditionAnalysis
  {
    /**
     * Number of fields on the left-hand side. Useful for identifying if a particular field is from on the left
     * or right side of a join.
     */
    private final int numLeftFields;

    /**
     * Each equality subcondition is an equality of the form f(LeftRel) = g(RightRel).
     */
    private final List<Pair<RexNode, RexInputRef>> equalitySubConditions;

    /**
     * Each literal subcondition is... a literal.
     */
    private final List<RexLiteral> literalSubConditions;

    ConditionAnalysis(
        int numLeftFields,
        List<Pair<RexNode, RexInputRef>> equalitySubConditions,
        List<RexLiteral> literalSubConditions
    )
    {
      this.numLeftFields = numLeftFields;
      this.equalitySubConditions = equalitySubConditions;
      this.literalSubConditions = literalSubConditions;
    }

    public ConditionAnalysis pushThroughLeftProject(final Project leftProject)
    {
      // Pushing through the project will shift right-hand field references by this amount.
      final int rhsShift =
          leftProject.getInput().getRowType().getFieldCount() - leftProject.getRowType().getFieldCount();

      return new ConditionAnalysis(
          leftProject.getInput().getRowType().getFieldCount(),
          equalitySubConditions
              .stream()
              .map(
                  equality -> Pair.of(
                      RelOptUtil.pushPastProject(equality.lhs, leftProject),
                      (RexInputRef) RexUtil.shift(equality.rhs, rhsShift)
                  )
              )
              .collect(Collectors.toList()),
          literalSubConditions
      );
    }

    public ConditionAnalysis pushThroughRightProject(final Project rightProject)
    {
      Preconditions.checkArgument(onlyUsesMappingsFromRightProject(rightProject), "Cannot push through");

      return new ConditionAnalysis(
          numLeftFields,
          equalitySubConditions
              .stream()
              .map(
                  equality -> Pair.of(
                      equality.lhs,
                      (RexInputRef) RexUtil.shift(
                          RelOptUtil.pushPastProject(
                              RexUtil.shift(equality.rhs, -numLeftFields),
                              rightProject
                          ),
                          numLeftFields
                      )
                  )
              )
              .collect(Collectors.toList()),
          literalSubConditions
      );
    }

    public boolean onlyUsesMappingsFromRightProject(final Project rightProject)
    {
      for (Pair<RexNode, RexInputRef> equality : equalitySubConditions) {
        final int rightIndex = equality.rhs.getIndex() - numLeftFields;

        if (!rightProject.getProjects().get(rightIndex).isA(SqlKind.INPUT_REF)) {
          return false;
        }
      }

      return true;
    }

    public RexNode getCondition(final RexBuilder rexBuilder)
    {
      return RexUtil.composeConjunction(
          rexBuilder,
          Iterables.concat(
              literalSubConditions,
              equalitySubConditions
                  .stream()
                  .map(equality -> rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, equality.lhs, equality.rhs))
                  .collect(Collectors.toList())
          ),
          false
      );
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ConditionAnalysis that = (ConditionAnalysis) o;
      return Objects.equals(equalitySubConditions, that.equalitySubConditions) &&
             Objects.equals(literalSubConditions, that.literalSubConditions);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(equalitySubConditions, literalSubConditions);
    }

    @Override
    public String toString()
    {
      return "ConditionAnalysis{" +
             "equalitySubConditions=" + equalitySubConditions +
             ", literalSubConditions=" + literalSubConditions +
             '}';
    }
  }
}
