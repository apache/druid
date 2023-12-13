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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidJoinQueryRel;
import org.apache.druid.sql.calcite.rel.DruidQueryRel;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

public class DruidJoinRule extends RelOptRule
{

  private final boolean enableLeftScanDirect;
  private final PlannerContext plannerContext;

  private DruidJoinRule(final PlannerContext plannerContext)
  {
    super(
        operand(
            Join.class,
            operand(DruidRel.class, any()),
            operand(DruidRel.class, any())
        )
    );
    this.enableLeftScanDirect = plannerContext.queryContext().getEnableJoinLeftScanDirect();
    this.plannerContext = plannerContext;
  }

  public static DruidJoinRule instance(PlannerContext plannerContext)
  {
    return new DruidJoinRule(plannerContext);
  }
  
  @Override
  public boolean matches(RelOptRuleCall call)
  {
    final Join join = call.rel(0);
    final DruidRel<?> left = call.rel(1);
    final DruidRel<?> right = call.rel(2);

    // 1) Can handle the join condition as a native join.
    // 2) Left has a PartialDruidQuery (i.e., is a real query, not top-level UNION ALL).
    // 3) Right has a PartialDruidQuery (i.e., is a real query, not top-level UNION ALL).
    return canHandleCondition(
        join.getCondition(),
        join.getLeft().getRowType(),
        right,
        join.getJoinType(),
        join.getSystemFieldList(),
        join.getCluster().getRexBuilder()
    ) && left.getPartialDruidQuery() != null && right.getPartialDruidQuery() != null;
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
    final Filter leftFilter;
    final List<RexNode> newProjectExprs = new ArrayList<>();

    // Can't be final, because we're going to reassign it up to a couple of times.
    ConditionAnalysis conditionAnalysis = analyzeCondition(
        join.getCondition(),
        join.getLeft().getRowType(),
        rexBuilder
    );
    final boolean isLeftDirectAccessPossible = enableLeftScanDirect && (left instanceof DruidQueryRel);

    if (!plannerContext.getJoinAlgorithm().requiresSubquery()
        && left.getPartialDruidQuery().stage() == PartialDruidQuery.Stage.SELECT_PROJECT
        && (isLeftDirectAccessPossible || left.getPartialDruidQuery().getWhereFilter() == null)) {
      // Swap the left-side projection above the join, so the left side is a simple scan or mapping. This helps us
      // avoid subqueries.
      final RelNode leftScan = left.getPartialDruidQuery().getScan();
      final Project leftProject = left.getPartialDruidQuery().getSelectProject();
      leftFilter = left.getPartialDruidQuery().getWhereFilter();

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
      leftFilter = null;
    }

    if (!plannerContext.getJoinAlgorithm().requiresSubquery()
        && right.getPartialDruidQuery().stage() == PartialDruidQuery.Stage.SELECT_PROJECT
        && right.getPartialDruidQuery().getWhereFilter() == null
        && !right.getPartialDruidQuery().getSelectProject().isMapping()
        && conditionAnalysis.onlyUsesMappingsFromRightProject(right.getPartialDruidQuery().getSelectProject())) {
      // Swap the right-side projection above the join, so the right side is a simple scan or mapping. This helps us
      // avoid subqueries.
      final RelNode rightScan = right.getPartialDruidQuery().getScan();
      final Project rightProject = right.getPartialDruidQuery().getSelectProject();

      // Right-side projection expressions rewritten to be on top of the join.
      for (final RexNode rexNode : RexUtil.shift(rightProject.getProjects(), newLeft.getRowType().getFieldCount())) {
        if (join.getJoinType().generatesNullsOnRight()) {
          newProjectExprs.add(makeNullableIfLiteral(rexNode, rexBuilder));
        } else {
          newProjectExprs.add(rexNode);
        }
      }

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
            conditionAnalysis.getConditionWithUnsupportedSubConditionsIgnored(rexBuilder),
            newLeft,
            newRight,
            join.getJoinType(),
            join.isSemiJoinDone()
        ),
        leftFilter,
        left.getPlannerContext()
    );

    RelBuilder relBuilder =
        call.builder()
            .push(druidJoin)
            .project(
                RexUtil.fixUp(
                    rexBuilder,
                    newProjectExprs,
                    RelOptUtil.getFieldTypeList(druidJoin.getRowType())
                )
            );

    // Build a post-join filter with whatever join sub-conditions were not supported.
    RexNode postJoinFilter = RexUtil.composeConjunction(rexBuilder, conditionAnalysis.getUnsupportedOnSubConditions(), true);
    if (postJoinFilter != null) {
      relBuilder = relBuilder.filter(postJoinFilter);
    }
    relBuilder.convert(join.getRowType(), false);
    call.transformTo(relBuilder.build());
  }

  private static RexNode makeNullableIfLiteral(final RexNode rexNode, final RexBuilder rexBuilder)
  {
    if (rexNode.isA(SqlKind.LITERAL)) {
      return rexBuilder.makeLiteral(
          RexLiteral.value(rexNode),
          rexBuilder.getTypeFactory().createTypeWithNullability(rexNode.getType(), true),
          true
      );
    } else {
      return rexNode;
    }
  }

  /**
   * Returns whether we can handle the join condition. In case, some conditions in an AND expression are not supported,
   * they are extracted into a post-join filter instead.
   */
  @VisibleForTesting
  public boolean canHandleCondition(
      final RexNode condition,
      final RelDataType leftRowType,
      DruidRel<?> right,
      JoinRelType joinType,
      List<RelDataTypeField> systemFieldList,
      final RexBuilder rexBuilder
  )
  {
    ConditionAnalysis conditionAnalysis = analyzeCondition(condition, leftRowType, rexBuilder);
    // if the right side requires a subquery, then even lookup will be transformed to a QueryDataSource
    // thereby allowing join conditions on both k and v columns of the lookup
    if (right != null
        && !DruidJoinQueryRel.computeRightRequiresSubquery(plannerContext, DruidJoinQueryRel.getSomeDruidChild(right))
        && right instanceof DruidQueryRel) {
      DruidQueryRel druidQueryRel = (DruidQueryRel) right;
      if (druidQueryRel.getDruidTable().getDataSource() instanceof LookupDataSource) {
        long distinctRightColumns = conditionAnalysis.rightColumns.stream().map(RexSlot::getIndex).distinct().count();
        if (distinctRightColumns > 1) {
          // it means that the join's right side is lookup and the join condition contains both key and value columns of lookup.
          // currently, the lookup datasource in the native engine doesn't support using value column in the join condition.
          plannerContext.setPlanningError(
              "SQL is resulting in a join involving lookup where value column is used in the condition.");
          return false;
        }
      }
    }

    if (joinType != JoinRelType.INNER || !systemFieldList.isEmpty() || NullHandling.replaceWithDefault()) {
      // I am not sure in what case, the list of system fields will be not empty. I have just picked up this logic
      // directly from https://github.com/apache/calcite/blob/calcite-1.35.0/core/src/main/java/org/apache/calcite/rel/rules/AbstractJoinExtractFilterRule.java#L58

      // Also to avoid results changes for existing queries in non-null handling mode, we don't handle unsupported
      // conditions. Otherwise, some left/right joins with a condition that doesn't allow nulls on join input will
      // be converted to inner joins. See Test CalciteJoinQueryTest#testFilterAndGroupByLookupUsingJoinOperatorBackwards
      // for an example.
      return conditionAnalysis.getUnsupportedOnSubConditions().isEmpty();
    }
    
    return true;
  }

  public static class ConditionAnalysis
  {
    /**
     * Number of fields on the left-hand side. Useful for identifying if a particular field is from on the left
     * or right side of a join.
     */
    private final int numLeftFields;

    /**
     * Each equality subcondition is an equality of the form f(LeftRel) = g(RightRel).
     */
    private final List<RexEquality> equalitySubConditions;

    /**
     * Each literal subcondition is... a literal.
     */
    private final List<RexLiteral> literalSubConditions;

    /**
     * Sub-conditions in join clause that cannot be handled by the DruidJoinRule.
     */
    private final List<RexNode> unsupportedOnSubConditions;

    private final Set<RexInputRef> rightColumns;

    ConditionAnalysis(
        int numLeftFields,
        List<RexEquality> equalitySubConditions,
        List<RexLiteral> literalSubConditions,
        List<RexNode> unsupportedOnSubConditions,
        Set<RexInputRef> rightColumns
    )
    {
      this.numLeftFields = numLeftFields;
      this.equalitySubConditions = equalitySubConditions;
      this.literalSubConditions = literalSubConditions;
      this.unsupportedOnSubConditions = unsupportedOnSubConditions;
      this.rightColumns = rightColumns;
    }

    public ConditionAnalysis pushThroughLeftProject(final Project leftProject)
    {
      // Pushing through the project will shift right-hand field references by this amount.
      final int rhsShift =
          leftProject.getInput().getRowType().getFieldCount() - leftProject.getRowType().getFieldCount();

      // We leave unsupportedSubConditions un-touched as they are evaluated above join anyway.
      return new ConditionAnalysis(
          leftProject.getInput().getRowType().getFieldCount(),
          equalitySubConditions
              .stream()
              .map(
                  equality -> new RexEquality(
                      RelOptUtil.pushPastProject(equality.left, leftProject),
                      (RexInputRef) RexUtil.shift(equality.right, rhsShift),
                      equality.kind
                  )
              )
              .collect(Collectors.toList()),
          literalSubConditions,
          unsupportedOnSubConditions,
          rightColumns
      );
    }

    public ConditionAnalysis pushThroughRightProject(final Project rightProject)
    {
      Preconditions.checkArgument(onlyUsesMappingsFromRightProject(rightProject), "Cannot push through");

      // We leave unsupportedSubConditions un-touched as they are evaluated above join anyway.
      return new ConditionAnalysis(
          numLeftFields,
          equalitySubConditions
              .stream()
              .map(
                  equality -> new RexEquality(
                      equality.left,
                      (RexInputRef) RexUtil.shift(
                          RelOptUtil.pushPastProject(
                              RexUtil.shift(equality.right, -numLeftFields),
                              rightProject
                          ),
                          numLeftFields
                      ),
                      equality.kind
                  )
              )
              .collect(Collectors.toList()),
          literalSubConditions,
          unsupportedOnSubConditions,
          rightColumns
      );
    }

    public boolean onlyUsesMappingsFromRightProject(final Project rightProject)
    {
      for (final RexEquality equality : equalitySubConditions) {
        final int rightIndex = equality.right.getIndex() - numLeftFields;

        if (!rightProject.getProjects().get(rightIndex).isA(SqlKind.INPUT_REF)) {
          return false;
        }
      }

      return true;
    }

    public RexNode getConditionWithUnsupportedSubConditionsIgnored(final RexBuilder rexBuilder)
    {
      return RexUtil.composeConjunction(
          rexBuilder,
          Iterables.concat(
              literalSubConditions,
              equalitySubConditions
                  .stream()
                  .map(equality -> equality.makeCall(rexBuilder))
                  .collect(Collectors.toList())
          ),
          false
      );
    }

    public List<RexNode> getUnsupportedOnSubConditions()
    {
      return unsupportedOnSubConditions;
    }

    @Override
    public String toString()
    {
      return "ConditionAnalysis{" +
             "numLeftFields=" + numLeftFields +
             ", equalitySubConditions=" + equalitySubConditions +
             ", literalSubConditions=" + literalSubConditions +
             ", unsupportedSubConditions=" + unsupportedOnSubConditions +
             ", rightColumns=" + rightColumns +
             '}';
    }
  }

  /**
   * If this condition is an AND of some combination of
   * (1) literals;
   * (2) equality conditions of the form
   * (3) unsupported conditions
   * <p>
   * Returns empty if the join cannot be supported at all. It can return non-empty with some unsupported conditions
   * that can be extracted into post join filter.
   * {@code f(LeftRel) = RightColumn}, then return a {@link ConditionAnalysis}.
   */
  public ConditionAnalysis analyzeCondition(
      final RexNode condition,
      final RelDataType leftRowType,
      final RexBuilder rexBuilder
  )
  {
    final List<RexNode> subConditions = decomposeAnd(condition);
    final List<RexEquality> equalitySubConditions = new ArrayList<>();
    final List<RexLiteral> literalSubConditions = new ArrayList<>();
    final List<RexNode> unSupportedSubConditions = new ArrayList<>();
    final Set<RexInputRef> rightColumns = new HashSet<>();
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
            // If the types are not the same, add to unsupported conditions.
            unSupportedSubConditions.add(subCondition);
            continue;
          }
        } else {
          // Literals are always OK.
          literalSubConditions.add((RexLiteral) subCondition);
        }
        continue;
      }

      RexNode firstOperand;
      RexNode secondOperand;
      SqlKind comparisonKind;

      if (subCondition.isA(SqlKind.INPUT_REF)) {
        firstOperand = rexBuilder.makeLiteral(true);
        secondOperand = subCondition;
        comparisonKind = SqlKind.EQUALS;

        if (!SqlTypeName.BOOLEAN_TYPES.contains(secondOperand.getType().getSqlTypeName())) {
          plannerContext.setPlanningError(
              "SQL requires a join with '%s' condition where the column is of the type %s, that is not supported",
              subCondition.getKind(),
              secondOperand.getType().getSqlTypeName()
          );
          unSupportedSubConditions.add(subCondition);
          continue;

        }
      } else if (subCondition.isA(SqlKind.EQUALS) || subCondition.isA(SqlKind.IS_NOT_DISTINCT_FROM)) {
        final List<RexNode> operands = ((RexCall) subCondition).getOperands();
        Preconditions.checkState(operands.size() == 2, "Expected 2 operands, got[%s]", operands.size());
        firstOperand = operands.get(0);
        secondOperand = operands.get(1);
        comparisonKind = subCondition.getKind();
      } else {
        // If it's not EQUALS or a BOOLEAN input ref, it's not supported.
        plannerContext.setPlanningError(
            "SQL requires a join with '%s' condition that is not supported.",
            subCondition.getKind()
        );
        unSupportedSubConditions.add(subCondition);
        continue;
      }

      if (isLeftExpression(firstOperand, numLeftFields) && isRightInputRef(secondOperand, numLeftFields)) {
        equalitySubConditions.add(new RexEquality(firstOperand, (RexInputRef) secondOperand, comparisonKind));
        rightColumns.add((RexInputRef) secondOperand);
      } else if (isRightInputRef(firstOperand, numLeftFields)
                 && isLeftExpression(secondOperand, numLeftFields)) {
        equalitySubConditions.add(new RexEquality(secondOperand, (RexInputRef) firstOperand, subCondition.getKind()));
        rightColumns.add((RexInputRef) firstOperand);
      } else {
        // Cannot handle this condition.
        plannerContext.setPlanningError("SQL is resulting in a join that has unsupported operand types.");
        unSupportedSubConditions.add(subCondition);
      }
    }

    return new ConditionAnalysis(
        numLeftFields,
        equalitySubConditions,
        literalSubConditions,
        unSupportedSubConditions,
        rightColumns
    );
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


  /**
   * Like {@link org.apache.druid.segment.join.Equality} but uses {@link RexNode} instead of
   * {@link org.apache.druid.math.expr.Expr}.
   */
  static class RexEquality
  {
    private final RexNode left;
    private final RexInputRef right;
    private final SqlKind kind;

    public RexEquality(RexNode left, RexInputRef right, SqlKind kind)
    {
      this.left = left;
      this.right = right;
      this.kind = kind;
    }

    public RexNode makeCall(final RexBuilder builder)
    {
      final SqlOperator operator;

      if (kind == SqlKind.EQUALS) {
        operator = SqlStdOperatorTable.EQUALS;
      } else if (kind == SqlKind.IS_NOT_DISTINCT_FROM) {
        operator = SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;
      } else {
        throw DruidException.defensive("Unexpected operator kind[%s]", kind);
      }

      return builder.makeCall(operator, left, right);
    }

    @Override
    public String toString()
    {
      return "RexEquality{" +
             "left=" + left +
             ", right=" + right +
             ", kind=" + kind +
             '}';
    }
  }
}
