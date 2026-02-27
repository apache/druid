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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.AggregateCaseToFilterRule;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Druid extension of {@link AggregateCaseToFilterRule}.
 *
 * Five rewrite styles are supported:
 * <pre>
 * A:  AGG(CASE WHEN x = 'foo' THEN expr END)
 *   => AGG(expr) FILTER (x = 'foo')
 * B:  SUM0(CASE WHEN x = 'foo' THEN 1 ELSE 0 END)
 *   => COUNT() FILTER (x = 'foo')
 * C:  COUNT(CASE WHEN x = 'foo' THEN 'dummy' END)
 *   => COUNT() FILTER (x = 'foo')
 * D:  SUM/SUM0(CASE WHEN x = 'foo' THEN expr ELSE 0 END)
 *   => COALESCE(SUM/SUM0(expr) FILTER (x = 'foo'), CASE WHEN COUNT(*) > 0 THEN 0 END)
 * E:  COUNT(DISTINCT CASE WHEN x = 'foo' THEN y END)
 *   => COUNT(DISTINCT y) FILTER (x = 'foo')
 * </pre>
 *
 * Case D rewrites are wrapped in COALESCE to ensure correct results when the condition does not match.
 * Per SQL standard, the aggregation must return NULL when the underlying relation is empty, and must
 * return 0 when the underlying relation is nonempty and the CASE WHEN does not match any rows.
 */
public class DruidAggregateCaseToFilterRule extends RelOptRule implements SubstitutionRule
{
  public DruidAggregateCaseToFilterRule()
  {
    super(operand(Aggregate.class, operand(Project.class, any())));
  }

  @Override
  public boolean matches(final RelOptRuleCall call)
  {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);

    for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
      final int singleArg = soleArgument(aggregateCall);
      if (singleArg >= 0
          && isThreeArgCase(project.getProjects().get(singleArg))) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);
    final List<AggregateCall> newCalls = new ArrayList<>(aggregate.getAggCallList().size());
    final List<RexNode> newProjects = new ArrayList<>(project.getProjects());
    final List<RexNode> coalesceValues = new ArrayList<>();

    for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
      final TransformResult result = transform(aggregateCall, project, newProjects);

      if (result == null) {
        newCalls.add(aggregateCall);
        coalesceValues.add(null);
      } else {
        newCalls.add(result.newCall);
        coalesceValues.add(result.coalesceValue);
      }
    }

    if (newCalls.equals(aggregate.getAggCallList())) {
      return;
    }

    final RelBuilder relBuilder = call.builder()
        .push(project.getInput())
        .project(newProjects);

    final RelBuilder.GroupKey groupKey = relBuilder.groupKey(aggregate.getGroupSet(), aggregate.getGroupSets());

    // If any D rewrites occurred, add a COUNT(*) to differentiate empty
    // vs non-empty input, so the wrapping COALESCE can return NULL for empty.
    final boolean needsCoalesce = coalesceValues.stream().anyMatch(Objects::nonNull);
    if (needsCoalesce) {
      newCalls.add(makeCountStarCall(aggregate.getCluster().getRexBuilder().getTypeFactory()));
    }

    relBuilder.aggregate(groupKey, newCalls);

    // If any D rewrites occurred, add a wrapping Project that applies
    // COALESCE(ref, CASE WHEN COUNT(*) > 0 THEN 0 END) for D columns
    // and passes through others.
    if (needsCoalesce) {
      addCoalesceProject(relBuilder, aggregate, coalesceValues);
    }

    relBuilder.convert(aggregate.getRowType(), false);

    call.transformTo(relBuilder.build());
    call.getPlanner().prune(aggregate);
  }

  /**
   * Creates a COUNT(*) aggregate call, used to detect whether the underlying
   * relation is empty.
   */
  private static AggregateCall makeCountStarCall(final RelDataTypeFactory typeFactory)
  {
    final RelDataType bigintType =
        typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), false);
    return AggregateCall.create(
        SqlStdOperatorTable.COUNT,
        false,
        false,
        false,
        ImmutableList.of(),
        ImmutableList.of(),
        -1,
        null,
        RelCollations.EMPTY,
        bigintType,
        null
    );
  }

  /**
   * Adds a Project on top of the current RelBuilder that wraps "Case D"
   * columns in {@code COALESCE(ref, CASE WHEN COUNT(*) > 0 THEN 0 END)} and
   * passes through all other columns. The COUNT(*) column (the last aggregate)
   * is excluded from the output.
   */
  private static void addCoalesceProject(
      final RelBuilder relBuilder,
      final Aggregate aggregate,
      final List<RexNode> coalesceValues
  )
  {
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final int groupCount = aggregate.getGroupCount();
    final int aggCallCount = coalesceValues.size();
    final RexNode countStarRef = rexBuilder.makeInputRef(
        relBuilder.peek(),
        groupCount + aggCallCount
    );
    final RexNode countGtZero = rexBuilder.makeCall(
        SqlStdOperatorTable.GREATER_THAN,
        countStarRef,
        rexBuilder.makeBigintLiteral(BigDecimal.ZERO)
    );
    final List<RexNode> projections = new ArrayList<>();

    // Pass through group-by columns
    for (int i = 0; i < groupCount; i++) {
      projections.add(rexBuilder.makeInputRef(relBuilder.peek(), i));
    }

    // Wrap aggregate columns
    for (int i = 0; i < aggCallCount; i++) {
      final RexNode ref = rexBuilder.makeInputRef(relBuilder.peek(), groupCount + i);
      final RexNode coalesceValue = coalesceValues.get(i);
      if (coalesceValue != null) {
        // COALESCE(ref, CASE WHEN COUNT(*) > 0 THEN 0 END)
        final RexNode caseWhen = rexBuilder.makeCall(
            SqlStdOperatorTable.CASE,
            countGtZero,
            rexBuilder.makeCast(ref.getType(), coalesceValue),
            rexBuilder.makeNullLiteral(ref.getType())
        );
        projections.add(
            rexBuilder.makeCall(
                SqlStdOperatorTable.COALESCE,
                ref,
                caseWhen
            )
        );
      } else {
        projections.add(ref);
      }
    }
    // Don't include COUNT(*) in output

    relBuilder.project(projections);
  }

  /**
   * Attempts to rewrite a single {@link AggregateCall} whose argument is a
   * three-operand CASE expression into a filtered aggregation. Returns null
   * if the call cannot be rewritten. When a rewrite is possible, the
   * replacement projects are appended to {@code newProjects} and a
   * {@link TransformResult} is returned containing the new call and an
   * optional COALESCE value for case D rewrites.
   */
  @Nullable
  private TransformResult transform(
      AggregateCall call,
      Project project,
      List<RexNode> newProjects
  )
  {
    final int singleArg = soleArgument(call);
    if (singleArg < 0) {
      return null;
    }

    final RexNode rexNode = project.getProjects().get(singleArg);
    if (!isThreeArgCase(rexNode)) {
      return null;
    }

    final RelOptCluster cluster = project.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RexCall caseCall = (RexCall) rexNode;

    // If one arg is null and the other is not, reverse them and set "flip",
    // which negates the filter.
    final boolean flip = RexLiteral.isNullLiteral(caseCall.operands.get(1))
        && !RexLiteral.isNullLiteral(caseCall.operands.get(2));
    final RexNode arg1 = caseCall.operands.get(flip ? 2 : 1);
    final RexNode arg2 = caseCall.operands.get(flip ? 1 : 2);

    // Operand 1: Filter
    final SqlPostfixOperator op = flip ? SqlStdOperatorTable.IS_NOT_TRUE : SqlStdOperatorTable.IS_TRUE;
    final RexNode filterFromCase = rexBuilder.makeCall(op, caseCall.operands.get(0));

    // Combine the CASE filter with an honest-to-goodness SQL FILTER, if the
    // latter is present.
    final RexNode filter;
    if (call.filterArg >= 0) {
      filter = rexBuilder.makeCall(
          SqlStdOperatorTable.AND,
          project.getProjects().get(call.filterArg),
          filterFromCase
      );
    } else {
      filter = filterFromCase;
    }

    final RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    final SqlKind kind = call.getAggregation().getKind();
    if (call.isDistinct()) {
      // Just one style supported:
      //   COUNT(DISTINCT CASE WHEN x = 'foo' THEN y END)
      // =>
      //   COUNT(DISTINCT y) FILTER(WHERE x = 'foo')

      if (kind == SqlKind.COUNT
          && RexLiteral.isNullLiteral(arg2)) { // Case E
        newProjects.add(arg1);
        newProjects.add(filter);
        return new TransformResult(
            AggregateCall.create(
                SqlStdOperatorTable.COUNT,
                true,
                false,
                false,
                call.rexList,
                ImmutableList.of(newProjects.size() - 2),
                newProjects.size() - 1,
                null,
                RelCollations.EMPTY,
                call.getType(),
                call.getName()
            ),
            null
        );
      }
      return null;
    }

    if (kind == SqlKind.COUNT // Case C
        && arg1.isA(SqlKind.LITERAL)
        && !RexLiteral.isNullLiteral(arg1)
        && RexLiteral.isNullLiteral(arg2)) {
      newProjects.add(filter);
      return new TransformResult(
          AggregateCall.create(
              SqlStdOperatorTable.COUNT,
              false,
              false,
              false,
              call.rexList, ImmutableList.of(), newProjects.size() - 1, null,
              RelCollations.EMPTY, call.getType(),
              call.getName()),
          null
      );
    } else if (kind == SqlKind.SUM0 // Case B
        && isIntLiteral(arg1, BigDecimal.ONE)
        && isIntLiteral(arg2, BigDecimal.ZERO)) {

      newProjects.add(filter);
      final RelDataType dataType = typeFactory
          .createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), false);
      return new TransformResult(
          AggregateCall.create(
              SqlStdOperatorTable.COUNT,
              false,
              false,
              false,
              call.rexList,
              ImmutableList.of(),
              newProjects.size() - 1,
              null,
              RelCollations.EMPTY,
              dataType,
              call.getName()
          ),
          null
      );
    } else if (RexLiteral.isNullLiteral(arg2)
        && call.getAggregation().allowsFilter()) { // Case A
      newProjects.add(arg1);
      newProjects.add(filter);
      return new TransformResult(
          AggregateCall.create(
              call.getAggregation(),
              false,
              false,
              false,
              call.rexList,
              ImmutableList.of(newProjects.size() - 2),
              newProjects.size() - 1,
              null,
              RelCollations.EMPTY,
              call.getType(),
              call.getName()
          ),
          null
      );
    } else if ((kind == SqlKind.SUM || kind == SqlKind.SUM0)
        && isIntLiteral(arg2, BigDecimal.ZERO)) { // Case D
      newProjects.add(arg1);
      newProjects.add(filter);

      final RelDataType newType = typeFactory.createTypeWithNullability(call.getType(), true);
      return new TransformResult(
          AggregateCall.create(
              call.getAggregation(),
              false,
              false,
              false,
              call.rexList,
              ImmutableList.of(newProjects.size() - 2),
              newProjects.size() - 1,
              null,
              RelCollations.EMPTY,
              newType,
              call.getName()
          ),
          arg2
      );
    }

    return null;
  }

  /**
   * Returns the argument, if an aggregate call has a single argument, otherwise
   * -1.
   */
  private static int soleArgument(AggregateCall aggregateCall)
  {
    return aggregateCall.getArgList().size() == 1
        ? aggregateCall.getArgList().get(0)
        : -1;
  }

  /**
   * Returns whether a {@link RexNode} is a CASE expression with exactly three
   * operands (condition, then-value, else-value).
   */
  private static boolean isThreeArgCase(final RexNode rexNode)
  {
    return rexNode.getKind() == SqlKind.CASE
        && ((RexCall) rexNode).operands.size() == 3;
  }

  /**
   * Returns whether a {@link RexNode} is an integer literal with the given
   * value.
   */
  private static boolean isIntLiteral(RexNode rexNode, BigDecimal value)
  {
    return rexNode instanceof RexLiteral
        && SqlTypeName.INT_TYPES.contains(rexNode.getType().getSqlTypeName())
        && value.equals(((RexLiteral) rexNode).getValueAs(BigDecimal.class));
  }

  private static class TransformResult
  {
    final AggregateCall newCall;
    @Nullable
    final RexNode coalesceValue;

    TransformResult(final AggregateCall newCall, @Nullable final RexNode coalesceValue)
    {
      this.newCall = newCall;
      this.coalesceValue = coalesceValue;
    }
  }
}
