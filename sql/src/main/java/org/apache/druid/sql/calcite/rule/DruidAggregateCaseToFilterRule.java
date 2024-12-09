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

/**
 * Druid extension of {@link AggregateCaseToFilterRule}.
 *
 * Turning on extendedFilteredSumRewrite enables rewrites of:
 * <pre>
 *    SUM(CASE WHEN COND THEN COL1 ELSE 0 END)
 * </pre>
 * to:
 * <pre>
 *    SUM(COL1) FILTER (WHERE COND)
 * </pre>
 * <p>
 * This rewrite improves performance but introduces a known inconsistency when
 * the condition never matches, as the expected result (0) is replaced with `null`.
 * <p>
 * Example behavior:
 * <pre>
 * +-----------------+--------------+----------+------+--------------+
 * | input row count | cond matches | valueCol | orig | filtered-SUM |
 * +-----------------+--------------+----------+------+--------------+
 * | 0               | *            | *        | null | null         |
 * | >0              | none         | *        | 0    | null         |
 * | >0              | all          | null     | null | null         |
 * | >0              | N>0          | 1        | N    | N            |
 * +-----------------+--------------+----------+------+--------------+
 * </pre>
 */
public class DruidAggregateCaseToFilterRule extends RelOptRule implements SubstitutionRule
{
  private boolean extendedFilteredSumRewrite;

  public DruidAggregateCaseToFilterRule(boolean extendedFilteredSumRewrite)
  {
    super(operand(Aggregate.class, operand(Project.class, any())));
    this.extendedFilteredSumRewrite = extendedFilteredSumRewrite;
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

    for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
      AggregateCall newCall = transform(aggregateCall, project, newProjects);

      if (newCall == null) {
        newCalls.add(aggregateCall);
      } else {
        newCalls.add(newCall);
      }
    }

    if (newCalls.equals(aggregate.getAggCallList())) {
      return;
    }

    final RelBuilder relBuilder = call.builder()
        .push(project.getInput())
        .project(newProjects);

    final RelBuilder.GroupKey groupKey = relBuilder.groupKey(aggregate.getGroupSet(), aggregate.getGroupSets());

    relBuilder.aggregate(groupKey, newCalls)
        .convert(aggregate.getRowType(), false);

    call.transformTo(relBuilder.build());
    call.getPlanner().prune(aggregate);
  }

  private @Nullable AggregateCall transform(AggregateCall call,
      Project project, List<RexNode> newProjects)
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

    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    final SqlKind kind = call.getAggregation().getKind();
    if (call.isDistinct()) {
      // Just one style supported:
      //   COUNT(DISTINCT CASE WHEN x = 'foo' THEN y END)
      // =>
      //   COUNT(DISTINCT y) FILTER(WHERE x = 'foo')

      if (kind == SqlKind.COUNT
          && RexLiteral.isNullLiteral(arg2)) {
        newProjects.add(arg1);
        newProjects.add(filter);
        return AggregateCall.create(
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
        );
      }
      return null;
    }

    // Four styles supported:
    //
    // A1: AGG(CASE WHEN x = 'foo' THEN expr END)
    //   => AGG(expr) FILTER (x = 'foo')
    // A2: SUM0(CASE WHEN x = 'foo' THEN cnt ELSE 0 END)
    //   => SUM0(cnt) FILTER (x = 'foo')
    // B: SUM0(CASE WHEN x = 'foo' THEN 1 ELSE 0 END)
    //   => COUNT() FILTER (x = 'foo')
    // C: COUNT(CASE WHEN x = 'foo' THEN 'dummy' END)
    //   => COUNT() FILTER (x = 'foo')

    if (kind == SqlKind.COUNT // Case C
        && arg1.isA(SqlKind.LITERAL)
        && !RexLiteral.isNullLiteral(arg1)
        && RexLiteral.isNullLiteral(arg2)) {
      newProjects.add(filter);
      return AggregateCall.create(
          SqlStdOperatorTable.COUNT,
          false,
          false,
          false,
          call.rexList, ImmutableList.of(), newProjects.size() - 1, null,
          RelCollations.EMPTY, call.getType(),
          call.getName());
    } else if (kind == SqlKind.SUM0 // Case B
        && isIntLiteral(arg1, BigDecimal.ONE)
        && isIntLiteral(arg2, BigDecimal.ZERO)) {

      newProjects.add(filter);
      final RelDataType dataType = typeFactory
          .createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), false);
      return AggregateCall.create(
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
      );
    } else if ((RexLiteral.isNullLiteral(arg2) // Case A1
            && call.getAggregation().allowsFilter())
        || (kind == SqlKind.SUM0 // Case A2
            && isIntLiteral(arg2, BigDecimal.ZERO))) {
      newProjects.add(arg1);
      newProjects.add(filter);
      return AggregateCall.create(
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
      );
    }

    // Rewrites
    // D1: SUM(CASE WHEN x = 'foo' THEN cnt ELSE 0 END)
    //   => SUM0(cnt) FILTER (x = 'foo')
    // D2: SUM(CASE WHEN x = 'foo' THEN 1 ELSE 0 END)
    //   => COUNT() FILTER (x = 'foo')
    //
    // https://issues.apache.org/jira/browse/CALCITE-5953
    // have restricted this rewrite as in case there are no rows it may not be equvivalent;
    // however it may have some performance impact in Druid
    if (extendedFilteredSumRewrite &&
        kind == SqlKind.SUM && isIntLiteral(arg2, BigDecimal.ZERO)) {
      if (isIntLiteral(arg1, BigDecimal.ONE)) { // D2
        newProjects.add(filter);
        final RelDataType dataType = typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.BIGINT), false
        );
        return AggregateCall.create(
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
        );

      } else { // D1
        newProjects.add(arg1);
        newProjects.add(filter);

        RelDataType newType = typeFactory.createTypeWithNullability(call.getType(), true);
        return AggregateCall.create(
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
        );
      }
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

  private static boolean isThreeArgCase(final RexNode rexNode)
  {
    return rexNode.getKind() == SqlKind.CASE
        && ((RexCall) rexNode).operands.size() == 3;
  }

  private static boolean isIntLiteral(RexNode rexNode, BigDecimal value)
  {
    return rexNode instanceof RexLiteral
        && SqlTypeName.INT_TYPES.contains(rexNode.getType().getSqlTypeName())
        && value.equals(((RexLiteral) rexNode).getValueAs(BigDecimal.class));
  }
}
