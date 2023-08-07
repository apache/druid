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

package org.apache.druid.sql.calcite.rule.logical;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
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
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A copy of {@link org.apache.calcite.rel.rules.AggregateCaseToFilterRule} except that it fixes a bug to eliminate
 * left-over projects for converted aggregates to filter-aggregates. The elimination of left-over projects is necessary
 * with the new planning since it determines the cost of the plan and hence determines which plan is going to get picked
 * as the cheapest one.
 * This fix will also be contributed upstream to Calcite project, and we can remove this rule once the fix is a part of
 * the Calcite version we use.
 */
public class DruidAggregateCaseToFilterRule extends RelOptRule
{
  public static final DruidAggregateCaseToFilterRule INSTANCE =
      new DruidAggregateCaseToFilterRule(RelFactories.LOGICAL_BUILDER, null);

  /**
   * Creates an AggregateCaseToFilterRule.
   */
  protected DruidAggregateCaseToFilterRule(
      RelBuilderFactory relBuilderFactory,
      String description
  )
  {
    super(operand(Aggregate.class, operand(Project.class, any())),
          relBuilderFactory, description
    );
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
    final List<AggregateCall> newCalls =
        new ArrayList<>(aggregate.getAggCallList().size());
    List<RexNode> newProjects;

    // TODO : fix grouping columns
    Set<Integer> groupUsedFields = new HashSet<>();
    for (int fieldNumber : aggregate.getGroupSet()) {
      groupUsedFields.add(fieldNumber);
    }

    List<RexNode> updatedProjects = new ArrayList<>();
    for (int i = 0; i < project.getProjects().size(); i++) {
      if (groupUsedFields.contains(i)) {
        updatedProjects.add(project.getProjects().get(i));
      }
    }
    newProjects = updatedProjects;

    for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
      AggregateCall newCall =
          transform(aggregateCall, project, newProjects);

      // Possibly CAST the new aggregator to an appropriate type.
      newCalls.add(newCall);
    }
    final RelBuilder relBuilder = call.builder()
                                      .push(project.getInput())
                                      .project(newProjects);

    final RelBuilder.GroupKey groupKey =
        relBuilder.groupKey(
            aggregate.getGroupSet(),
            aggregate.getGroupSets()
        );

    relBuilder.aggregate(groupKey, newCalls)
              .convert(aggregate.getRowType(), false);

    call.transformTo(relBuilder.build());
    call.getPlanner().prune(aggregate);
  }

  private AggregateCall transform(AggregateCall aggregateCall, Project project, List<RexNode> newProjects)
  {
    final int singleArg = soleArgument(aggregateCall);
    if (singleArg < 0) {
      Set<Integer> newFields = new HashSet<>();
      for (int fieldNumber : aggregateCall.getArgList()) {
        newProjects.add(project.getProjects().get(fieldNumber));
        newFields.add(newProjects.size() - 1);
      }
      int newFilterArg = -1;
      if (aggregateCall.hasFilter()) {
        newProjects.add(project.getProjects().get(aggregateCall.filterArg));
        newFilterArg = newProjects.size() - 1;
      }
      return AggregateCall.create(aggregateCall.getAggregation(),
                                  aggregateCall.isDistinct(),
                                  aggregateCall.isApproximate(),
                                  aggregateCall.ignoreNulls(),
                                  new ArrayList<>(newFields),
                                  newFilterArg,
                                  aggregateCall.getCollation(),
                                  aggregateCall.getType(),
                                  aggregateCall.getName()
      );
    }

    final RexNode rexNode = project.getProjects().get(singleArg);
    if (!isThreeArgCase(rexNode)) {
      newProjects.add(rexNode);
      int callArg = newProjects.size() - 1;
      int newFilterArg = -1;
      if (aggregateCall.hasFilter()) {
        newProjects.add(project.getProjects().get(aggregateCall.filterArg));
        newFilterArg = newProjects.size() - 1;
      }
      return AggregateCall.create(aggregateCall.getAggregation(),
                                  aggregateCall.isDistinct(),
                                  aggregateCall.isApproximate(),
                                  aggregateCall.ignoreNulls(),
                                  ImmutableList.of(callArg),
                                  newFilterArg,
                                  aggregateCall.getCollation(),
                                  aggregateCall.getType(),
                                  aggregateCall.getName()
      );
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
    final SqlPostfixOperator op =
        flip ? SqlStdOperatorTable.IS_FALSE : SqlStdOperatorTable.IS_TRUE;
    final RexNode filterFromCase =
        rexBuilder.makeCall(op, caseCall.operands.get(0));

    // Combine the CASE filter with an honest-to-goodness SQL FILTER, if the
    // latter is present.
    final RexNode filter;
    if (aggregateCall.filterArg >= 0) {
      filter = rexBuilder.makeCall(SqlStdOperatorTable.AND,
                                   project.getProjects().get(aggregateCall.filterArg), filterFromCase
      );
    } else {
      filter = filterFromCase;
    }

    final SqlKind kind = aggregateCall.getAggregation().getKind();
    if (aggregateCall.isDistinct()) {
      // Just one style supported:
      //   COUNT(DISTINCT CASE WHEN x = 'foo' THEN y END)
      // =>
      //   COUNT(DISTINCT y) FILTER(WHERE x = 'foo')

      if (kind == SqlKind.COUNT
          && RexLiteral.isNullLiteral(arg2)) {
        newProjects.add(arg1);
        newProjects.add(filter);
        return AggregateCall.create(SqlStdOperatorTable.COUNT, true, false,
                                    false, ImmutableList.of(newProjects.size() - 2),
                                    newProjects.size() - 1, RelCollations.EMPTY,
                                    aggregateCall.getType(), aggregateCall.getName()
        );
      }
      newProjects.add(rexNode);
      int callArg = newProjects.size() - 1;
      int newFilterArg = -1;
      if (aggregateCall.hasFilter()) {
        newProjects.add(project.getProjects().get(aggregateCall.filterArg));
        newFilterArg = newProjects.size() - 1;
      }
      return AggregateCall.create(aggregateCall.getAggregation(),
                                  aggregateCall.isDistinct(),
                                  aggregateCall.isApproximate(),
                                  aggregateCall.ignoreNulls(),
                                  ImmutableList.of(callArg),
                                  newFilterArg,
                                  aggregateCall.getCollation(),
                                  aggregateCall.getType(),
                                  aggregateCall.getName()
      );
    }

    // Four styles supported:
    //
    // A1: AGG(CASE WHEN x = 'foo' THEN cnt END)
    //   => operands (x = 'foo', cnt, null)
    // A2: SUM(CASE WHEN x = 'foo' THEN cnt ELSE 0 END)
    //   => operands (x = 'foo', cnt, 0); must be SUM
    // B: SUM(CASE WHEN x = 'foo' THEN 1 ELSE 0 END)
    //   => operands (x = 'foo', 1, 0); must be SUM
    // C: COUNT(CASE WHEN x = 'foo' THEN 'dummy' END)
    //   => operands (x = 'foo', 'dummy', null)

    if (kind == SqlKind.COUNT // Case C
        && arg1.isA(SqlKind.LITERAL)
        && !RexLiteral.isNullLiteral(arg1)
        && RexLiteral.isNullLiteral(arg2)) {
      newProjects.add(filter);
      return AggregateCall.create(SqlStdOperatorTable.COUNT, false, false,
                                  false, ImmutableList.of(), newProjects.size() - 1,
                                  RelCollations.EMPTY, aggregateCall.getType(),
                                  aggregateCall.getName()
      );
    } else if (kind == SqlKind.SUM // Case B
               && isIntLiteral(arg1) && RexLiteral.intValue(arg1) == 1
               && isIntLiteral(arg2) && RexLiteral.intValue(arg2) == 0) {

      newProjects.add(filter);
      final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
      final RelDataType dataType =
          typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(SqlTypeName.BIGINT), false);
      return AggregateCall.create(SqlStdOperatorTable.COUNT, false, false,
                                  false, ImmutableList.of(), newProjects.size() - 1,
                                  RelCollations.EMPTY, dataType, aggregateCall.getName()
      );
    } else if ((RexLiteral.isNullLiteral(arg2) // Case A1
                && aggregateCall.getAggregation().allowsFilter())
               || (kind == SqlKind.SUM // Case A2
                   && isIntLiteral(arg2)
                   && RexLiteral.intValue(arg2) == 0)) {
      newProjects.add(arg1);
      newProjects.add(filter);
      return AggregateCall.create(aggregateCall.getAggregation(), false,
                                  false, false, ImmutableList.of(newProjects.size() - 2),
                                  newProjects.size() - 1, RelCollations.EMPTY,
                                  aggregateCall.getType(), aggregateCall.getName()
      );
    } else {
      newProjects.add(rexNode);
      int callArg = newProjects.size() - 1;
      int newFilterArg = -1;
      if (aggregateCall.hasFilter()) {
        newProjects.add(project.getProjects().get(aggregateCall.filterArg));
        newFilterArg = newProjects.size() - 1;
      }
      return AggregateCall.create(aggregateCall.getAggregation(),
                                  aggregateCall.isDistinct(),
                                  aggregateCall.isApproximate(),
                                  aggregateCall.ignoreNulls(),
                                  ImmutableList.of(callArg),
                                  newFilterArg,
                                  aggregateCall.getCollation(),
                                  aggregateCall.getType(),
                                  aggregateCall.getName()
      );
    }
  }

  /**
   * Returns the argument, if an aggregate call has a single argument,
   * otherwise -1.
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

  private static boolean isIntLiteral(final RexNode rexNode)
  {
    return rexNode instanceof RexLiteral
           && SqlTypeName.INT_TYPES.contains(rexNode.getType().getSqlTypeName());
  }
}
