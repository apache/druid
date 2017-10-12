/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.druid.sql.calcite.planner.Calcites;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule that converts CASE-style filtered aggregation into true filtered aggregations.
 */
public class CaseFilteredAggregatorRule extends RelOptRule
{
  private static final CaseFilteredAggregatorRule INSTANCE = new CaseFilteredAggregatorRule();

  private CaseFilteredAggregatorRule()
  {
    super(operand(Aggregate.class, operand(Project.class, any())));
  }

  public static CaseFilteredAggregatorRule instance()
  {
    return INSTANCE;
  }

  @Override
  public boolean matches(final RelOptRuleCall call)
  {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);

    if (aggregate.indicator || aggregate.getGroupSets().size() != 1) {
      return false;
    }

    for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
      if (isNonDistinctOneArgAggregateCall(aggregateCall)
          && isThreeArgCase(project.getChildExps().get(Iterables.getOnlyElement(aggregateCall.getArgList())))) {
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
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final List<AggregateCall> newCalls = new ArrayList<>(aggregate.getAggCallList().size());
    final List<RexNode> newProjects = new ArrayList<>(project.getChildExps());
    final List<RexNode> newCasts = new ArrayList<>(aggregate.getGroupCount() + aggregate.getAggCallList().size());
    final RelDataTypeFactory typeFactory = aggregate.getCluster().getTypeFactory();

    for (int fieldNumber : aggregate.getGroupSet()) {
      newCasts.add(rexBuilder.makeInputRef(project.getChildExps().get(fieldNumber).getType(), fieldNumber));
    }

    for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
      AggregateCall newCall = null;

      if (isNonDistinctOneArgAggregateCall(aggregateCall)) {
        final RexNode rexNode = project.getChildExps().get(Iterables.getOnlyElement(aggregateCall.getArgList()));

        // Styles supported:
        //
        // A1: AGG(CASE WHEN x = 'foo' THEN cnt END) => operands (x = 'foo', cnt, null)
        // A2: SUM(CASE WHEN x = 'foo' THEN cnt ELSE 0 END) => operands (x = 'foo', cnt, 0); must be SUM
        // B: SUM(CASE WHEN x = 'foo' THEN 1 ELSE 0 END) => operands (x = 'foo', 1, 0)
        // C: COUNT(CASE WHEN x = 'foo' THEN 'dummy' END) => operands (x = 'foo', 'dummy', null)
        //
        // If the null and non-null args are switched, "flip" is set, which negates the filter.

        if (isThreeArgCase(rexNode)) {
          final RexCall caseCall = (RexCall) rexNode;

          final boolean flip = RexLiteral.isNullLiteral(caseCall.getOperands().get(1))
                               && !RexLiteral.isNullLiteral(caseCall.getOperands().get(2));
          final RexNode arg1 = caseCall.getOperands().get(flip ? 2 : 1);
          final RexNode arg2 = caseCall.getOperands().get(flip ? 1 : 2);

          // Operand 1: Filter
          final RexNode filter;
          final RelDataType booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
          final RexNode filterFromCase = rexBuilder.makeCall(
              booleanType,
              flip ? SqlStdOperatorTable.IS_FALSE : SqlStdOperatorTable.IS_TRUE,
              ImmutableList.of(caseCall.getOperands().get(0))
          );

          if (aggregateCall.filterArg >= 0) {
            filter = rexBuilder.makeCall(
                booleanType,
                SqlStdOperatorTable.AND,
                ImmutableList.of(project.getProjects().get(aggregateCall.filterArg), filterFromCase)
            );
          } else {
            filter = filterFromCase;
          }

          if (aggregateCall.getAggregation().getKind() == SqlKind.COUNT
              && arg1 instanceof RexLiteral
              && !RexLiteral.isNullLiteral(arg1)
              && RexLiteral.isNullLiteral(arg2)) {
            // Case C
            newProjects.add(filter);
            newCall = AggregateCall.create(
                SqlStdOperatorTable.COUNT,
                false,
                ImmutableList.of(),
                newProjects.size() - 1,
                aggregateCall.getType(),
                aggregateCall.getName()
            );
          } else if (aggregateCall.getAggregation().getKind() == SqlKind.SUM
                     && Calcites.isIntLiteral(arg1) && RexLiteral.intValue(arg1) == 1
                     && Calcites.isIntLiteral(arg2) && RexLiteral.intValue(arg2) == 0) {
            // Case B
            newProjects.add(filter);
            newCall = AggregateCall.create(
                SqlStdOperatorTable.COUNT,
                false,
                ImmutableList.of(),
                newProjects.size() - 1,
                typeFactory.createSqlType(SqlTypeName.BIGINT),
                aggregateCall.getName()
            );
          } else if (RexLiteral.isNullLiteral(arg2) /* Case A1 */
                     || (aggregateCall.getAggregation().getKind() == SqlKind.SUM
                         && Calcites.isIntLiteral(arg2)
                         && RexLiteral.intValue(arg2) == 0) /* Case A2 */) {
            newProjects.add(arg1);
            newProjects.add(filter);
            newCall = AggregateCall.create(
                aggregateCall.getAggregation(),
                false,
                ImmutableList.of(newProjects.size() - 2),
                newProjects.size() - 1,
                aggregateCall.getType(),
                aggregateCall.getName()
            );
          }
        }
      }

      newCalls.add(newCall == null ? aggregateCall : newCall);

      // Possibly CAST the new aggregator to an appropriate type.
      final int i = newCasts.size();
      final RelDataType oldType = aggregate.getRowType().getFieldList().get(i).getType();
      if (newCall == null) {
        newCasts.add(rexBuilder.makeInputRef(oldType, i));
      } else {
        newCasts.add(rexBuilder.makeCast(oldType, rexBuilder.makeInputRef(newCall.getType(), i)));
      }
    }

    if (!newCalls.equals(aggregate.getAggCallList())) {
      final RelBuilder relBuilder = call
          .builder()
          .push(project.getInput())
          .project(newProjects);

      final RelBuilder.GroupKey groupKey = relBuilder.groupKey(
          aggregate.getGroupSet(),
          aggregate.indicator,
          aggregate.getGroupSets()
      );

      final RelNode newAggregate = relBuilder.aggregate(groupKey, newCalls).project(newCasts).build();

      call.transformTo(newAggregate);
      call.getPlanner().setImportance(aggregate, 0.0);
    }
  }

  private static boolean isNonDistinctOneArgAggregateCall(final AggregateCall aggregateCall)
  {
    return aggregateCall.getArgList().size() == 1 && !aggregateCall.isDistinct();
  }

  private static boolean isThreeArgCase(final RexNode rexNode)
  {
    return rexNode.getKind() == SqlKind.CASE && ((RexCall) rexNode).getOperands().size() == 3;
  }
}
