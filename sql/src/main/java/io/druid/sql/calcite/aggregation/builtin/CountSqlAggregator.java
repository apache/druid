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

package io.druid.sql.calcite.aggregation.builtin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.druid.java.util.common.ISE;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.filter.DimFilter;
import io.druid.sql.calcite.aggregation.Aggregation;
import io.druid.sql.calcite.aggregation.Aggregations;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import javax.annotation.Nullable;
import java.util.List;

public class CountSqlAggregator implements SqlAggregator
{
  private static final ApproxCountDistinctSqlAggregator APPROX_COUNT_DISTINCT = new ApproxCountDistinctSqlAggregator();

  @Override
  public SqlAggFunction calciteFunction()
  {
    return SqlStdOperatorTable.COUNT;
  }

  @Nullable
  @Override
  public Aggregation toDruidAggregation(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexBuilder rexBuilder,
      final String name,
      final AggregateCall aggregateCall,
      final Project project,
      final List<Aggregation> existingAggregations
  )
  {
    final List<DruidExpression> args = Aggregations.getArgumentsForSimpleAggregator(
        plannerContext,
        rowSignature,
        aggregateCall,
        project
    );

    if (args == null) {
      return null;
    }

    if (args.isEmpty()) {
      // COUNT(*)
      return Aggregation.create(new CountAggregatorFactory(name));
    } else if (aggregateCall.isDistinct()) {
      // COUNT(DISTINCT x)
      if (plannerContext.getPlannerConfig().isUseApproximateCountDistinct()) {
        return APPROX_COUNT_DISTINCT.toDruidAggregation(
            plannerContext,
            rowSignature,
            rexBuilder,
            name,
            aggregateCall,
            project,
            existingAggregations
        );
      } else {
        return null;
      }
    } else {
      // Not COUNT(*), not distinct

      // COUNT(x) should count all non-null values of x.
      final RexNode rexNode = Expressions.fromFieldAccess(
          rowSignature,
          project,
          Iterables.getOnlyElement(aggregateCall.getArgList())
      );

      if (rexNode.getType().isNullable()) {
        final DimFilter nonNullFilter = Expressions.toFilter(
            plannerContext,
            rowSignature,
            rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, ImmutableList.of(rexNode))
        );

        if (nonNullFilter == null) {
          // Don't expect this to happen.
          throw new ISE("Could not create not-null filter for rexNode[%s]", rexNode);
        }

        return Aggregation.create(new CountAggregatorFactory(name)).filter(rowSignature, nonNullFilter);
      } else {
        return Aggregation.create(new CountAggregatorFactory(name));
      }
    }
  }
}
