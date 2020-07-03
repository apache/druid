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

package org.apache.druid.sql.calcite.aggregation.builtin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.Aggregations;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

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

  static AggregatorFactory createCountAggregatorFactory(
          final String countName,
          final PlannerContext plannerContext,
          final RowSignature rowSignature,
          final VirtualColumnRegistry virtualColumnRegistry,
          final RexBuilder rexBuilder,
          final AggregateCall aggregateCall,
          final Project project
  )
  {
    final RexNode rexNode = Expressions.fromFieldAccess(
            rowSignature,
            project,
            Iterables.getOnlyElement(aggregateCall.getArgList())
    );

    if (rexNode.getType().isNullable()) {
      final DimFilter nonNullFilter = Expressions.toFilter(
              plannerContext,
              rowSignature,
              virtualColumnRegistry,
              rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, ImmutableList.of(rexNode))
      );

      if (nonNullFilter == null) {
        // Don't expect this to happen.
        throw new ISE("Could not create not-null filter for rexNode[%s]", rexNode);
      }

      return new FilteredAggregatorFactory(new CountAggregatorFactory(countName), nonNullFilter);
    } else {
      return new CountAggregatorFactory(countName);
    }
  }

  @Nullable
  @Override
  public Aggregation toDruidAggregation(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final VirtualColumnRegistry virtualColumnRegistry,
      final RexBuilder rexBuilder,
      final String name,
      final AggregateCall aggregateCall,
      final Project project,
      final List<Aggregation> existingAggregations,
      final boolean finalizeAggregations
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
            virtualColumnRegistry,
            rexBuilder,
            name, aggregateCall, project, existingAggregations,
            finalizeAggregations
        );
      } else {
        return null;
      }
    } else {
      // Not COUNT(*), not distinct
      // COUNT(x) should count all non-null values of x.
      return Aggregation.create(createCountAggregatorFactory(
            name,
            plannerContext,
            rowSignature,
            virtualColumnRegistry,
            rexBuilder,
            aggregateCall,
            project
      ));
    }
  }
}
