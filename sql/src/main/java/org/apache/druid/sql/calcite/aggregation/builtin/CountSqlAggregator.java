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
import org.apache.druid.sql.calcite.aggregation.ApproxCountDistinctSqlAggregator;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.InputAccessor;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.List;

public class CountSqlAggregator implements SqlAggregator
{
  private final ApproxCountDistinctSqlAggregator approxCountDistinctAggregator;

  @Inject
  public CountSqlAggregator(ApproxCountDistinctSqlAggregator approxCountDistinctAggregator)
  {
    this.approxCountDistinctAggregator = approxCountDistinctAggregator;
  }

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
      final InputAccessor inputAccessor
  )
  {
    final RexNode rexNode = inputAccessor.getField(Iterables.getOnlyElement(aggregateCall.getArgList()));

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
      final VirtualColumnRegistry virtualColumnRegistry,
      final String name,
      final AggregateCall aggregateCall,
      final InputAccessor inputAccessor,
      final List<Aggregation> existingAggregations,
      final boolean finalizeAggregations
  )
  {
    final List<DruidExpression> args = Aggregations.getArgumentsForSimpleAggregator(
        plannerContext,
        aggregateCall,
        inputAccessor
    );

    if (args == null) {
      return null;
    }

    // FIXME: is-all-literal
    if (args.isEmpty()) {
      // COUNT(*)
      return Aggregation.create(new CountAggregatorFactory(name));
    } else if (aggregateCall.isDistinct()) {
      // COUNT(DISTINCT x)
      if (plannerContext.getPlannerConfig().isUseApproximateCountDistinct()) {
        return approxCountDistinctAggregator.toDruidAggregation(
            plannerContext,
            virtualColumnRegistry,
            name,
            aggregateCall,
            inputAccessor,
            existingAggregations,
            finalizeAggregations
        );
      } else {
        return null;
      }
    } else {
      // Not COUNT(*), not distinct
      // COUNT(x) should count all non-null values of x.
      AggregatorFactory theCount = createCountAggregatorFactory(
            name,
            plannerContext,
            inputAccessor.getInputRowSignature(),
            virtualColumnRegistry,
            inputAccessor.getRexBuilder(),
            aggregateCall,
            inputAccessor
      );

      return Aggregation.create(theCount);
    }
  }
}
