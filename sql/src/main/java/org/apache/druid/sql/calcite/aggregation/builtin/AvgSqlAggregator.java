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
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.Aggregations;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;

public class AvgSqlAggregator implements SqlAggregator
{
  @Override
  public SqlAggFunction calciteFunction()
  {
    return SqlStdOperatorTable.AVG;
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

    final List<DruidExpression> arguments = Aggregations.getArgumentsForSimpleAggregator(
        plannerContext,
        rowSignature,
        aggregateCall,
        project
    );

    if (arguments == null) {
      return null;
    }

    final String fieldName;
    final String expression;
    final DruidExpression arg = Iterables.getOnlyElement(arguments);

    if (arg.isDirectColumnAccess()) {
      fieldName = arg.getDirectColumn();
      expression = null;
    } else {
      fieldName = null;
      expression = arg.getExpression();
    }

    final ExprMacroTable macroTable = plannerContext.getExprMacroTable();

    final ValueType sumType;
    // Use 64-bit sum regardless of the type of the AVG aggregator.
    if (SqlTypeName.INT_TYPES.contains(aggregateCall.getType().getSqlTypeName())) {
      sumType = ValueType.LONG;
    } else {
      sumType = ValueType.DOUBLE;
    }

    final String sumName = Calcites.makePrefixedName(name, "sum");
    final String countName = Calcites.makePrefixedName(name, "count");
    final AggregatorFactory sum = SumSqlAggregator.createSumAggregatorFactory(
        sumType,
        sumName,
        fieldName,
        expression,
        macroTable
    );
    final AggregatorFactory count = CountSqlAggregator.createCountAggregatorFactory(
        countName,
        plannerContext,
        rowSignature,
        virtualColumnRegistry,
        rexBuilder,
        aggregateCall,
        project
    );

    return Aggregation.create(
        ImmutableList.of(sum, count),
        new ArithmeticPostAggregator(
            name,
            "quotient",
            ImmutableList.of(
                new FieldAccessPostAggregator(null, sumName),
                new FieldAccessPostAggregator(null, countName)
            )
        )
    );
  }
}
