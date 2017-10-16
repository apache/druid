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

import com.google.common.collect.Iterables;
import io.druid.java.util.common.ISE;
import io.druid.math.expr.ExprMacroTable;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.FloatSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.column.ValueType;
import io.druid.sql.calcite.aggregation.Aggregation;
import io.druid.sql.calcite.aggregation.Aggregations;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import javax.annotation.Nullable;
import java.util.List;

public class SumSqlAggregator implements SqlAggregator
{
  @Override
  public SqlAggFunction calciteFunction()
  {
    return SqlStdOperatorTable.SUM;
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
    if (aggregateCall.isDistinct()) {
      return null;
    }

    final List<DruidExpression> arguments = Aggregations.getArgumentsForSimpleAggregator(
        plannerContext,
        rowSignature,
        aggregateCall,
        project
    );

    if (arguments == null) {
      return null;
    }

    final DruidExpression arg = Iterables.getOnlyElement(arguments);
    final ValueType valueType = Calcites.getValueTypeForSqlTypeName(aggregateCall.getType().getSqlTypeName());
    final ExprMacroTable macroTable = plannerContext.getExprMacroTable();

    final String fieldName;
    final String expression;

    if (arg.isDirectColumnAccess()) {
      fieldName = arg.getDirectColumn();
      expression = null;
    } else {
      fieldName = null;
      expression = arg.getExpression();
    }

    return Aggregation.create(createSumAggregatorFactory(valueType, name, fieldName, expression, macroTable));
  }

  static AggregatorFactory createSumAggregatorFactory(
      final ValueType aggregationType,
      final String name,
      final String fieldName,
      final String expression,
      final ExprMacroTable macroTable
  )
  {
    switch (aggregationType) {
      case LONG:
        return new LongSumAggregatorFactory(name, fieldName, expression, macroTable);
      case FLOAT:
        return new FloatSumAggregatorFactory(name, fieldName, expression, macroTable);
      case DOUBLE:
        return new DoubleSumAggregatorFactory(name, fieldName, expression, macroTable);
      default:
        throw new ISE("Cannot create aggregator factory for type[%s]", aggregationType);
    }
  }
}
