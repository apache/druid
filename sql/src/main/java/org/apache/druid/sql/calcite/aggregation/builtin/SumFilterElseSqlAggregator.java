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

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.util.Optionality;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.Aggregations;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.InputAccessor;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.rule.DruidAggregateCaseToFilterRule;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Internal aggregate function that represents a filtered SUM with an else value. Produced by
 * {@link DruidAggregateCaseToFilterRule} to handle {@code SUM(CASE WHEN cond THEN expr ELSE 0 END)}.
 */
public class SumFilterElseSqlAggregator implements SqlAggregator
{
  public static final SqlAggFunction FUNCTION = new SumFilterElseAggFunction();

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION;
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
    final List<RexNode> args = inputAccessor.getFields(aggregateCall.getArgList());
    final RexNode exprRex = args.get(0);
    final RexNode condRex = args.get(1);
    final RexNode elseRex = args.get(2);

    final RowSignature rowSignature = inputAccessor.getInputRowSignature();

    final DruidExpression exprDruid = Aggregations.toDruidExpressionForNumericAggregator(
        plannerContext,
        rowSignature,
        exprRex
    );
    if (exprDruid == null) {
      return null;
    }

    final DimFilter condFilter = Expressions.toFilter(
        plannerContext,
        rowSignature,
        virtualColumnRegistry,
        condRex
    );
    if (condFilter == null) {
      return null;
    }

    final DimFilter optimizedFilter = Filtration.create(condFilter)
                                                .optimizeFilterOnly(virtualColumnRegistry.getFullRowSignature())
                                                .getDimFilter();

    // The producing rule only emits this aggregate when elseValue is an integer literal of value zero,
    // so Long is a safe target type.
    final Number elseValue = ((RexLiteral) elseRex).getValueAs(Long.class);

    final String fieldName;
    if (exprDruid.isDirectColumnAccess()) {
      fieldName = exprDruid.getDirectColumn();
    } else {
      fieldName = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(exprDruid, aggregateCall.getType());
    }

    final ColumnType valueType = Calcites.getColumnTypeForRelDataType(aggregateCall.getType());
    if (valueType == null) {
      return null;
    }

    final AggregatorFactory sumFactory = SumSqlAggregator.createSumAggregatorFactory(
        valueType,
        name,
        fieldName,
        plannerContext.getPlannerToolbox().exprMacroTable()
    );

    final FilteredAggregatorFactory filteredFactory = new FilteredAggregatorFactory(
        sumFactory,
        optimizedFilter,
        name,
        elseValue
    );

    return Aggregation.create(filteredFactory);
  }

  private static class SumFilterElseAggFunction extends SqlAggFunction
  {
    private static final SqlReturnTypeInference RETURN_TYPE =
        ReturnTypes.AGG_SUM.andThen(SqlTypeTransforms.FORCE_NULLABLE);

    SumFilterElseAggFunction()
    {
      super(
          "$SUM_FILTER_ELSE",
          null,
          SqlKind.OTHER_FUNCTION,
          RETURN_TYPE,
          null,
          OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.BOOLEAN, SqlTypeFamily.NUMERIC),
          SqlFunctionCategory.NUMERIC,
          false,
          false,
          Optionality.FORBIDDEN
      );
    }
  }
}
