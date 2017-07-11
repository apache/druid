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

package io.druid.sql.calcite.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.druid.java.util.common.ISE;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.ValueType;
import io.druid.segment.virtual.ExpressionVirtualColumn;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

public class ApproxCountDistinctSqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new ApproxCountDistinctSqlAggFunction();
  private static final String NAME = "APPROX_COUNT_DISTINCT";

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION_INSTANCE;
  }

  @Override
  public Aggregation toDruidAggregation(
      final String name,
      final RowSignature rowSignature,
      final PlannerContext plannerContext,
      final List<Aggregation> existingAggregations,
      final Project project,
      final AggregateCall aggregateCall,
      final DimFilter filter
  )
  {
    final RexNode rexNode = Expressions.fromFieldAccess(
        rowSignature,
        project,
        Iterables.getOnlyElement(aggregateCall.getArgList())
    );
    final DruidExpression input = Expressions.toDruidExpression(
        plannerContext,
        rowSignature,
        rexNode
    );
    if (input == null) {
      return null;
    }

    final List<VirtualColumn> virtualColumns = new ArrayList<>();
    final AggregatorFactory aggregatorFactory;

    if (input.isDirectColumnAccess() && rowSignature.getColumnType(input.getDirectColumn()) == ValueType.COMPLEX) {
      aggregatorFactory = new HyperUniquesAggregatorFactory(name, input.getDirectColumn());
    } else {
      final SqlTypeName sqlTypeName = rexNode.getType().getSqlTypeName();
      final ValueType inputType = Calcites.getValueTypeForSqlTypeName(sqlTypeName);
      if (inputType == null) {
        throw new ISE("Cannot translate sqlTypeName[%s] to Druid type for field[%s]", sqlTypeName, name);
      }

      final DimensionSpec dimensionSpec;

      if (input.isSimpleExtraction()) {
        dimensionSpec = input.getSimpleExtraction().toDimensionSpec(null, ValueType.STRING);
      } else {
        final ExpressionVirtualColumn virtualColumn = input.toVirtualColumn(
            String.format("%s:v", name),
            inputType,
            plannerContext.getExprMacroTable()
        );
        dimensionSpec = new DefaultDimensionSpec(virtualColumn.getOutputName(), null, inputType);
        virtualColumns.add(virtualColumn);
      }

      aggregatorFactory = new CardinalityAggregatorFactory(name, ImmutableList.of(dimensionSpec), false);
    }

    return Aggregation.create(virtualColumns, aggregatorFactory).filter(filter);
  }

  private static class ApproxCountDistinctSqlAggFunction extends SqlAggFunction
  {
    ApproxCountDistinctSqlAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.BIGINT),
          InferTypes.VARCHAR_1024,
          OperandTypes.ANY,
          SqlFunctionCategory.STRING,
          false,
          false
      );
    }
  }
}
