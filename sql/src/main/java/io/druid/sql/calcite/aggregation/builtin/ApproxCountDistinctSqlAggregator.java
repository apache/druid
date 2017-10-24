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
import io.druid.java.util.common.StringUtils;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.ValueType;
import io.druid.segment.virtual.ExpressionVirtualColumn;
import io.druid.sql.calcite.aggregation.Aggregation;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

import javax.annotation.Nullable;
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
    // Don't use Aggregations.getArgumentsForSimpleAggregator, since it won't let us use direct column access
    // for string columns.
    final RexNode rexNode = Expressions.fromFieldAccess(
        rowSignature,
        project,
        Iterables.getOnlyElement(aggregateCall.getArgList())
    );

    final DruidExpression arg = Expressions.toDruidExpression(plannerContext, rowSignature, rexNode);
    if (arg == null) {
      return null;
    }

    final List<VirtualColumn> virtualColumns = new ArrayList<>();
    final AggregatorFactory aggregatorFactory;

    if (arg.isDirectColumnAccess() && rowSignature.getColumnType(arg.getDirectColumn()) == ValueType.COMPLEX) {
      aggregatorFactory = new HyperUniquesAggregatorFactory(name, arg.getDirectColumn(), false, true);
    } else {
      final SqlTypeName sqlTypeName = rexNode.getType().getSqlTypeName();
      final ValueType inputType = Calcites.getValueTypeForSqlTypeName(sqlTypeName);
      if (inputType == null) {
        throw new ISE("Cannot translate sqlTypeName[%s] to Druid type for field[%s]", sqlTypeName, name);
      }

      final DimensionSpec dimensionSpec;

      if (arg.isSimpleExtraction()) {
        dimensionSpec = arg.getSimpleExtraction().toDimensionSpec(null, inputType);
      } else {
        final ExpressionVirtualColumn virtualColumn = arg.toVirtualColumn(
            StringUtils.format("%s:v", name),
            inputType,
            plannerContext.getExprMacroTable()
        );
        dimensionSpec = new DefaultDimensionSpec(virtualColumn.getOutputName(), null, inputType);
        virtualColumns.add(virtualColumn);
      }

      aggregatorFactory = new CardinalityAggregatorFactory(name, null, ImmutableList.of(dimensionSpec), false, true);
    }

    return Aggregation.create(virtualColumns, aggregatorFactory);
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
