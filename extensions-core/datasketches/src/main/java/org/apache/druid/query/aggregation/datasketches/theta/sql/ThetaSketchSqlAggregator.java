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

package org.apache.druid.query.aggregation.datasketches.theta.sql;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.theta.SketchAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.theta.SketchMergeAggregatorFactory;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ThetaSketchSqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new ThetaSketchSqlAggFunction();
  private static final String NAME = "APPROX_COUNT_DISTINCT_DS_THETA";

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION_INSTANCE;
  }

  @Nullable
  @Override
  public Aggregation toDruidAggregation(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      RexBuilder rexBuilder,
      String name,
      AggregateCall aggregateCall,
      Project project,
      List<Aggregation> existingAggregations,
      boolean finalizeAggregations
  )
  {
    // Don't use Aggregations.getArgumentsForSimpleAggregator, since it won't let us use direct column access
    // for string columns.
    final RexNode columnRexNode = Expressions.fromFieldAccess(
        rowSignature,
        project,
        aggregateCall.getArgList().get(0)
    );

    final DruidExpression columnArg = Expressions.toDruidExpression(plannerContext, rowSignature, columnRexNode);
    if (columnArg == null) {
      return null;
    }

    final int sketchSize;
    if (aggregateCall.getArgList().size() >= 2) {
      final RexNode sketchSizeArg = Expressions.fromFieldAccess(
          rowSignature,
          project,
          aggregateCall.getArgList().get(1)
      );

      if (!sketchSizeArg.isA(SqlKind.LITERAL)) {
        // logK must be a literal in order to plan.
        return null;
      }

      sketchSize = ((Number) RexLiteral.value(sketchSizeArg)).intValue();
    } else {
      sketchSize = SketchAggregatorFactory.DEFAULT_MAX_SKETCH_SIZE;
    }

    final List<VirtualColumn> virtualColumns = new ArrayList<>();
    final AggregatorFactory aggregatorFactory;
    final String aggregatorName = finalizeAggregations ? Calcites.makePrefixedName(name, "a") : name;

    if (columnArg.isDirectColumnAccess() && rowSignature.getColumnType(columnArg.getDirectColumn()) == ValueType.COMPLEX) {
      aggregatorFactory = new SketchMergeAggregatorFactory(
          aggregatorName,
          columnArg.getDirectColumn(),
          sketchSize,
          null,
          null,
          null
      );
    } else {
      final SqlTypeName sqlTypeName = columnRexNode.getType().getSqlTypeName();
      final ValueType inputType = Calcites.getValueTypeForSqlTypeName(sqlTypeName);
      if (inputType == null) {
        throw new ISE("Cannot translate sqlTypeName[%s] to Druid type for field[%s]", sqlTypeName, aggregatorName);
      }

      final DimensionSpec dimensionSpec;

      if (columnArg.isDirectColumnAccess()) {
        dimensionSpec = columnArg.getSimpleExtraction().toDimensionSpec(null, inputType);
      } else {
        final ExpressionVirtualColumn virtualColumn = columnArg.toVirtualColumn(
            Calcites.makePrefixedName(name, "v"),
            inputType,
            plannerContext.getExprMacroTable()
        );
        dimensionSpec = new DefaultDimensionSpec(virtualColumn.getOutputName(), null, inputType);
        virtualColumns.add(virtualColumn);
      }

      aggregatorFactory = new SketchMergeAggregatorFactory(
          aggregatorName,
          dimensionSpec.getDimension(),
          sketchSize,
          null,
          null,
          null
      );
    }

    return Aggregation.create(
        virtualColumns,
        Collections.singletonList(aggregatorFactory),
        finalizeAggregations ? new FinalizingFieldAccessPostAggregator(
            name,
            aggregatorFactory.getName()
        ) : null
    );
  }

  private static class ThetaSketchSqlAggFunction extends SqlAggFunction
  {
    private static final String SIGNATURE = "'" + NAME + "(column, size)'\n";

    ThetaSketchSqlAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.BIGINT),
          InferTypes.VARCHAR_1024,
          OperandTypes.or(
              OperandTypes.ANY,
              OperandTypes.and(
                  OperandTypes.sequence(SIGNATURE, OperandTypes.ANY, OperandTypes.LITERAL),
                  OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC)
              )
          ),
          SqlFunctionCategory.NUMERIC,
          false,
          false
      );
    }
  }
}
