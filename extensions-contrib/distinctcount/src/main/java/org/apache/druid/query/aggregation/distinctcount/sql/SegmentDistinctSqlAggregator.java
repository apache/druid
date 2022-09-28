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

package org.apache.druid.query.aggregation.distinctcount.sql;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.distinctcount.DistinctCountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;



public class SegmentDistinctSqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new SegmentDistinctAggFunction();
  private static final String NAME = "SEGMENT_DISTINCT";

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
          VirtualColumnRegistry virtualColumnRegistry,
          RexBuilder rexBuilder,
          String name,
          AggregateCall aggregateCall,
          Project project,
          List<Aggregation> list,
          boolean finalizeAggregations)
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

    final AggregatorFactory aggregatorFactory;
    final String aggregatorName = finalizeAggregations ? Calcites.makePrefixedName(name, "a") : name;

    if (columnArg.isDirectColumnAccess()
        && rowSignature.getColumnType(columnArg.getDirectColumn()).map(type -> type.is(ValueType.COMPLEX)).orElse(false)) {
      aggregatorFactory = new DistinctCountAggregatorFactory(name, columnArg.getDirectColumn(), null);
    } else {
      final RelDataType dataType = columnRexNode.getType();
      final ColumnType inputType = Calcites.getColumnTypeForRelDataType(dataType);

      if (inputType == null) {
        throw new ISE(
            "Cannot translate sqlTypeName[%s] to Druid type for field[%s]",
            dataType.getSqlTypeName(),
            aggregatorName
        );
      }

      final DimensionSpec dimensionSpec;

      if (columnArg.isDirectColumnAccess()) {
        dimensionSpec = columnArg.getSimpleExtraction().toDimensionSpec(null, inputType);
      } else {
        String virtualColumnName = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
            columnArg,
            dataType
        );
        dimensionSpec = new DefaultDimensionSpec(virtualColumnName, null, inputType);
      }

      aggregatorFactory = new DistinctCountAggregatorFactory(name, dimensionSpec.getDimension(), null);
    }

    return Aggregation.create(aggregatorFactory);
  }

  private static class SegmentDistinctAggFunction extends SqlAggFunction
  {
    private static final String SIGNATURE = "'" + NAME + "(column, bitMapFactory)'\n";

    SegmentDistinctAggFunction()
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
                  OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.STRING)
              )
          ),
          SqlFunctionCategory.NUMERIC,
          false,
          false,
          Optionality.IGNORED
      );
    }
  }
}
