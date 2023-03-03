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

package org.apache.druid.query.aggregation.datasketches.tuple.sql;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.datasketches.Util;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchAggregatorFactory;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ArrayOfDoublesSketchSqlAggregator implements SqlAggregator
{

  private static final SqlAggFunction FUNCTION_INSTANCE = new ArrayOfDoublesSqlAggFunction();
  private static final String NAME = "ARRAY_OF_DOUBLES_SKETCH";

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
      List<Aggregation> existingAggregations,
      boolean finalizeAggregations
  )
  {

    final List<Integer> argList = aggregateCall.getArgList();

    // check last argument for nomimalEntries
    final int nominalEntries;
    final int metricExpressionEndIndex;
    final int lastArgIndex = argList.size() - 1;
    final RexNode potentialNominalEntriesArg = Expressions.fromFieldAccess(
        rowSignature,
        project,
        argList.get(lastArgIndex)
    );

    if (potentialNominalEntriesArg.isA(SqlKind.LITERAL) &&
        RexLiteral.value(potentialNominalEntriesArg) instanceof Number) {

      nominalEntries = ((Number) RexLiteral.value(potentialNominalEntriesArg)).intValue();
      metricExpressionEndIndex = lastArgIndex - 1;
    } else {
      nominalEntries = Util.DEFAULT_NOMINAL_ENTRIES;
      metricExpressionEndIndex = lastArgIndex;
    }

    final List<String> fieldNames = new ArrayList<>();
    for (int i = 0; i <= metricExpressionEndIndex; i++) {
      final String fieldName;

      final RexNode columnRexNode = Expressions.fromFieldAccess(
          rowSignature,
          project,
          argList.get(i)
      );

      final DruidExpression columnArg = Expressions.toDruidExpression(
          plannerContext,
          rowSignature,
          columnRexNode
      );
      if (columnArg == null) {
        return null;
      }

      if (columnArg.isDirectColumnAccess() &&
          rowSignature.getColumnType(columnArg.getDirectColumn())
                      .map(type -> type.is(ValueType.COMPLEX))
                      .orElse(false)) {
        fieldName = columnArg.getDirectColumn();
      } else {
        final RelDataType dataType = columnRexNode.getType();
        final ColumnType inputType = Calcites.getColumnTypeForRelDataType(dataType);
        if (inputType == null) {
          throw new ISE(
              "Cannot translate sqlTypeName[%s] to Druid type for field[%s]",
              dataType.getSqlTypeName(),
              name
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
        fieldName = dimensionSpec.getDimension();
      }

      fieldNames.add(fieldName);
    }

    final AggregatorFactory aggregatorFactory;
    final List<String> metricColumns = fieldNames.size() > 1 ? fieldNames.subList(1, fieldNames.size()) : null;
    aggregatorFactory = new ArrayOfDoublesSketchAggregatorFactory(
          name,
          fieldNames.get(0), // first field is dimension
          nominalEntries,
          metricColumns,
          null
      );

    return Aggregation.create(
        Collections.singletonList(aggregatorFactory),
        null);
  }

  private static class ArrayOfDoublesSqlAggFunction extends SqlAggFunction
  {
    private static final String SIGNATURE = "'" + NAME + "(expr, nominalEntries)'\n";

    ArrayOfDoublesSqlAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          ArrayOfDoublesSketchSqlOperators.RETURN_TYPE_INFERENCE,
          InferTypes.VARCHAR_1024,
          OperandTypes.VARIADIC,
          SqlFunctionCategory.USER_DEFINED_FUNCTION,
          false,
          false
      );
    }
  }

}
