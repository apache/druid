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

package org.apache.druid.query.aggregation.exact.count.bitmap64.sql;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.exact.count.bitmap64.Bitmap64ExactCountBuildAggregatorFactory;
import org.apache.druid.query.aggregation.exact.count.bitmap64.Bitmap64ExactCountMergeAggregatorFactory;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.aggregation.builtin.SimpleSqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class Bitmap64ExactCountSqlAggregator implements SqlAggregator
{
  private static final String NAME = "BITMAP64_EXACT_COUNT";
  private static final SqlAggFunction FUNCTION_INSTANCE
      = OperatorConversions.aggregatorBuilder(NAME)
                           .operandTypeChecker(OperandTypes.or(
                               OperandTypes.NUMERIC,
                               RowSignatures.complexTypeChecker(Bitmap64ExactCountMergeAggregatorFactory.TYPE),
                               RowSignatures.complexTypeChecker(Bitmap64ExactCountBuildAggregatorFactory.TYPE)
                           ))
                           .returnTypeNonNull(SqlTypeName.BIGINT)
                           .functionCategory(SqlFunctionCategory.NUMERIC)
                           .build();

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
    // Don't use Aggregations.getArgumentsForSimpleAggregator, since it won't let us use direct column access
    // for Bitmap64ExactCountBuild / Bitmap64ExactCountMerge columns.
    final RexNode columnRexNode = Expressions.fromFieldAccess(
        rexBuilder.getTypeFactory(),
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

    // Create Merge Aggregator if is already Bitmap64 complex type, else create Build Aggregator.
    if (isBitmap64ComplexType(columnArg, rowSignature)) {
      aggregatorFactory = new Bitmap64ExactCountMergeAggregatorFactory(aggregatorName, columnArg.getDirectColumn());
    } else {
      rejectImplicitCastsToNumericType(columnRexNode, name);
      aggregatorFactory = createBuildAggregatorFactory(columnRexNode, columnArg, virtualColumnRegistry, aggregatorName);
    }

    return toAggregation(name, finalizeAggregations, aggregatorFactory);
  }

  private boolean isBitmap64ComplexType(DruidExpression columnArg, RowSignature rowSignature)
  {
    return columnArg.isDirectColumnAccess()
           && rowSignature.getColumnType(columnArg.getDirectColumn())
                          .map(type -> type.is(ValueType.COMPLEX))
                          .orElse(false);
  }

  /**
   * Rejects SQL queries pertaining to using non-numeric columns that can be implicitly cast to numeric values (e.g. String).
   * There is a second layer of protection at {@link Bitmap64ExactCountBuildAggregatorFactory#validateNumericColumn},
   * which checks the column types when given a query in Druid Native.
   *
   * <p>
   * This function aims to keep the Exception type consistent with the rest of Druid.
   * </p>
   */
  private void rejectImplicitCastsToNumericType(RexNode columnRexNode, String columnName)
  {
    if (columnRexNode.isA(SqlKind.CAST)) {
      final RelDataType operandType = ((RexCall) columnRexNode).operands.get(0).getType();
      final ColumnType operandDruidType = Calcites.getColumnTypeForRelDataType(operandType);
      if (operandDruidType == null || !operandDruidType.isNumeric()) {
        throw SimpleSqlAggregator.badTypeException(columnName, NAME, ColumnType.STRING);
      }
    }
  }

  private AggregatorFactory createBuildAggregatorFactory(
      final RexNode columnRexNode,
      final DruidExpression columnArg,
      final VirtualColumnRegistry virtualColumnRegistry,
      final String aggregatorName
  )
  {
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
      String virtualColumnName = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(columnArg, dataType);
      dimensionSpec = new DefaultDimensionSpec(virtualColumnName, null, inputType);
    }

    return new Bitmap64ExactCountBuildAggregatorFactory(aggregatorName, dimensionSpec.getDimension());
  }

  private Aggregation toAggregation(String name, boolean finalizeAggregations, AggregatorFactory aggregatorFactory)
  {
    return Aggregation.create(
        Collections.singletonList(aggregatorFactory),
        finalizeAggregations ? new FinalizingFieldAccessPostAggregator(name, aggregatorFactory.getName())
                             : null
    );
  }
}
