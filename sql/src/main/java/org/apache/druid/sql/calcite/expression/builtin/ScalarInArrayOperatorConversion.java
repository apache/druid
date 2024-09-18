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

package org.apache.druid.sql.calcite.expression.builtin;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DirectOperatorConversion;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.DruidLiteral;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class ScalarInArrayOperatorConversion extends DirectOperatorConversion
{
  public static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder("SCALAR_IN_ARRAY")
      .operandTypeChecker(
          OperandTypes.sequence(
              "'SCALAR_IN_ARRAY(expr, array)'",
              OperandTypes.or(
                  OperandTypes.family(SqlTypeFamily.CHARACTER),
                  OperandTypes.family(SqlTypeFamily.NUMERIC)
              ),
              OperandTypes.family(SqlTypeFamily.ARRAY)
          )
      )
      .returnTypeInference(ReturnTypes.BOOLEAN_NULLABLE)
      .build();

  public ScalarInArrayOperatorConversion()
  {
    super(SQL_FUNCTION, "scalar_in_array");
  }

  @Nullable
  @Override
  public DimFilter toDruidFilter(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      @Nullable final VirtualColumnRegistry virtualColumnRegistry,
      final RexNode rexNode
  )
  {
    final RexCall call = (RexCall) rexNode;
    final RexNode scalarOperand = call.getOperands().get(0);
    final RexNode arrayOperand = call.getOperands().get(1);
    final DruidExpression scalarExpression = Expressions.toDruidExpression(plannerContext, rowSignature, scalarOperand);
    final String scalarColumn;
    final ExtractionFn scalarExtractionFn;

    if (scalarExpression == null) {
      return null;
    }

    if (scalarExpression.isDirectColumnAccess()) {
      scalarColumn = scalarExpression.getDirectColumn();
      scalarExtractionFn = null;
    } else if (scalarExpression.isSimpleExtraction() && plannerContext.isUseLegacyInFilter()) {
      scalarColumn = scalarExpression.getSimpleExtraction().getColumn();
      scalarExtractionFn = scalarExpression.getSimpleExtraction().getExtractionFn();
    } else if (virtualColumnRegistry == null) {
      // virtual column registry unavailable, fallback to expression filter
      return null;
    } else {
      scalarColumn = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          scalarExpression,
          scalarExpression.getDruidType()
      );
      scalarExtractionFn = null;
    }

    if (Calcites.isLiteral(arrayOperand, true, true)) {
      final RelDataType elementType = arrayOperand.getType().getComponentType();
      final List<RexNode> arrayElements = ((RexCall) arrayOperand).getOperands();
      final List<Object> arrayElementLiteralValues = new ArrayList<>(arrayElements.size());

      for (final RexNode arrayElement : arrayElements) {
        final DruidLiteral arrayElementEval = Expressions.calciteLiteralToDruidLiteral(plannerContext, arrayElement);
        if (arrayElementEval == null) {
          return null;
        }

        arrayElementLiteralValues.add(arrayElementEval.value());
      }

      return makeInFilter(
          plannerContext,
          scalarColumn,
          scalarExtractionFn,
          arrayElementLiteralValues,
          Calcites.getColumnTypeForRelDataType(elementType)
      );
    }

    return null;
  }

  /**
   * Create an {@link InDimFilter} or {@link TypedInFilter} based on a list of provided values.
   */
  public static DimFilter makeInFilter(
      final PlannerContext plannerContext,
      final String columnName,
      @Nullable final ExtractionFn extractionFn,
      final List<Object> matchValues,
      final ColumnType matchValueType
  )
  {
    if (plannerContext.isUseLegacyInFilter() || extractionFn != null) {
      final InDimFilter.ValuesSet valuesSet = InDimFilter.ValuesSet.create();
      for (final Object matchValue : matchValues) {
        valuesSet.add(Evals.asString(matchValue));
      }

      return new InDimFilter(columnName, valuesSet, extractionFn, null);
    } else {
      return new TypedInFilter(columnName, matchValueType, matchValues, null, null);
    }
  }
}
