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

import com.google.common.collect.Sets;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;

public class ArrayOverlapOperatorConversion extends BaseExpressionDimFilterOperatorConversion
{
  private static final String EXPR_FUNCTION = "array_overlap";

  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder("ARRAY_OVERLAP")
      .operandTypeChecker(
          OperandTypes.sequence(
              "(array,array)",
              OperandTypes.or(
                  OperandTypes.family(SqlTypeFamily.ARRAY),
                  OperandTypes.family(SqlTypeFamily.STRING)
              ),
              OperandTypes.or(
                  OperandTypes.family(SqlTypeFamily.ARRAY),
                  OperandTypes.family(SqlTypeFamily.STRING)
              )
          )
      )
      .returnTypeInference(ReturnTypes.BOOLEAN)
      .build();

  public ArrayOverlapOperatorConversion()
  {
    super(SQL_FUNCTION, EXPR_FUNCTION);
  }

  @Nullable
  @Override
  public DimFilter toDruidFilter(
      final PlannerContext plannerContext,
      RowSignature rowSignature,
      @Nullable VirtualColumnRegistry virtualColumnRegistry,
      final RexNode rexNode
  )
  {
    final List<RexNode> operands = ((RexCall) rexNode).getOperands();
    final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
        plannerContext,
        rowSignature,
        operands
    );
    if (druidExpressions == null) {
      return null;
    }

    // Converts array_overlaps() function into an OR of Selector filters if possible.
    final boolean leftSimpleExtractionExpr = druidExpressions.get(0).isSimpleExtraction();
    final boolean rightSimpleExtractionExpr = druidExpressions.get(1).isSimpleExtraction();
    final DruidExpression simpleExtractionExpr;
    final DruidExpression complexExpr;

    if (leftSimpleExtractionExpr ^ rightSimpleExtractionExpr) {
      if (leftSimpleExtractionExpr) {
        simpleExtractionExpr = druidExpressions.get(0);
        complexExpr = druidExpressions.get(1);
      } else {
        simpleExtractionExpr = druidExpressions.get(1);
        complexExpr = druidExpressions.get(0);
      }
    } else {
      return toExpressionFilter(plannerContext, getDruidFunctionName(), druidExpressions);
    }

    Expr expr = Parser.parse(complexExpr.getExpression(), plannerContext.getExprMacroTable());
    if (expr.isLiteral()) {
      // Evaluate the expression to take out the array elements.
      // We can safely pass null if the expression is literal.
      ExprEval<?> exprEval = expr.eval(name -> null);
      String[] arrayElements = exprEval.asStringArray();
      if (arrayElements == null || arrayElements.length == 0) {
        // If arrayElements is empty which means complexExpr is an empty array,
        // it is technically more correct to return a TrueDimFiler here.
        // However, since both Calcite's SqlMultisetValueConstructor and Druid's ArrayConstructorFunction don't allow
        // to create an empty array with no argument, we just return null.
        return null;
      } else if (arrayElements.length == 1) {
        return newSelectorDimFilter(simpleExtractionExpr.getSimpleExtraction(), arrayElements[0]);
      } else {
        return new InDimFilter(
            simpleExtractionExpr.getSimpleExtraction().getColumn(),
            Sets.newHashSet(arrayElements),
            simpleExtractionExpr.getSimpleExtraction().getExtractionFn(),
            null
        );
      }
    }
    return toExpressionFilter(plannerContext, getDruidFunctionName(), druidExpressions);
  }
}
