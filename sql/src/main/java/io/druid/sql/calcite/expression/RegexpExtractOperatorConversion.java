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

package io.druid.sql.calcite.expression;

import io.druid.math.expr.Expr;
import io.druid.query.extraction.RegexDimExtractionFn;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

public class RegexpExtractOperatorConversion implements SqlOperatorConversion
{
  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder("REGEXP_EXTRACT")
      .operandTypes(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER)
      .requiredOperands(2)
      .returnType(SqlTypeName.VARCHAR)
      .functionCategory(SqlFunctionCategory.STRING)
      .build();

  private static final int DEFAULT_INDEX = 0;

  @Override
  public SqlFunction calciteOperator()
  {
    return SQL_FUNCTION;
  }

  @Override
  public DruidExpression toDruidExpression(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    return OperatorConversions.functionCall(
        plannerContext,
        rowSignature,
        rexNode,
        calciteOperator().getName().toLowerCase(),
        inputExpressions -> {
          final DruidExpression arg = inputExpressions.get(0);
          final Expr patternExpr = inputExpressions.get(1).parse(plannerContext.getExprMacroTable());
          final Expr indexExpr = inputExpressions.size() > 2
                                 ? inputExpressions.get(2).parse(plannerContext.getExprMacroTable())
                                 : null;

          if (arg.isSimpleExtraction() && patternExpr.isLiteral() && (indexExpr == null || indexExpr.isLiteral())) {
            return arg.getSimpleExtraction().cascade(
                new RegexDimExtractionFn(
                    (String) patternExpr.getLiteralValue(),
                    indexExpr == null ? DEFAULT_INDEX : ((Number) indexExpr.getLiteralValue()).intValue(),
                    true,
                    null
                )
            );
          } else {
            return null;
          }
        }
    );
  }
}
