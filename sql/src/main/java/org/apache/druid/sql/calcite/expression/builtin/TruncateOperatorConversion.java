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

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignature;

public class TruncateOperatorConversion implements SqlOperatorConversion
{
  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder("TRUNCATE")
      .operandTypes(SqlTypeFamily.NUMERIC, SqlTypeFamily.INTEGER)
      .requiredOperands(1)
      .returnTypeInference(ReturnTypes.ARG0)
      .functionCategory(SqlFunctionCategory.NUMERIC)
      .build();

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
    return OperatorConversions.convertCall(
        plannerContext,
        rowSignature,
        rexNode,
        inputExpressions -> {
          final DruidExpression arg = inputExpressions.get(0);
          final Expr digitsExpr = inputExpressions.size() > 1
                                  ? inputExpressions.get(1).parse(plannerContext.getExprMacroTable())
                                  : null;

          final String factorString;

          if (digitsExpr == null) {
            factorString = "1";
          } else if (digitsExpr.isLiteral()) {
            final int digits = ((Number) digitsExpr.getLiteralValue()).intValue();
            final double factor = Math.pow(10, digits);
            factorString = DruidExpression.numberLiteral(factor);
          } else {
            factorString = StringUtils.format("pow(10,%s)", inputExpressions.get(1));
          }

          return DruidExpression.fromExpression(
              StringUtils.format(
                  "(cast(cast(%s * %s,'long'),'double') / %s)",
                  arg.getExpression(),
                  factorString,
                  factorString
              )
          );
        }
    );
  }
}
