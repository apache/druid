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
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.UnsupportedSQLQueryException;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class EvalOperatorConversion implements SqlOperatorConversion
{

  private final SqlFunction sqlFunction;

  public EvalOperatorConversion(final ExprMacroTable macroTable)
  {
    final SqlReturnTypeInference expressionReturnTypeInference = opBinding -> {
      if (opBinding instanceof SqlCallBinding) {
        SqlCallBinding callBinding = (SqlCallBinding) opBinding;
        final String expression = opBinding.getOperandLiteralValue(0, String.class);
        Map<String, ExpressionType> typeMap = new HashMap<>();
        for (int i = 1; i < callBinding.getOperandCount(); i++) {
          if (callBinding.operand(i) instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) callBinding.operand(i);
            String[] split = identifier.toString().split("\\.");
            typeMap.put(
                split[split.length - 1],
                ExpressionType.fromColumnType(Calcites.getColumnTypeForRelDataType(opBinding.getOperandType(i)))
            );
          } else {
            throw new IAE("EVAL arguments must be identifiers, no expressions or literals allowed");
          }
        }
        Expr expr = Parser.parse(expression, macroTable);
        for (String inputBinding : expr.analyzeInputs().getRequiredBindings()) {
          if (!typeMap.containsKey(inputBinding)) {
            throw new IAE("EVAL must be supplied with all required inputs as arguments, missing [%s]", inputBinding);
          }
        }
        final ExpressionType expressionType = expr.getOutputType(InputBindings.inspectorFromTypeMap(typeMap));
        if (expressionType != null) {
          return Calcites.getRelDataTypeForColumnType(
              ExpressionType.toColumnType(expressionType),
              opBinding.getTypeFactory()
          );
        }
      }
      return Calcites.createSqlTypeWithNullability(opBinding.getTypeFactory(), SqlTypeName.ANY, true);
    };
    this.sqlFunction = OperatorConversions
        .operatorBuilder("EVAL")
        .operandTypeChecker(OperandTypes.variadic(SqlOperandCountRanges.any()))
        .returnTypeInference(expressionReturnTypeInference)
        .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
        .build();
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return sqlFunction;
  }

  @Nullable
  @Override
  public DruidExpression toDruidExpression(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      RexNode rexNode
  )
  {
    if (!QueryContexts.parseBoolean(plannerContext.queryContextMap(), QueryContexts.CTX_SQL_ALLOW_EVAL, false)) {
      throw new UnsupportedSQLQueryException(
          "'EVAL' is not enabled, the query context parameter '%s' must be set to true.",
          QueryContexts.CTX_SQL_ALLOW_EVAL
      );
    }
    return OperatorConversions.convertCall(
        plannerContext,
        rowSignature,
        rexNode,
        druidExpressions -> {
          final String arg0 = druidExpressions.get(0).getExpression();
          final String expression = arg0.substring(1, arg0.length() - 1);
          // ugly stuff shows up sometimes in our strings, fix them
          final String adjustedExpression = expression.indexOf('\\') >= 0 ? StringEscapeUtils.unescapeJava(expression) : expression;
          return DruidExpression.ofExpression(
              Calcites.getColumnTypeForRelDataType(rexNode.getType()),
              (args) -> adjustedExpression,
              druidExpressions
          );
        }
    );
  }

  // todo: this should 100% live somewhere else, maybe Calcites? Very similar to some other code in other places, e.g. RowSignatures

}
