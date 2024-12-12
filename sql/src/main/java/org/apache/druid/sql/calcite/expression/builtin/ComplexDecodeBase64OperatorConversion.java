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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.BuiltInExprMacros;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;

public class ComplexDecodeBase64OperatorConversion implements SqlOperatorConversion
{

  public static final SqlReturnTypeInference ARBITRARY_COMPLEX_RETURN_TYPE_INFERENCE = opBinding -> {
    String typeName = opBinding.getOperandLiteralValue(0, String.class);
    return RowSignatures.makeComplexType(
        opBinding.getTypeFactory(),
        ColumnType.ofComplex(typeName),
        true
    );
  };

  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder(StringUtils.toUpperCase(BuiltInExprMacros.ComplexDecodeBase64ExprMacro.NAME))
      .operandNames("typeName", "base64")
      .operandTypes(SqlTypeFamily.STRING, SqlTypeFamily.ANY)
      .requiredOperandCount(2)
      .literalOperands(0)
      .returnTypeInference(ARBITRARY_COMPLEX_RETURN_TYPE_INFERENCE)
      .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
      .build();


  @Override
  public SqlOperator calciteOperator()
  {
    return SQL_FUNCTION;
  }

  @Nullable
  @Override
  public DruidExpression toDruidExpression(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      RexNode rexNode
  )
  {
    return OperatorConversions.convertCall(
        plannerContext,
        rowSignature,
        rexNode,
        druidExpressions -> {
          String arg0 = druidExpressions.get(0).getExpression();
          return DruidExpression.ofExpression(
              ColumnType.ofComplex(arg0.substring(1, arg0.length() - 1)),
              DruidExpression.functionCall(BuiltInExprMacros.ComplexDecodeBase64ExprMacro.NAME),
              druidExpressions
          );
        }
    );
  }
}
