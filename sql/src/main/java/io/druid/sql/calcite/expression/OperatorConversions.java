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

import com.google.common.base.Preconditions;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Utilities for assisting in writing {@link SqlOperatorConversion} implementations.
 */
public class OperatorConversions
{
  public static DruidExpression functionCall(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      final String functionName
  )
  {
    return functionCall(plannerContext, rowSignature, rexNode, functionName, null);
  }

  public static DruidExpression functionCall(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      final String functionName,
      final Function<List<DruidExpression>, SimpleExtraction> simpleExtractionFunction
  )
  {
    final RexCall call = (RexCall) rexNode;

    final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
        plannerContext,
        rowSignature,
        call.getOperands()
    );

    if (druidExpressions == null) {
      return null;
    }

    return DruidExpression.of(
        simpleExtractionFunction == null ? null : simpleExtractionFunction.apply(druidExpressions),
        DruidExpression.functionCall(functionName, druidExpressions)
    );
  }

  public static OperatorBuilder operatorBuilder(final String name)
  {
    return new OperatorBuilder(name);
  }

  public static class OperatorBuilder
  {
    private String name;
    private SqlKind kind = SqlKind.OTHER_FUNCTION;
    private SqlReturnTypeInference returnTypeInference;
    private SqlFunctionCategory functionCategory = SqlFunctionCategory.USER_DEFINED_FUNCTION;

    // For operand type checking
    private List<SqlTypeFamily> operandTypes;
    private int requiredOperands = Integer.MAX_VALUE;

    private OperatorBuilder(final String name)
    {
      this.name = Preconditions.checkNotNull(name, "name");
    }

    public OperatorBuilder kind(final SqlKind kind)
    {
      this.kind = kind;
      return this;
    }

    public OperatorBuilder returnType(final SqlTypeName typeName)
    {
      this.returnTypeInference = ReturnTypes.explicit(typeName);
      return this;
    }

    public OperatorBuilder nullableReturnType(final SqlTypeName typeName)
    {
      this.returnTypeInference = ReturnTypes.explicit(
          factory ->
              factory.createTypeWithNullability(
                  factory.createSqlType(typeName),
                  true
              )
      );
      return this;
    }

    public OperatorBuilder functionCategory(final SqlFunctionCategory functionCategory)
    {
      this.functionCategory = functionCategory;
      return this;
    }

    public OperatorBuilder operandTypes(final SqlTypeFamily... operandTypes)
    {
      this.operandTypes = Arrays.asList(operandTypes);
      return this;
    }

    public OperatorBuilder requiredOperands(final int requiredOperands)
    {
      this.requiredOperands = requiredOperands;
      return this;
    }

    public SqlFunction build()
    {
      return new SqlFunction(
          name,
          kind,
          Preconditions.checkNotNull(returnTypeInference, "returnTypeInference"),
          null,
          OperandTypes.family(
              Preconditions.checkNotNull(operandTypes, "operandTypes"),
              i -> i + 1 > requiredOperands
          ),
          functionCategory
      );
    }
  }
}
