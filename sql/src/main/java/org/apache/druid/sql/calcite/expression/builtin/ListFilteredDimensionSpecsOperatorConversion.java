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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import java.util.ArrayList;
import java.util.List;

public class ListFilteredDimensionSpecsOperatorConversion implements SqlOperatorConversion
{
  private static final String FUNC_NAME = "LIST_FILTER";

  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder(FUNC_NAME)
      .operandTypeChecker(new ListFilteredOperandTypeChecker())
      .functionCategory(SqlFunctionCategory.STRING)
      .returnTypeNonNull(SqlTypeName.VARCHAR)
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
    final RexCall call = (RexCall) rexNode;
    final DruidExpression input = Expressions.toDruidExpression(
        plannerContext,
        rowSignature,
        call.getOperands().get(0)
    );

    if (input == null || !input.isDirectColumnAccess()) {
      throw new IAE("first operand expected direct column, got [%s]", input);
    }

    final List<RexNode> operands = call.getOperands();
    List<String> values = new ArrayList<>();
    for (int i = 1; i < operands.size(); i++) {
      RexNode operand = operands.get(i);
      values.add(RexLiteral.stringValue(operand));
    }

    return input.map(
      simpleExtraction -> simpleExtraction.toFDSOExtraction(FUNC_NAME, values),
      expression -> StringUtils.format(
              "%s",
              input.getDirectColumn()
      )
    );
  }


  public static class ListFilteredOperandTypeChecker implements SqlOperandTypeChecker
  {
    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure)
    {
      for (int i = 0; i < callBinding.getOperandCount(); i++) {
        final RelDataType firstArgType = callBinding.getOperandType(i);
        if (!SqlTypeName.CHAR_TYPES.contains(firstArgType.getSqlTypeName())) {
          if (throwOnFailure) {
            throw callBinding.newValidationSignatureError();
          } else {
            return false;
          }
        }
      }
      return true;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange()
    {
      return SqlOperandCountRanges.from(2);
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName)
    {
      return StringUtils.format("%s(CHARACTER, CHARACTER, [ANY, ...])", opName);
    }

    @Override
    public Consistency getConsistency()
    {
      return Consistency.NONE;
    }

    @Override
    public boolean isOptional(int i)
    {
      return i > 1;
    }
  }


}
