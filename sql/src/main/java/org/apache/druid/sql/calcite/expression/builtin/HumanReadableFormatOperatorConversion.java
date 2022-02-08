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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;

public class HumanReadableFormatOperatorConversion implements SqlOperatorConversion
{
  public static final SqlOperatorConversion BINARY_BYTE_FORMAT = new HumanReadableFormatOperatorConversion("human_readable_binary_byte_format");
  public static final SqlOperatorConversion DECIMAL_BYTE_FORMAT = new HumanReadableFormatOperatorConversion("human_readable_decimal_byte_format");
  public static final SqlOperatorConversion DECIMAL_FORMAT = new HumanReadableFormatOperatorConversion("human_readable_decimal_format");

  private final String name;
  private final SqlFunction sqlFunction;

  private HumanReadableFormatOperatorConversion(String name)
  {
    this.sqlFunction = OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(name))
        .operandTypeChecker(new HumanReadableFormatOperandTypeChecker())
        .functionCategory(SqlFunctionCategory.STRING)
        .returnTypeCascadeNullable(SqlTypeName.VARCHAR)
        .build();

    this.name = name;
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return sqlFunction;
  }

  @Override
  public DruidExpression toDruidExpression(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    return OperatorConversions.convertDirectCall(plannerContext, rowSignature, rexNode, name);
  }

  private static class HumanReadableFormatOperandTypeChecker implements SqlOperandTypeChecker
  {
    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure)
    {
      boolean isSigatureError = false;
      final RelDataType firstArgType = callBinding.getOperandType(0);
      if (!SqlTypeName.NUMERIC_TYPES.contains(firstArgType.getSqlTypeName())) {
        isSigatureError = true;
      }
      if (callBinding.getOperandCount() > 1) {
        final RelDataType secondArgType = callBinding.getOperandType(1);
        if (!SqlTypeName.NUMERIC_TYPES.contains(secondArgType.getSqlTypeName())) {
          isSigatureError = true;
        }
      }
      if (isSigatureError && throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      } else {
        return isSigatureError;
      }
    }

    @Override
    public SqlOperandCountRange getOperandCountRange()
    {
      return SqlOperandCountRanges.between(1, 2);
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName)
    {
      return StringUtils.format("%s(Number, [Precision])", opName);
    }

    @Override
    public Consistency getConsistency()
    {
      return Consistency.NONE;
    }

    @Override
    public boolean isOptional(int i)
    {
      return i > 0;
    }
  }
}
