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

public class StringFormatOperatorConversion implements SqlOperatorConversion
{
  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder("STRING_FORMAT")
      .operandTypeChecker(new StringFormatOperandTypeChecker())
      .functionCategory(SqlFunctionCategory.STRING)
      .returnTypeNonNull(SqlTypeName.VARCHAR)
      .build();

  @Override
  public SqlOperator calciteOperator()
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
    return OperatorConversions.convertCall(plannerContext, rowSignature, rexNode, "format");
  }

  private static class StringFormatOperandTypeChecker implements SqlOperandTypeChecker
  {
    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure)
    {
      final RelDataType firstArgType = callBinding.getOperandType(0);
      if (SqlTypeName.CHAR_TYPES.contains(firstArgType.getSqlTypeName())) {
        return true;
      } else {
        if (throwOnFailure) {
          throw callBinding.newValidationSignatureError();
        } else {
          return false;
        }
      }
    }

    @Override
    public SqlOperandCountRange getOperandCountRange()
    {
      return SqlOperandCountRanges.from(1);
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName)
    {
      return StringUtils.format("%s(CHARACTER, [ANY, ...])", opName);
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
