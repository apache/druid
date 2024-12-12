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

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.expression.IPv6AddressMatchExprMacro;
import org.apache.druid.sql.calcite.expression.DirectOperatorConversion;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
 
public class IPv6AddressMatchOperatorConversion extends DirectOperatorConversion
{
  private static final SqlSingleOperandTypeChecker ADDRESS_OPERAND = OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.STRING)
  );

  private static final SqlSingleOperandTypeChecker SUBNET_OPERAND = OperandTypes.family(SqlTypeFamily.STRING);
 
  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder(StringUtils.toUpperCase(IPv6AddressMatchExprMacro.FN_NAME))
      .operandTypeChecker(
      OperandTypes.sequence(
        "'" + StringUtils.toUpperCase(IPv6AddressMatchExprMacro.FN_NAME) + "(expr, string)'",
        ADDRESS_OPERAND,
        SUBNET_OPERAND
      ))
      .returnTypeInference(ReturnTypes.BOOLEAN_NULLABLE)
      .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
      .build();
 
  public IPv6AddressMatchOperatorConversion()
  {
    super(SQL_FUNCTION, IPv6AddressMatchExprMacro.FN_NAME);
  }
 
  @Override
  public SqlOperator calciteOperator()
  {
    return SQL_FUNCTION;
  }
}
