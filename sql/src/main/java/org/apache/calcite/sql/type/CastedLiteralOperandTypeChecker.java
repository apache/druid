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

//CHECKSTYLE.OFF: PackageName - Must be in Calcite

package org.apache.calcite.sql.type;

import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.Util;

/**
 * Like {@link LiteralOperandTypeChecker}, but also allows casted literals.
 *
 * "Casted literals" are like `CAST(100 AS INTEGER)`. While it doesn't make sense to cast a literal that the user
 * themselves enter, it is important to add a broader validation to allow these literals because Calcite's JDBC driver
 * doesn't allow the wildcards (?)to work without a cast, and there's no workaround it.
 * <p>
 * This makes sure that the functions using the literal operand type checker can be workaround the JDBC's restriction,
 * without being marked as invalid SQL input
 */

public class CastedLiteralOperandTypeChecker implements SqlSingleOperandTypeChecker
{
  public static SqlSingleOperandTypeChecker LITERAL = new CastedLiteralOperandTypeChecker(false);

  private final boolean allowNull;

  CastedLiteralOperandTypeChecker(boolean allowNull)
  {
    this.allowNull = allowNull;
  }

  @Override
  public boolean checkSingleOperandType(
      SqlCallBinding callBinding,
      SqlNode node,
      int iFormalOperand,
      boolean throwOnFailure
  )
  {
    Util.discard(iFormalOperand);

    if (SqlUtil.isNullLiteral(node, true)) {
      if (allowNull) {
        return true;
      }
      if (throwOnFailure) {
        throw callBinding.newError(
            Static.RESOURCE.argumentMustNotBeNull(
                callBinding.getOperator().getName()));
      }
      return false;
    }
    // The following line of code is the only difference between the OperandTypes.LITERAL and this type checker
    if (!SqlUtil.isLiteral(node, true) && !SqlUtil.isLiteralChain(node)) {
      if (throwOnFailure) {
        throw callBinding.newError(
            Static.RESOURCE.argumentMustBeLiteral(
                callBinding.getOperator().getName()));
      }
      return false;
    }

    return true;
  }

  @Override
  public String getAllowedSignatures(SqlOperator op, String opName)
  {
    return "<LITERAL>";
  }
}
