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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Static;
import org.apache.druid.error.DruidException;

import java.math.BigDecimal;

public class CastedLiteralOperandTypeCheckers
{
  public static final SqlSingleOperandTypeChecker LITERAL = new CastedLiteralOperandTypeChecker(false);

  /**
   * Blatantly copied from {@link OperandTypes#POSITIVE_INTEGER_LITERAL}, however the reference to the {@link #LITERAL}
   * is the one which accepts casted literals
   */
  public static final SqlSingleOperandTypeChecker POSITIVE_INTEGER_LITERAL =
      new FamilyOperandTypeChecker(
          ImmutableList.of(SqlTypeFamily.INTEGER),
          i -> false
      )
      {
        @Override
        public boolean checkSingleOperandType(
            SqlCallBinding callBinding,
            SqlNode operand,
            int iFormalOperand,
            SqlTypeFamily family,
            boolean throwOnFailure
        )
        {
          // This LITERAL refers to the above implementation, the one which allows casted literals
          if (!LITERAL.checkSingleOperandType(
              callBinding,
              operand,
              iFormalOperand,
              throwOnFailure
          )) {
            return false;
          }

          if (!super.checkSingleOperandType(
              callBinding,
              operand,
              iFormalOperand,
              family,
              throwOnFailure
          )) {
            return false;
          }

          final SqlLiteral arg = fetchPrimitiveLiteralFromCasts(operand);
          final BigDecimal value = arg.getValueAs(BigDecimal.class);
          if (value.compareTo(BigDecimal.ZERO) < 0
              || hasFractionalPart(value)) {
            if (throwOnFailure) {
              throw callBinding.newError(
                  Static.RESOURCE.argumentMustBePositiveInteger(
                      callBinding.getOperator().getName()));
            }
            return false;
          }
          if (value.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0) {
            if (throwOnFailure) {
              throw callBinding.newError(
                  Static.RESOURCE.numberLiteralOutOfRange(value.toString()));
            }
            return false;
          }
          return true;
        }

        /** Returns whether a number has any fractional part.
         *
         * @see BigDecimal#longValueExact() */
        private boolean hasFractionalPart(BigDecimal bd)
        {
          return bd.precision() - bd.scale() <= 0;
        }
      };

  /**
   * Fetches primitive literals from the casts, including NULL literal.
   * It throws if the entered node isn't a primitive literal, which can be cast multiple times.
   *
   * Therefore, it would fail on the following types:
   *  1. Nodes that are not of the form CAST(....(CAST LITERAL AS TYPE).....)
   *  2. ARRAY and MAP literals. This won't be required since we are only using this method in the type checker for
   *      primitive types
   */
  private static SqlLiteral fetchPrimitiveLiteralFromCasts(SqlNode node)
  {
    if (node == null) {
      throw DruidException.defensive("'node' cannot be null");
    }
    if (node instanceof SqlLiteral) {
      return (SqlLiteral) node;
    }

    switch (node.getKind()) {
      case CAST:
        return fetchPrimitiveLiteralFromCasts(((SqlCall) node).operand(0));
      case DEFAULT:
        return SqlLiteral.createNull(SqlParserPos.ZERO);
      default:
        throw DruidException.defensive("Expected a literal or a cast on the literal. Found [%s] instead", node.getKind());
    }
  }
}
