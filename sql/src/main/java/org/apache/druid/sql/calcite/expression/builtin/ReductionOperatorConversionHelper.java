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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.ExpressionTypeConversion;
import org.apache.druid.math.expr.Function;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.planner.Calcites;

class ReductionOperatorConversionHelper
{
  private ReductionOperatorConversionHelper()
  {
  }

  /**
   * Implements type precedence rules similar to:
   * https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#function_least
   *
   * @see org.apache.druid.math.expr.Function.ReduceFunction#apply
   * @see ExpressionTypeConversion#function
   */
  static final SqlReturnTypeInference TYPE_INFERENCE =
      opBinding -> {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

        final int n = opBinding.getOperandCount();
        if (n == 0) {
          return typeFactory.createSqlType(SqlTypeName.NULL);
        }

        SqlTypeName returnSqlTypeName = SqlTypeName.NULL;
        boolean hasDouble = false;
        for (int i = 0; i < n; i++) {
          final RelDataType type = opBinding.getOperandType(i);
          final SqlTypeName sqlTypeName = type.getSqlTypeName();
          final ColumnType valueType;

          if (SqlTypeName.INTERVAL_TYPES.contains(type.getSqlTypeName())) {
            // handle intervals as a LONG type even though it is a string
            valueType = ColumnType.LONG;
          } else {
            valueType = Calcites.getColumnTypeForRelDataType(type);
          }

          // Return types are listed in order of preference:
          if (valueType != null) {
            if (valueType.is(ValueType.STRING)) {
              returnSqlTypeName = sqlTypeName;
              break;
            } else if (valueType.anyOf(ValueType.DOUBLE, ValueType.FLOAT)) {
              returnSqlTypeName = SqlTypeName.DOUBLE;
              hasDouble = true;
            } else if (valueType.is(ValueType.LONG) && !hasDouble) {
              returnSqlTypeName = SqlTypeName.BIGINT;
            } else {
              // The operand checker of the function should prevent other types from reaching us.
              // Throw a defensive exception if we encounter one.
              throw DruidException.defensive("Got type[%s], which should have been a validation error.", type);
            }
          } else if (sqlTypeName != SqlTypeName.NULL) {
            throw new IAE("Argument %d has invalid type: %s", i, sqlTypeName);
          }
        }

        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(returnSqlTypeName), true);
      };

  /**
   * Type checker that matches the implementation of {@link Function.ReduceFunction}: only accept SQL types that
   * are represented by string or number at the execution layer.
   */
  static final SqlOperandTypeChecker OPERAND_TYPE_CHECKER = new SqlOperandTypeChecker()
  {
    @Override
    public boolean checkOperandTypes(
        final SqlCallBinding callBinding,
        final boolean throwOnFailure
    )
    {
      for (SqlNode operand : callBinding.operands()) {
        final RelDataType type = callBinding.getValidator().deriveType(callBinding.getScope(), operand);
        final boolean validType =
            SqlTypeFamily.STRING.contains(type)
            || SqlTypeFamily.NUMERIC.contains(type)
            || SqlTypeFamily.DATETIME.contains(type)
            || SqlTypeFamily.DATETIME_INTERVAL.contains(type)
            || SqlTypeFamily.BOOLEAN.contains(type)
            || SqlTypeFamily.NULL.contains(type);

        if (!validType) {
          return OperatorConversions.throwOrReturn(
              throwOnFailure,
              callBinding,
              SqlCallBinding::newValidationSignatureError
          );
        }
      }

      return true;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange()
    {
      return SqlOperandCountRanges.any();
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName)
    {
      return opName + "(<STRING | NUMERIC | DATETIME | BOOLEAN | NULL>, ...)";
    }
  };
}

