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
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.ExpressionTypeConversion;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
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
        boolean isString = false;
        for (int i = 0; i < n; i++) {
          RelDataType type = opBinding.getOperandType(i);
          SqlTypeName sqlTypeName = type.getSqlTypeName();
          ColumnType valueType = Calcites.getColumnTypeForRelDataType(type);

          // Return types are listed in order of preference:
          if (valueType != null) {
            if (valueType.is(ValueType.STRING)) {
              returnSqlTypeName = sqlTypeName;
              isString = true;
              break;
            } else if (valueType.anyOf(ValueType.DOUBLE, ValueType.FLOAT)) {
              returnSqlTypeName = SqlTypeName.DOUBLE;
              hasDouble = true;
            } else if (valueType.is(ValueType.LONG) && !hasDouble) {
              returnSqlTypeName = SqlTypeName.BIGINT;
            }
          } else if (sqlTypeName != SqlTypeName.NULL) {
            throw new IAE("Argument %d has invalid type: %s", i, sqlTypeName);
          }
        }

        if (isString || NullHandling.sqlCompatible()) {
          // String can be null in both modes
          return typeFactory.createTypeWithNullability(typeFactory.createSqlType(returnSqlTypeName), true);
        } else {
          return typeFactory.createSqlType(returnSqlTypeName);
        }
      };
}

