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
import org.apache.calcite.sql.type.SqlTypeUtil;

class ReductionOperatorConversionHelper
{
  private ReductionOperatorConversionHelper()
  {
  }

  /**
   * Implements rules similar to: https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#function_least
   *
   * @see org.apache.druid.math.expr.Function.ReduceFunc#apply
   * @see org.apache.druid.math.expr.Function.ReduceFunc#getComparisionType
   */
  static final SqlReturnTypeInference TYPE_INFERENCE =
      opBinding -> {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

        final int n = opBinding.getOperandCount();
        if (n == 0) {
          return typeFactory.createSqlType(SqlTypeName.NULL);
        }

        boolean hasDouble = false;
        for (int i = 0; i < n; i++) {
          RelDataType type = opBinding.getOperandType(i);
          if (SqlTypeUtil.isString(type) || SqlTypeUtil.isCharacter(type)) {
            return type;
          } else if (SqlTypeUtil.isDouble(type)) {
            hasDouble = true;
          }
        }

        return typeFactory.createSqlType(hasDouble ? SqlTypeName.DOUBLE : SqlTypeName.BIGINT);
      };
}
