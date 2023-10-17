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

package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.druid.error.InvalidSqlInput;

public abstract class DruidSqlPostfixOperator extends SqlPostfixOperator {

  public static final SqlPostfixOperator DRUID_NULLS_FIRST = new DruidSqlPostfixOperator(SqlStdOperatorTable.NULLS_FIRST) {
    @Override
    void validateCall(SqlCall call)
    {
      if (call.getOperandList().get(0).getKind() == SqlKind.DESCENDING) {
        throw InvalidSqlInput.exception("DESCENDING ordering with NULLS FIRST is not supported!");
      }
    }
  };

  public static final SqlPostfixOperator DRUID_NULLS_LAST = new DruidSqlPostfixOperator(SqlStdOperatorTable.NULLS_LAST) {
    @Override
    void validateCall(SqlCall call)
    {
      if (call.getOperandList().get(0).getKind() != SqlKind.DESCENDING) {
        throw InvalidSqlInput.exception("ASCENDING ordering with NULLS LAST is not supported!");
      }
    }
  };

  public DruidSqlPostfixOperator(SqlPostfixOperator core)
  {
    super(core.getName(),
        core.getKind(),
        core.getLeftPrec(),
        core.getReturnTypeInference(),
        core.getOperandTypeInference(),
        core.getOperandTypeChecker()
        );

      assert(core.getLeftPrec() == getLeftPrec());
      assert(core.getRightPrec() == getRightPrec());
  }

  abstract void validateCall(SqlCall call);

  @Override
  public final void validateCall(SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope)
  {
    validateCall(call);
    super.validateCall(call, validator, scope, operandScope);
  }
}