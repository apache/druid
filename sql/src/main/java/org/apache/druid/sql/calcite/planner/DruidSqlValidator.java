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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.prepare.BaseDruidSqlValidator;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Util;
import org.apache.commons.collections4.CollectionUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Druid extended SQL validator. (At present, it doesn't actually have any
 * extensions yet, but it will soon.)
 */
class DruidSqlValidator extends BaseDruidSqlValidator
{
  private SqlWindow withinWindow;

  protected DruidSqlValidator(
      SqlOperatorTable opTab,
      CalciteCatalogReader catalogReader,
      JavaTypeFactory typeFactory,
      Config validatorConfig)
  {
    super(opTab, catalogReader, typeFactory, validatorConfig);
  }

  @Override
  public void validateCall(SqlCall call, SqlValidatorScope scope)
  {
    if (call.getKind() == SqlKind.NULLS_FIRST) {
      SqlNode op0 = call.getOperandList().get(0);
      if (op0.getKind() == SqlKind.DESCENDING) {
        throw buildCalciteContextException("DESCENDING ordering with NULLS FIRST is not supported!", call);
      }
    }
    if (call.getKind() == SqlKind.NULLS_LAST) {
      SqlNode op0 = call.getOperandList().get(0);
      if (op0.getKind() != SqlKind.DESCENDING) {
        throw buildCalciteContextException("ASCENDING ordering with NULLS LAST is not supported!", call);
      }
    }
    if (call.getKind() == SqlKind.LAST_VALUE || call.getKind() == SqlKind.FIRST_VALUE) {
      if (!CollectionUtils.isEmpty(withinWindow.getOrderList())) {
        throw buildCalciteContextException("FIRST_VALUE/LAST_VALUE is not supported in ordered WINDOW-s", call);
      }
    }
    super.validateCall(call, scope);
  }

  private CalciteContextException buildCalciteContextException(String message, SqlCall call)
  {
    SqlParserPos pos = call.getParserPosition();
    return new CalciteContextException(message,
        new CalciteException(message, null),
        pos.getLineNum(),
        pos.getColumnNum(),
        pos.getEndLineNum(),
        pos.getEndColumnNum());
  }

  @Override
  public void validateWindow(SqlNode windowOrId, SqlValidatorScope scope, @Nullable SqlCall call)
  {
    // copied from SqlValidatorImpl#validateWindow
    final SqlWindow targetWindow;
    switch (windowOrId.getKind())
    {
    case IDENTIFIER:
      targetWindow = getWindowByName((SqlIdentifier) windowOrId, scope);
      break;
    case WINDOW:
      targetWindow = (SqlWindow) windowOrId;
      break;
    default:
      throw Util.unexpected(windowOrId.getKind());
    }

    try {
      withinWindow = targetWindow;
      super.validateWindow(windowOrId, scope, call);
    } finally {
      withinWindow = null;
    }
  }
}
