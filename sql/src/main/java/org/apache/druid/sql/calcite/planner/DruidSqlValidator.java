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
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Util;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Druid extended SQL validator. (At present, it doesn't actually
 * have any extensions yet, but it will soon.)
 */
class DruidSqlValidator extends BaseDruidSqlValidator
{
  private final PlannerContext plannerContext;

  protected DruidSqlValidator(
      SqlOperatorTable opTab,
      CalciteCatalogReader catalogReader,
      JavaTypeFactory typeFactory,
      Config validatorConfig,
      PlannerContext plannerContext
  )
  {
    super(opTab, catalogReader, typeFactory, validatorConfig);
    this.plannerContext = plannerContext;
  }

  @Override
  public void validateWindow(SqlNode windowOrId, SqlValidatorScope scope, @Nullable SqlCall call)
  {
    final SqlWindow targetWindow;
    switch (windowOrId.getKind()) {
      case IDENTIFIER:
        targetWindow = getWindowByName((SqlIdentifier) windowOrId, scope);
        break;
      case WINDOW:
        targetWindow = (SqlWindow) windowOrId;
        break;
      default:
        throw Util.unexpected(windowOrId.getKind());
    }


    @Nullable
    SqlNode lowerBound = targetWindow.getLowerBound();
    @Nullable
    SqlNode upperBound = targetWindow.getUpperBound();
    if (!isValidEndpoint(lowerBound) || !isValidEndpoint(upperBound)) {
      throw buildCalciteContextException(
          "Window frames with expression based lower/upper bounds are not supported.",
          windowOrId
      );
    }

    if (isPrecedingOrFollowing(lowerBound) &&
        isPrecedingOrFollowing(upperBound) &&
        lowerBound.getKind() == upperBound.getKind()) {
      // this limitation can be lifted when https://github.com/apache/druid/issues/15739 is addressed
      throw buildCalciteContextException(
          "Query bounds with both lower and upper bounds as PRECEDING or FOLLOWING is not supported.",
          windowOrId
      );
    }

    if (plannerContext.queryContext().isWindowingStrictValidation()) {
      if (!targetWindow.isRows() &&
          (!isValidRangeEndpoint(lowerBound) || !isValidRangeEndpoint(upperBound))) {
        // this limitation can be lifted when https://github.com/apache/druid/issues/15767 is addressed
        throw buildCalciteContextException(
            StringUtils.format(
                "The query contains a window frame which may return incorrect results. To disregard this warning, set [%s] to false in the query context.",
                QueryContexts.WINDOWING_STRICT_VALIDATION
            ),
            windowOrId
        );
      }
    }

    super.validateWindow(windowOrId, scope, call);
  }

  private boolean isPrecedingOrFollowing(@Nullable SqlNode bound)
  {
    if (bound == null) {
      return false;
    }
    SqlKind kind = bound.getKind();
    return kind == SqlKind.PRECEDING || kind == SqlKind.FOLLOWING;
  }

  /**
   * Checks if the given endpoint is acceptable.
   */
  private boolean isValidEndpoint(@Nullable SqlNode bound)
  {
    if (isValidRangeEndpoint(bound)) {
      return true;
    }
    if (bound.getKind() == SqlKind.FOLLOWING || bound.getKind() == SqlKind.PRECEDING) {
      final SqlNode boundVal = ((SqlCall) bound).operand(0);
      if (SqlUtil.isLiteral(boundVal)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if the given endpoint is valid for a RANGE window frame.
   */
  private boolean isValidRangeEndpoint(@Nullable SqlNode bound)
  {
    return bound == null
        || SqlWindow.isCurrentRow(bound)
        || SqlWindow.isUnboundedFollowing(bound)
        || SqlWindow.isUnboundedPreceding(bound);
  }

  @Override
  public void validateCall(SqlCall call, SqlValidatorScope scope)
  {
    if (call.getKind() == SqlKind.OVER) {
      if (!plannerContext.featureAvailable(EngineFeature.WINDOW_FUNCTIONS)) {
        throw buildCalciteContextException(
            StringUtils.format(
                "The query contains window functions; To run these window functions, specify [%s] in query context.",
                PlannerContext.CTX_ENABLE_WINDOW_FNS),
            call);
      }
    }
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
    super.validateCall(call, scope);
  }

  private CalciteContextException buildCalciteContextException(String message, SqlNode call)
  {
    SqlParserPos pos = call.getParserPosition();
    return new CalciteContextException(message,
        new CalciteException(message, null),
        pos.getLineNum(),
        pos.getColumnNum(),
        pos.getEndLineNum(),
        pos.getEndColumnNum());
  }
}
