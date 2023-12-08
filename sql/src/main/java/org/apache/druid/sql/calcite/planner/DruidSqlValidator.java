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

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.prepare.BaseDruidSqlValidator;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeMappingRule;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.run.EngineFeature;

import java.util.Map;

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

  @Override
  public SqlTypeMappingRule getTypeMappingRule()
  {
    SqlTypeMappingRule base = super.getTypeMappingRule();
    return new SqlTypeMappingRule()
    {
      @Override
      public Map<SqlTypeName, ImmutableSet<SqlTypeName>> getTypeMapping()
      {
        return base.getTypeMapping();
      }

      @Override
      public boolean canApplyFrom(SqlTypeName to, SqlTypeName from)
      {
        return SqlTypeMappingRule.super.canApplyFrom(to, from);
      }
    };
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
}
