/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.expression;

import com.google.inject.Inject;
import io.druid.query.lookup.LookupReferencesManager;
import io.druid.query.lookup.RegisteredLookupExtractionFn;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.PlannerContext;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public class LookupExtractionOperator implements SqlExtractionOperator
{
  private static final String NAME = "LOOKUP";
  private static final SqlFunction SQL_FUNCTION = new LookupSqlFunction();

  private final LookupReferencesManager lookupReferencesManager;

  @Inject
  public LookupExtractionOperator(final LookupReferencesManager lookupReferencesManager)
  {
    this.lookupReferencesManager = lookupReferencesManager;
  }

  @Override
  public SqlFunction calciteFunction()
  {
    return SQL_FUNCTION;
  }

  @Override
  public RowExtraction convert(
      final DruidOperatorTable operatorTable,
      final PlannerContext plannerContext,
      final List<String> rowOrder,
      final RexNode expression
  )
  {
    final RexCall call = (RexCall) expression;
    final RowExtraction rex = Expressions.toRowExtraction(
        operatorTable,
        plannerContext,
        rowOrder,
        call.getOperands().get(0)
    );
    if (rex == null) {
      return null;
    }

    final String lookupName = RexLiteral.stringValue(call.getOperands().get(1));
    final RegisteredLookupExtractionFn extractionFn = new RegisteredLookupExtractionFn(
        lookupReferencesManager,
        lookupName,
        false,
        null,
        false,
        true
    );

    return RowExtraction.of(
        rex.getColumn(),
        ExtractionFns.compose(extractionFn, rex.getExtractionFn())
    );
  }

  private static class LookupSqlFunction extends SqlFunction
  {
    private static final String SIGNATURE = "'" + NAME + "(expression, lookupName)'\n";

    LookupSqlFunction()
    {
      super(
          NAME,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.VARCHAR),
          null,
          OperandTypes.and(
              OperandTypes.sequence(SIGNATURE, OperandTypes.CHARACTER, OperandTypes.LITERAL),
              OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)
          ),
          SqlFunctionCategory.STRING
      );
    }
  }
}
