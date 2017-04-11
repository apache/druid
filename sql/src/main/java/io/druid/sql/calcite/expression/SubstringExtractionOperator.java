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

import io.druid.query.extraction.SubstringDimExtractionFn;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.PlannerContext;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.List;

public class SubstringExtractionOperator implements SqlExtractionOperator
{
  @Override
  public SqlFunction calciteFunction()
  {
    return SqlStdOperatorTable.SUBSTRING;
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
    final RowExtraction arg = Expressions.toRowExtraction(
        operatorTable,
        plannerContext,
        rowOrder,
        call.getOperands().get(0)
    );
    if (arg == null) {
      return null;
    }
    final int index = RexLiteral.intValue(call.getOperands().get(1)) - 1;
    final Integer length;
    if (call.getOperands().size() > 2) {
      length = RexLiteral.intValue(call.getOperands().get(2));
    } else {
      length = null;
    }

    return RowExtraction.of(
        arg.getColumn(),
        ExtractionFns.compose(
            new SubstringDimExtractionFn(index, length),
            arg.getExtractionFn()
        )
    );
  }
}
