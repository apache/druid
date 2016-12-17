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

import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.TimeFormatExtractionFn;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

public class ExtractExpressionConversion extends AbstractExpressionConversion
{
  private static final ExtractExpressionConversion INSTANCE = new ExtractExpressionConversion();

  private ExtractExpressionConversion()
  {
    super(SqlKind.EXTRACT);
  }

  public static ExtractExpressionConversion instance()
  {
    return INSTANCE;
  }

  @Override
  public RowExtraction convert(
      final ExpressionConverter converter,
      final List<String> rowOrder,
      final RexNode expression
  )
  {
    // EXTRACT(timeUnit FROM expr)
    final RexCall call = (RexCall) expression;
    final RexLiteral flag = (RexLiteral) call.getOperands().get(0);
    final TimeUnitRange timeUnit = (TimeUnitRange) flag.getValue();
    final RexNode expr = call.getOperands().get(1);

    final RowExtraction rex = converter.convert(rowOrder, expr);
    if (rex == null) {
      return null;
    }

    final String dateTimeFormat = TimeUnits.toDateTimeFormat(timeUnit);
    if (dateTimeFormat == null) {
      return null;
    }

    final ExtractionFn baseExtractionFn;

    if (call.getOperator().getName().equals("EXTRACT_DATE")) {
      // Expr will be in number of days since the epoch. Can't translate.
      return null;
    } else {
      // Expr will be in millis since the epoch
      baseExtractionFn = rex.getExtractionFn();
    }

    return RowExtraction.of(
        rex.getColumn(),
        ExtractionFns.compose(
            new TimeFormatExtractionFn(dateTimeFormat, null, null, null, true),
            baseExtractionFn
        )
    );
  }
}
