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

import com.google.common.collect.ImmutableMap;
import io.druid.query.expression.TimestampExtractExprMacro;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTimeZone;

import java.util.Map;

public class TimeExtractOperatorConversion implements SqlOperatorConversion
{
  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder("TIME_EXTRACT")
      .operandTypes(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)
      .requiredOperands(1)
      .returnType(SqlTypeName.BIGINT)
      .functionCategory(SqlFunctionCategory.TIMEDATE)
      .build();

  // Note that QUARTER is not supported here.
  private static final Map<TimestampExtractExprMacro.Unit, String> EXTRACT_FORMAT_MAP =
      ImmutableMap.<TimestampExtractExprMacro.Unit, String>builder()
          .put(TimestampExtractExprMacro.Unit.SECOND, "s")
          .put(TimestampExtractExprMacro.Unit.MINUTE, "m")
          .put(TimestampExtractExprMacro.Unit.HOUR, "H")
          .put(TimestampExtractExprMacro.Unit.DAY, "d")
          .put(TimestampExtractExprMacro.Unit.DOW, "e")
          .put(TimestampExtractExprMacro.Unit.DOY, "D")
          .put(TimestampExtractExprMacro.Unit.WEEK, "w")
          .put(TimestampExtractExprMacro.Unit.MONTH, "M")
          .put(TimestampExtractExprMacro.Unit.YEAR, "Y")
          .build();

  public static DruidExpression applyTimeExtract(
      final DruidExpression timeExpression,
      final TimestampExtractExprMacro.Unit unit,
      final DateTimeZone timeZone
  )
  {
    return timeExpression.map(
        simpleExtraction -> {
          final String formatString = EXTRACT_FORMAT_MAP.get(unit);
          if (formatString == null) {
            return null;
          } else {
            return TimeFormatOperatorConversion.applyTimestampFormat(
                simpleExtraction,
                formatString,
                timeZone
            );
          }
        },
        expression -> String.format(
            "timestamp_extract(%s,%s,%s)",
            expression,
            DruidExpression.stringLiteral(unit.name()),
            DruidExpression.stringLiteral(timeZone.getID())
        )
    );
  }

  @Override
  public SqlFunction calciteOperator()
  {
    return SQL_FUNCTION;
  }

  @Override
  public DruidExpression toDruidExpression(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    final RexCall call = (RexCall) rexNode;
    final RexNode timeArg = call.getOperands().get(0);
    final DruidExpression timeExpression = Expressions.toDruidExpression(plannerContext, rowSignature, timeArg);
    if (timeExpression == null) {
      return null;
    }

    final TimestampExtractExprMacro.Unit unit = TimestampExtractExprMacro.Unit.valueOf(
        RexLiteral.stringValue(call.getOperands().get(1)).toUpperCase()
    );

    final DateTimeZone timeZone = call.getOperands().size() > 2 && !RexLiteral.isNullLiteral(call.getOperands().get(2))
                                  ? DateTimeZone.forID(RexLiteral.stringValue(call.getOperands().get(2)))
                                  : plannerContext.getTimeZone();

    return applyTimeExtract(timeExpression, unit, timeZone);
  }
}
