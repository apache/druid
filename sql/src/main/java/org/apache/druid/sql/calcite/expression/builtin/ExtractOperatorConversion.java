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

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.druid.query.expression.TimestampExtractExprMacro;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignature;

import java.util.Map;

public class ExtractOperatorConversion implements SqlOperatorConversion
{
  private static final Map<TimeUnitRange, TimestampExtractExprMacro.Unit> EXTRACT_UNIT_MAP =
      ImmutableMap.<TimeUnitRange, TimestampExtractExprMacro.Unit>builder()
          .put(TimeUnitRange.EPOCH, TimestampExtractExprMacro.Unit.EPOCH)
          .put(TimeUnitRange.MICROSECOND, TimestampExtractExprMacro.Unit.MICROSECOND)
          .put(TimeUnitRange.MILLISECOND, TimestampExtractExprMacro.Unit.MILLISECOND)
          .put(TimeUnitRange.SECOND, TimestampExtractExprMacro.Unit.SECOND)
          .put(TimeUnitRange.MINUTE, TimestampExtractExprMacro.Unit.MINUTE)
          .put(TimeUnitRange.HOUR, TimestampExtractExprMacro.Unit.HOUR)
          .put(TimeUnitRange.DAY, TimestampExtractExprMacro.Unit.DAY)
          .put(TimeUnitRange.DOW, TimestampExtractExprMacro.Unit.DOW)
          .put(TimeUnitRange.ISODOW, TimestampExtractExprMacro.Unit.ISODOW)
          .put(TimeUnitRange.DOY, TimestampExtractExprMacro.Unit.DOY)
          .put(TimeUnitRange.WEEK, TimestampExtractExprMacro.Unit.WEEK)
          .put(TimeUnitRange.MONTH, TimestampExtractExprMacro.Unit.MONTH)
          .put(TimeUnitRange.QUARTER, TimestampExtractExprMacro.Unit.QUARTER)
          .put(TimeUnitRange.YEAR, TimestampExtractExprMacro.Unit.YEAR)
          .put(TimeUnitRange.ISOYEAR, TimestampExtractExprMacro.Unit.ISOYEAR)
          .put(TimeUnitRange.DECADE, TimestampExtractExprMacro.Unit.DECADE)
          .put(TimeUnitRange.CENTURY, TimestampExtractExprMacro.Unit.CENTURY)
          .put(TimeUnitRange.MILLENNIUM, TimestampExtractExprMacro.Unit.MILLENNIUM)
          .build();

  @Override
  public SqlFunction calciteOperator()
  {
    return SqlStdOperatorTable.EXTRACT;
  }

  @Override
  public DruidExpression toDruidExpression(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    // EXTRACT(timeUnit FROM arg)
    final RexCall call = (RexCall) rexNode;
    final RexLiteral flag = (RexLiteral) call.getOperands().get(0);
    final TimeUnitRange calciteUnit = (TimeUnitRange) flag.getValue();
    final RexNode arg = call.getOperands().get(1);

    final DruidExpression input = Expressions.toDruidExpression(plannerContext, rowSignature, arg);
    if (input == null) {
      return null;
    }

    final TimestampExtractExprMacro.Unit druidUnit = EXTRACT_UNIT_MAP.get(calciteUnit);
    if (druidUnit == null) {
      // Don't know how to extract this time unit.
      return null;
    }

    return TimeExtractOperatorConversion.applyTimeExtract(input, druidUnit, plannerContext.getTimeZone());
  }
}
