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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.TimestampFloorExprMacro;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignature;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TimeFloorOperatorConversion implements SqlOperatorConversion
{
  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder("TIME_FLOOR")
      .operandTypes(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER, SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER)
      .requiredOperands(2)
      .returnType(SqlTypeName.TIMESTAMP)
      .functionCategory(SqlFunctionCategory.TIMEDATE)
      .build();

  public static DruidExpression applyTimestampFloor(
      final DruidExpression input,
      final PeriodGranularity granularity,
      final ExprMacroTable macroTable
  )
  {
    Preconditions.checkNotNull(input, "input");
    Preconditions.checkNotNull(granularity, "granularity");

    // Collapse floor chains if possible. Useful for constructs like CAST(FLOOR(__time TO QUARTER) AS DATE).
    if (granularity.getPeriod().equals(Period.days(1))) {
      final TimestampFloorExprMacro.TimestampFloorExpr floorExpr = Expressions.asTimestampFloorExpr(
          input,
          macroTable
      );

      if (floorExpr != null) {
        final PeriodGranularity inputGranularity = floorExpr.getGranularity();
        if (Objects.equals(inputGranularity.getTimeZone(), granularity.getTimeZone())
            && Objects.equals(inputGranularity.getOrigin(), granularity.getOrigin())
            && periodIsDayMultiple(inputGranularity.getPeriod())) {
          return input;
        }
      }
    }

    return DruidExpression.fromFunctionCall(
        "timestamp_floor",
        ImmutableList.of(
            input.getExpression(),
            DruidExpression.stringLiteral(granularity.getPeriod().toString()),
            DruidExpression.numberLiteral(
                granularity.getOrigin() == null ? null : granularity.getOrigin().getMillis()
            ),
            DruidExpression.stringLiteral(granularity.getTimeZone().toString())
        ).stream().map(DruidExpression::fromExpression).collect(Collectors.toList())
    );
  }

  private static boolean periodIsDayMultiple(final Period period)
  {
    return period.getMillis() == 0
           && period.getSeconds() == 0
           && period.getMinutes() == 0
           && period.getHours() == 0
           && (period.getDays() > 0 || period.getWeeks() > 0 || period.getMonths() > 0 || period.getYears() > 0);
  }

  @Override
  public SqlOperator calciteOperator()
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
    final List<RexNode> operands = call.getOperands();
    final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
        plannerContext,
        rowSignature,
        operands
    );

    if (druidExpressions == null) {
      return null;
    } else if (operands.get(1).isA(SqlKind.LITERAL)
               && (operands.size() <= 2 || operands.get(2).isA(SqlKind.LITERAL))
               && (operands.size() <= 3 || operands.get(3).isA(SqlKind.LITERAL))) {
      // Granularity is a literal. Special case since we can use an extractionFn here.
      final Period period = new Period(RexLiteral.stringValue(operands.get(1)));

      final DateTime origin = OperatorConversions.getOperandWithDefault(
          call.getOperands(),
          2,
          operand -> Calcites.calciteDateTimeLiteralToJoda(operands.get(2), plannerContext.getTimeZone()),
          null
      );

      final DateTimeZone timeZone = OperatorConversions.getOperandWithDefault(
          call.getOperands(),
          3,
          operand -> DateTimes.inferTzFromString(RexLiteral.stringValue(operand)),
          plannerContext.getTimeZone()
      );

      final PeriodGranularity granularity = new PeriodGranularity(period, origin, timeZone);
      return applyTimestampFloor(druidExpressions.get(0), granularity, plannerContext.getExprMacroTable());
    } else {
      // Granularity is dynamic
      return DruidExpression.fromFunctionCall("timestamp_floor", druidExpressions);
    }
  }
}
