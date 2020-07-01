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
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.TimestampFloorExprMacro;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.expression.TimeUnits;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TimeFloorOperatorConversion implements SqlOperatorConversion
{
  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder("TIME_FLOOR")
      .operandTypes(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER, SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER)
      .requiredOperands(2)
      .returnTypeNonNull(SqlTypeName.TIMESTAMP)
      .functionCategory(SqlFunctionCategory.TIMEDATE)
      .build();

  /**
   * Function that floors a DruidExpression to a particular granularity. Not actually used by the
   * TimeFloorOperatorConversion, but I'm not sure where else to put this. It makes some sense in this file, since
   * it's responsible for generating "timestamp_floor" calls.
   */
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

  /**
   * Function that converts SQL TIME_FLOOR or TIME_CEIL args to Druid expression "timestamp_floor" or "timestamp_ceil"
   * args. The main reason this function is necessary is because the handling of origin and timezone must take into
   * account the SQL context timezone. It also helps with handling SQL FLOOR and CEIL, by offering handling of
   * TimeUnitRange args.
   */
  @Nullable
  public static List<DruidExpression> toTimestampFloorOrCeilArgs(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final List<RexNode> operands
  )
  {
    final List<DruidExpression> functionArgs = new ArrayList<>();

    // Timestamp
    functionArgs.add(Expressions.toDruidExpression(plannerContext, rowSignature, operands.get(0)));

    // Period
    final RexNode periodOperand = operands.get(1);
    if (periodOperand.isA(SqlKind.LITERAL) && RexLiteral.value(periodOperand) instanceof TimeUnitRange) {
      // TimeUnitRange literals are used by FLOOR(t TO unit) and CEIL(t TO unit)
      final Period period = TimeUnits.toPeriod((TimeUnitRange) RexLiteral.value(periodOperand));

      if (period == null) {
        // Unrecognized time unit, bail out.
        return null;
      }

      functionArgs.add(DruidExpression.fromExpression(DruidExpression.stringLiteral(period.toString())));
    } else {
      // Other literal types are used by TIME_FLOOR and TIME_CEIL
      functionArgs.add(Expressions.toDruidExpression(plannerContext, rowSignature, periodOperand));
    }

    // Origin
    functionArgs.add(
        OperatorConversions.getOperandWithDefault(
            operands,
            2,
            operand -> {
              if (operand.isA(SqlKind.LITERAL)) {
                return DruidExpression.fromExpression(
                    DruidExpression.numberLiteral(
                        Calcites.calciteDateTimeLiteralToJoda(operand, plannerContext.getTimeZone()).getMillis()
                    )
                );
              } else {
                return Expressions.toDruidExpression(plannerContext, rowSignature, operand);
              }
            },
            DruidExpression.fromExpression(DruidExpression.nullLiteral())
        )
    );

    // Time zone
    functionArgs.add(
        OperatorConversions.getOperandWithDefault(
            operands,
            3,
            operand -> Expressions.toDruidExpression(plannerContext, rowSignature, operand),
            DruidExpression.fromExpression(DruidExpression.stringLiteral(plannerContext.getTimeZone().getID()))
        )
    );

    return functionArgs.stream().noneMatch(Objects::isNull) ? functionArgs : null;
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
  @Nullable
  public DruidExpression toDruidExpression(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    final RexCall call = (RexCall) rexNode;
    final List<DruidExpression> functionArgs = toTimestampFloorOrCeilArgs(
        plannerContext,
        rowSignature,
        call.getOperands()
    );

    if (functionArgs == null) {
      return null;
    }

    return DruidExpression.fromFunctionCall("timestamp_floor", functionArgs);
  }
}
