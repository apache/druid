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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.druid.java.util.common.granularity.PeriodGranularity;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import java.util.List;
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
      final PeriodGranularity granularity
  )
  {
    Preconditions.checkNotNull(input, "input");
    Preconditions.checkNotNull(granularity, "granularity");

    return input.map(
        simpleExtraction -> simpleExtraction.cascade(ExtractionFns.fromQueryGranularity(granularity)),
        expression -> DruidExpression.functionCall(
            "timestamp_floor",
            ImmutableList.of(
                expression,
                DruidExpression.stringLiteral(granularity.getPeriod().toString()),
                DruidExpression.numberLiteral(
                    granularity.getOrigin() == null ? null : granularity.getOrigin().getMillis()
                ),
                DruidExpression.stringLiteral(granularity.getTimeZone().toString())
            ).stream().map(DruidExpression::fromExpression).collect(Collectors.toList())
        )
    );
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
      final DateTime origin =
          operands.size() > 2 && !RexLiteral.isNullLiteral(operands.get(2))
          ? Calcites.calciteDateTimeLiteralToJoda(operands.get(2), plannerContext.getTimeZone())
          : null;
      final DateTimeZone timeZone =
          operands.size() > 3 && !RexLiteral.isNullLiteral(operands.get(3))
          ? DateTimeZone.forID(RexLiteral.stringValue(operands.get(3)))
          : plannerContext.getTimeZone();
      final PeriodGranularity granularity = new PeriodGranularity(period, origin, timeZone);
      return applyTimestampFloor(druidExpressions.get(0), granularity);
    } else {
      // Granularity is dynamic
      return DruidExpression.fromFunctionCall("timestamp_floor", druidExpressions);
    }
  }
}
