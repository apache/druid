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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.joda.time.Period;

import java.util.Map;

/**
 * DATE_TRUNC function similar to PostgreSQL.
 */
public class DateTruncOperatorConversion implements SqlOperatorConversion
{
  private static final Map<String, Period> TRUNC_PERIOD_MAP =
      ImmutableMap.<String, Period>builder()
          .put("microseconds", Period.millis(1)) // We don't support microsecond precision, so millis is fine.
          .put("milliseconds", Period.millis(1))
          .put("second", Period.seconds(1))
          .put("minute", Period.minutes(1))
          .put("hour", Period.hours(1))
          .put("day", Period.days(1))
          .put("week", Period.weeks(1))
          .put("month", Period.months(1))
          .put("quarter", Period.months(3))
          .put("year", Period.years(1))
          .put("decade", Period.years(10))
          .put("century", Period.years(100))
          .put("millennium", Period.years(1000))
          .build();


  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder("DATE_TRUNC")
      .operandTypes(SqlTypeFamily.CHARACTER, SqlTypeFamily.TIMESTAMP)
      .requiredOperandCount(2)
      .literalOperands(0)
      .returnTypeCascadeNullable(SqlTypeName.TIMESTAMP)
      .functionCategory(SqlFunctionCategory.TIMEDATE)
      .build();

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
    return OperatorConversions.convertCall(
        plannerContext,
        rowSignature,
        rexNode,
        inputExpressions -> {
          final DruidExpression arg = inputExpressions.get(1);
          final Expr truncTypeExpr = plannerContext.parseExpression(inputExpressions.get(0).getExpression());

          final String truncType = (String) truncTypeExpr.getLiteralValue();
          final Period truncPeriod = TRUNC_PERIOD_MAP.get(StringUtils.toLowerCase(truncType));

          if (truncPeriod == null) {
            throw InvalidSqlInput.exception(
                "Operator[%s] cannot truncate to[%s]",
                calciteOperator().getName(),
                truncType
            );
          }

          return DruidExpression.ofFunctionCall(
              Calcites.getColumnTypeForRelDataType(rexNode.getType()),
              "timestamp_floor",
              ImmutableList.of(
                  arg,
                  DruidExpression.ofStringLiteral(truncPeriod.toString()),
                  DruidExpression.ofStringLiteral(null),
                  DruidExpression.ofStringLiteral(plannerContext.getTimeZone().getID())
              )
          );
        }
    );
  }
}
