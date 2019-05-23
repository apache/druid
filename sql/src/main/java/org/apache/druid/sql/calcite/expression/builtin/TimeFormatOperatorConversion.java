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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignature;
import org.joda.time.DateTimeZone;

import java.util.stream.Collectors;

public class TimeFormatOperatorConversion implements SqlOperatorConversion
{
  private static final String DEFAULT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ";

  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder("TIME_FORMAT")
      .operandTypes(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)
      .requiredOperands(1)
      .returnType(SqlTypeName.VARCHAR)
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
    final RexCall call = (RexCall) rexNode;
    final RexNode timeArg = call.getOperands().get(0);
    final DruidExpression timeExpression = Expressions.toDruidExpression(plannerContext, rowSignature, timeArg);
    if (timeExpression == null) {
      return null;
    }

    final String pattern = OperatorConversions.getOperandWithDefault(
        call.getOperands(),
        1,
        RexLiteral::stringValue,
        DEFAULT_PATTERN
    );

    final DateTimeZone timeZone = OperatorConversions.getOperandWithDefault(
        call.getOperands(),
        2,
        operand -> DateTimes.inferTzFromString(RexLiteral.stringValue(operand)),
        plannerContext.getTimeZone()
    );

    return DruidExpression.fromFunctionCall(
        "timestamp_format",
        ImmutableList.of(
            timeExpression.getExpression(),
            DruidExpression.stringLiteral(pattern),
            DruidExpression.stringLiteral(timeZone.getID())
        ).stream().map(DruidExpression::fromExpression).collect(Collectors.toList())
    );
  }
}
