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
import io.druid.java.util.common.granularity.Granularity;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.TimeFormatExtractionFn;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTimeZone;

import java.util.stream.Collectors;

public class TimeFormatOperatorConversion implements SqlOperatorConversion
{
  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder("TIME_FORMAT")
      .operandTypes(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)
      .requiredOperands(1)
      .returnType(SqlTypeName.VARCHAR)
      .functionCategory(SqlFunctionCategory.TIMEDATE)
      .build();

  public static SimpleExtraction applyTimestampFormat(
      final SimpleExtraction simpleExtraction,
      final String pattern,
      final DateTimeZone timeZone
  )
  {
    Preconditions.checkNotNull(simpleExtraction, "simpleExtraction");
    Preconditions.checkNotNull(pattern, "pattern");
    Preconditions.checkNotNull(timeZone, "timeZone");

    final ExtractionFn baseExtractionFn = simpleExtraction.getExtractionFn();

    if (baseExtractionFn instanceof TimeFormatExtractionFn) {
      final TimeFormatExtractionFn baseTimeFormatFn = (TimeFormatExtractionFn) baseExtractionFn;
      final Granularity queryGranularity = ExtractionFns.toQueryGranularity(baseTimeFormatFn);
      if (queryGranularity != null) {
        // Combine EXTRACT(X FROM FLOOR(Y TO Z)) into a single extractionFn.
        return SimpleExtraction.of(
            simpleExtraction.getColumn(),
            new TimeFormatExtractionFn(pattern, timeZone, null, queryGranularity, true)
        );
      }
    }

    return simpleExtraction.cascade(new TimeFormatExtractionFn(pattern, timeZone, null, null, true));
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
    final RexNode timeArg = call.getOperands().get(0);
    final DruidExpression timeExpression = Expressions.toDruidExpression(plannerContext, rowSignature, timeArg);
    if (timeExpression == null) {
      return null;
    }

    final String pattern = call.getOperands().size() > 1 && !RexLiteral.isNullLiteral(call.getOperands().get(1))
                           ? RexLiteral.stringValue(call.getOperands().get(1))
                           : "yyyy-MM-dd'T'HH:mm:ss.SSSZZ";
    final DateTimeZone timeZone = call.getOperands().size() > 2 && !RexLiteral.isNullLiteral(call.getOperands().get(2))
                                  ? DateTimeZone.forID(RexLiteral.stringValue(call.getOperands().get(2)))
                                  : plannerContext.getTimeZone();

    return timeExpression.map(
        simpleExtraction -> applyTimestampFormat(simpleExtraction, pattern, timeZone),
        expression -> DruidExpression.functionCall(
            "timestamp_format",
            ImmutableList.of(
                expression,
                DruidExpression.stringLiteral(pattern),
                DruidExpression.stringLiteral(timeZone.getID())
            ).stream().map(DruidExpression::fromExpression).collect(Collectors.toList())
        )
    );
  }
}
