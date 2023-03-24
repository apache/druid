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

package org.apache.druid.sql.calcite.planner.convertlet;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.util.Static;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;

import java.util.Collections;
import java.util.List;

public class TimeInIntervalConvertletFactory implements DruidConvertletFactory
{
  public static final TimeInIntervalConvertletFactory INSTANCE = new TimeInIntervalConvertletFactory();

  private static final String NAME = "TIME_IN_INTERVAL";

  private static final SqlOperator OPERATOR = OperatorConversions
      .operatorBuilder(NAME)
      .operandTypeChecker(
          OperandTypes.sequence(
              NAME + "(<TIMESTAMP>, <LITERAL ISO8601 INTERVAL>)",
              OperandTypes.family(SqlTypeFamily.TIMESTAMP),
              OperandTypes.and(OperandTypes.family(SqlTypeFamily.CHARACTER), OperandTypes.LITERAL)
          )
      )
      .returnTypeNonNull(SqlTypeName.BOOLEAN)
      .functionCategory(SqlFunctionCategory.TIMEDATE)
      .build();

  private TimeInIntervalConvertletFactory()
  {
    // Singleton.
  }

  @Override
  public SqlRexConvertlet createConvertlet(PlannerContext plannerContext)
  {
    return new TimeInIntervalConvertlet(plannerContext.getTimeZone());
  }

  @Override
  public List<SqlOperator> operators()
  {
    return Collections.singletonList(OPERATOR);
  }

  private static Interval intervalFromStringArgument(
      final SqlParserPos parserPos,
      final String intervalString,
      final DateTimeZone sessionTimeZone
  )
  {
    try {
      return new Interval(intervalString, ISOChronology.getInstance(sessionTimeZone));
    }
    catch (IllegalArgumentException e) {
      final RuntimeException ex =
          new IAE("Function '%s' second argument is not a valid ISO8601 interval: %s", NAME, e.getMessage());

      throw Static.RESOURCE.validatorContext(
          parserPos.getLineNum(),
          parserPos.getColumnNum(),
          parserPos.getEndLineNum(),
          parserPos.getEndColumnNum()
      ).ex(ex);
    }
  }

  private static class TimeInIntervalConvertlet implements SqlRexConvertlet
  {
    private final DateTimeZone sessionTimeZone;

    private TimeInIntervalConvertlet(final DateTimeZone sessionTimeZone)
    {
      this.sessionTimeZone = sessionTimeZone;
    }

    @Override
    public RexNode convertCall(final SqlRexContext cx, final SqlCall call)
    {
      final RexBuilder rexBuilder = cx.getRexBuilder();
      final RexNode timeOperand = cx.convertExpression(call.getOperandList().get(0));
      final RexNode intervalOperand = cx.convertExpression(call.getOperandList().get(1));

      final Interval interval = intervalFromStringArgument(
          call.getParserPosition(),
          RexLiteral.stringValue(intervalOperand),
          sessionTimeZone
      );

      final RexNode lowerBound = rexBuilder.makeCall(
          SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
          timeOperand,
          Calcites.jodaToCalciteTimestampLiteral(
              rexBuilder,
              interval.getStart(),
              sessionTimeZone,
              DruidTypeSystem.DEFAULT_TIMESTAMP_PRECISION
          )
      );

      final RexNode upperBound = rexBuilder.makeCall(
          SqlStdOperatorTable.LESS_THAN,
          timeOperand,
          Calcites.jodaToCalciteTimestampLiteral(
              rexBuilder,
              interval.getEnd(),
              sessionTimeZone,
              DruidTypeSystem.DEFAULT_TIMESTAMP_PRECISION
          )
      );

      return rexBuilder.makeCall(SqlStdOperatorTable.AND, lowerBound, upperBound);
    }
  }
}
