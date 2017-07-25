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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.granularity.PeriodGranularity;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Parser;
import io.druid.query.extraction.RegexDimExtractionFn;
import io.druid.query.extraction.TimeFormatExtractionFn;
import io.druid.segment.column.ValueType;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import io.druid.sql.calcite.util.CalciteTests;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Map;

public class ExpressionsTest
{
  private static final DateTimeZone LOS_ANGELES = DateTimeZone.forID("America/Los_Angeles");

  private final PlannerContext plannerContext = PlannerContext.create(
      CalciteTests.createOperatorTable(),
      CalciteTests.createExprMacroTable(),
      new PlannerConfig(),
      ImmutableMap.of()
  );
  private final RowSignature rowSignature = RowSignature
      .builder()
      .add("t", ValueType.LONG)
      .add("a", ValueType.LONG)
      .add("b", ValueType.LONG)
      .add("x", ValueType.FLOAT)
      .add("y", ValueType.LONG)
      .add("s", ValueType.STRING)
      .add("tstr", ValueType.STRING)
      .add("dstr", ValueType.STRING)
      .build();
  private final Map<String, Object> bindings = ImmutableMap.<String, Object>builder()
      .put("t", new DateTime("2000-02-03T04:05:06").getMillis())
      .put("a", 10)
      .put("b", 20)
      .put("x", 2.5)
      .put("y", 3.0)
      .put("s", "foo")
      .put("tstr", "2000-02-03 04:05:06")
      .put("dstr", "2000-02-03")
      .build();
  private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
  private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
  private final RelDataType relDataType = rowSignature.getRelDataType(typeFactory);

  @Test
  public void testConcat()
  {
    testExpression(
        rexBuilder.makeCall(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            SqlStdOperatorTable.CONCAT,
            ImmutableList.of(
                inputRef("s"),
                rexBuilder.makeLiteral("bar")
            )
        ),
        DruidExpression.fromExpression("concat(\"s\",'bar')"),
        "foobar"
    );
  }

  @Test
  public void testCharacterLength()
  {
    testExpression(
        rexBuilder.makeCall(
            SqlStdOperatorTable.CHARACTER_LENGTH,
            inputRef("s")
        ),
        DruidExpression.fromExpression("strlen(\"s\")"),
        3L
    );
  }

  @Test
  public void testRegexpExtract()
  {
    testExpression(
        rexBuilder.makeCall(
            new RegexpExtractOperatorConversion().calciteOperator(),
            inputRef("s"),
            rexBuilder.makeLiteral("f(.)"),
            integerLiteral(1)
        ),
        DruidExpression.of(
            SimpleExtraction.of("s", new RegexDimExtractionFn("f(.)", 1, true, null)),
            "regexp_extract(\"s\",'f(.)',1)"
        ),
        "o"
    );

    testExpression(
        rexBuilder.makeCall(
            new RegexpExtractOperatorConversion().calciteOperator(),
            inputRef("s"),
            rexBuilder.makeLiteral("f(.)")
        ),
        DruidExpression.of(
            SimpleExtraction.of("s", new RegexDimExtractionFn("f(.)", 0, true, null)),
            "regexp_extract(\"s\",'f(.)')"
        ),
        "fo"
    );
  }

  @Test
  public void testTimeFloor()
  {
    testExpression(
        rexBuilder.makeCall(
            new TimeFloorOperatorConversion().calciteOperator(),
            timestampLiteral(new DateTime("2000-02-03T04:05:06Z")),
            rexBuilder.makeLiteral("PT1H")
        ),
        DruidExpression.fromExpression("timestamp_floor(949550706000,'PT1H','','UTC')"),
        new DateTime("2000-02-03T04:00:00").getMillis()
    );

    testExpression(
        rexBuilder.makeCall(
            new TimeFloorOperatorConversion().calciteOperator(),
            inputRef("t"),
            rexBuilder.makeLiteral("P1D"),
            rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.TIMESTAMP)),
            rexBuilder.makeLiteral("America/Los_Angeles")
        ),
        DruidExpression.of(
            SimpleExtraction.of(
                "t",
                new TimeFormatExtractionFn(
                    null,
                    null,
                    null,
                    new PeriodGranularity(Period.days(1), null, LOS_ANGELES),
                    true
                )
            ),
            "timestamp_floor(\"t\",'P1D','','America/Los_Angeles')"
        ),
        new DateTime("2000-02-02T08:00:00").getMillis()
    );
  }

  @Test
  public void testOtherTimeFloor()
  {
    // FLOOR(__time TO unit)

    testExpression(
        rexBuilder.makeCall(
            SqlStdOperatorTable.FLOOR,
            inputRef("t"),
            rexBuilder.makeFlag(TimeUnitRange.YEAR)
        ),
        DruidExpression.of(
            SimpleExtraction.of(
                "t",
                new TimeFormatExtractionFn(null, null, null, Granularities.YEAR, true)
            ),
            "timestamp_floor(\"t\",'P1Y','','UTC')"
        ),
        new DateTime("2000").getMillis()
    );
  }

  @Test
  public void testOtherTimeCeil()
  {
    // CEIL(__time TO unit)

    testExpression(
        rexBuilder.makeCall(
            SqlStdOperatorTable.CEIL,
            inputRef("t"),
            rexBuilder.makeFlag(TimeUnitRange.YEAR)
        ),
        DruidExpression.fromExpression("timestamp_ceil(\"t\",'P1Y','','UTC')"),
        new DateTime("2001").getMillis()
    );
  }

  @Test
  public void testTimeShift()
  {
    testExpression(
        rexBuilder.makeCall(
            new TimeShiftOperatorConversion().calciteOperator(),
            inputRef("t"),
            rexBuilder.makeLiteral("PT2H"),
            rexBuilder.makeLiteral(-3, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        ),
        DruidExpression.fromExpression("timestamp_shift(\"t\",'PT2H',-3)"),
        new DateTime("2000-02-02T22:05:06").getMillis()
    );
  }

  @Test
  public void testTimeExtract()
  {
    testExpression(
        rexBuilder.makeCall(
            new TimeExtractOperatorConversion().calciteOperator(),
            inputRef("t"),
            rexBuilder.makeLiteral("QUARTER")
        ),
        DruidExpression.of(
            null,
            "timestamp_extract(\"t\",'QUARTER','UTC')"
        ),
        1L
    );

    testExpression(
        rexBuilder.makeCall(
            new TimeExtractOperatorConversion().calciteOperator(),
            inputRef("t"),
            rexBuilder.makeLiteral("DAY"),
            rexBuilder.makeLiteral("America/Los_Angeles")
        ),
        DruidExpression.of(
            SimpleExtraction.of(
                "t",
                new TimeFormatExtractionFn("d", LOS_ANGELES, null, Granularities.NONE, true)
            ),
            "timestamp_extract(\"t\",'DAY','America/Los_Angeles')"
        ),
        2L
    );
  }

  @Test
  public void testTimePlusDayTimeInterval()
  {
    final Period period = new Period("P1DT1H1M");

    testExpression(
        rexBuilder.makeCall(
            SqlStdOperatorTable.DATETIME_PLUS,
            inputRef("t"),
            rexBuilder.makeIntervalLiteral(
                new BigDecimal(period.toStandardDuration().getMillis()), // DAY-TIME literals value is millis
                new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.MINUTE, SqlParserPos.ZERO)
            )
        ),
        DruidExpression.of(
            null,
            "(\"t\" + 90060000)"
        ),
        new DateTime("2000-02-03T04:05:06").plus(period).getMillis()
    );
  }

  @Test
  public void testTimePlusYearMonthInterval()
  {
    final Period period = new Period("P1Y1M");

    testExpression(
        rexBuilder.makeCall(
            SqlStdOperatorTable.DATETIME_PLUS,
            inputRef("t"),
            rexBuilder.makeIntervalLiteral(
                new BigDecimal(13), // YEAR-MONTH literals value is months
                new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO)
            )
        ),
        DruidExpression.of(
            null,
            "timestamp_shift(\"t\",concat('P', 13, 'M'),1)"
        ),
        new DateTime("2000-02-03T04:05:06").plus(period).getMillis()
    );
  }

  @Test
  public void testTimeMinusDayTimeInterval()
  {
    final Period period = new Period("P1DT1H1M");

    testExpression(
        rexBuilder.makeCall(
            typeFactory.createSqlType(SqlTypeName.TIMESTAMP),
            SqlStdOperatorTable.MINUS_DATE,
            ImmutableList.of(
                inputRef("t"),
                rexBuilder.makeIntervalLiteral(
                    new BigDecimal(period.toStandardDuration().getMillis()), // DAY-TIME literals value is millis
                    new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.MINUTE, SqlParserPos.ZERO)
                )
            )
        ),
        DruidExpression.of(
            null,
            "(\"t\" - 90060000)"
        ),
        new DateTime("2000-02-03T04:05:06").minus(period).getMillis()
    );
  }

  @Test
  public void testTimeMinusYearMonthInterval()
  {
    final Period period = new Period("P1Y1M");

    testExpression(
        rexBuilder.makeCall(
            typeFactory.createSqlType(SqlTypeName.TIMESTAMP),
            SqlStdOperatorTable.MINUS_DATE,
            ImmutableList.of(
                inputRef("t"),
                rexBuilder.makeIntervalLiteral(
                    new BigDecimal(13), // YEAR-MONTH literals value is months
                    new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO)
                )
            )
        ),
        DruidExpression.of(
            null,
            "timestamp_shift(\"t\",concat('P', 13, 'M'),-1)"
        ),
        new DateTime("2000-02-03T04:05:06").minus(period).getMillis()
    );
  }

  @Test
  public void testTimeParse()
  {
    testExpression(
        rexBuilder.makeCall(
            new TimeParseOperatorConversion().calciteOperator(),
            inputRef("tstr"),
            rexBuilder.makeLiteral("yyyy-MM-dd HH:mm:ss")
        ),
        DruidExpression.fromExpression("timestamp_parse(\"tstr\",'yyyy-MM-dd HH:mm:ss')"),
        new DateTime("2000-02-03T04:05:06").getMillis()
    );

    testExpression(
        rexBuilder.makeCall(
            new TimeParseOperatorConversion().calciteOperator(),
            inputRef("tstr"),
            rexBuilder.makeLiteral("yyyy-MM-dd HH:mm:ss"),
            rexBuilder.makeLiteral("America/Los_Angeles")
        ),
        DruidExpression.fromExpression("timestamp_parse(\"tstr\",'yyyy-MM-dd HH:mm:ss','America/Los_Angeles')"),
        new DateTime("2000-02-03T04:05:06-08:00").getMillis()
    );
  }

  @Test
  public void testTimeFormat()
  {
    testExpression(
        rexBuilder.makeCall(
            new TimeFormatOperatorConversion().calciteOperator(),
            inputRef("t"),
            rexBuilder.makeLiteral("yyyy-MM-dd HH:mm:ss")
        ),
        DruidExpression.of(
            SimpleExtraction.of(
                "t",
                new TimeFormatExtractionFn("yyyy-MM-dd HH:mm:ss", DateTimeZone.UTC, null, Granularities.NONE, true)
            ),
            "timestamp_format(\"t\",'yyyy-MM-dd HH:mm:ss','UTC')"
        ),
        "2000-02-03 04:05:06"
    );

    testExpression(
        rexBuilder.makeCall(
            new TimeFormatOperatorConversion().calciteOperator(),
            inputRef("t"),
            rexBuilder.makeLiteral("yyyy-MM-dd HH:mm:ss"),
            rexBuilder.makeLiteral("America/Los_Angeles")
        ),
        DruidExpression.of(
            SimpleExtraction.of(
                "t",
                new TimeFormatExtractionFn(
                    "yyyy-MM-dd HH:mm:ss",
                    LOS_ANGELES,
                    null,
                    Granularities.NONE,
                    true
                )
            ),
            "timestamp_format(\"t\",'yyyy-MM-dd HH:mm:ss','America/Los_Angeles')"
        ),
        "2000-02-02 20:05:06"
    );
  }

  @Test
  public void testExtract()
  {
    testExpression(
        rexBuilder.makeCall(
            SqlStdOperatorTable.EXTRACT,
            rexBuilder.makeFlag(TimeUnitRange.QUARTER),
            inputRef("t")
        ),
        DruidExpression.of(
            null,
            "timestamp_extract(\"t\",'QUARTER','UTC')"
        ),
        1L
    );

    testExpression(
        rexBuilder.makeCall(
            SqlStdOperatorTable.EXTRACT,
            rexBuilder.makeFlag(TimeUnitRange.DAY),
            inputRef("t")
        ),
        DruidExpression.of(
            SimpleExtraction.of(
                "t",
                new TimeFormatExtractionFn("d", DateTimeZone.UTC, null, Granularities.NONE, true)
            ),
            "timestamp_extract(\"t\",'DAY','UTC')"
        ),
        3L
    );
  }

  @Test
  public void testCastAsTimestamp()
  {
    testExpression(
        rexBuilder.makeAbstractCast(
            typeFactory.createSqlType(SqlTypeName.TIMESTAMP),
            inputRef("t")
        ),
        DruidExpression.of(
            SimpleExtraction.of("t", null),
            "\"t\""
        ),
        new DateTime("2000-02-03T04:05:06Z").getMillis()
    );

    testExpression(
        rexBuilder.makeAbstractCast(
            typeFactory.createSqlType(SqlTypeName.TIMESTAMP),
            inputRef("tstr")
        ),
        DruidExpression.of(
            null,
            "timestamp_parse(\"tstr\",'yyyy-MM-dd HH:mm:ss')"
        ),
        new DateTime("2000-02-03T04:05:06Z").getMillis()
    );
  }

  @Test
  public void testCastFromTimestamp()
  {
    testExpression(
        rexBuilder.makeAbstractCast(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            rexBuilder.makeAbstractCast(
                typeFactory.createSqlType(SqlTypeName.TIMESTAMP),
                inputRef("t")
            )
        ),
        DruidExpression.fromExpression(
            "timestamp_format(\"t\",'yyyy-MM-dd HH:mm:ss')"
        ),
        "2000-02-03 04:05:06"
    );

    testExpression(
        rexBuilder.makeAbstractCast(
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            rexBuilder.makeAbstractCast(
                typeFactory.createSqlType(SqlTypeName.TIMESTAMP),
                inputRef("t")
            )
        ),
        DruidExpression.of(
            SimpleExtraction.of("t", null),
            "\"t\""
        ),
        new DateTime("2000-02-03T04:05:06").getMillis()
    );
  }

  @Test
  public void testCastAsDate()
  {
    testExpression(
        rexBuilder.makeAbstractCast(
            typeFactory.createSqlType(SqlTypeName.DATE),
            inputRef("t")
        ),
        DruidExpression.of(
            SimpleExtraction.of("t", new TimeFormatExtractionFn(null, null, null, Granularities.DAY, true)),
            "timestamp_floor(\"t\",'P1D','','UTC')"
        ),
        new DateTime("2000-02-03").getMillis()
    );

    testExpression(
        rexBuilder.makeAbstractCast(
            typeFactory.createSqlType(SqlTypeName.DATE),
            inputRef("dstr")
        ),
        DruidExpression.fromExpression(
            "timestamp_floor(timestamp_parse(\"dstr\",'yyyy-MM-dd'),'P1D','','UTC')"
        ),
        new DateTime("2000-02-03").getMillis()
    );
  }

  @Test
  public void testCastFromDate()
  {
    testExpression(
        rexBuilder.makeAbstractCast(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            rexBuilder.makeAbstractCast(
                typeFactory.createSqlType(SqlTypeName.DATE),
                inputRef("t")
            )
        ),
        DruidExpression.fromExpression(
            "timestamp_format(timestamp_floor(\"t\",'P1D','','UTC'),'yyyy-MM-dd')"
        ),
        "2000-02-03"
    );

    testExpression(
        rexBuilder.makeAbstractCast(
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            rexBuilder.makeAbstractCast(
                typeFactory.createSqlType(SqlTypeName.DATE),
                inputRef("t")
            )
        ),
        DruidExpression.of(
            SimpleExtraction.of("t", new TimeFormatExtractionFn(null, null, null, Granularities.DAY, true)),
            "timestamp_floor(\"t\",'P1D','','UTC')"
        ),
        new DateTime("2000-02-03").getMillis()
    );
  }

  private RexNode inputRef(final String columnName)
  {
    final int columnNumber = rowSignature.getRowOrder().indexOf(columnName);
    return rexBuilder.makeInputRef(relDataType.getFieldList().get(columnNumber).getType(), columnNumber);
  }

  private RexNode timestampLiteral(final DateTime timestamp)
  {
    return rexBuilder.makeTimestampLiteral(Calcites.jodaToCalciteCalendarLiteral(timestamp, DateTimeZone.UTC), 0);
  }

  private RexNode integerLiteral(final int integer)
  {
    return rexBuilder.makeLiteral(new BigDecimal(integer), typeFactory.createSqlType(SqlTypeName.INTEGER), true);
  }

  private void testExpression(
      final RexNode rexNode,
      final DruidExpression expectedExpression,
      final Object expectedResult
  )
  {
    final DruidExpression expression = Expressions.toDruidExpression(plannerContext, rowSignature, rexNode);
    Assert.assertEquals("Expression for: " + rexNode.toString(), expectedExpression, expression);

    final ExprEval result = Parser.parse(expression.getExpression(), plannerContext.getExprMacroTable())
                                  .eval(Parser.withMap(bindings));
    Assert.assertEquals("Result for: " + rexNode.toString(), expectedResult, result.value());
  }
}
