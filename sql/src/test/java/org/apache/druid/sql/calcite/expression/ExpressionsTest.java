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

package org.apache.druid.sql.calcite.expression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.extraction.RegexDimExtractionFn;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.expression.builtin.DateTruncOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.LPadOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.LeftOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.ParseLongOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.RPadOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.RegexpExtractOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.RepeatOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.ReverseOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.RightOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.RoundOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.StringFormatOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.StrposOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeExtractOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeFloorOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeFormatOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeParseOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeShiftOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TruncateOperatorConversion;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignature;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.math.BigDecimal;
import java.util.Map;

public class ExpressionsTest extends CalciteTestBase
{

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final PlannerContext plannerContext = PlannerContext.create(
      CalciteTests.createOperatorTable(),
      CalciteTests.createExprMacroTable(),
      new PlannerConfig(),
      ImmutableMap.of(),
      CalciteTests.REGULAR_USER_AUTH_RESULT
  );
  private final RowSignature rowSignature = RowSignature
      .builder()
      .add("t", ValueType.LONG)
      .add("a", ValueType.LONG)
      .add("b", ValueType.LONG)
      .add("x", ValueType.FLOAT)
      .add("y", ValueType.LONG)
      .add("z", ValueType.FLOAT)
      .add("s", ValueType.STRING)
      .add("hexstr", ValueType.STRING)
      .add("intstr", ValueType.STRING)
      .add("spacey", ValueType.STRING)
      .add("tstr", ValueType.STRING)
      .add("dstr", ValueType.STRING)
      .build();
  private final Map<String, Object> bindings = ImmutableMap.<String, Object>builder()
      .put("t", DateTimes.of("2000-02-03T04:05:06").getMillis())
      .put("a", 10)
      .put("b", 25)
      .put("x", 2.25)
      .put("y", 3.0)
      .put("z", -2.25)
      .put("s", "foo")
      .put("hexstr", "EF")
      .put("intstr", "-100")
      .put("spacey", "  hey there  ")
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
  public void testStringFormat()
  {
    testExpression(
        rexBuilder.makeCall(
            new StringFormatOperatorConversion().calciteOperator(),
            rexBuilder.makeLiteral("%x"),
            inputRef("b")
        ),
        DruidExpression.fromExpression("format('%x',\"b\")"),
        "19"
    );

    testExpression(
        rexBuilder.makeCall(
            new StringFormatOperatorConversion().calciteOperator(),
            rexBuilder.makeLiteral("%s %,d"),
            inputRef("s"),
            integerLiteral(1234)
        ),
        DruidExpression.fromExpression("format('%s %,d',\"s\",1234)"),
        "foo 1,234"
    );

    testExpression(
        rexBuilder.makeCall(
            new StringFormatOperatorConversion().calciteOperator(),
            rexBuilder.makeLiteral("%s %,d"),
            inputRef("s")
        ),
        DruidExpression.fromExpression("format('%s %,d',\"s\")"),
        "%s %,d; foo"
    );

    testExpression(
        rexBuilder.makeCall(
            new StringFormatOperatorConversion().calciteOperator(),
            rexBuilder.makeLiteral("%s %,d"),
            inputRef("s"),
            integerLiteral(1234),
            integerLiteral(6789)
        ),
        DruidExpression.fromExpression("format('%s %,d',\"s\",1234,6789)"),
        "foo 1,234"
    );
  }

  @Test
  public void testStrpos()
  {
    testExpression(
        rexBuilder.makeCall(
            new StrposOperatorConversion().calciteOperator(),
            inputRef("s"),
            rexBuilder.makeLiteral("oo")
        ),
        DruidExpression.fromExpression("(strpos(\"s\",'oo') + 1)"),
        2L
    );

    testExpression(
        rexBuilder.makeCall(
            new StrposOperatorConversion().calciteOperator(),
            inputRef("s"),
            rexBuilder.makeLiteral("ax")
        ),
        DruidExpression.fromExpression("(strpos(\"s\",'ax') + 1)"),
        0L
    );

    testExpression(
        rexBuilder.makeCall(
            new StrposOperatorConversion().calciteOperator(),
            rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            rexBuilder.makeLiteral("ax")
        ),
        DruidExpression.fromExpression("(strpos(null,'ax') + 1)"),
        NullHandling.replaceWithDefault() ? 0L : null
    );
  }

  @Test
  public void testParseLong()
  {
    testExpression(
        rexBuilder.makeCall(
            new ParseLongOperatorConversion().calciteOperator(),
            inputRef("intstr")
        ),
        DruidExpression.fromExpression("parse_long(\"intstr\")"),
        -100L
    );

    testExpression(
        rexBuilder.makeCall(
            new ParseLongOperatorConversion().calciteOperator(),
            inputRef("hexstr"),
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(16))
        ),
        DruidExpression.fromExpression("parse_long(\"hexstr\",16)"),
        239L
    );

    testExpression(
        rexBuilder.makeCall(
            new ParseLongOperatorConversion().calciteOperator(),
            rexBuilder.makeCall(
                SqlStdOperatorTable.CONCAT,
                rexBuilder.makeLiteral("0x"),
                inputRef("hexstr")
            ),
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(16))
        ),
        DruidExpression.fromExpression("parse_long(concat('0x',\"hexstr\"),16)"),
        239L
    );

    testExpression(
        rexBuilder.makeCall(
            new ParseLongOperatorConversion().calciteOperator(),
            inputRef("hexstr")
        ),
        DruidExpression.fromExpression("parse_long(\"hexstr\")"),
        NullHandling.sqlCompatible() ? null : 0L
    );
  }

  @Test
  public void testPosition()
  {
    testExpression(
        rexBuilder.makeCall(
            SqlStdOperatorTable.POSITION,
            rexBuilder.makeLiteral("oo"),
            inputRef("s")
        ),
        DruidExpression.fromExpression("(strpos(\"s\",'oo',0) + 1)"),
        2L
    );

    testExpression(
        rexBuilder.makeCall(
            SqlStdOperatorTable.POSITION,
            rexBuilder.makeLiteral("oo"),
            inputRef("s"),
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(2))
        ),
        DruidExpression.fromExpression("(strpos(\"s\",'oo',(2 - 1)) + 1)"),
        2L
    );

    testExpression(
        rexBuilder.makeCall(
            SqlStdOperatorTable.POSITION,
            rexBuilder.makeLiteral("oo"),
            inputRef("s"),
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(3))
        ),
        DruidExpression.fromExpression("(strpos(\"s\",'oo',(3 - 1)) + 1)"),
        0L
    );
  }

  @Test
  public void testPower()
  {
    testExpression(
        rexBuilder.makeCall(SqlStdOperatorTable.POWER, inputRef("a"), integerLiteral(2)),
        DruidExpression.fromExpression("pow(\"a\",2)"),
        100.0
    );
  }

  @Test
  public void testFloor()
  {
    testExpression(
        rexBuilder.makeCall(SqlStdOperatorTable.FLOOR, inputRef("a")),
        DruidExpression.fromExpression("floor(\"a\")"),
        10.0
    );

    testExpression(
        rexBuilder.makeCall(SqlStdOperatorTable.FLOOR, inputRef("x")),
        DruidExpression.fromExpression("floor(\"x\")"),
        2.0
    );

    testExpression(
        rexBuilder.makeCall(SqlStdOperatorTable.FLOOR, inputRef("y")),
        DruidExpression.fromExpression("floor(\"y\")"),
        3.0
    );

    testExpression(
        rexBuilder.makeCall(SqlStdOperatorTable.FLOOR, inputRef("z")),
        DruidExpression.fromExpression("floor(\"z\")"),
        -3.0
    );
  }

  @Test
  public void testCeil()
  {
    testExpression(
        rexBuilder.makeCall(SqlStdOperatorTable.CEIL, inputRef("a")),
        DruidExpression.fromExpression("ceil(\"a\")"),
        10.0
    );

    testExpression(
        rexBuilder.makeCall(SqlStdOperatorTable.CEIL, inputRef("x")),
        DruidExpression.fromExpression("ceil(\"x\")"),
        3.0
    );

    testExpression(
        rexBuilder.makeCall(SqlStdOperatorTable.CEIL, inputRef("y")),
        DruidExpression.fromExpression("ceil(\"y\")"),
        3.0
    );

    testExpression(
        rexBuilder.makeCall(SqlStdOperatorTable.CEIL, inputRef("z")),
        DruidExpression.fromExpression("ceil(\"z\")"),
        -2.0
    );
  }

  @Test
  public void testTruncate()
  {
    final SqlFunction truncateFunction = new TruncateOperatorConversion().calciteOperator();

    testExpression(
        rexBuilder.makeCall(truncateFunction, inputRef("a")),
        DruidExpression.fromExpression("(cast(cast(\"a\" * 1,'long'),'double') / 1)"),
        10.0
    );

    testExpression(
        rexBuilder.makeCall(truncateFunction, inputRef("x")),
        DruidExpression.fromExpression("(cast(cast(\"x\" * 1,'long'),'double') / 1)"),
        2.0
    );

    testExpression(
        rexBuilder.makeCall(truncateFunction, inputRef("y")),
        DruidExpression.fromExpression("(cast(cast(\"y\" * 1,'long'),'double') / 1)"),
        3.0
    );

    testExpression(
        rexBuilder.makeCall(truncateFunction, inputRef("z")),
        DruidExpression.fromExpression("(cast(cast(\"z\" * 1,'long'),'double') / 1)"),
        -2.0
    );

    testExpression(
        rexBuilder.makeCall(truncateFunction, inputRef("x"), integerLiteral(1)),
        DruidExpression.fromExpression("(cast(cast(\"x\" * 10.0,'long'),'double') / 10.0)"),
        2.2
    );

    testExpression(
        rexBuilder.makeCall(truncateFunction, inputRef("z"), integerLiteral(1)),
        DruidExpression.fromExpression("(cast(cast(\"z\" * 10.0,'long'),'double') / 10.0)"),
        -2.2
    );

    testExpression(
        rexBuilder.makeCall(truncateFunction, inputRef("b"), integerLiteral(-1)),
        DruidExpression.fromExpression("(cast(cast(\"b\" * 0.1,'long'),'double') / 0.1)"),
        20.0
    );

    testExpression(
        rexBuilder.makeCall(truncateFunction, inputRef("z"), integerLiteral(-1)),
        DruidExpression.fromExpression("(cast(cast(\"z\" * 0.1,'long'),'double') / 0.1)"),
        0.0
    );
  }

  @Test
  public void testRound()
  {
    final SqlFunction roundFunction = new RoundOperatorConversion().calciteOperator();

    testExpression(
        rexBuilder.makeCall(roundFunction, inputRef("a")),
        DruidExpression.fromExpression("round(\"a\")"),
        10L
    );

    testExpression(
        rexBuilder.makeCall(roundFunction, inputRef("b")),
        DruidExpression.fromExpression("round(\"b\")"),
        25L
    );

    testExpression(
        rexBuilder.makeCall(roundFunction, inputRef("b"), integerLiteral(-1)),
        DruidExpression.fromExpression("round(\"b\",-1)"),
        30L
    );

    testExpression(
        rexBuilder.makeCall(roundFunction, inputRef("x")),
        DruidExpression.fromExpression("round(\"x\")"),
        2.0
    );

    testExpression(
        rexBuilder.makeCall(roundFunction, inputRef("x"), integerLiteral(1)),
        DruidExpression.fromExpression("round(\"x\",1)"),
        2.3
    );

    testExpression(
        rexBuilder.makeCall(roundFunction, inputRef("y")),
        DruidExpression.fromExpression("round(\"y\")"),
        3.0
    );

    testExpression(
        rexBuilder.makeCall(roundFunction, inputRef("z")),
        DruidExpression.fromExpression("round(\"z\")"),
        -2.0
    );
  }

  @Test
  public void testRoundWithInvalidArgument()
  {
    final SqlFunction roundFunction = new RoundOperatorConversion().calciteOperator();

    expectedException.expect(IAE.class);
    expectedException.expectMessage("The first argument to the function[round] should be integer or double type but get the STRING type");
    testExpression(
        rexBuilder.makeCall(roundFunction, inputRef("s")),
        DruidExpression.fromExpression("round(\"s\")"),
        "IAE Exception"
    );
  }

  @Test
  public void testRoundWithInvalidSecondArgument()
  {
    final SqlFunction roundFunction = new RoundOperatorConversion().calciteOperator();

    expectedException.expect(IAE.class);
    expectedException.expectMessage("The second argument to the function[round] should be integer type but get the STRING type");
    testExpression(
        rexBuilder.makeCall(roundFunction, inputRef("x"), rexBuilder.makeLiteral("foo")),
        DruidExpression.fromExpression("round(\"x\",'foo')"),
        "IAE Exception"
    );
  }

  @Test
  public void testDateTrunc()
  {
    testExpression(
        rexBuilder.makeCall(
            new DateTruncOperatorConversion().calciteOperator(),
            rexBuilder.makeLiteral("hour"),
            timestampLiteral(DateTimes.of("2000-02-03T04:05:06Z"))
        ),
        DruidExpression.fromExpression("timestamp_floor(949550706000,'PT1H',null,'UTC')"),
        DateTimes.of("2000-02-03T04:00:00").getMillis()
    );

    testExpression(
        rexBuilder.makeCall(
            new DateTruncOperatorConversion().calciteOperator(),
            rexBuilder.makeLiteral("DAY"),
            timestampLiteral(DateTimes.of("2000-02-03T04:05:06Z"))
        ),
        DruidExpression.fromExpression("timestamp_floor(949550706000,'P1D',null,'UTC')"),
        DateTimes.of("2000-02-03T00:00:00").getMillis()
    );
  }

  @Test
  public void testTrim()
  {
    testExpression(
        rexBuilder.makeCall(
            SqlStdOperatorTable.TRIM,
            rexBuilder.makeFlag(SqlTrimFunction.Flag.BOTH),
            rexBuilder.makeLiteral(" "),
            inputRef("spacey")
        ),
        DruidExpression.fromExpression("trim(\"spacey\",' ')"),
        "hey there"
    );

    testExpression(
        rexBuilder.makeCall(
            SqlStdOperatorTable.TRIM,
            rexBuilder.makeFlag(SqlTrimFunction.Flag.LEADING),
            rexBuilder.makeLiteral(" h"),
            inputRef("spacey")
        ),
        DruidExpression.fromExpression("ltrim(\"spacey\",' h')"),
        "ey there  "
    );

    testExpression(
        rexBuilder.makeCall(
            SqlStdOperatorTable.TRIM,
            rexBuilder.makeFlag(SqlTrimFunction.Flag.TRAILING),
            rexBuilder.makeLiteral(" e"),
            inputRef("spacey")
        ),
        DruidExpression.fromExpression("rtrim(\"spacey\",' e')"),
        "  hey ther"
    );
  }

  @Test
  public void testPad()
  {
    testExpression(
        rexBuilder.makeCall(
            new LPadOperatorConversion().calciteOperator(),
            inputRef("s"),
            rexBuilder.makeLiteral(5, typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            rexBuilder.makeLiteral("x")
        ),
        DruidExpression.fromExpression("lpad(\"s\",5,'x')"),
        "xxfoo"
    );

    testExpression(
        rexBuilder.makeCall(
            new RPadOperatorConversion().calciteOperator(),
            inputRef("s"),
            rexBuilder.makeLiteral(5, typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            rexBuilder.makeLiteral("x")
        ),
        DruidExpression.fromExpression("rpad(\"s\",5,'x')"),
        "fooxx"
    );
  }


  @Test
  public void testTimeFloor()
  {
    testExpression(
        rexBuilder.makeCall(
            new TimeFloorOperatorConversion().calciteOperator(),
            timestampLiteral(DateTimes.of("2000-02-03T04:05:06Z")),
            rexBuilder.makeLiteral("PT1H")
        ),
        DruidExpression.fromExpression("timestamp_floor(949550706000,'PT1H',null,'UTC')"),
        DateTimes.of("2000-02-03T04:00:00").getMillis()
    );

    testExpression(
        rexBuilder.makeCall(
            new TimeFloorOperatorConversion().calciteOperator(),
            inputRef("t"),
            rexBuilder.makeLiteral("P1D"),
            rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.TIMESTAMP)),
            rexBuilder.makeLiteral("America/Los_Angeles")
        ),
        DruidExpression.fromExpression("timestamp_floor(\"t\",'P1D',null,'America/Los_Angeles')"),
        DateTimes.of("2000-02-02T08:00:00").getMillis()
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
        DruidExpression.fromExpression("timestamp_floor(\"t\",'P1Y',null,'UTC')"),
        DateTimes.of("2000").getMillis()
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
        DruidExpression.fromExpression("timestamp_ceil(\"t\",'P1Y',null,'UTC')"),
        DateTimes.of("2001").getMillis()
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
        DruidExpression.fromExpression("timestamp_shift(\"t\",'PT2H',-3,'UTC')"),
        DateTimes.of("2000-02-02T22:05:06").getMillis()
    );

    testExpression(
        rexBuilder.makeCall(
            new TimeShiftOperatorConversion().calciteOperator(),
            inputRef("t"),
            rexBuilder.makeLiteral("PT2H"),
            rexBuilder.makeLiteral(-3, typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            rexBuilder.makeLiteral("America/Los_Angeles")
        ),
        DruidExpression.fromExpression("timestamp_shift(\"t\",'PT2H',-3,'America/Los_Angeles')"),
        DateTimes.of("2000-02-02T22:05:06").getMillis()
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
        DruidExpression.fromExpression("timestamp_extract(\"t\",'QUARTER','UTC')"),
        1L
    );

    testExpression(
        rexBuilder.makeCall(
            new TimeExtractOperatorConversion().calciteOperator(),
            inputRef("t"),
            rexBuilder.makeLiteral("DAY"),
            rexBuilder.makeLiteral("America/Los_Angeles")
        ),
        DruidExpression.fromExpression("timestamp_extract(\"t\",'DAY','America/Los_Angeles')"),
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
        DateTimes.of("2000-02-03T04:05:06").plus(period).getMillis()
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
            "timestamp_shift(\"t\",concat('P', 13, 'M'),1,'UTC')"
        ),
        DateTimes.of("2000-02-03T04:05:06").plus(period).getMillis()
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
        DateTimes.of("2000-02-03T04:05:06").minus(period).getMillis()
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
            "timestamp_shift(\"t\",concat('P', 13, 'M'),-1,'UTC')"
        ),
        DateTimes.of("2000-02-03T04:05:06").minus(period).getMillis()
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
        DruidExpression.fromExpression("timestamp_parse(\"tstr\",'yyyy-MM-dd HH:mm:ss','UTC')"),
        DateTimes.of("2000-02-03T04:05:06").getMillis()
    );

    testExpression(
        rexBuilder.makeCall(
            new TimeParseOperatorConversion().calciteOperator(),
            inputRef("tstr"),
            rexBuilder.makeLiteral("yyyy-MM-dd HH:mm:ss"),
            rexBuilder.makeLiteral("America/Los_Angeles")
        ),
        DruidExpression.fromExpression("timestamp_parse(\"tstr\",'yyyy-MM-dd HH:mm:ss','America/Los_Angeles')"),
        DateTimes.of("2000-02-03T04:05:06-08:00").getMillis()
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
        DruidExpression.fromExpression("timestamp_format(\"t\",'yyyy-MM-dd HH:mm:ss','UTC')"),
        "2000-02-03 04:05:06"
    );

    testExpression(
        rexBuilder.makeCall(
            new TimeFormatOperatorConversion().calciteOperator(),
            inputRef("t"),
            rexBuilder.makeLiteral("yyyy-MM-dd HH:mm:ss"),
            rexBuilder.makeLiteral("America/Los_Angeles")
        ),
        DruidExpression.fromExpression("timestamp_format(\"t\",'yyyy-MM-dd HH:mm:ss','America/Los_Angeles')"),
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
        DruidExpression.fromExpression("timestamp_extract(\"t\",'QUARTER','UTC')"),
        1L
    );

    testExpression(
        rexBuilder.makeCall(
            SqlStdOperatorTable.EXTRACT,
            rexBuilder.makeFlag(TimeUnitRange.DAY),
            inputRef("t")
        ),
        DruidExpression.fromExpression("timestamp_extract(\"t\",'DAY','UTC')"),
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
        DateTimes.of("2000-02-03T04:05:06Z").getMillis()
    );

    testExpression(
        rexBuilder.makeAbstractCast(
            typeFactory.createSqlType(SqlTypeName.TIMESTAMP),
            inputRef("tstr")
        ),
        DruidExpression.of(
            null,
            "timestamp_parse(\"tstr\",null,'UTC')"
        ),
        DateTimes.of("2000-02-03T04:05:06Z").getMillis()
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
            "timestamp_format(\"t\",'yyyy-MM-dd HH:mm:ss','UTC')"
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
        DateTimes.of("2000-02-03T04:05:06").getMillis()
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
        DruidExpression.fromExpression("timestamp_floor(\"t\",'P1D',null,'UTC')"),
        DateTimes.of("2000-02-03").getMillis()
    );

    testExpression(
        rexBuilder.makeAbstractCast(
            typeFactory.createSqlType(SqlTypeName.DATE),
            inputRef("dstr")
        ),
        DruidExpression.fromExpression(
            "timestamp_floor(timestamp_parse(\"dstr\",null,'UTC'),'P1D',null,'UTC')"
        ),
        DateTimes.of("2000-02-03").getMillis()
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
            "timestamp_format(timestamp_floor(\"t\",'P1D',null,'UTC'),'yyyy-MM-dd','UTC')"
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
        DruidExpression.fromExpression("timestamp_floor(\"t\",'P1D',null,'UTC')"),
        DateTimes.of("2000-02-03").getMillis()
    );
  }

  @Test
  public void testReverse()
  {
    testExpression(
        rexBuilder.makeCall(
            new ReverseOperatorConversion().calciteOperator(),
            inputRef("s")
        ),
        DruidExpression.fromExpression("reverse(\"s\")"),
        "oof"
    );

    testExpression(
        rexBuilder.makeCall(
            new ReverseOperatorConversion().calciteOperator(),
            inputRef("spacey")
        ),
        DruidExpression.fromExpression("reverse(\"spacey\")"),
        "  ereht yeh  "
    );

    testExpression(
        rexBuilder.makeCall(
            new ReverseOperatorConversion().calciteOperator(),
            inputRef("tstr")
        ),
        DruidExpression.fromExpression("reverse(\"tstr\")"),
        "60:50:40 30-20-0002"
    );

    testExpression(
        rexBuilder.makeCall(
            new ReverseOperatorConversion().calciteOperator(),
            inputRef("dstr")
        ),
        DruidExpression.fromExpression("reverse(\"dstr\")"),
        "30-20-0002"
    );
  }

  @Test
  public void testAbnormalReverseWithWrongType()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Function[reverse] needs a string argument");

    testExpression(
        rexBuilder.makeCall(
            new ReverseOperatorConversion().calciteOperator(),
            inputRef("a")
        ),
        DruidExpression.fromExpression("reverse(\"a\")"),
        null
    );
  }

  @Test
  public void testRight()
  {
    testExpression(
        rexBuilder.makeCall(
            new RightOperatorConversion().calciteOperator(),
            inputRef("s"),
            integerLiteral(1)
        ),
        DruidExpression.fromExpression("right(\"s\",1)"),
        "o"
    );

    testExpression(
        rexBuilder.makeCall(
            new RightOperatorConversion().calciteOperator(),
            inputRef("s"),
            integerLiteral(2)
        ),
        DruidExpression.fromExpression("right(\"s\",2)"),
        "oo"
    );

    testExpression(
        rexBuilder.makeCall(
            new RightOperatorConversion().calciteOperator(),
            inputRef("s"),
            integerLiteral(3)
        ),
        DruidExpression.fromExpression("right(\"s\",3)"),
        "foo"
    );

    testExpression(
        rexBuilder.makeCall(
            new RightOperatorConversion().calciteOperator(),
            inputRef("s"),
            integerLiteral(4)
        ),
        DruidExpression.fromExpression("right(\"s\",4)"),
        "foo"
    );

    testExpression(
        rexBuilder.makeCall(
            new RightOperatorConversion().calciteOperator(),
            inputRef("tstr"),
            integerLiteral(5)
        ),
        DruidExpression.fromExpression("right(\"tstr\",5)"),
        "05:06"
    );
  }

  @Test
  public void testAbnormalRightWithNegativeNumber()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Function[right] needs a postive integer as second argument");

    testExpression(
        rexBuilder.makeCall(
            new RightOperatorConversion().calciteOperator(),
            inputRef("s"),
            integerLiteral(-1)
        ),
        DruidExpression.fromExpression("right(\"s\",-1)"),
        null
    );
  }

  @Test
  public void testAbnormalRightWithWrongType()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Function[right] needs a string as first argument "
                                    + "and an integer as second argument");

    testExpression(
        rexBuilder.makeCall(
            new RightOperatorConversion().calciteOperator(),
            inputRef("s"),
            inputRef("s")
        ),
        DruidExpression.fromExpression("right(\"s\",\"s\")"),
        null
    );
  }

  @Test
  public void testLeft()
  {
    testExpression(
        rexBuilder.makeCall(
            new LeftOperatorConversion().calciteOperator(),
            inputRef("s"),
            integerLiteral(1)
        ),
        DruidExpression.fromExpression("left(\"s\",1)"),
        "f"
    );

    testExpression(
        rexBuilder.makeCall(
            new LeftOperatorConversion().calciteOperator(),
            inputRef("s"),
            integerLiteral(2)
        ),
        DruidExpression.fromExpression("left(\"s\",2)"),
        "fo"
    );

    testExpression(
        rexBuilder.makeCall(
            new LeftOperatorConversion().calciteOperator(),
            inputRef("s"),
            integerLiteral(3)
        ),
        DruidExpression.fromExpression("left(\"s\",3)"),
        "foo"
    );

    testExpression(
        rexBuilder.makeCall(
            new LeftOperatorConversion().calciteOperator(),
            inputRef("s"),
            integerLiteral(4)
        ),
        DruidExpression.fromExpression("left(\"s\",4)"),
        "foo"
    );

    testExpression(
        rexBuilder.makeCall(
            new LeftOperatorConversion().calciteOperator(),
            inputRef("tstr"),
            integerLiteral(10)
        ),
        DruidExpression.fromExpression("left(\"tstr\",10)"),
        "2000-02-03"
    );
  }

  @Test
  public void testAbnormalLeftWithNegativeNumber()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Function[left] needs a postive integer as second argument");

    testExpression(
        rexBuilder.makeCall(
            new LeftOperatorConversion().calciteOperator(),
            inputRef("s"),
            integerLiteral(-1)
        ),
        DruidExpression.fromExpression("left(\"s\",-1)"),
        null
    );
  }

  @Test
  public void testAbnormalLeftWithWrongType()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Function[left] needs a string as first argument "
                                    + "and an integer as second argument");

    testExpression(
        rexBuilder.makeCall(
            new LeftOperatorConversion().calciteOperator(),
            inputRef("s"),
            inputRef("s")
        ),
        DruidExpression.fromExpression("left(\"s\",\"s\")"),
        null
    );
  }

  @Test
  public void testRepeat()
  {
    testExpression(
        rexBuilder.makeCall(
            new RepeatOperatorConversion().calciteOperator(),
            inputRef("s"),
            integerLiteral(1)
        ),
        DruidExpression.fromExpression("repeat(\"s\",1)"),
        "foo"
    );

    testExpression(
        rexBuilder.makeCall(
            new RepeatOperatorConversion().calciteOperator(),
            inputRef("s"),
            integerLiteral(3)
        ),
        DruidExpression.fromExpression("repeat(\"s\",3)"),
        "foofoofoo"
    );

    testExpression(
        rexBuilder.makeCall(
            new RepeatOperatorConversion().calciteOperator(),
            inputRef("s"),
            integerLiteral(-1)
        ),
        DruidExpression.fromExpression("repeat(\"s\",-1)"),
        null
    );
  }

  @Test
  public void testAbnormalRepeatWithWrongType()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Function[repeat] needs a string as first argument "
                                    + "and an integer as second argument");

    testExpression(
        rexBuilder.makeCall(
            new RepeatOperatorConversion().calciteOperator(),
            inputRef("s"),
            inputRef("s")
        ),
        DruidExpression.fromExpression("repeat(\"s\",\"s\")"),
        null
    );
  }

  private RexNode inputRef(final String columnName)
  {
    final int columnNumber = rowSignature.getRowOrder().indexOf(columnName);
    return rexBuilder.makeInputRef(relDataType.getFieldList().get(columnNumber).getType(), columnNumber);
  }

  private RexNode timestampLiteral(final DateTime timestamp)
  {
    return rexBuilder.makeTimestampLiteral(Calcites.jodaToCalciteTimestampString(timestamp, DateTimeZone.UTC), 0);
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
    Assert.assertEquals("Expression for: " + rexNode, expectedExpression, expression);

    final ExprEval result = Parser.parse(expression.getExpression(), plannerContext.getExprMacroTable())
                                  .eval(Parser.withMap(bindings));
    Assert.assertEquals("Result for: " + rexNode, expectedResult, result.value());
  }
}
