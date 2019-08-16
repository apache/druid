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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
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
import org.apache.druid.sql.calcite.expression.builtin.TimeCeilOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeExtractOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeFloorOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeFormatOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeParseOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeShiftOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TruncateOperatorConversion;
import org.apache.druid.sql.calcite.table.RowSignature;
import org.joda.time.Period;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Map;

public class ExpressionsTest extends ExpressionTestBase
{
  private static final RowSignature ROW_SIGNATURE = RowSignature
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

  private static final Map<String, Object> BINDINGS = ImmutableMap.<String, Object>builder()
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

  private ExpressionTestHelper testHelper;

  @Before
  public void setUp()
  {
    testHelper = new ExpressionTestHelper(ROW_SIGNATURE, BINDINGS);
  }

  @Test
  public void testConcat()
  {
    testHelper.testExpression(
        SqlTypeName.VARCHAR,
        SqlStdOperatorTable.CONCAT,
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("bar")
        ),
        DruidExpression.fromExpression("concat(\"s\",'bar')"),
        "foobar"
    );
  }

  @Test
  public void testCharacterLength()
  {
    testHelper.testExpression(
        SqlStdOperatorTable.CHARACTER_LENGTH,
        testHelper.makeInputRef("s"),
        DruidExpression.fromExpression("strlen(\"s\")"),
        3L
    );
  }

  @Test
  public void testRegexpExtract()
  {
    testHelper.testExpression(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("f(.)"),
            testHelper.makeLiteral(1)
        ),
        DruidExpression.of(
            SimpleExtraction.of("s", new RegexDimExtractionFn("f(.)", 1, true, null)),
            "regexp_extract(\"s\",'f(.)',1)"
        ),
        "o"
    );

    testHelper.testExpression(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("f(.)")
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
    testHelper.testExpression(
        new StringFormatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("%x"),
            testHelper.makeInputRef("b")
        ),
        DruidExpression.fromExpression("format('%x',\"b\")"),
        "19"
    );

    testHelper.testExpression(
        new StringFormatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("%s %,d"),
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1234)
        ),
        DruidExpression.fromExpression("format('%s %,d',\"s\",1234)"),
        "foo 1,234"
    );

    testHelper.testExpression(
        new StringFormatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("%s %,d"),
            testHelper.makeInputRef("s")
        ),
        DruidExpression.fromExpression("format('%s %,d',\"s\")"),
        "%s %,d; foo"
    );

    testHelper.testExpression(
        new StringFormatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("%s %,d"),
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1234),
            testHelper.makeLiteral(6789)
        ),
        DruidExpression.fromExpression("format('%s %,d',\"s\",1234,6789)"),
        "foo 1,234"
    );
  }

  @Test
  public void testStrpos()
  {
    testHelper.testExpression(
        new StrposOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("oo")
        ),
        DruidExpression.fromExpression("(strpos(\"s\",'oo') + 1)"),
        2L
    );

    testHelper.testExpression(
        new StrposOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("ax")
        ),
        DruidExpression.fromExpression("(strpos(\"s\",'ax') + 1)"),
        0L
    );

    testHelper.testExpression(
        new StrposOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeNullLiteral(SqlTypeName.VARCHAR),
            testHelper.makeLiteral("ax")
        ),
        DruidExpression.fromExpression("(strpos(null,'ax') + 1)"),
        NullHandling.replaceWithDefault() ? 0L : null
    );
  }

  @Test
  public void testParseLong()
  {
    testHelper.testExpression(
        new ParseLongOperatorConversion().calciteOperator(),
        testHelper.makeInputRef("intstr"),
        DruidExpression.fromExpression("parse_long(\"intstr\")"),
        -100L
    );

    testHelper.testExpression(
        new ParseLongOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("hexstr"),
            testHelper.makeLiteral(BigDecimal.valueOf(16))
        ),
        DruidExpression.fromExpression("parse_long(\"hexstr\",16)"),
        239L
    );

    testHelper.testExpression(
        new ParseLongOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeCall(
                SqlStdOperatorTable.CONCAT,
                testHelper.makeLiteral("0x"),
                testHelper.makeInputRef("hexstr")
            ),
            testHelper.makeLiteral(BigDecimal.valueOf(16))
        ),
        DruidExpression.fromExpression("parse_long(concat('0x',\"hexstr\"),16)"),
        239L
    );

    testHelper.testExpression(
        new ParseLongOperatorConversion().calciteOperator(),
        testHelper.makeInputRef("hexstr"),
        DruidExpression.fromExpression("parse_long(\"hexstr\")"),
        NullHandling.sqlCompatible() ? null : 0L
    );
  }

  @Test
  public void testPosition()
  {
    testHelper.testExpression(
        SqlStdOperatorTable.POSITION,
        ImmutableList.of(
            testHelper.makeLiteral("oo"),
            testHelper.makeInputRef("s")
        ),
        DruidExpression.fromExpression("(strpos(\"s\",'oo',0) + 1)"),
        2L
    );

    testHelper.testExpression(
        SqlStdOperatorTable.POSITION,
        ImmutableList.of(
            testHelper.makeLiteral("oo"),
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(BigDecimal.valueOf(2))
        ),
        DruidExpression.fromExpression("(strpos(\"s\",'oo',(2 - 1)) + 1)"),
        2L
    );

    testHelper.testExpression(
        SqlStdOperatorTable.POSITION,
        ImmutableList.of(
            testHelper.makeLiteral("oo"),
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(BigDecimal.valueOf(3))
        ),
        DruidExpression.fromExpression("(strpos(\"s\",'oo',(3 - 1)) + 1)"),
        0L
    );
  }

  @Test
  public void testPower()
  {
    testHelper.testExpression(
        SqlStdOperatorTable.POWER,
        ImmutableList.of(
            testHelper.makeInputRef("a"),
            testHelper.makeLiteral(2)
        ),
        DruidExpression.fromExpression("pow(\"a\",2)"),
        100.0
    );
  }

  @Test
  public void testFloor()
  {
    testHelper.testExpression(
        SqlStdOperatorTable.FLOOR,
        testHelper.makeInputRef("a"),
        DruidExpression.fromExpression("floor(\"a\")"),
        10.0
    );

    testHelper.testExpression(
        SqlStdOperatorTable.FLOOR,
        testHelper.makeInputRef("x"),
        DruidExpression.fromExpression("floor(\"x\")"),
        2.0
    );

    testHelper.testExpression(
        SqlStdOperatorTable.FLOOR,
        testHelper.makeInputRef("y"),
        DruidExpression.fromExpression("floor(\"y\")"),
        3.0
    );

    testHelper.testExpression(
        SqlStdOperatorTable.FLOOR,
        testHelper.makeInputRef("z"),
        DruidExpression.fromExpression("floor(\"z\")"),
        -3.0
    );
  }

  @Test
  public void testCeil()
  {
    testHelper.testExpression(
        SqlStdOperatorTable.CEIL,
        testHelper.makeInputRef("a"),
        DruidExpression.fromExpression("ceil(\"a\")"),
        10.0
    );

    testHelper.testExpression(
        SqlStdOperatorTable.CEIL,
        testHelper.makeInputRef("x"),
        DruidExpression.fromExpression("ceil(\"x\")"),
        3.0
    );

    testHelper.testExpression(
        SqlStdOperatorTable.CEIL,
        testHelper.makeInputRef("y"),
        DruidExpression.fromExpression("ceil(\"y\")"),
        3.0
    );

    testHelper.testExpression(
        SqlStdOperatorTable.CEIL,
        testHelper.makeInputRef("z"),
        DruidExpression.fromExpression("ceil(\"z\")"),
        -2.0
    );
  }

  @Test
  public void testTruncate()
  {
    final SqlFunction truncateFunction = new TruncateOperatorConversion().calciteOperator();

    testHelper.testExpression(
        truncateFunction,
        testHelper.makeInputRef("a"),
        DruidExpression.fromExpression("(cast(cast(\"a\" * 1,'long'),'double') / 1)"),
        10.0
    );

    testHelper.testExpression(
        truncateFunction,
        testHelper.makeInputRef("x"),
        DruidExpression.fromExpression("(cast(cast(\"x\" * 1,'long'),'double') / 1)"),
        2.0
    );

    testHelper.testExpression(
        truncateFunction,
        testHelper.makeInputRef("y"),
        DruidExpression.fromExpression("(cast(cast(\"y\" * 1,'long'),'double') / 1)"),
        3.0
    );

    testHelper.testExpression(
        truncateFunction,
        testHelper.makeInputRef("z"),
        DruidExpression.fromExpression("(cast(cast(\"z\" * 1,'long'),'double') / 1)"),
        -2.0
    );

    testHelper.testExpression(
        truncateFunction,
        ImmutableList.of(
            testHelper.makeInputRef("x"),
            testHelper.makeLiteral(1)
        ),
        DruidExpression.fromExpression("(cast(cast(\"x\" * 10.0,'long'),'double') / 10.0)"),
        2.2
    );

    testHelper.testExpression(
        truncateFunction,
        ImmutableList.of(
            testHelper.makeInputRef("z"),
            testHelper.makeLiteral(1)
        ),
        DruidExpression.fromExpression("(cast(cast(\"z\" * 10.0,'long'),'double') / 10.0)"),
        -2.2
    );

    testHelper.testExpression(
        truncateFunction,
        ImmutableList.of(
            testHelper.makeInputRef("b"),
            testHelper.makeLiteral(-1)
        ),
        DruidExpression.fromExpression("(cast(cast(\"b\" * 0.1,'long'),'double') / 0.1)"),
        20.0
    );

    testHelper.testExpression(
        truncateFunction,
        ImmutableList.of(
            testHelper.makeInputRef("z"),
            testHelper.makeLiteral(-1)
        ),
        DruidExpression.fromExpression("(cast(cast(\"z\" * 0.1,'long'),'double') / 0.1)"),
        0.0
    );
  }

  @Test
  public void testRound()
  {
    final SqlFunction roundFunction = new RoundOperatorConversion().calciteOperator();

    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("a"),
        DruidExpression.fromExpression("round(\"a\")"),
        10L
    );

    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("b"),
        DruidExpression.fromExpression("round(\"b\")"),
        25L
    );

    testHelper.testExpression(
        roundFunction,
        ImmutableList.of(
            testHelper.makeInputRef("b"),
            testHelper.makeLiteral(-1)
        ),
        DruidExpression.fromExpression("round(\"b\",-1)"),
        30L
    );

    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("x"),
        DruidExpression.fromExpression("round(\"x\")"),
        2.0
    );

    testHelper.testExpression(
        roundFunction,
        ImmutableList.of(
            testHelper.makeInputRef("x"),
            testHelper.makeLiteral(1)
        ),
        DruidExpression.fromExpression("round(\"x\",1)"),
        2.3
    );

    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("y"),
        DruidExpression.fromExpression("round(\"y\")"),
        3.0
    );

    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("z"),
        DruidExpression.fromExpression("round(\"z\")"),
        -2.0
    );
  }

  @Test
  public void testRoundWithInvalidArgument()
  {
    final SqlFunction roundFunction = new RoundOperatorConversion().calciteOperator();

    expectException(
        IAE.class,
        "The first argument to the function[round] should be integer or double type but get the STRING type"
    );
    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("s"),
        DruidExpression.fromExpression("round(\"s\")"),
        "IAE Exception"
    );
  }

  @Test
  public void testRoundWithInvalidSecondArgument()
  {
    final SqlFunction roundFunction = new RoundOperatorConversion().calciteOperator();

    expectException(
        IAE.class,
        "The second argument to the function[round] should be integer type but get the STRING type"
    );
    testHelper.testExpression(
        roundFunction,
        ImmutableList.of(
            testHelper.makeInputRef("x"),
            testHelper.makeLiteral("foo")
        ),
        DruidExpression.fromExpression("round(\"x\",'foo')"),
        "IAE Exception"
    );
  }

  @Test
  public void testDateTrunc()
  {
    testHelper.testExpression(
        new DateTruncOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("hour"),
            testHelper.makeLiteral(DateTimes.of("2000-02-03T04:05:06Z"))
        ),
        DruidExpression.fromExpression("timestamp_floor(949550706000,'PT1H',null,'UTC')"),
        DateTimes.of("2000-02-03T04:00:00").getMillis()
    );

    testHelper.testExpression(
        new DateTruncOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("DAY"),
            testHelper.makeLiteral(DateTimes.of("2000-02-03T04:05:06Z"))
        ),
        DruidExpression.fromExpression("timestamp_floor(949550706000,'P1D',null,'UTC')"),
        DateTimes.of("2000-02-03T00:00:00").getMillis()
    );
  }

  @Test
  public void testTrim()
  {
    testHelper.testExpression(
        SqlStdOperatorTable.TRIM,
        ImmutableList.of(
            testHelper.makeFlag(SqlTrimFunction.Flag.BOTH),
            testHelper.makeLiteral(" "),
            testHelper.makeInputRef("spacey")
        ),
        DruidExpression.fromExpression("trim(\"spacey\",' ')"),
        "hey there"
    );

    testHelper.testExpression(
        SqlStdOperatorTable.TRIM,
        ImmutableList.of(
            testHelper.makeFlag(SqlTrimFunction.Flag.LEADING),
            testHelper.makeLiteral(" h"),
            testHelper.makeInputRef("spacey")
        ),
        DruidExpression.fromExpression("ltrim(\"spacey\",' h')"),
        "ey there  "
    );

    testHelper.testExpression(
        SqlStdOperatorTable.TRIM,
        ImmutableList.of(
            testHelper.makeFlag(SqlTrimFunction.Flag.TRAILING),
            testHelper.makeLiteral(" e"),
            testHelper.makeInputRef("spacey")
        ),
        DruidExpression.fromExpression("rtrim(\"spacey\",' e')"),
        "  hey ther"
    );
  }

  @Test
  public void testPad()
  {
    testHelper.testExpression(
        new LPadOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(5),
            testHelper.makeLiteral("x")
        ),
        DruidExpression.fromExpression("lpad(\"s\",5,'x')"),
        "xxfoo"
    );

    testHelper.testExpression(
        new RPadOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(5),
            testHelper.makeLiteral("x")
        ),
        DruidExpression.fromExpression("rpad(\"s\",5,'x')"),
        "fooxx"
    );
  }


  @Test
  public void testTimeFloor()
  {
    testHelper.testExpression(
        new TimeFloorOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(DateTimes.of("2000-02-03T04:05:06Z")),
            testHelper.makeLiteral("PT1H")
        ),
        DruidExpression.fromExpression("timestamp_floor(949550706000,'PT1H',null,'UTC')"),
        DateTimes.of("2000-02-03T04:00:00").getMillis()
    );

    testHelper.testExpression(
        new TimeFloorOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral("P1D"),
            testHelper.makeNullLiteral(SqlTypeName.TIMESTAMP),
            testHelper.makeLiteral("America/Los_Angeles")
        ),
        DruidExpression.fromExpression("timestamp_floor(\"t\",'P1D',null,'America/Los_Angeles')"),
        DateTimes.of("2000-02-02T08:00:00").getMillis()
    );
  }

  @Test
  public void testOtherTimeFloor()
  {
    // FLOOR(__time TO unit)

    testHelper.testExpression(
        SqlStdOperatorTable.FLOOR,
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeFlag(TimeUnitRange.YEAR)
        ),
        DruidExpression.fromExpression("timestamp_floor(\"t\",'P1Y',null,'UTC')"),
        DateTimes.of("2000").getMillis()
    );
  }

  @Test
  public void testTimeCeil()
  {
    testHelper.testExpression(
        new TimeCeilOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(DateTimes.of("2000-02-03T04:05:06Z")),
            testHelper.makeLiteral("PT1H")
        ),
        DruidExpression.fromExpression("timestamp_ceil(949550706000,'PT1H',null,'UTC')"),
        DateTimes.of("2000-02-03T05:00:00").getMillis()
    );

    testHelper.testExpression(
        new TimeCeilOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral("P1D"),
            testHelper.makeNullLiteral(SqlTypeName.TIMESTAMP),
            testHelper.makeLiteral("America/Los_Angeles")
        ),
        DruidExpression.fromExpression("timestamp_ceil(\"t\",'P1D',null,'America/Los_Angeles')"),
        DateTimes.of("2000-02-03T08:00:00").getMillis()
    );
  }

  @Test
  public void testOtherTimeCeil()
  {
    // CEIL(__time TO unit)

    testHelper.testExpression(
        SqlStdOperatorTable.CEIL,
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeFlag(TimeUnitRange.YEAR)
        ),
        DruidExpression.fromExpression("timestamp_ceil(\"t\",'P1Y',null,'UTC')"),
        DateTimes.of("2001").getMillis()
    );
  }

  @Test
  public void testTimeShift()
  {
    testHelper.testExpression(
        new TimeShiftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral("PT2H"),
            testHelper.makeLiteral(-3)
        ),
        DruidExpression.fromExpression("timestamp_shift(\"t\",'PT2H',-3,'UTC')"),
        DateTimes.of("2000-02-02T22:05:06").getMillis()
    );

    testHelper.testExpression(
        new TimeShiftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral("PT2H"),
            testHelper.makeLiteral(-3),
            testHelper.makeLiteral("America/Los_Angeles")
        ),
        DruidExpression.fromExpression("timestamp_shift(\"t\",'PT2H',-3,'America/Los_Angeles')"),
        DateTimes.of("2000-02-02T22:05:06").getMillis()
    );
  }

  @Test
  public void testTimeExtract()
  {
    testHelper.testExpression(
        new TimeExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral("QUARTER")
        ),
        DruidExpression.fromExpression("timestamp_extract(\"t\",'QUARTER','UTC')"),
        1L
    );

    testHelper.testExpression(
        new TimeExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral("DAY"),
            testHelper.makeLiteral("America/Los_Angeles")
        ),
        DruidExpression.fromExpression("timestamp_extract(\"t\",'DAY','America/Los_Angeles')"),
        2L
    );
  }

  @Test
  public void testTimePlusDayTimeInterval()
  {
    final Period period = new Period("P1DT1H1M");

    testHelper.testExpression(
        SqlStdOperatorTable.DATETIME_PLUS,
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral(
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

    testHelper.testExpression(
        SqlStdOperatorTable.DATETIME_PLUS,
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral(
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

    testHelper.testExpression(
        SqlTypeName.TIMESTAMP,
        SqlStdOperatorTable.MINUS_DATE,
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral(
                new BigDecimal(period.toStandardDuration().getMillis()), // DAY-TIME literals value is millis
                new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.MINUTE, SqlParserPos.ZERO)
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

    testHelper.testExpression(
        SqlTypeName.TIMESTAMP,
        SqlStdOperatorTable.MINUS_DATE,
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral(
                new BigDecimal(13), // YEAR-MONTH literals value is months
                new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO)
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
    testHelper.testExpression(
        new TimeParseOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("tstr"),
            testHelper.makeLiteral("yyyy-MM-dd HH:mm:ss")
        ),
        DruidExpression.fromExpression("timestamp_parse(\"tstr\",'yyyy-MM-dd HH:mm:ss','UTC')"),
        DateTimes.of("2000-02-03T04:05:06").getMillis()
    );

    testHelper.testExpression(
        new TimeParseOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("tstr"),
            testHelper.makeLiteral("yyyy-MM-dd HH:mm:ss"),
            testHelper.makeLiteral("America/Los_Angeles")
        ),
        DruidExpression.fromExpression("timestamp_parse(\"tstr\",'yyyy-MM-dd HH:mm:ss','America/Los_Angeles')"),
        DateTimes.of("2000-02-03T04:05:06-08:00").getMillis()
    );
  }

  @Test
  public void testTimeFormat()
  {
    testHelper.testExpression(
        new TimeFormatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral("yyyy-MM-dd HH:mm:ss")
        ),
        DruidExpression.fromExpression("timestamp_format(\"t\",'yyyy-MM-dd HH:mm:ss','UTC')"),
        "2000-02-03 04:05:06"
    );

    testHelper.testExpression(
        new TimeFormatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral("yyyy-MM-dd HH:mm:ss"),
            testHelper.makeLiteral("America/Los_Angeles")
        ),
        DruidExpression.fromExpression("timestamp_format(\"t\",'yyyy-MM-dd HH:mm:ss','America/Los_Angeles')"),
        "2000-02-02 20:05:06"
    );
  }

  @Test
  public void testExtract()
  {
    testHelper.testExpression(
        SqlStdOperatorTable.EXTRACT,
        ImmutableList.of(
            testHelper.makeFlag(TimeUnitRange.QUARTER),
            testHelper.makeInputRef("t")
        ),
        DruidExpression.fromExpression("timestamp_extract(\"t\",'QUARTER','UTC')"),
        1L
    );

    testHelper.testExpression(
        SqlStdOperatorTable.EXTRACT,
        ImmutableList.of(
            testHelper.makeFlag(TimeUnitRange.DAY),
            testHelper.makeInputRef("t")
        ),
        DruidExpression.fromExpression("timestamp_extract(\"t\",'DAY','UTC')"),
        3L
    );
  }

  @Test
  public void testCastAsTimestamp()
  {
    testHelper.testExpression(
        testHelper.makeAbstractCast(
            testHelper.createSqlType(SqlTypeName.TIMESTAMP),
            testHelper.makeInputRef("t")
        ),
        DruidExpression.of(
            SimpleExtraction.of("t", null),
            "\"t\""
        ),
        DateTimes.of("2000-02-03T04:05:06Z").getMillis()
    );

    testHelper.testExpression(
        testHelper.makeAbstractCast(
            testHelper.createSqlType(SqlTypeName.TIMESTAMP),
            testHelper.makeInputRef("tstr")
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
    testHelper.testExpression(
        testHelper.makeAbstractCast(
            testHelper.createSqlType(SqlTypeName.VARCHAR),
            testHelper.makeAbstractCast(
                testHelper.createSqlType(SqlTypeName.TIMESTAMP),
                testHelper.makeInputRef("t")
            )
        ),
        DruidExpression.fromExpression(
            "timestamp_format(\"t\",'yyyy-MM-dd HH:mm:ss','UTC')"
        ),
        "2000-02-03 04:05:06"
    );

    testHelper.testExpression(
        testHelper.makeAbstractCast(
            testHelper.createSqlType(SqlTypeName.BIGINT),
            testHelper.makeAbstractCast(
                testHelper.createSqlType(SqlTypeName.TIMESTAMP),
                testHelper.makeInputRef("t")
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
    testHelper.testExpression(
        testHelper.makeAbstractCast(
            testHelper.createSqlType(SqlTypeName.DATE),
            testHelper.makeInputRef("t")
        ),
        DruidExpression.fromExpression("timestamp_floor(\"t\",'P1D',null,'UTC')"),
        DateTimes.of("2000-02-03").getMillis()
    );

    testHelper.testExpression(
        testHelper.makeAbstractCast(
            testHelper.createSqlType(SqlTypeName.DATE),
            testHelper.makeInputRef("dstr")
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
    testHelper.testExpression(
        testHelper.makeAbstractCast(
            testHelper.createSqlType(SqlTypeName.VARCHAR),
            testHelper.makeAbstractCast(
                testHelper.createSqlType(SqlTypeName.DATE),
                testHelper.makeInputRef("t")
            )
        ),
        DruidExpression.fromExpression(
            "timestamp_format(timestamp_floor(\"t\",'P1D',null,'UTC'),'yyyy-MM-dd','UTC')"
        ),
        "2000-02-03"
    );

    testHelper.testExpression(
        testHelper.makeAbstractCast(
            testHelper.createSqlType(SqlTypeName.BIGINT),
            testHelper.makeAbstractCast(
                testHelper.createSqlType(SqlTypeName.DATE),
                testHelper.makeInputRef("t")
            )
        ),
        DruidExpression.fromExpression("timestamp_floor(\"t\",'P1D',null,'UTC')"),
        DateTimes.of("2000-02-03").getMillis()
    );
  }

  @Test
  public void testReverse()
  {
    testHelper.testExpression(
        new ReverseOperatorConversion().calciteOperator(),
        testHelper.makeInputRef("s"),
        DruidExpression.fromExpression("reverse(\"s\")"),
        "oof"
    );

    testHelper.testExpression(
        new ReverseOperatorConversion().calciteOperator(),
        testHelper.makeInputRef("spacey"),
        DruidExpression.fromExpression("reverse(\"spacey\")"),
        "  ereht yeh  "
    );

    testHelper.testExpression(
        new ReverseOperatorConversion().calciteOperator(),
        testHelper.makeInputRef("tstr"),
        DruidExpression.fromExpression("reverse(\"tstr\")"),
        "60:50:40 30-20-0002"
    );

    testHelper.testExpression(
        new ReverseOperatorConversion().calciteOperator(),
        testHelper.makeInputRef("dstr"),
        DruidExpression.fromExpression("reverse(\"dstr\")"),
        "30-20-0002"
    );
  }

  @Test
  public void testAbnormalReverseWithWrongType()
  {
    expectException(IAE.class, "Function[reverse] needs a string argument");

    testHelper.testExpression(
        new ReverseOperatorConversion().calciteOperator(),
        testHelper.makeInputRef("a"),
        DruidExpression.fromExpression("reverse(\"a\")"),
        null
    );
  }

  @Test
  public void testRight()
  {
    testHelper.testExpression(
        new RightOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1)
        ),
        DruidExpression.fromExpression("right(\"s\",1)"),
        "o"
    );

    testHelper.testExpression(
        new RightOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(2)
        ),
        DruidExpression.fromExpression("right(\"s\",2)"),
        "oo"
    );

    testHelper.testExpression(
        new RightOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(3)
        ),
        DruidExpression.fromExpression("right(\"s\",3)"),
        "foo"
    );

    testHelper.testExpression(
        new RightOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(4)
        ),
        DruidExpression.fromExpression("right(\"s\",4)"),
        "foo"
    );

    testHelper.testExpression(
        new RightOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("tstr"),
            testHelper.makeLiteral(5)
        ),
        DruidExpression.fromExpression("right(\"tstr\",5)"),
        "05:06"
    );
  }

  @Test
  public void testAbnormalRightWithNegativeNumber()
  {
    expectException(IAE.class, "Function[right] needs a postive integer as second argument");

    testHelper.testExpression(
        new RightOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(-1)
        ),
        DruidExpression.fromExpression("right(\"s\",-1)"),
        null
    );
  }

  @Test
  public void testAbnormalRightWithWrongType()
  {
    expectException(IAE.class, "Function[right] needs a string as first argument and an integer as second argument");

    testHelper.testExpression(
        new RightOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeInputRef("s")
        ),
        DruidExpression.fromExpression("right(\"s\",\"s\")"),
        null
    );
  }

  @Test
  public void testLeft()
  {
    testHelper.testExpression(
        new LeftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1)
        ),
        DruidExpression.fromExpression("left(\"s\",1)"),
        "f"
    );

    testHelper.testExpression(
        new LeftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(2)
        ),
        DruidExpression.fromExpression("left(\"s\",2)"),
        "fo"
    );

    testHelper.testExpression(
        new LeftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(3)
        ),
        DruidExpression.fromExpression("left(\"s\",3)"),
        "foo"
    );

    testHelper.testExpression(
        new LeftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(4)
        ),
        DruidExpression.fromExpression("left(\"s\",4)"),
        "foo"
    );

    testHelper.testExpression(
        new LeftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("tstr"),
            testHelper.makeLiteral(10)
        ),
        DruidExpression.fromExpression("left(\"tstr\",10)"),
        "2000-02-03"
    );
  }

  @Test
  public void testAbnormalLeftWithNegativeNumber()
  {
    expectException(IAE.class, "Function[left] needs a postive integer as second argument");

    testHelper.testExpression(
        new LeftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(-1)
        ),
        DruidExpression.fromExpression("left(\"s\",-1)"),
        null
    );
  }

  @Test
  public void testAbnormalLeftWithWrongType()
  {
    expectException(IAE.class, "Function[left] needs a string as first argument and an integer as second argument");

    testHelper.testExpression(
        new LeftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeInputRef("s")
        ),
        DruidExpression.fromExpression("left(\"s\",\"s\")"),
        null
    );
  }

  @Test
  public void testRepeat()
  {
    testHelper.testExpression(
        new RepeatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1)
        ),
        DruidExpression.fromExpression("repeat(\"s\",1)"),
        "foo"
    );

    testHelper.testExpression(
        new RepeatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(3)
        ),
        DruidExpression.fromExpression("repeat(\"s\",3)"),
        "foofoofoo"
    );

    testHelper.testExpression(
        new RepeatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(-1)
        ),
        DruidExpression.fromExpression("repeat(\"s\",-1)"),
        null
    );
  }

  @Test
  public void testAbnormalRepeatWithWrongType()
  {
    expectException(IAE.class, "Function[repeat] needs a string as first argument and an integer as second argument");

    testHelper.testExpression(
        new RepeatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeInputRef("s")
        ),
        DruidExpression.fromExpression("repeat(\"s\",\"s\")"),
        null
    );
  }
}
