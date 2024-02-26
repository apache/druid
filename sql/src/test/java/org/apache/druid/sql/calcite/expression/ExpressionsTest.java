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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.extraction.RegexDimExtractionFn;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.query.filter.RegexDimFilter;
import org.apache.druid.query.filter.SearchQueryDimFilter;
import org.apache.druid.query.search.ContainsSearchQuerySpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.expression.builtin.ContainsOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.DateTruncOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.HumanReadableFormatOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.LPadOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.LeftOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.ParseLongOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.RPadOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.RegexpExtractOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.RegexpLikeOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.RegexpReplaceOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.RepeatOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.ReverseOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.RightOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.StringFormatOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.StrposOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.SubstringOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeCeilOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeExtractOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeFloorOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeFormatOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeParseOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeShiftOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TruncateOperatorConversion;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;

public class ExpressionsTest extends CalciteTestBase
{
  private static final RowSignature ROW_SIGNATURE = RowSignature
      .builder()
      .add("t", ColumnType.LONG)
      .add("a", ColumnType.LONG)
      .add("b", ColumnType.LONG)
      .add("p", ColumnType.LONG)
      .add("x", ColumnType.FLOAT)
      .add("y", ColumnType.LONG)
      .add("z", ColumnType.FLOAT)
      .add("s", ColumnType.STRING)
      .add("nan", ColumnType.DOUBLE)
      .add("inf", ColumnType.DOUBLE)
      .add("-inf", ColumnType.DOUBLE)
      .add("fnan", ColumnType.FLOAT)
      .add("finf", ColumnType.FLOAT)
      .add("-finf", ColumnType.FLOAT)
      .add("hexstr", ColumnType.STRING)
      .add("intstr", ColumnType.STRING)
      .add("spacey", ColumnType.STRING)
      .add("newliney", ColumnType.STRING)
      .add("tstr", ColumnType.STRING)
      .add("dstr", ColumnType.STRING)
      .add("timezone", ColumnType.STRING)
      .build();

  private static final Map<String, Object> BINDINGS = ImmutableMap.<String, Object>builder()
      .put("t", DateTimes.of("2000-02-03T04:05:06").getMillis())
      .put("a", 10)
      .put("b", 25)
      .put("p", 3)
      .put("x", 2.25)
      .put("y", 3.0)
      .put("z", -2.25)
      .put("o", 0)
      .put("nan", Double.NaN)
      .put("inf", Double.POSITIVE_INFINITY)
      .put("-inf", Double.NEGATIVE_INFINITY)
      .put("fnan", Float.NaN)
      .put("finf", Float.POSITIVE_INFINITY)
      .put("-finf", Float.NEGATIVE_INFINITY)
      .put("s", "foo")
      .put("hexstr", "EF")
      .put("intstr", "-100")
      .put("spacey", "  hey there  ")
      .put("newliney", "beep\nboop")
      .put("tstr", "2000-02-03 04:05:06")
      .put("dstr", "2000-02-03")
      .put("timezone", "America/Los_Angeles")
      .build();

  private ExpressionTestHelper testHelper;

  final DruidOperatorTable operatorTable = new DruidOperatorTable(Collections.emptySet(), Collections.emptySet());

  @Before
  public void setUp()
  {
    testHelper = new ExpressionTestHelper(ROW_SIGNATURE, BINDINGS);
  }

  private SqlOperatorConversion getOperatorConversion(SqlFunction round)
  {
    return operatorTable.lookupOperatorConversion(round);
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
        DruidExpression.ofExpression(
            ColumnType.STRING,
            DruidExpression.functionCall("concat"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofStringLiteral("bar")
            )
        ),
        "foobar"
    );
  }

  @Test
  public void testCharacterLength()
  {
    testHelper.testExpression(
        SqlStdOperatorTable.CHARACTER_LENGTH,
        testHelper.makeInputRef("s"),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            DruidExpression.functionCall("strlen"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s")
            )
        ),
        3L
    );
  }

  @Test
  public void testSubstring()
  {
    testHelper.testExpressionString(
        new SubstringOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1),
            testHelper.makeLiteral(2)
        ),
        makeExpression(
            SimpleExtraction.of("s", new SubstringDimExtractionFn(0, 2)),
            "substring(\"s\", 0, 2)"
        ),
        "fo"
    );

    testHelper.testExpressionString(
        new SubstringOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(2),
            testHelper.makeLiteral(1)
        ),
        makeExpression(
            SimpleExtraction.of("s", new SubstringDimExtractionFn(1, 1)),
            "substring(\"s\", 1, 1)"
        ),
        "o"
    );

    testHelper.testExpressionString(
        new SubstringOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1)
        ),
        makeExpression(
            SimpleExtraction.of("s", new SubstringDimExtractionFn(0, null)),
            "substring(\"s\", 0, -1)"
        ),
        "foo"
    );

    testHelper.testExpressionString(
        new SubstringOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(2)
        ),
        makeExpression(
            SimpleExtraction.of("s", new SubstringDimExtractionFn(1, null)),
            "substring(\"s\", 1, -1)"
        ),
        "oo"
    );

    testHelper.testExpressionString(
        new SubstringOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1),
            testHelper.makeInputRef("p") // p is 3
        ),
        makeExpression("substring(\"s\", 0, \"p\")"),
        "foo"
    );

    testHelper.testExpressionString(
        new SubstringOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("spacey"),
            testHelper.makeInputRef("p") // p is 3
        ),
        makeExpression("substring(\"spacey\", (\"p\" - 1), -1)"),
        "hey there  "
    );

    testHelper.testExpressionString(
        new SubstringOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("spacey"),
            testHelper.makeInputRef("p"), // p is 3
            testHelper.makeInputRef("p") // p is 3
        ),
        makeExpression("substring(\"spacey\", (\"p\" - 1), \"p\")"),
        "hey"
    );
  }

  @Test
  public void testRegexpExtract()
  {
    testHelper.testExpressionString(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("x(.)"),
            testHelper.makeLiteral(1)
        ),
        makeExpression(
            SimpleExtraction.of("s", new RegexDimExtractionFn("x(.)", 1, true, null)),
            "regexp_extract(\"s\",'x(.)',1)"
        ),
        null
    );

    testHelper.testExpressionString(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("(o)"),
            testHelper.makeLiteral(1)
        ),
        makeExpression(
            SimpleExtraction.of("s", new RegexDimExtractionFn("(o)", 1, true, null)),
            "regexp_extract(\"s\",'(o)',1)"
        ),

        // Column "s" contains an 'o', but not at the beginning; we do match this.
        "o"
    );

    testHelper.testExpressionString(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeCall(
                SqlStdOperatorTable.CONCAT,
                testHelper.makeLiteral("Z"),
                testHelper.makeInputRef("s")
            ),
            testHelper.makeLiteral("Zf(.)")
        ),
        makeExpression("regexp_extract(concat('Z',\"s\"),'Zf(.)')"),
        "Zfo"
    );

    testHelper.testExpressionString(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("f(.)"),
            testHelper.makeLiteral(1)
        ),
        makeExpression(
            SimpleExtraction.of("s", new RegexDimExtractionFn("f(.)", 1, true, null)),
            "regexp_extract(\"s\",'f(.)',1)"
        ),
        "o"
    );

    testHelper.testExpressionString(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("f(.)")
        ),
        makeExpression(
            SimpleExtraction.of("s", new RegexDimExtractionFn("f(.)", 0, true, null)),
            "regexp_extract(\"s\",'f(.)')"
        ),
        "fo"
    );

    testHelper.testExpressionString(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("")
        ),
        makeExpression(
            SimpleExtraction.of("s", new RegexDimExtractionFn("", 0, true, null)),
            "regexp_extract(\"s\",'')"
        ),
        NullHandling.emptyToNullIfNeeded("")
    );

    testHelper.testExpressionString(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("")
        ),
        makeExpression(
            SimpleExtraction.of("s", new RegexDimExtractionFn("", 0, true, null)),
            "regexp_extract(\"s\",'')"
        ),
        NullHandling.emptyToNullIfNeeded("")
    );

    testHelper.testExpressionString(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeNullLiteral(SqlTypeName.VARCHAR),
            testHelper.makeLiteral("(.)")
        ),
        makeExpression("regexp_extract(null,'(.)')"),
        null
    );

    testHelper.testExpressionString(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeNullLiteral(SqlTypeName.VARCHAR),
            testHelper.makeLiteral("")
        ),
        makeExpression("regexp_extract(null,'')"),
        null
    );

    testHelper.testExpressionString(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeNullLiteral(SqlTypeName.VARCHAR),
            testHelper.makeLiteral("null")
        ),
        makeExpression("regexp_extract(null,'null')"),
        null
    );
  }

  @Test
  public void testRegexpReplace()
  {
    testHelper.testExpressionString(
        new RegexpReplaceOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("x(.)"),
            testHelper.makeLiteral("z")
        ),
        makeExpression("regexp_replace(\"s\",'x(.)','z')"),
        "foo"
    );

    testHelper.testExpressionString(
        new RegexpReplaceOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("(o)"),
            testHelper.makeLiteral("z")
        ),
        makeExpression("regexp_replace(\"s\",'(o)','z')"),
        "fzz"
    );

    testHelper.testExpressionString(
        new RegexpReplaceOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeCall(
                SqlStdOperatorTable.CONCAT,
                testHelper.makeLiteral("Z"),
                testHelper.makeInputRef("s")
            ),
            testHelper.makeLiteral("Zf(.)"),
            testHelper.makeLiteral("z")
        ),
        makeExpression("regexp_replace(concat('Z',\"s\"),'Zf(.)','z')"),
        "zo"
    );

    testHelper.testExpressionString(
        new RegexpReplaceOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("f(.)"),
            testHelper.makeLiteral("$1")
        ),
        makeExpression("regexp_replace(\"s\",'f(.)','$1')"),
        "oo"
    );
  }

  @Test
  public void testRegexpLike()
  {
    testHelper.testExpressionString(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("f.")
        ),
        makeExpression("regexp_like(\"s\",'f.')"),
        1L
    );

    testHelper.testExpressionString(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("o")
        ),
        makeExpression("regexp_like(\"s\",'o')"),

        // Column "s" contains an 'o', but not at the beginning; we do match this.
        1L
    );

    testHelper.testExpressionString(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("x.")
        ),
        makeExpression("regexp_like(\"s\",'x.')"),
        0L
    );

    testHelper.testExpressionString(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("")
        ),
        makeExpression("regexp_like(\"s\",'')"),
        1L
    );

    testHelper.testExpressionString(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("beep\nboop"),
            testHelper.makeLiteral("^beep$")
        ),
        makeExpression("regexp_like('beep\\u000Aboop','^beep$')"),
        0L
    );

    testHelper.testExpressionString(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("beep\nboop"),
            testHelper.makeLiteral("^beep\\nboop$")
        ),
        makeExpression("regexp_like('beep\\u000Aboop','^beep\\u005Cnboop$')"),
        1L
    );

    testHelper.testExpressionString(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("newliney"),
            testHelper.makeLiteral("^beep$")
        ),
        makeExpression("regexp_like(\"newliney\",'^beep$')"),
        0L
    );

    testHelper.testExpressionString(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("newliney"),
            testHelper.makeLiteral("^beep\\nboop$")
        ),
        makeExpression("regexp_like(\"newliney\",'^beep\\u005Cnboop$')"),
        1L
    );

    testHelper.testExpressionString(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("newliney"),
            testHelper.makeLiteral("boo")
        ),
        makeExpression("regexp_like(\"newliney\",'boo')"),
        1L
    );

    testHelper.testExpressionString(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("newliney"),
            testHelper.makeLiteral("^boo")
        ),
        makeExpression("regexp_like(\"newliney\",'^boo')"),
        0L
    );

    testHelper.testExpressionString(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeCall(
                SqlStdOperatorTable.CONCAT,
                testHelper.makeLiteral("Z"),
                testHelper.makeInputRef("s")
            ),
            testHelper.makeLiteral("x(.)")
        ),
        makeExpression("regexp_like(concat('Z',\"s\"),'x(.)')"),
        0L
    );

    testHelper.testExpressionString(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeNullLiteral(SqlTypeName.VARCHAR),
            testHelper.makeLiteral("(.)")
        ),
        makeExpression("regexp_like(null,'(.)')"),
        0L
    );

    testHelper.testExpressionString(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeNullLiteral(SqlTypeName.VARCHAR),
            testHelper.makeLiteral("")
        ),
        makeExpression("regexp_like(null,'')"),

        // In SQL-compatible mode, nulls don't match anything. Otherwise, they match like empty strings.
        NullHandling.sqlCompatible() ? 0L : 1L
    );

    testHelper.testExpressionString(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeNullLiteral(SqlTypeName.VARCHAR),
            testHelper.makeLiteral("null")
        ),
        makeExpression("regexp_like(null,'null')"),
        0L
    );
  }

  @Test
  public void testRegexpLikeAsFilter()
  {
    testHelper.testFilter(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("f.")
        ),
        Collections.emptyList(),
        new RegexDimFilter("s", "f.", null),
        true
    );

    testHelper.testFilter(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("o")
        ),
        Collections.emptyList(),
        // Column "s" contains an 'o', but not at the beginning, so we don't match
        new RegexDimFilter("s", "o", null),
        true
    );

    testHelper.testFilter(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("x.")
        ),
        Collections.emptyList(),
        new RegexDimFilter("s", "x.", null),
        false
    );

    testHelper.testFilter(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("")
        ),
        Collections.emptyList(),
        new RegexDimFilter("s", "", null),
        true
    );

    testHelper.testFilter(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("newliney"),
            testHelper.makeLiteral("^beep$")
        ),
        Collections.emptyList(),
        new RegexDimFilter("newliney", "^beep$", null),
        false
    );

    testHelper.testFilter(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("newliney"),
            testHelper.makeLiteral("^beep\\nboop$")
        ),
        Collections.emptyList(),
        new RegexDimFilter("newliney", "^beep\\nboop$", null),
        true
    );

    testHelper.testFilter(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeCall(
                SqlStdOperatorTable.CONCAT,
                testHelper.makeLiteral("Z"),
                testHelper.makeInputRef("s")
            ),
            testHelper.makeLiteral("x(.)")
        ),
        ImmutableList.of(
            new ExpressionVirtualColumn(
                "v0",
                "concat('Z',\"s\")",
                ColumnType.STRING,
                TestExprMacroTable.INSTANCE
            )
        ),
        new RegexDimFilter("v0", "x(.)", null),
        false
    );
  }

  @Test
  public void testStringFormat()
  {
    testHelper.testExpressionString(
        new StringFormatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("%x"),
            testHelper.makeInputRef("b")
        ),
        makeExpression("format('%x',\"b\")"),
        "19"
    );

    testHelper.testExpressionString(
        new StringFormatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("%s %,d"),
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1234)
        ),
        makeExpression("format('%s %,d',\"s\",1234)"),
        "foo 1,234"
    );

    testHelper.testExpressionString(
        new StringFormatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("%s %,d"),
            testHelper.makeInputRef("s")
        ),
        makeExpression("format('%s %,d',\"s\")"),
        "%s %,d; foo"
    );

    testHelper.testExpressionString(
        new StringFormatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("%s %,d"),
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1234),
            testHelper.makeLiteral(6789)
        ),
        makeExpression("format('%s %,d',\"s\",1234,6789)"),
        "foo 1,234"
    );

    testHelper.testExpressionString(
        new StringFormatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("%.2f %.2f %.2f"),
            testHelper.makeLiteral(1234d),
            testHelper.makeLiteral(123.4),
            testHelper.makeLiteral(123.456789)
        ),
        makeExpression("format('%.2f %.2f %.2f',1234.0,123.4,123.456789)"),
        "1234.00 123.40 123.46"
    );
  }

  @Test
  public void testStrpos()
  {
    testHelper.testExpressionString(
        new StrposOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("oo")
        ),
        makeExpression("(strpos(\"s\",'oo') + 1)"),
        2L
    );

    testHelper.testExpressionString(
        new StrposOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("ax")
        ),
        makeExpression("(strpos(\"s\",'ax') + 1)"),
        0L
    );

    testHelper.testExpressionString(
        new StrposOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeNullLiteral(SqlTypeName.VARCHAR),
            testHelper.makeLiteral("ax")
        ),
        makeExpression("(strpos(null,'ax') + 1)"),
        NullHandling.replaceWithDefault() ? 0L : null
    );
  }

  @Test
  public void testParseLong()
  {
    testHelper.testExpression(
        new ParseLongOperatorConversion().calciteOperator(),
        testHelper.makeInputRef("intstr"),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            DruidExpression.functionCall("parse_long"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "intstr")
            )
        ),
        -100L
    );

    testHelper.testExpressionString(
        new ParseLongOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("hexstr"),
            testHelper.makeLiteral(BigDecimal.valueOf(16))
        ),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            DruidExpression.functionCall("parse_long"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "hexstr"),
                DruidExpression.ofLiteral(ColumnType.LONG, DruidExpression.longLiteral(16))
            )
        ),
        239L
    );

    testHelper.testExpressionString(
        new ParseLongOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeCall(
                SqlStdOperatorTable.CONCAT,
                testHelper.makeLiteral("0x"),
                testHelper.makeInputRef("hexstr")
            ),
            testHelper.makeLiteral(BigDecimal.valueOf(16))
        ),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            DruidExpression.functionCall("parse_long"),
            ImmutableList.of(
                DruidExpression.ofExpression(
                    ColumnType.STRING,
                    DruidExpression.functionCall("concat"),
                    ImmutableList.of(
                        DruidExpression.ofStringLiteral("0x"),
                        DruidExpression.ofColumn(ColumnType.STRING, "hexstr")
                    )
                ),
                DruidExpression.ofLiteral(ColumnType.LONG, DruidExpression.longLiteral(16))
            )
        ),
        239L
    );

    testHelper.testExpression(
        new ParseLongOperatorConversion().calciteOperator(),
        testHelper.makeInputRef("hexstr"),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            DruidExpression.functionCall("parse_long"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "hexstr")
            )
        ),
        null
    );
  }

  @Test
  public void testPosition()
  {
    testHelper.testExpressionString(
        SqlStdOperatorTable.POSITION,
        ImmutableList.of(
            testHelper.makeLiteral("oo"),
            testHelper.makeInputRef("s")
        ),
        makeExpression(ColumnType.LONG, "(strpos(\"s\",'oo',0) + 1)"),
        2L
    );

    testHelper.testExpressionString(
        SqlStdOperatorTable.POSITION,
        ImmutableList.of(
            testHelper.makeLiteral("oo"),
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(BigDecimal.valueOf(2))
        ),
        makeExpression(ColumnType.LONG, "(strpos(\"s\",'oo',(2 - 1)) + 1)"),
        2L
    );

    testHelper.testExpressionString(
        SqlStdOperatorTable.POSITION,
        ImmutableList.of(
            testHelper.makeLiteral("oo"),
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(BigDecimal.valueOf(3))
        ),
        makeExpression(ColumnType.LONG, "(strpos(\"s\",'oo',(3 - 1)) + 1)"),
        0L
    );
  }

  @Test
  public void testPower()
  {
    testHelper.testExpressionString(
        SqlStdOperatorTable.POWER,
        ImmutableList.of(
            testHelper.makeInputRef("a"),
            testHelper.makeLiteral(2)
        ),
        makeExpression(ColumnType.DOUBLE, "pow(\"a\",2)"),
        100.0
    );
  }

  @Test
  public void testFloor()
  {
    testHelper.testExpression(
        SqlStdOperatorTable.FLOOR,
        testHelper.makeInputRef("a"),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            DruidExpression.functionCall("floor"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "a")
            )
        ),
        10.0
    );

    testHelper.testExpression(
        SqlStdOperatorTable.FLOOR,
        testHelper.makeInputRef("x"),
        DruidExpression.ofExpression(
            ColumnType.FLOAT,
            DruidExpression.functionCall("floor"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.FLOAT, "x")
            )
        ),
        2.0
    );

    testHelper.testExpression(
        SqlStdOperatorTable.FLOOR,
        testHelper.makeInputRef("y"),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            DruidExpression.functionCall("floor"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "y")
            )
        ),
        3.0
    );

    testHelper.testExpression(
        SqlStdOperatorTable.FLOOR,
        testHelper.makeInputRef("z"),
        DruidExpression.ofExpression(
            ColumnType.FLOAT,
            DruidExpression.functionCall("floor"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.FLOAT, "z")
            )
        ),
        -3.0
    );
  }

  @Test
  public void testCeil()
  {
    testHelper.testExpression(
        SqlStdOperatorTable.CEIL,
        testHelper.makeInputRef("a"),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            DruidExpression.functionCall("ceil"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "a")
            )
        ),
        10.0
    );

    testHelper.testExpression(
        SqlStdOperatorTable.CEIL,
        testHelper.makeInputRef("x"),
        DruidExpression.ofExpression(
            ColumnType.FLOAT,
            DruidExpression.functionCall("ceil"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.FLOAT, "x")
            )
        ),
        3.0
    );

    testHelper.testExpression(
        SqlStdOperatorTable.CEIL,
        testHelper.makeInputRef("y"),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            DruidExpression.functionCall("ceil"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "y")
            )
        ),
        3.0
    );

    testHelper.testExpression(
        SqlStdOperatorTable.CEIL,
        testHelper.makeInputRef("z"),
        DruidExpression.ofExpression(
            ColumnType.FLOAT,
            DruidExpression.functionCall("ceil"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.FLOAT, "z")
            )
        ),
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
        DruidExpression.ofExpression(
            ColumnType.LONG,
            (args) -> "(cast(cast(" + args.get(0).getExpression() + " * 1,'long'),'double') / 1)",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "a")
            )
        ),
        10.0
    );

    testHelper.testExpression(
        truncateFunction,
        testHelper.makeInputRef("x"),
        DruidExpression.ofExpression(
            ColumnType.FLOAT,
            (args) -> "(cast(cast(" + args.get(0).getExpression() + " * 1,'long'),'double') / 1)",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.FLOAT, "x")
            )
        ),
        2.0
    );

    testHelper.testExpression(
        truncateFunction,
        testHelper.makeInputRef("y"),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            (args) -> "(cast(cast(" + args.get(0).getExpression() + " * 1,'long'),'double') / 1)",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "y")
            )
        ),
        3.0
    );

    testHelper.testExpression(
        truncateFunction,
        testHelper.makeInputRef("z"),
        DruidExpression.ofExpression(
            ColumnType.FLOAT,
            (args) -> "(cast(cast(" + args.get(0).getExpression() + " * 1,'long'),'double') / 1)",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.FLOAT, "z")
            )
        ),
        -2.0
    );

    testHelper.testExpressionString(
        truncateFunction,
        ImmutableList.of(
            testHelper.makeInputRef("x"),
            testHelper.makeLiteral(1)
        ),
        DruidExpression.ofExpression(
            ColumnType.FLOAT,
            (args) -> "(cast(cast(" + args.get(0).getExpression() + " * 10.0,'long'),'double') / 10.0)",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.FLOAT, "x"),
                DruidExpression.ofLiteral(ColumnType.LONG, DruidExpression.longLiteral(1))
            )
        ),
        2.2
    );

    testHelper.testExpressionString(
        truncateFunction,
        ImmutableList.of(
            testHelper.makeInputRef("z"),
            testHelper.makeLiteral(1)
        ),
        DruidExpression.ofExpression(
            ColumnType.FLOAT,
            (args) -> "(cast(cast(" + args.get(0).getExpression() + " * 10.0,'long'),'double') / 10.0)",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.FLOAT, "z"),
                DruidExpression.ofLiteral(ColumnType.LONG, DruidExpression.longLiteral(1))
            )
        ),
        -2.2
    );

    testHelper.testExpressionString(
        truncateFunction,
        ImmutableList.of(
            testHelper.makeInputRef("b"),
            testHelper.makeLiteral(-1)
        ),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            (args) -> "(cast(cast(" + args.get(0).getExpression() + " * 0.1,'long'),'double') / 0.1)",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "b"),
                DruidExpression.ofLiteral(ColumnType.LONG, DruidExpression.longLiteral(-1))
            )
        ),
        20.0
    );

    testHelper.testExpressionString(
        truncateFunction,
        ImmutableList.of(
            testHelper.makeInputRef("z"),
            testHelper.makeLiteral(-1)
        ),
        DruidExpression.ofExpression(
            ColumnType.FLOAT,
            (args) -> "(cast(cast(" + args.get(0).getExpression() + " * 0.1,'long'),'double') / 0.1)",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.FLOAT, "z"),
                DruidExpression.ofLiteral(ColumnType.LONG, DruidExpression.longLiteral(-1))
            )
        ),
        0.0
    );
  }

  @Test
  public void testRound()
  {
    final SqlOperator roundFunction = getOperatorConversion(SqlStdOperatorTable.ROUND).calciteOperator();

    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("a"),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            DruidExpression.functionCall("round"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "a")
            )
        ),
        10L
    );

    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("b"),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            DruidExpression.functionCall("round"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "b")
            )
        ),
        25L
    );

    testHelper.testExpressionString(
        roundFunction,
        ImmutableList.of(
            testHelper.makeInputRef("b"),
            testHelper.makeLiteral(-1)
        ),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            DruidExpression.functionCall("round"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "b"),
                DruidExpression.ofLiteral(ColumnType.LONG, DruidExpression.longLiteral(-1))
            )
        ),
        30L
    );

    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("x"),
        DruidExpression.ofExpression(
            ColumnType.FLOAT,
            DruidExpression.functionCall("round"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.FLOAT, "x")
            )
        ),
        2.0
    );

    testHelper.testExpressionString(
        roundFunction,
        ImmutableList.of(
            testHelper.makeInputRef("x"),
            testHelper.makeLiteral(1)
        ),
        DruidExpression.ofExpression(
            ColumnType.FLOAT,
            DruidExpression.functionCall("round"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.FLOAT, "x"),
                DruidExpression.ofLiteral(ColumnType.LONG, DruidExpression.longLiteral(1))
            )
        ),
        2.3
    );

    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("y"),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            DruidExpression.functionCall("round"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "y")
            )
        ),
        3L
    );

    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("z"),
        DruidExpression.ofExpression(
            ColumnType.FLOAT,
            DruidExpression.functionCall("round"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.FLOAT, "z")
            )
        ),
        -2.0
    );
  }

  @Test
  public void testRoundWithInvalidArgument()
  {

    final SqlOperator roundFunction = getOperatorConversion(SqlStdOperatorTable.ROUND).calciteOperator();

    if (!NullHandling.sqlCompatible()) {
      Throwable t = Assert.assertThrows(
          DruidException.class,
          () -> testHelper.testExpression(
              roundFunction,
              testHelper.makeInputRef("s"),
              DruidExpression.ofExpression(
                  ColumnType.STRING,
                  DruidExpression.functionCall("round"),
                  ImmutableList.of(
                      DruidExpression.ofColumn(ColumnType.STRING, "s")
                  )
              ),
              NullHandling.sqlCompatible() ? null : "IAE Exception"
          )
      );
      Assert.assertEquals(
          "Function[round] first argument should be a LONG or DOUBLE but got STRING instead",
          t.getMessage()
      );
    }
  }

  @Test
  public void testRoundWithInvalidSecondArgument()
  {
    final SqlOperator roundFunction = getOperatorConversion(SqlStdOperatorTable.ROUND).calciteOperator();

    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> testHelper.testExpressionString(
            roundFunction,
            ImmutableList.of(
                testHelper.makeInputRef("x"),
                testHelper.makeLiteral("foo")
            ),
            DruidExpression.ofExpression(
                ColumnType.FLOAT,
                DruidExpression.functionCall("round"),
                ImmutableList.of(
                    DruidExpression.ofColumn(ColumnType.FLOAT, "x"),
                    DruidExpression.ofStringLiteral("foo")
                )
            ),
            "IAE Exception"
        )
    );
    Assert.assertEquals("Function[round] second argument should be a LONG but got STRING instead", t.getMessage());
  }

  @Test
  public void testRoundWithNanShouldRoundTo0()
  {
    final SqlOperator roundFunction = getOperatorConversion(SqlStdOperatorTable.ROUND).calciteOperator();

    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("nan"),
        DruidExpression.ofExpression(
            ColumnType.DOUBLE,
            DruidExpression.functionCall("round"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.DOUBLE, "nan")
            )
        ),
        0D
    );
    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("fnan"),
        DruidExpression.ofExpression(
            ColumnType.FLOAT,
            DruidExpression.functionCall("round"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.FLOAT, "fnan")
            )
        ),
        0D
    );
  }

  @Test
  public void testRoundWithInfinityShouldRoundTo0()
  {
    final SqlOperator roundFunction = getOperatorConversion(SqlStdOperatorTable.ROUND).calciteOperator();

    //CHECKSTYLE.OFF: Regexp
    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("inf"),
        DruidExpression.ofExpression(
            ColumnType.DOUBLE,
            DruidExpression.functionCall("round"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.DOUBLE, "inf")
            )
        ),
        Double.MAX_VALUE
    );
    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("-inf"),
        DruidExpression.ofExpression(
            ColumnType.DOUBLE,
            DruidExpression.functionCall("round"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.DOUBLE, "-inf")
            )
        ),
        -1 * Double.MAX_VALUE
    );
    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("finf"),
        DruidExpression.ofExpression(
            ColumnType.FLOAT,
            DruidExpression.functionCall("round"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.FLOAT, "finf")
            )
        ),
        Double.MAX_VALUE
    );
    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("-finf"),
        DruidExpression.ofExpression(
            ColumnType.FLOAT,
            DruidExpression.functionCall("round"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.FLOAT, "-finf")
            )
        ),
        -1 * Double.MAX_VALUE
    );
    //CHECKSTYLE.ON: Regexp
  }

  @Test
  public void testDateTrunc()
  {
    testHelper.testExpressionString(
        new DateTruncOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("hour"),
            testHelper.makeLiteral(DateTimes.of("2000-02-03T04:05:06Z"))
        ),
        makeExpression(ColumnType.LONG, "timestamp_floor(949550706000,'PT1H',null,'UTC')"),
        DateTimes.of("2000-02-03T04:00:00").getMillis()
    );

    testHelper.testExpressionString(
        new DateTruncOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("DAY"),
            testHelper.makeLiteral(DateTimes.of("2000-02-03T04:05:06Z"))
        ),
        makeExpression(ColumnType.LONG, "timestamp_floor(949550706000,'P1D',null,'UTC')"),
        DateTimes.of("2000-02-03T00:00:00").getMillis()
    );
  }

  @Test
  public void testTrim()
  {
    testHelper.testExpressionString(
        SqlStdOperatorTable.TRIM,
        ImmutableList.of(
            testHelper.makeFlag(SqlTrimFunction.Flag.BOTH),
            testHelper.makeLiteral(" "),
            testHelper.makeInputRef("spacey")
        ),
        makeExpression("trim(\"spacey\",' ')"),
        "hey there"
    );

    testHelper.testExpressionString(
        SqlStdOperatorTable.TRIM,
        ImmutableList.of(
            testHelper.makeFlag(SqlTrimFunction.Flag.LEADING),
            testHelper.makeLiteral(" h"),
            testHelper.makeInputRef("spacey")
        ),
        makeExpression("ltrim(\"spacey\",' h')"),
        "ey there  "
    );

    testHelper.testExpressionString(
        SqlStdOperatorTable.TRIM,
        ImmutableList.of(
            testHelper.makeFlag(SqlTrimFunction.Flag.TRAILING),
            testHelper.makeLiteral(" e"),
            testHelper.makeInputRef("spacey")
        ),
        makeExpression("rtrim(\"spacey\",' e')"),
        "  hey ther"
    );
  }

  @Test
  public void testPad()
  {
    testHelper.testExpressionString(
        new LPadOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(5),
            testHelper.makeLiteral("x")
        ),
        makeExpression("lpad(\"s\",5,'x')"),
        "xxfoo"
    );

    testHelper.testExpressionString(
        new RPadOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(5),
            testHelper.makeLiteral("x")
        ),
        makeExpression("rpad(\"s\",5,'x')"),
        "fooxx"
    );
  }

  @Test
  public void testContains()
  {
    testHelper.testExpressionString(
        ContainsOperatorConversion.caseSensitive().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("spacey"),
            testHelper.makeLiteral("there")
        ),
        makeExpression("contains_string(\"spacey\",'there')"),
        1L
    );

    testHelper.testExpressionString(
        ContainsOperatorConversion.caseSensitive().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("spacey"),
            testHelper.makeLiteral("There")
        ),
        makeExpression(ColumnType.LONG, "contains_string(\"spacey\",'There')"),
        0L
    );

    testHelper.testExpressionString(
        ContainsOperatorConversion.caseInsensitive().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("spacey"),
            testHelper.makeLiteral("There")
        ),
        makeExpression(ColumnType.LONG, "icontains_string(\"spacey\",'There')"),
        1L
    );

    testHelper.testExpressionString(
        ContainsOperatorConversion.caseSensitive().calciteOperator(),
        ImmutableList.of(
            testHelper.makeCall(
                SqlStdOperatorTable.CONCAT,
                testHelper.makeLiteral("what is"),
                testHelper.makeInputRef("spacey")
            ),
            testHelper.makeLiteral("what")
        ),
        makeExpression(ColumnType.LONG, "contains_string(concat('what is',\"spacey\"),'what')"),
        1L
    );

    testHelper.testExpressionString(
        ContainsOperatorConversion.caseSensitive().calciteOperator(),
        ImmutableList.of(
            testHelper.makeCall(
                SqlStdOperatorTable.CONCAT,
                testHelper.makeLiteral("what is"),
                testHelper.makeInputRef("spacey")
            ),
            testHelper.makeLiteral("there")
        ),
        makeExpression(ColumnType.LONG, "contains_string(concat('what is',\"spacey\"),'there')"),
        1L
    );

    testHelper.testExpressionString(
        ContainsOperatorConversion.caseInsensitive().calciteOperator(),
        ImmutableList.of(
            testHelper.makeCall(
                SqlStdOperatorTable.CONCAT,
                testHelper.makeLiteral("what is"),
                testHelper.makeInputRef("spacey")
            ),
            testHelper.makeLiteral("There")
        ),
        makeExpression(ColumnType.LONG, "icontains_string(concat('what is',\"spacey\"),'There')"),
        1L
    );

    testHelper.testExpressionString(
        SqlStdOperatorTable.AND,
        ImmutableList.of(
            testHelper.makeCall(
                ContainsOperatorConversion.caseSensitive().calciteOperator(),
                testHelper.makeInputRef("spacey"),
                testHelper.makeLiteral("there")
            ),
            testHelper.makeCall(
                SqlStdOperatorTable.EQUALS,
                testHelper.makeLiteral("yes"),
                testHelper.makeLiteral("yes")
            )
        ),
        makeExpression(ColumnType.LONG, "(contains_string(\"spacey\",'there') && ('yes' == 'yes'))"),
        1L
    );

    testHelper.testExpressionString(
        SqlStdOperatorTable.AND,
        ImmutableList.of(
            testHelper.makeCall(
                ContainsOperatorConversion.caseInsensitive().calciteOperator(),
                testHelper.makeInputRef("spacey"),
                testHelper.makeLiteral("There")
            ),
            testHelper.makeCall(
                SqlStdOperatorTable.EQUALS,
                testHelper.makeLiteral("yes"),
                testHelper.makeLiteral("yes")
            )
        ),
        makeExpression(ColumnType.LONG, "(icontains_string(\"spacey\",'There') && ('yes' == 'yes'))"),
        1L
    );
  }

  @Test
  public void testContainsAsFilter()
  {
    testHelper.testFilter(
        ContainsOperatorConversion.caseSensitive().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("spacey"),
            testHelper.makeLiteral("there")
        ),
        Collections.emptyList(),
        new SearchQueryDimFilter("spacey", new ContainsSearchQuerySpec("there", true), null),
        true
    );

    testHelper.testFilter(
        ContainsOperatorConversion.caseSensitive().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("spacey"),
            testHelper.makeLiteral("There")
        ),
        Collections.emptyList(),
        new SearchQueryDimFilter("spacey", new ContainsSearchQuerySpec("There", true), null),
        false
    );

    testHelper.testFilter(
        ContainsOperatorConversion.caseInsensitive().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("spacey"),
            testHelper.makeLiteral("There")
        ),
        Collections.emptyList(),
        new SearchQueryDimFilter("spacey", new ContainsSearchQuerySpec("There", false), null),
        true
    );

    testHelper.testFilter(
        ContainsOperatorConversion.caseSensitive().calciteOperator(),
        ImmutableList.of(
            testHelper.makeCall(
                SqlStdOperatorTable.CONCAT,
                testHelper.makeLiteral("what is"),
                testHelper.makeInputRef("spacey")
            ),
            testHelper.makeLiteral("what")
        ),
        ImmutableList.of(
            new ExpressionVirtualColumn(
                "v0",
                "concat('what is',\"spacey\")",
                ColumnType.STRING,
                TestExprMacroTable.INSTANCE
            )
        ),
        new SearchQueryDimFilter("v0", new ContainsSearchQuerySpec("what", true), null),
        true
    );

    testHelper.testFilter(
        ContainsOperatorConversion.caseSensitive().calciteOperator(),
        ImmutableList.of(
            testHelper.makeCall(
                SqlStdOperatorTable.CONCAT,
                testHelper.makeLiteral("what is"),
                testHelper.makeInputRef("spacey")
            ),
            testHelper.makeLiteral("there")
        ),
        ImmutableList.of(
            new ExpressionVirtualColumn(
                "v0",
                "concat('what is',\"spacey\")",
                ColumnType.STRING,
                TestExprMacroTable.INSTANCE
            )
        ),
        new SearchQueryDimFilter("v0", new ContainsSearchQuerySpec("there", true), null),
        true
    );

    testHelper.testFilter(
        ContainsOperatorConversion.caseInsensitive().calciteOperator(),
        ImmutableList.of(
            testHelper.makeCall(
                SqlStdOperatorTable.CONCAT,
                testHelper.makeLiteral("what is"),
                testHelper.makeInputRef("spacey")
            ),
            testHelper.makeLiteral("What")
        ),
        ImmutableList.of(
            new ExpressionVirtualColumn(
                "v0",
                "concat('what is',\"spacey\")",
                ColumnType.STRING,
                TestExprMacroTable.INSTANCE
            )
        ),
        new SearchQueryDimFilter("v0", new ContainsSearchQuerySpec("What", false), null),
        true
    );

    testHelper.testFilter(
        ContainsOperatorConversion.caseSensitive().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("spacey"),
            testHelper.makeLiteral("")
        ),
        Collections.emptyList(),
        new SearchQueryDimFilter("spacey", new ContainsSearchQuerySpec("", true), null),
        true
    );
  }

  @Test
  public void testTimeFloor()
  {
    testHelper.testExpressionString(
        new TimeFloorOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(DateTimes.of("2000-02-03T04:05:06Z")),
            testHelper.makeLiteral("PT1H")
        ),
        makeExpression(ColumnType.LONG, "timestamp_floor(949550706000,'PT1H',null,'UTC')"),
        DateTimes.of("2000-02-03T04:00:00").getMillis()
    );

    testHelper.testExpressionString(
        new TimeFloorOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral("P1D"),
            testHelper.makeNullLiteral(SqlTypeName.TIMESTAMP),
            testHelper.makeLiteral("America/Los_Angeles")
        ),
        makeExpression(ColumnType.LONG, "timestamp_floor(\"t\",'P1D',null,'America/Los_Angeles')"),
        DateTimes.of("2000-02-02T08:00:00").getMillis()
    );
  }

  @Test
  public void testOtherTimeFloor()
  {
    // FLOOR(__time TO unit)

    testHelper.testExpressionString(
        SqlStdOperatorTable.FLOOR,
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeFlag(TimeUnitRange.YEAR)
        ),
        makeExpression(ColumnType.LONG, "timestamp_floor(\"t\",'P1Y',null,'UTC')"),
        DateTimes.of("2000").getMillis()
    );
  }

  @Test
  public void testTimeCeil()
  {
    testHelper.testExpressionString(
        new TimeCeilOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(DateTimes.of("2000-02-03T04:05:06Z")),
            testHelper.makeLiteral("PT1H")
        ),
        makeExpression(ColumnType.LONG, "timestamp_ceil(949550706000,'PT1H',null,'UTC')"),
        DateTimes.of("2000-02-03T05:00:00").getMillis()
    );

    testHelper.testExpressionString(
        new TimeCeilOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral("P1D"),
            testHelper.makeNullLiteral(SqlTypeName.TIMESTAMP),
            testHelper.makeLiteral("America/Los_Angeles")
        ),
        makeExpression(ColumnType.LONG, "timestamp_ceil(\"t\",'P1D',null,'America/Los_Angeles')"),
        DateTimes.of("2000-02-03T08:00:00").getMillis()
    );
  }

  @Test
  public void testOtherTimeCeil()
  {
    // CEIL(__time TO unit)

    testHelper.testExpressionString(
        SqlStdOperatorTable.CEIL,
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeFlag(TimeUnitRange.YEAR)
        ),
        makeExpression(ColumnType.LONG, "timestamp_ceil(\"t\",'P1Y',null,'UTC')"),
        DateTimes.of("2001").getMillis()
    );
  }

  @Test
  public void testTimeShift()
  {
    testHelper.testExpressionString(
        new TimeShiftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral("PT2H"),
            testHelper.makeLiteral(-3)
        ),
        makeExpression(ColumnType.LONG, "timestamp_shift(\"t\",'PT2H',-3,'UTC')"),
        DateTimes.of("2000-02-02T22:05:06").getMillis()
    );

    testHelper.testExpressionString(
        new TimeShiftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral("PT2H"),
            testHelper.makeLiteral(-3),
            testHelper.makeLiteral("America/Los_Angeles")
        ),
        makeExpression(ColumnType.LONG, "timestamp_shift(\"t\",'PT2H',-3,'America/Los_Angeles')"),
        DateTimes.of("2000-02-02T22:05:06").getMillis()
    );
  }

  @Test
  public void testTimeExtract()
  {
    testHelper.testExpressionString(
        new TimeExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral("QUARTER")
        ),
        makeExpression(ColumnType.LONG, "timestamp_extract(\"t\",'QUARTER','UTC')"),
        1L
    );

    testHelper.testExpressionString(
        new TimeExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral("DAY"),
            testHelper.makeLiteral("America/Los_Angeles")
        ),
        makeExpression(ColumnType.LONG, "timestamp_extract(\"t\",'DAY','America/Los_Angeles')"),
        2L
    );

    testHelper.testExpressionString(
        new TimeExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral("DAY"),
            testHelper.makeInputRef("timezone")
        ),
        makeExpression(ColumnType.LONG, "timestamp_extract(\"t\",'DAY',\"timezone\")"),
        2L
    );
  }

  @Test
  public void testTimePlusDayTimeInterval()
  {
    final Period period = new Period("P1DT1H1M");

    testHelper.testExpressionString(
        SqlStdOperatorTable.DATETIME_PLUS,
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral(
                new BigDecimal(period.toStandardDuration().getMillis()), // DAY-TIME literals value is millis
                new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.MINUTE, SqlParserPos.ZERO)
            )
        ),
        makeExpression(ColumnType.LONG, "(\"t\" + 90060000)"),
        DateTimes.of("2000-02-03T04:05:06").plus(period).getMillis()
    );
  }

  @Test
  public void testTimePlusYearMonthInterval()
  {
    final Period period = new Period("P1Y1M");

    testHelper.testExpressionString(
        SqlStdOperatorTable.DATETIME_PLUS,
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral(
                new BigDecimal(13), // YEAR-MONTH literals value is months
                new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO)
            )
        ),
        makeExpression(ColumnType.LONG, "timestamp_shift(\"t\",'P13M',1,'UTC')"),
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
        DruidExpression.ofExpression(
            ColumnType.LONG,
            (args) -> "(" + args.get(0).getExpression() + " - " + args.get(1).getExpression() + ")",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "t"),
                DruidExpression.ofLiteral(ColumnType.STRING, "90060000")
            )
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
        DruidExpression.ofExpression(
            ColumnType.LONG,
            DruidExpression.functionCall("timestamp_shift"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "t"),
                DruidExpression.ofLiteral(ColumnType.STRING, DruidExpression.stringLiteral("P13M")),
                DruidExpression.ofLiteral(ColumnType.LONG, DruidExpression.longLiteral(-1)),
                DruidExpression.ofStringLiteral("UTC")
            )
        ),
        DateTimes.of("2000-02-03T04:05:06").minus(period).getMillis()
    );
  }

  @Test
  public void testTimeParse()
  {
    testHelper.testExpressionString(
        new TimeParseOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("tstr"),
            testHelper.makeLiteral("yyyy-MM-dd HH:mm:ss")
        ),
        makeExpression(ColumnType.LONG, "timestamp_parse(\"tstr\",'yyyy-MM-dd HH:mm:ss','UTC')"),
        DateTimes.of("2000-02-03T04:05:06").getMillis()
    );

    testHelper.testExpressionString(
        new TimeParseOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("tstr"),
            testHelper.makeLiteral("yyyy-MM-dd HH:mm:ss"),
            testHelper.makeLiteral("America/Los_Angeles")
        ),
        makeExpression(ColumnType.LONG, "timestamp_parse(\"tstr\",'yyyy-MM-dd HH:mm:ss','America/Los_Angeles')"),
        DateTimes.of("2000-02-03T04:05:06-08:00").getMillis()
    );
  }

  @Test
  public void testTimeFormat()
  {
    testHelper.testExpressionString(
        new TimeFormatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral("yyyy-MM-dd HH:mm:ss")
        ),
        makeExpression("timestamp_format(\"t\",'yyyy-MM-dd HH:mm:ss','UTC')"),
        "2000-02-03 04:05:06"
    );

    testHelper.testExpressionString(
        new TimeFormatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral("yyyy-MM-dd HH:mm:ss"),
            testHelper.makeLiteral("America/Los_Angeles")
        ),
        makeExpression("timestamp_format(\"t\",'yyyy-MM-dd HH:mm:ss','America/Los_Angeles')"),
        "2000-02-02 20:05:06"
    );
  }

  @Test
  public void testExtract()
  {
    testHelper.testExpressionString(
        SqlStdOperatorTable.EXTRACT,
        ImmutableList.of(
            testHelper.makeFlag(TimeUnitRange.QUARTER),
            testHelper.makeInputRef("t")
        ),
        makeExpression(ColumnType.LONG, "timestamp_extract(\"t\",'QUARTER','UTC')"),
        1L
    );

    testHelper.testExpressionString(
        SqlStdOperatorTable.EXTRACT,
        ImmutableList.of(
            testHelper.makeFlag(TimeUnitRange.DAY),
            testHelper.makeInputRef("t")
        ),
        makeExpression(ColumnType.LONG, "timestamp_extract(\"t\",'DAY','UTC')"),
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
        DruidExpression.ofColumn(
            ColumnType.LONG,
            "t",
            SimpleExtraction.of("t", null)
        ),
        DateTimes.of("2000-02-03T04:05:06Z").getMillis()
    );

    testHelper.testExpression(
        testHelper.makeAbstractCast(
            testHelper.createSqlType(SqlTypeName.TIMESTAMP),
            testHelper.makeInputRef("tstr")
        ),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            DruidExpression.functionCall("timestamp_parse"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "tstr"),
                DruidExpression.ofLiteral(null, DruidExpression.nullLiteral()),
                DruidExpression.ofStringLiteral("UTC")
            )
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
        DruidExpression.ofExpression(
            ColumnType.STRING,
            DruidExpression.functionCall("timestamp_format"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "t"),
                DruidExpression.ofStringLiteral("yyyy-MM-dd HH:mm:ss"),
                DruidExpression.ofStringLiteral("UTC")
            )
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
        DruidExpression.ofColumn(ColumnType.LONG, "t", SimpleExtraction.of("t", null)),
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
        DruidExpression.ofExpression(
            ColumnType.LONG,
            DruidExpression.functionCall("timestamp_floor"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "t"),
                DruidExpression.ofStringLiteral("P1D"),
                DruidExpression.ofLiteral(ColumnType.LONG, DruidExpression.nullLiteral()),
                DruidExpression.ofStringLiteral("UTC")
            )
        ),
        DateTimes.of("2000-02-03").getMillis()
    );

    testHelper.testExpression(
        testHelper.makeAbstractCast(
            testHelper.createSqlType(SqlTypeName.DATE),
            testHelper.makeInputRef("dstr")
        ),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            DruidExpression.functionCall("timestamp_floor"),
            ImmutableList.of(
                DruidExpression.ofExpression(
                    ColumnType.LONG,
                    DruidExpression.functionCall("timestamp_parse"),
                    ImmutableList.of(
                        DruidExpression.ofColumn(ColumnType.STRING, "dstr"),
                        DruidExpression.ofLiteral(null, DruidExpression.nullLiteral()),
                        DruidExpression.ofStringLiteral("UTC")
                    )
                ),
                DruidExpression.ofStringLiteral("P1D"),
                DruidExpression.ofLiteral(ColumnType.LONG, DruidExpression.nullLiteral()),
                DruidExpression.ofStringLiteral("UTC")
            )
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
        DruidExpression.ofExpression(
            ColumnType.STRING,
            DruidExpression.functionCall("timestamp_format"),
            ImmutableList.of(
                DruidExpression.ofExpression(
                    ColumnType.LONG,
                    DruidExpression.functionCall("timestamp_floor"),
                    ImmutableList.of(
                        DruidExpression.ofColumn(ColumnType.LONG, "t"),
                        DruidExpression.ofStringLiteral("P1D"),
                        DruidExpression.ofLiteral(ColumnType.LONG, DruidExpression.nullLiteral()),
                        DruidExpression.ofStringLiteral("UTC")
                    )
                ),
                DruidExpression.ofStringLiteral("yyyy-MM-dd"),
                DruidExpression.ofStringLiteral("UTC")
            )
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
        DruidExpression.ofExpression(
            ColumnType.LONG,
            DruidExpression.functionCall("timestamp_floor"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "t"),
                DruidExpression.ofStringLiteral("P1D"),
                DruidExpression.ofLiteral(ColumnType.LONG, DruidExpression.nullLiteral()),
                DruidExpression.ofStringLiteral("UTC")
            )
        ),
        DateTimes.of("2000-02-03").getMillis()
    );
  }

  @Test
  public void testReverse()
  {
    testHelper.testExpression(
        new ReverseOperatorConversion().calciteOperator(),
        testHelper.makeInputRef("s"),
        DruidExpression.ofExpression(
            ColumnType.STRING,
            DruidExpression.functionCall("reverse"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s")
            )
        ),
        "oof"
    );

    testHelper.testExpression(
        new ReverseOperatorConversion().calciteOperator(),
        testHelper.makeInputRef("spacey"),
        DruidExpression.ofExpression(
            ColumnType.STRING,
            DruidExpression.functionCall("reverse"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "spacey")
            )
        ),
        "  ereht yeh  "
    );

    testHelper.testExpression(
        new ReverseOperatorConversion().calciteOperator(),
        testHelper.makeInputRef("tstr"),
        DruidExpression.ofExpression(
            ColumnType.STRING,
            DruidExpression.functionCall("reverse"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "tstr")
            )
        ),
        "60:50:40 30-20-0002"
    );

    testHelper.testExpression(
        new ReverseOperatorConversion().calciteOperator(),
        testHelper.makeInputRef("dstr"),
        DruidExpression.ofExpression(
            ColumnType.STRING,
            DruidExpression.functionCall("reverse"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "dstr")
            )
        ),
        "30-20-0002"
    );
  }

  @Test
  public void testAbnormalReverseWithWrongType()
  {
    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> testHelper.testExpression(
            new ReverseOperatorConversion().calciteOperator(),
            testHelper.makeInputRef("a"),
            DruidExpression.ofExpression(
                ColumnType.STRING,
                DruidExpression.functionCall("reverse"),
                ImmutableList.of(
                    DruidExpression.ofColumn(ColumnType.LONG, "a")
                )
            ),
            null
        )
    );
    Assert.assertEquals("Function[reverse] needs a STRING argument but got LONG instead", t.getMessage());
  }

  @Test
  public void testRight()
  {
    testHelper.testExpressionString(
        new RightOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1)
        ),
        makeExpression("right(\"s\",1)"),
        "o"
    );

    testHelper.testExpressionString(
        new RightOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(2)
        ),
        makeExpression("right(\"s\",2)"),
        "oo"
    );

    testHelper.testExpressionString(
        new RightOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(3)
        ),
        makeExpression("right(\"s\",3)"),
        "foo"
    );

    testHelper.testExpressionString(
        new RightOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(4)
        ),
        makeExpression("right(\"s\",4)"),
        "foo"
    );

    testHelper.testExpressionString(
        new RightOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("tstr"),
            testHelper.makeLiteral(5)
        ),
        makeExpression("right(\"tstr\",5)"),
        "05:06"
    );
  }

  @Test
  public void testAbnormalRightWithNegativeNumber()
  {
    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> testHelper.testExpressionString(
            new RightOperatorConversion().calciteOperator(),
            ImmutableList.of(
                testHelper.makeInputRef("s"),
                testHelper.makeLiteral(-1)
            ),
            makeExpression("right(\"s\",-1)"),
            null
        )
    );
    Assert.assertEquals("Function[right] needs a positive integer as the second argument", t.getMessage());
  }

  @Test
  public void testAbnormalRightWithWrongType()
  {
    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> testHelper.testExpressionString(
            new RightOperatorConversion().calciteOperator(),
            ImmutableList.of(
                testHelper.makeInputRef("s"),
                testHelper.makeInputRef("s")
            ),
            makeExpression("right(\"s\",\"s\")"),
            null
        )
    );
    Assert.assertEquals(
        "Function[right] needs a STRING as first argument and a LONG as second argument",
        t.getMessage()
    );
  }

  @Test
  public void testLeft()
  {
    testHelper.testExpressionString(
        new LeftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1)
        ),
        makeExpression("left(\"s\",1)"),
        "f"
    );

    testHelper.testExpressionString(
        new LeftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(2)
        ),
        makeExpression("left(\"s\",2)"),
        "fo"
    );

    testHelper.testExpressionString(
        new LeftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(3)
        ),
        makeExpression("left(\"s\",3)"),
        "foo"
    );

    testHelper.testExpressionString(
        new LeftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(4)
        ),
        makeExpression("left(\"s\",4)"),
        "foo"
    );

    testHelper.testExpressionString(
        new LeftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("tstr"),
            testHelper.makeLiteral(10)
        ),
        makeExpression("left(\"tstr\",10)"),
        "2000-02-03"
    );
  }

  @Test
  public void testAbnormalLeftWithNegativeNumber()
  {
    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> testHelper.testExpressionString(
            new LeftOperatorConversion().calciteOperator(),
            ImmutableList.of(
                testHelper.makeInputRef("s"),
                testHelper.makeLiteral(-1)
            ),
            makeExpression("left(\"s\",-1)"),
            null
        )
    );
    Assert.assertEquals("Function[left] needs a postive integer as second argument", t.getMessage());
  }

  @Test
  public void testAbnormalLeftWithWrongType()
  {
    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> testHelper.testExpressionString(
            new LeftOperatorConversion().calciteOperator(),
            ImmutableList.of(
                testHelper.makeInputRef("s"),
                testHelper.makeInputRef("s")
            ),
            makeExpression("left(\"s\",\"s\")"),
            null
        )
    );
    Assert.assertEquals(
        "Function[left] needs a STRING as first argument and a LONG as second argument",
        t.getMessage()
    );
  }

  @Test
  public void testRepeat()
  {
    testHelper.testExpressionString(
        new RepeatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1)
        ),
        makeExpression("repeat(\"s\",1)"),
        "foo"
    );

    testHelper.testExpressionString(
        new RepeatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(3)
        ),
        makeExpression("repeat(\"s\",3)"),
        "foofoofoo"
    );

    testHelper.testExpressionString(
        new RepeatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(-1)
        ),
        makeExpression("repeat(\"s\",-1)"),
        null
    );
  }

  @Test
  public void testAbnormalRepeatWithWrongType()
  {
    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> testHelper.testExpressionString(
            new RepeatOperatorConversion().calciteOperator(),
            ImmutableList.of(
                testHelper.makeInputRef("s"),
                testHelper.makeInputRef("s")
            ),
            makeExpression("repeat(\"s\",\"s\")"),
            null
        )
    );
    Assert.assertEquals(
        "Function[repeat] needs a STRING as first argument and a LONG as second argument",
        t.getMessage()
    );
  }

  @Test
  public void testOperatorConversionsDruidUnaryLongFn()
  {
    testHelper.testExpressionString(
        OperatorConversions.druidUnaryLongFn("BITWISE_COMPLEMENT", "bitwiseComplement").calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("a")
        ),
        makeExpression(ColumnType.LONG, "bitwiseComplement(\"a\")"),
        -11L
    );

    testHelper.testExpressionString(
        OperatorConversions.druidUnaryLongFn("BITWISE_COMPLEMENT", "bitwiseComplement").calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("x")
        ),
        makeExpression(ColumnType.LONG, "bitwiseComplement(\"x\")"),
        -3L
    );

    testHelper.testExpressionString(
        OperatorConversions.druidUnaryLongFn("BITWISE_COMPLEMENT", "bitwiseComplement").calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s")
        ),
        makeExpression(ColumnType.LONG, "bitwiseComplement(\"s\")"),
        null
    );
  }

  @Test
  public void testOperatorConversionsDruidUnaryDoubleFn()
  {
    testHelper.testExpressionString(
        OperatorConversions.druidUnaryDoubleFn("BITWISE_CONVERT_LONG_BITS_TO_DOUBLE", "bitwiseConvertLongBitsToDouble")
                           .calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("a")
        ),
        makeExpression(ColumnType.LONG, "bitwiseConvertLongBitsToDouble(\"a\")"),
        4.9E-323
    );

    testHelper.testExpressionString(
        OperatorConversions.druidUnaryDoubleFn("BITWISE_CONVERT_LONG_BITS_TO_DOUBLE", "bitwiseConvertLongBitsToDouble")
                           .calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("x")
        ),
        makeExpression(ColumnType.LONG, "bitwiseConvertLongBitsToDouble(\"x\")"),
        1.0E-323
    );

    testHelper.testExpressionString(
        OperatorConversions.druidUnaryDoubleFn("BITWISE_CONVERT_LONG_BITS_TO_DOUBLE", "bitwiseConvertLongBitsToDouble")
                           .calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s")
        ),
        makeExpression(ColumnType.LONG, "bitwiseConvertLongBitsToDouble(\"s\")"),
        null
    );
  }

  @Test
  public void testOperatorConversionsDruidBinaryLongFn()
  {
    testHelper.testExpressionString(
        OperatorConversions.druidBinaryLongFn("BITWISE_AND", "bitwiseAnd").calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("a"),
            testHelper.makeInputRef("b")
        ),
        makeExpression(ColumnType.LONG, "bitwiseAnd(\"a\",\"b\")"),
        8L
    );

    testHelper.testExpressionString(
        OperatorConversions.druidBinaryLongFn("BITWISE_AND", "bitwiseAnd").calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("x"),
            testHelper.makeInputRef("y")
        ),
        makeExpression(ColumnType.LONG, "bitwiseAnd(\"x\",\"y\")"),
        2L
    );

    testHelper.testExpressionString(
        OperatorConversions.druidBinaryLongFn("BITWISE_AND", "bitwiseAnd").calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeInputRef("s")
        ),
        makeExpression(ColumnType.LONG, "bitwiseAnd(\"s\",\"s\")"),
        null
    );
  }

  @Test
  public void testHumanReadableBinaryByteFormat()
  {
    /*
     * Basic Test
     */
    testHelper.testExpressionString(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(1000)
        ),
        makeExpression("human_readable_binary_byte_format(1000)"),
        "1000 B"
    );
    testHelper.testExpressionString(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(1024)
        ),
        makeExpression("human_readable_binary_byte_format(1024)"),
        "1.00 KiB"
    );
    testHelper.testExpressionString(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(Long.MAX_VALUE)
        ),
        makeExpression("human_readable_binary_byte_format(9223372036854775807)"),
        "8.00 EiB"
    );
    testHelper.testExpressionString(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(Long.MIN_VALUE)
        ),
        makeExpression("human_readable_binary_byte_format(-9223372036854775808)"),
        "-8.00 EiB"
    );

    /*
     * test input with variable reference
     */
    testHelper.testExpressionString(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("b"),
            testHelper.makeInputRef("p")
        ),
        makeExpression("human_readable_binary_byte_format(\"b\",\"p\")"),
        "25 B"
    );

    /*
     * test different precision
     */
    testHelper.testExpressionString(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45000),
            //precision 0
            testHelper.makeLiteral(0)
        ),
        makeExpression("human_readable_binary_byte_format(45000,0)"),
        "44 KiB"
    );
    testHelper.testExpressionString(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45000),
            //precision 1
            testHelper.makeLiteral(1)
        ),
        makeExpression("human_readable_binary_byte_format(45000,1)"),
        "43.9 KiB"
    );
    testHelper.testExpressionString(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45000),
            //precision 2
            testHelper.makeLiteral(2)
        ),
        makeExpression("human_readable_binary_byte_format(45000,2)"),
        "43.95 KiB"
    );
    testHelper.testExpressionString(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45000),
            //precision 3
            testHelper.makeLiteral(3)
        ),
        makeExpression("human_readable_binary_byte_format(45000,3)"),
        "43.945 KiB"
    );
  }

  @Test
  public void testHumanReadableDecimalByteFormat()
  {
    /*
     * Basic Test
     */
    testHelper.testExpressionString(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(999)
        ),
        makeExpression("human_readable_decimal_byte_format(999)"),
        "999 B"
    );
    testHelper.testExpressionString(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(1024)
        ),
        makeExpression("human_readable_decimal_byte_format(1024)"),
        "1.02 KB"
    );
    testHelper.testExpressionString(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(Long.MAX_VALUE)
        ),
        makeExpression("human_readable_decimal_byte_format(9223372036854775807)"),
        "9.22 EB"
    );
    testHelper.testExpressionString(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(Long.MIN_VALUE)
        ),
        makeExpression("human_readable_decimal_byte_format(-9223372036854775808)"),
        "-9.22 EB"
    );

    /*
     * test input with variable reference
     */
    testHelper.testExpressionString(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("b"),
            testHelper.makeInputRef("p")
        ),
        makeExpression("human_readable_decimal_byte_format(\"b\",\"p\")"),
        "25 B"
    );

    /*
     * test different precision
     */
    testHelper.testExpressionString(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45678),
            //precision 0
            testHelper.makeLiteral(0)
        ),
        makeExpression("human_readable_decimal_byte_format(45678,0)"),
        "46 KB"
    );
    testHelper.testExpressionString(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45678),
            //precision 1
            testHelper.makeLiteral(1)
        ),
        makeExpression("human_readable_decimal_byte_format(45678,1)"),
        "45.7 KB"
    );
    testHelper.testExpressionString(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45678),
            //precision 2
            testHelper.makeLiteral(2)
        ),
        makeExpression("human_readable_decimal_byte_format(45678,2)"),
        "45.68 KB"
    );
    testHelper.testExpressionString(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45678),
            //precision 3
            testHelper.makeLiteral(3)
        ),
        makeExpression("human_readable_decimal_byte_format(45678,3)"),
        "45.678 KB"
    );
  }
}
