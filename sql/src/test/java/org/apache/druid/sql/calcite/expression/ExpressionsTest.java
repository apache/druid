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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.extraction.ExtractionFn;
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
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.CannotBuildQueryException;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.annotation.Nullable;

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
                                                                  .put(
                                                                      "t",
                                                                      DateTimes.of("2000-02-03T04:05:06").getMillis()
                                                                  )
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

  @BeforeEach
  public void setUp()
  {
    testHelper = new ExpressionTestHelper(ROW_SIGNATURE, BINDINGS);
  }

  private SqlOperatorConversion getOperatorConversion(SqlFunction round)
  {
    return operatorTable.lookupOperatorConversion(round);
  }
  /**
   * Expected result of SUBSTRING on a string column. The index and length are the Druid expression text of the
   * arguments. The conversion keeps the node type and the (empty) arguments of the column expression, which cannot be
   * built other than by mapping a column expression. The extraction function, the type and the expression text are
   * spelled out here instead of being taken from the code under test.
   *
   * @param extractionFn expected extraction function of the result, null if it is not a simple extraction
   */
  private static DruidExpression expectedSubstring(
      String column,
      @Nullable ExtractionFn extractionFn,
      String index,
      String length
  )
  {
    final SimpleExtraction expectedExtraction = extractionFn == null ? null : SimpleExtraction.of(column, extractionFn);
    return DruidExpression.ofColumn(ColumnType.STRING, column).map(
        extraction -> expectedExtraction,
        expression -> StringUtils.format("substring(%s, %s, %s)", expression, index, length)
    );
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
        testHelper.expectedFunctionCall(ColumnType.STRING, "concat", testHelper.makeVariable("s"), "bar"),
        "foobar"
    );
  }

  @Test
  public void testCharacterLength()
  {
    testHelper.testExpression(
        SqlStdOperatorTable.CHARACTER_LENGTH,
        testHelper.makeInputRef("s"),
        testHelper.expectedFunctionCall(ColumnType.LONG, "strlen", testHelper.makeVariable("s")),
        3L
    );
  }

  @Test
  public void testSubstring()
  {
    testHelper.testExpression(
        new SubstringOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1),
            testHelper.makeLiteral(2)
        ),
        expectedSubstring("s", null, "0", "2"),
        "fo"
    );

    testHelper.testExpression(
        new SubstringOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(2),
            testHelper.makeLiteral(1)
        ),
        expectedSubstring("s", null, "1", "1"),
        "o"
    );

    testHelper.testExpression(
        new SubstringOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1)
        ),
        expectedSubstring("s", null, "0", "-1"),
        "foo"
    );

    testHelper.testExpression(
        new SubstringOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(2)
        ),
        expectedSubstring("s", null, "1", "-1"),
        "oo"
    );

    testHelper.testExpression(
        new SubstringOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1),
            testHelper.makeInputRef("p") // p is 3
        ),
        expectedSubstring("s", null, "0", "\"p\""),
        "foo"
    );

    testHelper.testExpression(
        new SubstringOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("spacey"),
            testHelper.makeInputRef("p") // p is 3
        ),
        expectedSubstring("spacey", null, "(\"p\" - 1)", "-1"),
        "hey there  "
    );

    testHelper.testExpression(
        new SubstringOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("spacey"),
            testHelper.makeInputRef("p"), // p is 3
            testHelper.makeInputRef("p") // p is 3
        ),
        expectedSubstring("spacey", null, "(\"p\" - 1)", "\"p\""),
        "hey"
    );
  }

  @Test
  public void testSubstringWithExtractionFns()
  {
    testHelper.testExpressionWithExtractionFns(
        testHelper.makeCall(
            new SubstringOperatorConversion().calciteOperator(),
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1),
            testHelper.makeLiteral(2)
        ),
        expectedSubstring("s", new SubstringDimExtractionFn(0, 2), "0", "2"),
        "fo"
    );

    testHelper.testExpressionWithExtractionFns(
        testHelper.makeCall(
            new SubstringOperatorConversion().calciteOperator(),
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(2)
        ),
        expectedSubstring("s", new SubstringDimExtractionFn(1, null), "1", "-1"),
        "oo"
    );

    // Length is not a literal, so there is no extraction function even though they are enabled.
    testHelper.testExpressionWithExtractionFns(
        testHelper.makeCall(
            new SubstringOperatorConversion().calciteOperator(),
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1),
            testHelper.makeInputRef("p") // p is 3
        ),
        expectedSubstring("s", null, "0", "\"p\""),
        "foo"
    );
  }

  @Test
  public void testRegexpExtractWithExtractionFns()
  {
    testHelper.testExpressionWithExtractionFns(
        testHelper.makeCall(
            new RegexpExtractOperatorConversion().calciteOperator(),
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("f(.)"),
            testHelper.makeLiteral(1)
        ),
        DruidExpression.ofExpression(
            ColumnType.STRING,
            SimpleExtraction.of("s", new RegexDimExtractionFn("f(.)", 1, true, null)),
            DruidExpression.functionCall("regexp_extract"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofStringLiteral("f(.)"),
                DruidExpression.ofLiteral(ColumnType.LONG, "1")
            )
        ),
        "o"
    );

    // Without an index the whole match is returned.
    testHelper.testExpressionWithExtractionFns(
        testHelper.makeCall(
            new RegexpExtractOperatorConversion().calciteOperator(),
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("f(.)")
        ),
        DruidExpression.ofExpression(
            ColumnType.STRING,
            SimpleExtraction.of("s", new RegexDimExtractionFn("f(.)", 0, true, null)),
            DruidExpression.functionCall("regexp_extract"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofStringLiteral("f(.)")
            )
        ),
        "fo"
    );

    // The input is not a simple column, so there is no extraction function even though they are enabled.
    testHelper.testExpressionWithExtractionFns(
        testHelper.makeCall(
            new RegexpExtractOperatorConversion().calciteOperator(),
            testHelper.makeCall(
                SqlStdOperatorTable.CONCAT,
                testHelper.makeLiteral("Z"),
                testHelper.makeInputRef("s")
            ),
            testHelper.makeLiteral("Zf(.)")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.STRING,
            "regexp_extract",
            testHelper.expectedFunctionCall(ColumnType.STRING, "concat", "Z", testHelper.makeVariable("s")),
            "Zf(.)"
        ),
        "Zfo"
    );
  }

  @Test
  public void testRegexpExtract()
  {
    testHelper.testExpression(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("x(.)"),
            testHelper.makeLiteral(1)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "regexp_extract", testHelper.makeVariable("s"), "x(.)", 1),
        null
    );

    testHelper.testExpression(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("(o)"),
            testHelper.makeLiteral(1)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "regexp_extract", testHelper.makeVariable("s"), "(o)", 1),

        // Column "s" contains an 'o', but not at the beginning; we do match this.
        "o"
    );

    testHelper.testExpression(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeCall(
                SqlStdOperatorTable.CONCAT,
                testHelper.makeLiteral("Z"),
                testHelper.makeInputRef("s")
            ),
            testHelper.makeLiteral("Zf(.)")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.STRING,
            "regexp_extract",
            testHelper.expectedFunctionCall(ColumnType.STRING, "concat", "Z", testHelper.makeVariable("s")),
            "Zf(.)"
        ),
        "Zfo"
    );

    testHelper.testExpression(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("f(.)"),
            testHelper.makeLiteral(1)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "regexp_extract", testHelper.makeVariable("s"), "f(.)", 1),
        "o"
    );

    testHelper.testExpression(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("f(.)")
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "regexp_extract", testHelper.makeVariable("s"), "f(.)"),
        "fo"
    );

    testHelper.testExpression(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("")
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "regexp_extract", testHelper.makeVariable("s"), ""),
        ""
    );

    testHelper.testExpression(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("")
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "regexp_extract", testHelper.makeVariable("s"), ""),
        ""
    );

    testHelper.testExpression(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeNullLiteral(SqlTypeName.VARCHAR),
            testHelper.makeLiteral("(.)")
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "regexp_extract", null, "(.)"),
        null
    );

    testHelper.testExpression(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeNullLiteral(SqlTypeName.VARCHAR),
            testHelper.makeLiteral("")
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "regexp_extract", null, ""),
        null
    );

    testHelper.testExpression(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeNullLiteral(SqlTypeName.VARCHAR),
            testHelper.makeLiteral("null")
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "regexp_extract", null, "null"),
        null
    );
  }

  @Test
  public void testRegexpReplace()
  {
    testHelper.testExpression(
        new RegexpReplaceOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("x(.)"),
            testHelper.makeLiteral("z")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.STRING,
            "regexp_replace",
            testHelper.makeVariable("s"),
            "x(.)",
            "z"
        ),
        "foo"
    );

    testHelper.testExpression(
        new RegexpReplaceOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("(o)"),
            testHelper.makeLiteral("z")
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "regexp_replace", testHelper.makeVariable("s"), "(o)", "z"),
        "fzz"
    );

    testHelper.testExpression(
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
        testHelper.expectedFunctionCall(
            ColumnType.STRING,
            "regexp_replace",
            testHelper.expectedFunctionCall(ColumnType.STRING, "concat", "Z", testHelper.makeVariable("s")),
            "Zf(.)",
            "z"
        ),
        "zo"
    );

    testHelper.testExpression(
        new RegexpReplaceOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("f(.)"),
            testHelper.makeLiteral("$1")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.STRING,
            "regexp_replace",
            testHelper.makeVariable("s"),
            "f(.)",
            "$1"
        ),
        "oo"
    );
  }

  @Test
  public void testRegexpLike()
  {
    testHelper.testExpression(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("f.")
        ),
        testHelper.expectedFunctionCall(ColumnType.LONG, "regexp_like", testHelper.makeVariable("s"), "f."),
        1L
    );

    testHelper.testExpression(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("o")
        ),
        testHelper.expectedFunctionCall(ColumnType.LONG, "regexp_like", testHelper.makeVariable("s"), "o"),

        // Column "s" contains an 'o', but not at the beginning; we do match this.
        1L
    );

    testHelper.testExpression(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("x.")
        ),
        testHelper.expectedFunctionCall(ColumnType.LONG, "regexp_like", testHelper.makeVariable("s"), "x."),
        0L
    );

    testHelper.testExpression(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("")
        ),
        testHelper.expectedFunctionCall(ColumnType.LONG, "regexp_like", testHelper.makeVariable("s"), ""),
        1L
    );

    testHelper.testExpression(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("beep\nboop"),
            testHelper.makeLiteral("^beep$")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "regexp_like",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.STRING, "'beep\\u000Aboop'"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'^beep$'")
            )
        ),
        0L
    );

    testHelper.testExpression(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("beep\nboop"),
            testHelper.makeLiteral("^beep\\nboop$")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "regexp_like",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.STRING, "'beep\\u000Aboop'"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'^beep\\u005Cnboop$'")
            )
        ),
        1L
    );

    testHelper.testExpression(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("newliney"),
            testHelper.makeLiteral("^beep$")
        ),
        testHelper.expectedFunctionCall(ColumnType.LONG, "regexp_like", testHelper.makeVariable("newliney"), "^beep$"),
        0L
    );

    testHelper.testExpression(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("newliney"),
            testHelper.makeLiteral("^beep\\nboop$")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "regexp_like",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "newliney"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'^beep\\u005Cnboop$'")
            )
        ),
        1L
    );

    testHelper.testExpression(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("newliney"),
            testHelper.makeLiteral("boo")
        ),
        testHelper.expectedFunctionCall(ColumnType.LONG, "regexp_like", testHelper.makeVariable("newliney"), "boo"),
        1L
    );

    testHelper.testExpression(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("newliney"),
            testHelper.makeLiteral("^boo")
        ),
        testHelper.expectedFunctionCall(ColumnType.LONG, "regexp_like", testHelper.makeVariable("newliney"), "^boo"),
        0L
    );

    testHelper.testExpression(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeCall(
                SqlStdOperatorTable.CONCAT,
                testHelper.makeLiteral("Z"),
                testHelper.makeInputRef("s")
            ),
            testHelper.makeLiteral("x(.)")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "regexp_like",
            testHelper.expectedFunctionCall(ColumnType.STRING, "concat", "Z", testHelper.makeVariable("s")),
            "x(.)"
        ),
        0L
    );

    testHelper.testExpression(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeNullLiteral(SqlTypeName.VARCHAR),
            testHelper.makeLiteral("(.)")
        ),
        testHelper.expectedFunctionCall(ColumnType.LONG, "regexp_like", null, "(.)"),
        null
    );

    testHelper.testExpression(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeNullLiteral(SqlTypeName.VARCHAR),
            testHelper.makeLiteral("")
        ),
        testHelper.expectedFunctionCall(ColumnType.LONG, "regexp_like", null, ""),
        null
    );

    testHelper.testExpression(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeNullLiteral(SqlTypeName.VARCHAR),
            testHelper.makeLiteral("null")
        ),
        testHelper.expectedFunctionCall(ColumnType.LONG, "regexp_like", null, "null"),
        null
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
    testHelper.testExpression(
        new StringFormatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("%x"),
            testHelper.makeInputRef("b")
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "format", "%x", testHelper.makeVariable("b")),
        "19"
    );

    testHelper.testExpression(
        new StringFormatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("%s %,d"),
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1234)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "format", "%s %,d", testHelper.makeVariable("s"), 1234),
        "foo 1,234"
    );

    testHelper.testExpression(
        new StringFormatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("%s %,d"),
            testHelper.makeInputRef("s")
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "format", "%s %,d", testHelper.makeVariable("s")),
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
        testHelper.expectedFunctionCall(
            ColumnType.STRING,
            "format",
            "%s %,d",
            testHelper.makeVariable("s"),
            1234,
            6789
        ),
        "foo 1,234"
    );

    testHelper.testExpression(
        new StringFormatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("%.2f %.2f %.2f"),
            testHelper.makeLiteral(1234d),
            testHelper.makeLiteral(123.4),
            testHelper.makeLiteral(123.456789)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "format", "%.2f %.2f %.2f", 1234.0, 123.4, 123.456789),
        "1234.00 123.40 123.46"
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
        DruidExpression.ofExpression(
            ColumnType.LONG,
            (args) -> StringUtils.format(
                "(strpos(%s,%s) + 1)",
                args.get(0).getExpression(),
                args.get(1).getExpression()
            ),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'oo'")
            )
        ),
        2L
    );

    testHelper.testExpression(
        new StrposOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("ax")
        ),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            (args) -> StringUtils.format(
                "(strpos(%s,%s) + 1)",
                args.get(0).getExpression(),
                args.get(1).getExpression()
            ),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'ax'")
            )
        ),
        0L
    );

    testHelper.testExpression(
        new StrposOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeNullLiteral(SqlTypeName.VARCHAR),
            testHelper.makeLiteral("ax")
        ),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            (args) -> StringUtils.format(
                "(strpos(%s,%s) + 1)",
                args.get(0).getExpression(),
                args.get(1).getExpression()
            ),
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.STRING, "null"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'ax'")
            )
        ),
        null
    );
  }

  @Test
  public void testParseLong()
  {
    testHelper.testExpression(
        new ParseLongOperatorConversion().calciteOperator(),
        testHelper.makeInputRef("intstr"),
        testHelper.expectedFunctionCall(ColumnType.LONG, "parse_long", testHelper.makeVariable("intstr")),
        -100L
    );

    testHelper.testExpression(
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
        DruidExpression.ofExpression(
            ColumnType.LONG,
            DruidExpression.functionCall("parse_long"),
            ImmutableList.of(
                testHelper.expectedFunctionCall(ColumnType.STRING, "concat", "0x", testHelper.makeVariable("hexstr")),
                DruidExpression.ofLiteral(ColumnType.LONG, DruidExpression.longLiteral(16))
            )
        ),
        239L
    );

    testHelper.testExpression(
        new ParseLongOperatorConversion().calciteOperator(),
        testHelper.makeInputRef("hexstr"),
        testHelper.expectedFunctionCall(ColumnType.LONG, "parse_long", testHelper.makeVariable("hexstr")),
        null
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
        DruidExpression.ofExpression(
            ColumnType.LONG,
            (args) -> StringUtils.format(
                "(strpos(%s,%s,0) + 1)",
                args.get(1).getExpression(),
                args.get(0).getExpression()
            ),
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.STRING, "'oo'"),
                DruidExpression.ofColumn(ColumnType.STRING, "s")
            )
        ),
        2L
    );

    testHelper.testExpression(
        SqlStdOperatorTable.POSITION,
        ImmutableList.of(
            testHelper.makeLiteral("oo"),
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(BigDecimal.valueOf(2))
        ),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            (args) -> StringUtils.format(
                "(strpos(%s,%s,(%s - 1)) + 1)",
                args.get(1).getExpression(),
                args.get(0).getExpression(),
                args.get(2).getExpression()
            ),
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.STRING, "'oo'"),
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.LONG, "2")
            )
        ),
        2L
    );

    testHelper.testExpression(
        SqlStdOperatorTable.POSITION,
        ImmutableList.of(
            testHelper.makeLiteral("oo"),
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(BigDecimal.valueOf(3))
        ),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            (args) -> StringUtils.format(
                "(strpos(%s,%s,(%s - 1)) + 1)",
                args.get(1).getExpression(),
                args.get(0).getExpression(),
                args.get(2).getExpression()
            ),
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.STRING, "'oo'"),
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.LONG, "3")
            )
        ),
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
        testHelper.expectedFunctionCall(ColumnType.DOUBLE, "pow", testHelper.makeVariable("a"), 2),
        100.0
    );
  }

  @Test
  public void testFloor()
  {
    testHelper.testExpression(
        SqlStdOperatorTable.FLOOR,
        testHelper.makeInputRef("a"),
        testHelper.expectedFunctionCall(ColumnType.LONG, "floor", testHelper.makeVariable("a")),
        10.0
    );

    testHelper.testExpression(
        SqlStdOperatorTable.FLOOR,
        testHelper.makeInputRef("x"),
        testHelper.expectedFunctionCall(ColumnType.FLOAT, "floor", testHelper.makeVariable("x")),
        2.0
    );

    testHelper.testExpression(
        SqlStdOperatorTable.FLOOR,
        testHelper.makeInputRef("y"),
        testHelper.expectedFunctionCall(ColumnType.LONG, "floor", testHelper.makeVariable("y")),
        3.0
    );

    testHelper.testExpression(
        SqlStdOperatorTable.FLOOR,
        testHelper.makeInputRef("z"),
        testHelper.expectedFunctionCall(ColumnType.FLOAT, "floor", testHelper.makeVariable("z")),
        -3.0
    );
  }

  @Test
  public void testCeil()
  {
    testHelper.testExpression(
        SqlStdOperatorTable.CEIL,
        testHelper.makeInputRef("a"),
        testHelper.expectedFunctionCall(ColumnType.LONG, "ceil", testHelper.makeVariable("a")),
        10.0
    );

    testHelper.testExpression(
        SqlStdOperatorTable.CEIL,
        testHelper.makeInputRef("x"),
        testHelper.expectedFunctionCall(ColumnType.FLOAT, "ceil", testHelper.makeVariable("x")),
        3.0
    );

    testHelper.testExpression(
        SqlStdOperatorTable.CEIL,
        testHelper.makeInputRef("y"),
        testHelper.expectedFunctionCall(ColumnType.LONG, "ceil", testHelper.makeVariable("y")),
        3.0
    );

    testHelper.testExpression(
        SqlStdOperatorTable.CEIL,
        testHelper.makeInputRef("z"),
        testHelper.expectedFunctionCall(ColumnType.FLOAT, "ceil", testHelper.makeVariable("z")),
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

    testHelper.testExpression(
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

    testHelper.testExpression(
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

    testHelper.testExpression(
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

    testHelper.testExpression(
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
        testHelper.expectedFunctionCall(ColumnType.LONG, "round", testHelper.makeVariable("a")),
        10L
    );

    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("b"),
        testHelper.expectedFunctionCall(ColumnType.LONG, "round", testHelper.makeVariable("b")),
        25L
    );

    testHelper.testExpression(
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
        testHelper.expectedFunctionCall(ColumnType.FLOAT, "round", testHelper.makeVariable("x")),
        2.0
    );

    testHelper.testExpression(
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
        testHelper.expectedFunctionCall(ColumnType.LONG, "round", testHelper.makeVariable("y")),
        3L
    );

    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("z"),
        testHelper.expectedFunctionCall(ColumnType.FLOAT, "round", testHelper.makeVariable("z")),
        -2.0
    );
  }

  @Test
  public void testRoundWithInvalidSecondArgument()
  {
    final SqlOperator roundFunction = getOperatorConversion(SqlStdOperatorTable.ROUND).calciteOperator();

    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> testHelper.testExpression(
            roundFunction,
            ImmutableList.of(
                testHelper.makeInputRef("x"),
                testHelper.makeLiteral("foo")
            ),
            testHelper.expectedFunctionCall(ColumnType.FLOAT, "round", testHelper.makeVariable("x"), "foo"),
            "IAE Exception"
        )
    );
    Assertions.assertEquals("Function[round] second argument should be a LONG but got STRING instead", t.getMessage());
  }

  @Test
  public void testRoundWithNanShouldRoundTo0()
  {
    final SqlOperator roundFunction = getOperatorConversion(SqlStdOperatorTable.ROUND).calciteOperator();

    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("nan"),
        testHelper.expectedFunctionCall(ColumnType.DOUBLE, "round", testHelper.makeVariable("nan")),
        0D
    );
    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("fnan"),
        testHelper.expectedFunctionCall(ColumnType.FLOAT, "round", testHelper.makeVariable("fnan")),
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
        testHelper.expectedFunctionCall(ColumnType.DOUBLE, "round", testHelper.makeVariable("inf")),
        Double.MAX_VALUE
    );
    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("-inf"),
        testHelper.expectedFunctionCall(ColumnType.DOUBLE, "round", testHelper.makeVariable("-inf")),
        -1 * Double.MAX_VALUE
    );
    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("finf"),
        testHelper.expectedFunctionCall(ColumnType.FLOAT, "round", testHelper.makeVariable("finf")),
        Double.MAX_VALUE
    );
    testHelper.testExpression(
        roundFunction,
        testHelper.makeInputRef("-finf"),
        testHelper.expectedFunctionCall(ColumnType.FLOAT, "round", testHelper.makeVariable("-finf")),
        -1 * Double.MAX_VALUE
    );
    //CHECKSTYLE.ON: Regexp
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
        testHelper.expectedFunctionCall(ColumnType.LONG, "timestamp_floor", 949550706000L, "PT1H", null, "UTC"),
        DateTimes.of("2000-02-03T04:00:00").getMillis()
    );

    testHelper.testExpression(
        new DateTruncOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("DAY"),
            testHelper.makeLiteral(DateTimes.of("2000-02-03T04:05:06Z"))
        ),
        testHelper.expectedFunctionCall(ColumnType.LONG, "timestamp_floor", 949550706000L, "P1D", null, "UTC"),
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
        testHelper.expectedFunctionCall(ColumnType.STRING, "trim", testHelper.makeVariable("spacey"), " "),
        "hey there"
    );

    testHelper.testExpression(
        SqlStdOperatorTable.TRIM,
        ImmutableList.of(
            testHelper.makeFlag(SqlTrimFunction.Flag.LEADING),
            testHelper.makeLiteral(" h"),
            testHelper.makeInputRef("spacey")
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "ltrim", testHelper.makeVariable("spacey"), " h"),
        "ey there  "
    );

    testHelper.testExpression(
        SqlStdOperatorTable.TRIM,
        ImmutableList.of(
            testHelper.makeFlag(SqlTrimFunction.Flag.TRAILING),
            testHelper.makeLiteral(" e"),
            testHelper.makeInputRef("spacey")
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "rtrim", testHelper.makeVariable("spacey"), " e"),
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
        testHelper.expectedFunctionCall(ColumnType.STRING, "lpad", testHelper.makeVariable("s"), 5, "x"),
        "xxfoo"
    );

    testHelper.testExpression(
        new RPadOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(5),
            testHelper.makeLiteral("x")
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "rpad", testHelper.makeVariable("s"), 5, "x"),
        "fooxx"
    );
  }

  @Test
  public void testContains()
  {
    testHelper.testExpression(
        ContainsOperatorConversion.caseSensitive().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("spacey"),
            testHelper.makeLiteral("there")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "contains_string",
            testHelper.makeVariable("spacey"),
            "there"
        ),
        1L
    );

    testHelper.testExpression(
        ContainsOperatorConversion.caseSensitive().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("spacey"),
            testHelper.makeLiteral("There")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "contains_string",
            testHelper.makeVariable("spacey"),
            "There"
        ),
        0L
    );

    testHelper.testExpression(
        ContainsOperatorConversion.caseInsensitive().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("spacey"),
            testHelper.makeLiteral("There")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "icontains_string",
            testHelper.makeVariable("spacey"),
            "There"
        ),
        1L
    );

    testHelper.testExpression(
        ContainsOperatorConversion.caseSensitive().calciteOperator(),
        ImmutableList.of(
            testHelper.makeCall(
                SqlStdOperatorTable.CONCAT,
                testHelper.makeLiteral("what is"),
                testHelper.makeInputRef("spacey")
            ),
            testHelper.makeLiteral("what")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "contains_string",
            testHelper.expectedFunctionCall(ColumnType.STRING, "concat", "what is", testHelper.makeVariable("spacey")),
            "what"
        ),
        1L
    );

    testHelper.testExpression(
        ContainsOperatorConversion.caseSensitive().calciteOperator(),
        ImmutableList.of(
            testHelper.makeCall(
                SqlStdOperatorTable.CONCAT,
                testHelper.makeLiteral("what is"),
                testHelper.makeInputRef("spacey")
            ),
            testHelper.makeLiteral("there")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "contains_string",
            testHelper.expectedFunctionCall(ColumnType.STRING, "concat", "what is", testHelper.makeVariable("spacey")),
            "there"
        ),
        1L
    );

    testHelper.testExpression(
        ContainsOperatorConversion.caseInsensitive().calciteOperator(),
        ImmutableList.of(
            testHelper.makeCall(
                SqlStdOperatorTable.CONCAT,
                testHelper.makeLiteral("what is"),
                testHelper.makeInputRef("spacey")
            ),
            testHelper.makeLiteral("There")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "icontains_string",
            testHelper.expectedFunctionCall(ColumnType.STRING, "concat", "what is", testHelper.makeVariable("spacey")),
            "There"
        ),
        1L
    );

    testHelper.testExpression(
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
        DruidExpression.ofExpression(
            ColumnType.LONG,
            (args) -> StringUtils.format(
                "(%s && %s)",
                args.get(0).getExpression(),
                args.get(1).getExpression()
            ),
            ImmutableList.of(
                testHelper.expectedFunctionCall(
                    ColumnType.LONG,
                    "contains_string",
                    testHelper.makeVariable("spacey"),
                    "there"
                ),
                DruidExpression.ofExpression(
                    ColumnType.LONG,
                    (args) -> StringUtils.format(
                        "(%s == %s)",
                        args.get(0).getExpression(),
                        args.get(1).getExpression()
                    ),
                    ImmutableList.of(
                        DruidExpression.ofLiteral(ColumnType.STRING, "'yes'"),
                        DruidExpression.ofLiteral(ColumnType.STRING, "'yes'")
                    )
                )
            )
        ),
        1L
    );

    testHelper.testExpression(
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
        DruidExpression.ofExpression(
            ColumnType.LONG,
            (args) -> StringUtils.format(
                "(%s && %s)",
                args.get(0).getExpression(),
                args.get(1).getExpression()
            ),
            ImmutableList.of(
                testHelper.expectedFunctionCall(
                    ColumnType.LONG,
                    "icontains_string",
                    testHelper.makeVariable("spacey"),
                    "There"
                ),
                DruidExpression.ofExpression(
                    ColumnType.LONG,
                    (args) -> StringUtils.format(
                        "(%s == %s)",
                        args.get(0).getExpression(),
                        args.get(1).getExpression()
                    ),
                    ImmutableList.of(
                        DruidExpression.ofLiteral(ColumnType.STRING, "'yes'"),
                        DruidExpression.ofLiteral(ColumnType.STRING, "'yes'")
                    )
                )
            )
        ),
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
    testHelper.testExpression(
        new TimeFloorOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(DateTimes.of("2000-02-03T04:05:06Z")),
            testHelper.makeLiteral("PT1H")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "timestamp_floor",
            949550706000L,
            "PT1H",
            testHelper.makeTypedNullLiteral(null),
            "UTC"
        ),
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
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "timestamp_floor",
            testHelper.makeVariable("t"),
            "P1D",
            testHelper.makeTypedNullLiteral(null),
            "America/Los_Angeles"
        ),
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
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "timestamp_floor",
            testHelper.makeVariable("t"),
            "P1Y",
            testHelper.makeTypedNullLiteral(null),
            "UTC"
        ),
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
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "timestamp_ceil",
            949550706000L,
            "PT1H",
            testHelper.makeTypedNullLiteral(null),
            "UTC"
        ),
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
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "timestamp_ceil",
            testHelper.makeVariable("t"),
            "P1D",
            testHelper.makeTypedNullLiteral(null),
            "America/Los_Angeles"
        ),
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
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "timestamp_ceil",
            testHelper.makeVariable("t"),
            "P1Y",
            testHelper.makeTypedNullLiteral(null),
            "UTC"
        ),
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
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "timestamp_shift",
            testHelper.makeVariable("t"),
            "PT2H",
            -3,
            "UTC"
        ),
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
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "timestamp_shift",
            testHelper.makeVariable("t"),
            "PT2H",
            -3,
            "America/Los_Angeles"
        ),
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
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "timestamp_extract",
            testHelper.makeVariable("t"),
            "QUARTER",
            "UTC"
        ),
        1L
    );

    testHelper.testExpression(
        new TimeExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral("DAY"),
            testHelper.makeLiteral("America/Los_Angeles")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "timestamp_extract",
            testHelper.makeVariable("t"),
            "DAY",
            "America/Los_Angeles"
        ),
        2L
    );

    testHelper.testExpression(
        new TimeExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral("DAY"),
            testHelper.makeInputRef("timezone")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "timestamp_extract",
            testHelper.makeVariable("t"),
            "DAY",
            testHelper.makeVariable("timezone")
        ),
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
        DruidExpression.ofExpression(
            ColumnType.LONG,
            (args) -> StringUtils.format(
                "(%s + %s)",
                args.get(0).getExpression(),
                args.get(1).getExpression()
            ),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "t"),
                DruidExpression.ofLiteral(ColumnType.LONG, "90060000")
            )
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
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "timestamp_shift",
            testHelper.makeVariable("t"),
            "P13M",
            1,
            "UTC"
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
        DruidExpression.ofExpression(
            ColumnType.LONG,
            (args) -> "(" + args.get(0).getExpression() + " - " + args.get(1).getExpression() + ")",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "t"),
                DruidExpression.ofLiteral(ColumnType.LONG, "90060000")
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
    testHelper.testExpression(
        new TimeParseOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("tstr"),
            testHelper.makeLiteral("yyyy-MM-dd HH:mm:ss")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "timestamp_parse",
            testHelper.makeVariable("tstr"),
            "yyyy-MM-dd HH:mm:ss",
            "UTC"
        ),
        DateTimes.of("2000-02-03T04:05:06").getMillis()
    );

    testHelper.testExpression(
        new TimeParseOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("tstr"),
            testHelper.makeLiteral("yyyy-MM-dd HH:mm:ss"),
            testHelper.makeLiteral("America/Los_Angeles")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "timestamp_parse",
            testHelper.makeVariable("tstr"),
            "yyyy-MM-dd HH:mm:ss",
            "America/Los_Angeles"
        ),
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
        testHelper.expectedFunctionCall(
            ColumnType.STRING,
            "timestamp_format",
            testHelper.makeVariable("t"),
            "yyyy-MM-dd HH:mm:ss",
            "UTC"
        ),
        "2000-02-03 04:05:06"
    );

    testHelper.testExpression(
        new TimeFormatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("t"),
            testHelper.makeLiteral("yyyy-MM-dd HH:mm:ss"),
            testHelper.makeLiteral("America/Los_Angeles")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.STRING,
            "timestamp_format",
            testHelper.makeVariable("t"),
            "yyyy-MM-dd HH:mm:ss",
            "America/Los_Angeles"
        ),
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
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "timestamp_extract",
            testHelper.makeVariable("t"),
            "QUARTER",
            "UTC"
        ),
        1L
    );

    testHelper.testExpression(
        SqlStdOperatorTable.EXTRACT,
        ImmutableList.of(
            testHelper.makeFlag(TimeUnitRange.DAY),
            testHelper.makeInputRef("t")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "timestamp_extract",
            testHelper.makeVariable("t"),
            "DAY",
            "UTC"
        ),
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
        testHelper.expectedFunctionCall(
            ColumnType.STRING,
            "timestamp_format",
            testHelper.makeVariable("t"),
            "yyyy-MM-dd HH:mm:ss",
            "UTC"
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
        testHelper.expectedFunctionCall(ColumnType.STRING, "reverse", testHelper.makeVariable("s")),
        "oof"
    );

    testHelper.testExpression(
        new ReverseOperatorConversion().calciteOperator(),
        testHelper.makeInputRef("spacey"),
        testHelper.expectedFunctionCall(ColumnType.STRING, "reverse", testHelper.makeVariable("spacey")),
        "  ereht yeh  "
    );

    testHelper.testExpression(
        new ReverseOperatorConversion().calciteOperator(),
        testHelper.makeInputRef("tstr"),
        testHelper.expectedFunctionCall(ColumnType.STRING, "reverse", testHelper.makeVariable("tstr")),
        "60:50:40 30-20-0002"
    );

    testHelper.testExpression(
        new ReverseOperatorConversion().calciteOperator(),
        testHelper.makeInputRef("dstr"),
        testHelper.expectedFunctionCall(ColumnType.STRING, "reverse", testHelper.makeVariable("dstr")),
        "30-20-0002"
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
        testHelper.expectedFunctionCall(ColumnType.STRING, "right", testHelper.makeVariable("s"), 1),
        "o"
    );

    testHelper.testExpression(
        new RightOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(2)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "right", testHelper.makeVariable("s"), 2),
        "oo"
    );

    testHelper.testExpression(
        new RightOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(3)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "right", testHelper.makeVariable("s"), 3),
        "foo"
    );

    testHelper.testExpression(
        new RightOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(4)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "right", testHelper.makeVariable("s"), 4),
        "foo"
    );

    testHelper.testExpression(
        new RightOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("tstr"),
            testHelper.makeLiteral(5)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "right", testHelper.makeVariable("tstr"), 5),
        "05:06"
    );
  }

  @Test
  public void testAbnormalRightWithNegativeNumber()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> testHelper.testExpression(
            new RightOperatorConversion().calciteOperator(),
            ImmutableList.of(
                testHelper.makeInputRef("s"),
                testHelper.makeLiteral(-1)
            ),
            testHelper.expectedFunctionCall(ColumnType.STRING, "right", testHelper.makeVariable("s"), -1),
            null
        )
    );
    Assertions.assertEquals("Function[right] needs a positive integer as the second argument", t.getMessage());
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
        testHelper.expectedFunctionCall(ColumnType.STRING, "left", testHelper.makeVariable("s"), 1),
        "f"
    );

    testHelper.testExpression(
        new LeftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(2)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "left", testHelper.makeVariable("s"), 2),
        "fo"
    );

    testHelper.testExpression(
        new LeftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(3)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "left", testHelper.makeVariable("s"), 3),
        "foo"
    );

    testHelper.testExpression(
        new LeftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(4)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "left", testHelper.makeVariable("s"), 4),
        "foo"
    );

    testHelper.testExpression(
        new LeftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("tstr"),
            testHelper.makeLiteral(10)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "left", testHelper.makeVariable("tstr"), 10),
        "2000-02-03"
    );
  }

  @Test
  public void testAbnormalLeftWithNegativeNumber()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> testHelper.testExpression(
            new LeftOperatorConversion().calciteOperator(),
            ImmutableList.of(
                testHelper.makeInputRef("s"),
                testHelper.makeLiteral(-1)
            ),
            testHelper.expectedFunctionCall(ColumnType.STRING, "left", testHelper.makeVariable("s"), -1),
            null
        )
    );
    Assertions.assertEquals("Function[left] needs a positive integer as the second argument", t.getMessage());
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
        testHelper.expectedFunctionCall(ColumnType.STRING, "repeat", testHelper.makeVariable("s"), 1),
        "foo"
    );

    testHelper.testExpression(
        new RepeatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(3)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "repeat", testHelper.makeVariable("s"), 3),
        "foofoofoo"
    );

    testHelper.testExpression(
        new RepeatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(-1)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "repeat", testHelper.makeVariable("s"), -1),
        null
    );
  }

  @Test
  public void testOperatorConversionsDruidUnaryLongFn()
  {
    testHelper.testExpression(
        OperatorConversions.druidUnaryLongFn("BITWISE_COMPLEMENT", "bitwiseComplement").calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("a")
        ),
        testHelper.expectedFunctionCall(ColumnType.LONG, "bitwiseComplement", testHelper.makeVariable("a")),
        -11L
    );

    testHelper.testExpression(
        OperatorConversions.druidUnaryLongFn("BITWISE_COMPLEMENT", "bitwiseComplement").calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("x")
        ),
        testHelper.expectedFunctionCall(ColumnType.LONG, "bitwiseComplement", testHelper.makeVariable("x")),
        -3L
    );

    testHelper.testExpression(
        OperatorConversions.druidUnaryLongFn("BITWISE_COMPLEMENT", "bitwiseComplement").calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s")
        ),
        testHelper.expectedFunctionCall(ColumnType.LONG, "bitwiseComplement", testHelper.makeVariable("s")),
        null
    );
  }

  @Test
  public void testOperatorConversionsDruidUnaryDoubleFn()
  {
    testHelper.testExpression(
        OperatorConversions.druidUnaryDoubleFn("BITWISE_CONVERT_LONG_BITS_TO_DOUBLE", "bitwiseConvertLongBitsToDouble")
                           .calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("a")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.DOUBLE,
            "bitwiseConvertLongBitsToDouble",
            testHelper.makeVariable("a")
        ),
        4.9E-323
    );

    testHelper.testExpression(
        OperatorConversions.druidUnaryDoubleFn("BITWISE_CONVERT_LONG_BITS_TO_DOUBLE", "bitwiseConvertLongBitsToDouble")
                           .calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("x")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.DOUBLE,
            "bitwiseConvertLongBitsToDouble",
            testHelper.makeVariable("x")
        ),
        1.0E-323
    );

    testHelper.testExpression(
        OperatorConversions.druidUnaryDoubleFn("BITWISE_CONVERT_LONG_BITS_TO_DOUBLE", "bitwiseConvertLongBitsToDouble")
                           .calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.DOUBLE,
            "bitwiseConvertLongBitsToDouble",
            testHelper.makeVariable("s")
        ),
        null
    );
  }

  @Test
  public void testOperatorConversionsDruidBinaryLongFn()
  {
    testHelper.testExpression(
        OperatorConversions.druidBinaryLongFn("BITWISE_AND", "bitwiseAnd").calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("a"),
            testHelper.makeInputRef("b")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "bitwiseAnd",
            testHelper.makeVariable("a"),
            testHelper.makeVariable("b")
        ),
        8L
    );

    testHelper.testExpression(
        OperatorConversions.druidBinaryLongFn("BITWISE_AND", "bitwiseAnd").calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("x"),
            testHelper.makeInputRef("y")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "bitwiseAnd",
            testHelper.makeVariable("x"),
            testHelper.makeVariable("y")
        ),
        2L
    );

    testHelper.testExpression(
        OperatorConversions.druidBinaryLongFn("BITWISE_AND", "bitwiseAnd").calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeInputRef("s")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.LONG,
            "bitwiseAnd",
            testHelper.makeVariable("s"),
            testHelper.makeVariable("s")
        ),
        null
    );
  }

  @Test
  public void testHumanReadableBinaryByteFormat()
  {
    /*
     * Basic Test
     */
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(1000)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "human_readable_binary_byte_format", 1000),
        "1000 B"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(1024)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "human_readable_binary_byte_format", 1024),
        "1.00 KiB"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(Long.MAX_VALUE)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "human_readable_binary_byte_format", 9223372036854775807L),
        "8.00 EiB"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(Long.MIN_VALUE)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "human_readable_binary_byte_format", -9223372036854775808L),
        "-8.00 EiB"
    );

    /*
     * test input with variable reference
     */
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("b"),
            testHelper.makeInputRef("p")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.STRING,
            "human_readable_binary_byte_format",
            testHelper.makeVariable("b"),
            testHelper.makeVariable("p")
        ),
        "25 B"
    );

    /*
     * test different precision
     */
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45000),
            //precision 0
            testHelper.makeLiteral(0)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "human_readable_binary_byte_format", 45000, 0),
        "44 KiB"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45000),
            //precision 1
            testHelper.makeLiteral(1)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "human_readable_binary_byte_format", 45000, 1),
        "43.9 KiB"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45000),
            //precision 2
            testHelper.makeLiteral(2)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "human_readable_binary_byte_format", 45000, 2),
        "43.95 KiB"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45000),
            //precision 3
            testHelper.makeLiteral(3)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "human_readable_binary_byte_format", 45000, 3),
        "43.945 KiB"
    );
  }

  @Test
  public void testHumanReadableDecimalByteFormat()
  {
    /*
     * Basic Test
     */
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(999)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "human_readable_decimal_byte_format", 999),
        "999 B"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(1024)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "human_readable_decimal_byte_format", 1024),
        "1.02 KB"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(Long.MAX_VALUE)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "human_readable_decimal_byte_format", 9223372036854775807L),
        "9.22 EB"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(Long.MIN_VALUE)
        ),
        testHelper.expectedFunctionCall(
            ColumnType.STRING,
            "human_readable_decimal_byte_format",
            -9223372036854775808L
        ),
        "-9.22 EB"
    );

    /*
     * test input with variable reference
     */
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("b"),
            testHelper.makeInputRef("p")
        ),
        testHelper.expectedFunctionCall(
            ColumnType.STRING,
            "human_readable_decimal_byte_format",
            testHelper.makeVariable("b"),
            testHelper.makeVariable("p")
        ),
        "25 B"
    );

    /*
     * test different precision
     */
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45678),
            //precision 0
            testHelper.makeLiteral(0)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "human_readable_decimal_byte_format", 45678, 0),
        "46 KB"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45678),
            //precision 1
            testHelper.makeLiteral(1)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "human_readable_decimal_byte_format", 45678, 1),
        "45.7 KB"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45678),
            //precision 2
            testHelper.makeLiteral(2)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "human_readable_decimal_byte_format", 45678, 2),
        "45.68 KB"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45678),
            //precision 3
            testHelper.makeLiteral(3)
        ),
        testHelper.expectedFunctionCall(ColumnType.STRING, "human_readable_decimal_byte_format", 45678, 3),
        "45.678 KB"
    );
  }

  @Test
  public void testPresenceOfOverIsInvalid()
  {
    final RexBuilder rexBuilder = new RexBuilder(DruidTypeSystem.TYPE_FACTORY);
    final PlannerContext plannerContext = Mockito.mock(PlannerContext.class);
    Mockito.when(plannerContext.getTimeZone()).thenReturn(DateTimeZone.UTC);

    RexNode rexNode = rexBuilder.makeOver(
        testHelper.createSqlType(SqlTypeName.BIGINT),
        SqlStdOperatorTable.SUM,
        Collections.emptyList(),
        Collections.emptyList(),
        ImmutableList.of(),
        RexWindowBounds.CURRENT_ROW,
        RexWindowBounds.CURRENT_ROW,
        false,
        true,
        false,
        false,
        false
    );

    CannotBuildQueryException t = Assertions.assertThrows(
        CannotBuildQueryException.class,
        () -> testHelper.testExpression(rexNode, null, plannerContext)
    );

    Assertions.assertTrue(t.getMessage().contains("Unexpected OVER expression"));
  }

  @Test
  public void testCalciteLiteralToDruidLiteral()
  {
    final RexBuilder rexBuilder = new RexBuilder(DruidTypeSystem.TYPE_FACTORY);
    final PlannerContext plannerContext = Mockito.mock(PlannerContext.class);
    Mockito.when(plannerContext.getTimeZone()).thenReturn(DateTimeZone.UTC);

    assertDruidLiteral(
        new DruidLiteral(ExpressionType.STRING, null),
        Expressions.calciteLiteralToDruidLiteral(
            plannerContext,
            rexBuilder.makeNullLiteral(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR))
        )
    );

    assertDruidLiteral(
        new DruidLiteral(ExpressionType.STRING, ""),
        Expressions.calciteLiteralToDruidLiteral(
            plannerContext,
            rexBuilder.makeLiteral("")
        )
    );

    assertDruidLiteral(
        new DruidLiteral(ExpressionType.LONG, null),
        Expressions.calciteLiteralToDruidLiteral(
            plannerContext,
            rexBuilder.makeNullLiteral(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT))
        )
    );

    assertDruidLiteral(
        new DruidLiteral(ExpressionType.STRING, null),
        Expressions.calciteLiteralToDruidLiteral(
            plannerContext,
            rexBuilder.makeNullLiteral(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.NULL))
        )
    );

    assertDruidLiteral(
        new DruidLiteral(ExpressionType.STRING, "abc"),
        Expressions.calciteLiteralToDruidLiteral(plannerContext, rexBuilder.makeLiteral("abc"))
    );

    assertDruidLiteral(
        new DruidLiteral(ExpressionType.LONG, 1L),
        Expressions.calciteLiteralToDruidLiteral(plannerContext, rexBuilder.makeLiteral(true))
    );

    assertDruidLiteral(
        new DruidLiteral(ExpressionType.LONG, 123L),
        Expressions.calciteLiteralToDruidLiteral(
            plannerContext,
            rexBuilder.makeExactLiteral(
                BigDecimal.valueOf(123L),
                rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER)
            )
        )
    );

    assertDruidLiteral(
        new DruidLiteral(ExpressionType.DOUBLE, 123.0),
        Expressions.calciteLiteralToDruidLiteral(
            plannerContext,
            rexBuilder.makeExactLiteral(
                BigDecimal.valueOf(123L),
                rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DECIMAL, 19, 0)
            )
        )
    );

    assertDruidLiteral(
        new DruidLiteral(ExpressionType.LONG, DateTimes.of("2000").getMillis()),
        Expressions.calciteLiteralToDruidLiteral(
            plannerContext,
            Calcites.jodaToCalciteTimestampLiteral(
                rexBuilder,
                DateTimes.of("2000"),
                DateTimeZone.UTC,
                DruidTypeSystem.DEFAULT_TIMESTAMP_PRECISION
            )
        )
    );

    assertDruidLiteral(
        new DruidLiteral(ExpressionType.LONG, DateTimes.of("2000").getMillis()),
        Expressions.calciteLiteralToDruidLiteral(
            plannerContext,
            rexBuilder.makeDateLiteral(Calcites.jodaToCalciteDateString(DateTimes.of("2000"), DateTimeZone.UTC))
        )
    );

    assertDruidLiteral(
        new DruidLiteral(ExpressionType.LONG, 3L),
        Expressions.calciteLiteralToDruidLiteral(
            plannerContext,
            rexBuilder.makeIntervalLiteral(
                BigDecimal.valueOf(3),
                new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.HOUR, SqlParserPos.ZERO)
            )
        )
    );

    assertDruidLiteral(
        new DruidLiteral(ExpressionType.LONG, 3),
        Expressions.calciteLiteralToDruidLiteral(
            plannerContext,
            rexBuilder.makeIntervalLiteral(
                BigDecimal.valueOf(3),
                new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO)
            )
        )
    );

    assertDruidLiteral(
        new DruidLiteral(ExpressionType.STRING, "123"),
        Expressions.calciteLiteralToDruidLiteral(
            plannerContext,
            rexBuilder.makeCast(
                rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
                rexBuilder.makeExactLiteral(
                    BigDecimal.valueOf(123.7),
                    rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER)
                )
            )
        )
    );

    assertDruidLiteral(
        new DruidLiteral(ExpressionType.DOUBLE, 123.0),
        Expressions.calciteLiteralToDruidLiteral(
            plannerContext,
            rexBuilder.makeCast(
                rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE),
                rexBuilder.makeExactLiteral(
                    BigDecimal.valueOf(123L),
                    rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER)
                )
            )
        )
    );

    Assertions.assertNull(
        Expressions.calciteLiteralToDruidLiteral(
            plannerContext,
            rexBuilder.makeCast(
                rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DATE),
                Calcites.jodaToCalciteTimestampLiteral(
                    rexBuilder,
                    DateTimes.of("2000-01-02T03:04:05"),
                    DateTimeZone.UTC,
                    DruidTypeSystem.DEFAULT_TIMESTAMP_PRECISION
                )
            )
        )
    );
  }

  private void assertDruidLiteral(
      final DruidLiteral expected,
      final DruidLiteral actual
  )
  {
    Assertions.assertEquals(
        StringUtils.format("%s: %s", expected.type(), expected.value()),
        StringUtils.format("%s: %s", actual.type(), actual.value())
    );
  }
}
