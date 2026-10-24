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
    testHelper.testExpression(
        new SubstringOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1),
            testHelper.makeLiteral(2)
        ),
        DruidExpression.ofColumn(ColumnType.STRING, "s").map(
            extraction -> null,
            expression -> "substring(\"s\", 0, 2)"
        ),
        "fo"
    );

    testHelper.testExpression(
        new SubstringOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(2),
            testHelper.makeLiteral(1)
        ),
        DruidExpression.ofColumn(ColumnType.STRING, "s").map(
            extraction -> null,
            expression -> "substring(\"s\", 1, 1)"
        ),
        "o"
    );

    testHelper.testExpression(
        new SubstringOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1)
        ),
        DruidExpression.ofColumn(ColumnType.STRING, "s").map(
            extraction -> null,
            expression -> "substring(\"s\", 0, -1)"
        ),
        "foo"
    );

    testHelper.testExpression(
        new SubstringOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(2)
        ),
        DruidExpression.ofColumn(ColumnType.STRING, "s").map(
            extraction -> null,
            expression -> "substring(\"s\", 1, -1)"
        ),
        "oo"
    );

    testHelper.testExpression(
        new SubstringOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1),
            testHelper.makeInputRef("p") // p is 3
        ),
        DruidExpression.ofColumn(ColumnType.STRING, "s").map(
            extraction -> null,
            expression -> "substring(\"s\", 0, \"p\")"
        ),
        "foo"
    );

    testHelper.testExpression(
        new SubstringOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("spacey"),
            testHelper.makeInputRef("p") // p is 3
        ),
        DruidExpression.ofColumn(ColumnType.STRING, "spacey").map(
            extraction -> null,
            expression -> "substring(\"spacey\", (\"p\" - 1), -1)"
        ),
        "hey there  "
    );

    testHelper.testExpression(
        new SubstringOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("spacey"),
            testHelper.makeInputRef("p"), // p is 3
            testHelper.makeInputRef("p") // p is 3
        ),
        DruidExpression.ofColumn(ColumnType.STRING, "spacey").map(
            extraction -> null,
            expression -> "substring(\"spacey\", (\"p\" - 1), \"p\")"
        ),
        "hey"
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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "regexp_extract",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'x(.)'"),
                DruidExpression.ofLiteral(ColumnType.LONG, "1")
            )
        ),
        null
    );

    testHelper.testExpression(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("(o)"),
            testHelper.makeLiteral(1)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "regexp_extract",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'(o)'"),
                DruidExpression.ofLiteral(ColumnType.LONG, "1")
            )
        ),

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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "regexp_extract",
            ImmutableList.of(
                DruidExpression.ofFunctionCall(
                    ColumnType.STRING,
                    "concat",
                    ImmutableList.of(
                        DruidExpression.ofLiteral(ColumnType.STRING, "'Z'"),
                        DruidExpression.ofColumn(ColumnType.STRING, "s")
                    )
                ),
                DruidExpression.ofLiteral(ColumnType.STRING, "'Zf(.)'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "regexp_extract",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'f(.)'"),
                DruidExpression.ofLiteral(ColumnType.LONG, "1")
            )
        ),
        "o"
    );

    testHelper.testExpression(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("f(.)")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "regexp_extract",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'f(.)'")
            )
        ),
        "fo"
    );

    testHelper.testExpression(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "regexp_extract",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.STRING, "''")
            )
        ),
        ""
    );

    testHelper.testExpression(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "regexp_extract",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.STRING, "''")
            )
        ),
        ""
    );

    testHelper.testExpression(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeNullLiteral(SqlTypeName.VARCHAR),
            testHelper.makeLiteral("(.)")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "regexp_extract",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.STRING, "null"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'(.)'")
            )
        ),
        null
    );

    testHelper.testExpression(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeNullLiteral(SqlTypeName.VARCHAR),
            testHelper.makeLiteral("")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "regexp_extract",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.STRING, "null"),
                DruidExpression.ofLiteral(ColumnType.STRING, "''")
            )
        ),
        null
    );

    testHelper.testExpression(
        new RegexpExtractOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeNullLiteral(SqlTypeName.VARCHAR),
            testHelper.makeLiteral("null")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "regexp_extract",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.STRING, "null"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'null'")
            )
        ),
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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "regexp_replace",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'x(.)'"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'z'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "regexp_replace",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'(o)'"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'z'")
            )
        ),
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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "regexp_replace",
            ImmutableList.of(
                DruidExpression.ofFunctionCall(
                    ColumnType.STRING,
                    "concat",
                    ImmutableList.of(
                        DruidExpression.ofLiteral(ColumnType.STRING, "'Z'"),
                        DruidExpression.ofColumn(ColumnType.STRING, "s")
                    )
                ),
                DruidExpression.ofLiteral(ColumnType.STRING, "'Zf(.)'"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'z'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "regexp_replace",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'f(.)'"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'$1'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "regexp_like",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'f.'")
            )
        ),
        1L
    );

    testHelper.testExpression(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("o")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "regexp_like",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'o'")
            )
        ),

        // Column "s" contains an 'o', but not at the beginning; we do match this.
        1L
    );

    testHelper.testExpression(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("x.")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "regexp_like",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'x.'")
            )
        ),
        0L
    );

    testHelper.testExpression(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral("")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "regexp_like",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.STRING, "''")
            )
        ),
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "regexp_like",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "newliney"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'^beep$'")
            )
        ),
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "regexp_like",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "newliney"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'boo'")
            )
        ),
        1L
    );

    testHelper.testExpression(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("newliney"),
            testHelper.makeLiteral("^boo")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "regexp_like",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "newliney"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'^boo'")
            )
        ),
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "regexp_like",
            ImmutableList.of(
                DruidExpression.ofFunctionCall(
                    ColumnType.STRING,
                    "concat",
                    ImmutableList.of(
                        DruidExpression.ofLiteral(ColumnType.STRING, "'Z'"),
                        DruidExpression.ofColumn(ColumnType.STRING, "s")
                    )
                ),
                DruidExpression.ofLiteral(ColumnType.STRING, "'x(.)'")
            )
        ),
        0L
    );

    testHelper.testExpression(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeNullLiteral(SqlTypeName.VARCHAR),
            testHelper.makeLiteral("(.)")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "regexp_like",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.STRING, "null"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'(.)'")
            )
        ),
        null
    );

    testHelper.testExpression(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeNullLiteral(SqlTypeName.VARCHAR),
            testHelper.makeLiteral("")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "regexp_like",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.STRING, "null"),
                DruidExpression.ofLiteral(ColumnType.STRING, "''")
            )
        ),
        null
    );

    testHelper.testExpression(
        new RegexpLikeOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeNullLiteral(SqlTypeName.VARCHAR),
            testHelper.makeLiteral("null")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "regexp_like",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.STRING, "null"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'null'")
            )
        ),
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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "format",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.STRING, "'%x'"),
                DruidExpression.ofColumn(ColumnType.LONG, "b")
            )
        ),
        "19"
    );

    testHelper.testExpression(
        new StringFormatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("%s %,d"),
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1234)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "format",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.STRING, "'%s %,d'"),
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.LONG, "1234")
            )
        ),
        "foo 1,234"
    );

    testHelper.testExpression(
        new StringFormatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("%s %,d"),
            testHelper.makeInputRef("s")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "format",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.STRING, "'%s %,d'"),
                DruidExpression.ofColumn(ColumnType.STRING, "s")
            )
        ),
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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "format",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.STRING, "'%s %,d'"),
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.LONG, "1234"),
                DruidExpression.ofLiteral(ColumnType.LONG, "6789")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "format",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.STRING, "'%.2f %.2f %.2f'"),
                DruidExpression.ofLiteral(ColumnType.DOUBLE, "1234.0"),
                DruidExpression.ofLiteral(ColumnType.DOUBLE, "123.4"),
                DruidExpression.ofLiteral(ColumnType.DOUBLE, "123.456789")
            )
        ),
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
            (args) -> "(strpos(\"s\",'oo') + 1)",
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
            (args) -> "(strpos(\"s\",'ax') + 1)",
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
            (args) -> "(strpos(null,'ax') + 1)",
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
        DruidExpression.ofExpression(
            ColumnType.LONG,
            DruidExpression.functionCall("parse_long"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "intstr")
            )
        ),
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
    testHelper.testExpression(
        SqlStdOperatorTable.POSITION,
        ImmutableList.of(
            testHelper.makeLiteral("oo"),
            testHelper.makeInputRef("s")
        ),
        DruidExpression.ofExpression(
            ColumnType.LONG,
            (args) -> "(strpos(\"s\",'oo',0) + 1)",
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
            (args) -> "(strpos(\"s\",'oo',(2 - 1)) + 1)",
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
            (args) -> "(strpos(\"s\",'oo',(3 - 1)) + 1)",
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
        DruidExpression.ofFunctionCall(
            ColumnType.DOUBLE,
            "pow",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "a"),
                DruidExpression.ofLiteral(ColumnType.LONG, "2")
            )
        ),
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
        DruidExpression.ofExpression(
            ColumnType.FLOAT,
            DruidExpression.functionCall("round"),
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.FLOAT, "x")
            )
        ),
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
    Assertions.assertEquals("Function[round] second argument should be a LONG but got STRING instead", t.getMessage());
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
    testHelper.testExpression(
        new DateTruncOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("hour"),
            testHelper.makeLiteral(DateTimes.of("2000-02-03T04:05:06Z"))
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "timestamp_floor",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.LONG, "949550706000"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'PT1H'"),
                DruidExpression.ofLiteral(ColumnType.STRING, "null"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'UTC'")
            )
        ),
        DateTimes.of("2000-02-03T04:00:00").getMillis()
    );

    testHelper.testExpression(
        new DateTruncOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral("DAY"),
            testHelper.makeLiteral(DateTimes.of("2000-02-03T04:05:06Z"))
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "timestamp_floor",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.LONG, "949550706000"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'P1D'"),
                DruidExpression.ofLiteral(ColumnType.STRING, "null"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'UTC'")
            )
        ),
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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "trim",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "spacey"),
                DruidExpression.ofLiteral(ColumnType.STRING, "' '")
            )
        ),
        "hey there"
    );

    testHelper.testExpression(
        SqlStdOperatorTable.TRIM,
        ImmutableList.of(
            testHelper.makeFlag(SqlTrimFunction.Flag.LEADING),
            testHelper.makeLiteral(" h"),
            testHelper.makeInputRef("spacey")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "ltrim",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "spacey"),
                DruidExpression.ofLiteral(ColumnType.STRING, "' h'")
            )
        ),
        "ey there  "
    );

    testHelper.testExpression(
        SqlStdOperatorTable.TRIM,
        ImmutableList.of(
            testHelper.makeFlag(SqlTrimFunction.Flag.TRAILING),
            testHelper.makeLiteral(" e"),
            testHelper.makeInputRef("spacey")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "rtrim",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "spacey"),
                DruidExpression.ofLiteral(ColumnType.STRING, "' e'")
            )
        ),
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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "lpad",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.LONG, "5"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'x'")
            )
        ),
        "xxfoo"
    );

    testHelper.testExpression(
        new RPadOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(5),
            testHelper.makeLiteral("x")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "rpad",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.LONG, "5"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'x'")
            )
        ),
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "contains_string",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "spacey"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'there'")
            )
        ),
        1L
    );

    testHelper.testExpression(
        ContainsOperatorConversion.caseSensitive().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("spacey"),
            testHelper.makeLiteral("There")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "contains_string",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "spacey"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'There'")
            )
        ),
        0L
    );

    testHelper.testExpression(
        ContainsOperatorConversion.caseInsensitive().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("spacey"),
            testHelper.makeLiteral("There")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "icontains_string",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "spacey"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'There'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "contains_string",
            ImmutableList.of(
                DruidExpression.ofFunctionCall(
                    ColumnType.STRING,
                    "concat",
                    ImmutableList.of(
                        DruidExpression.ofLiteral(ColumnType.STRING, "'what is'"),
                        DruidExpression.ofColumn(ColumnType.STRING, "spacey")
                    )
                ),
                DruidExpression.ofLiteral(ColumnType.STRING, "'what'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "contains_string",
            ImmutableList.of(
                DruidExpression.ofFunctionCall(
                    ColumnType.STRING,
                    "concat",
                    ImmutableList.of(
                        DruidExpression.ofLiteral(ColumnType.STRING, "'what is'"),
                        DruidExpression.ofColumn(ColumnType.STRING, "spacey")
                    )
                ),
                DruidExpression.ofLiteral(ColumnType.STRING, "'there'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "icontains_string",
            ImmutableList.of(
                DruidExpression.ofFunctionCall(
                    ColumnType.STRING,
                    "concat",
                    ImmutableList.of(
                        DruidExpression.ofLiteral(ColumnType.STRING, "'what is'"),
                        DruidExpression.ofColumn(ColumnType.STRING, "spacey")
                    )
                ),
                DruidExpression.ofLiteral(ColumnType.STRING, "'There'")
            )
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
            (args) -> "(contains_string(\"spacey\",'there') && ('yes' == 'yes'))",
            ImmutableList.of(
                DruidExpression.ofFunctionCall(
                    ColumnType.LONG,
                    "contains_string",
                    ImmutableList.of(
                        DruidExpression.ofColumn(ColumnType.STRING, "spacey"),
                        DruidExpression.ofLiteral(ColumnType.STRING, "'there'")
                    )
                ),
                DruidExpression.ofExpression(
                    ColumnType.LONG,
                    (args) -> "('yes' == 'yes')",
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
            (args) -> "(icontains_string(\"spacey\",'There') && ('yes' == 'yes'))",
            ImmutableList.of(
                DruidExpression.ofFunctionCall(
                    ColumnType.LONG,
                    "icontains_string",
                    ImmutableList.of(
                        DruidExpression.ofColumn(ColumnType.STRING, "spacey"),
                        DruidExpression.ofLiteral(ColumnType.STRING, "'There'")
                    )
                ),
                DruidExpression.ofExpression(
                    ColumnType.LONG,
                    (args) -> "('yes' == 'yes')",
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "timestamp_floor",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.LONG, "949550706000"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'PT1H'"),
                DruidExpression.ofLiteral(null, "null"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'UTC'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "timestamp_floor",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "t"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'P1D'"),
                DruidExpression.ofLiteral(null, "null"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'America/Los_Angeles'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "timestamp_floor",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "t"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'P1Y'"),
                DruidExpression.ofLiteral(null, "null"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'UTC'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "timestamp_ceil",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.LONG, "949550706000"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'PT1H'"),
                DruidExpression.ofLiteral(null, "null"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'UTC'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "timestamp_ceil",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "t"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'P1D'"),
                DruidExpression.ofLiteral(null, "null"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'America/Los_Angeles'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "timestamp_ceil",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "t"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'P1Y'"),
                DruidExpression.ofLiteral(null, "null"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'UTC'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "timestamp_shift",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "t"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'PT2H'"),
                DruidExpression.ofLiteral(ColumnType.LONG, "-3"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'UTC'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "timestamp_shift",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "t"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'PT2H'"),
                DruidExpression.ofLiteral(ColumnType.LONG, "-3"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'America/Los_Angeles'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "timestamp_extract",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "t"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'QUARTER'"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'UTC'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "timestamp_extract",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "t"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'DAY'"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'America/Los_Angeles'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "timestamp_extract",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "t"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'DAY'"),
                DruidExpression.ofColumn(ColumnType.STRING, "timezone")
            )
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
            (args) -> "(\"t\" + 90060000)",
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "timestamp_shift",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "t"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'P13M'"),
                DruidExpression.ofLiteral(ColumnType.LONG, "1"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'UTC'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "timestamp_parse",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "tstr"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'yyyy-MM-dd HH:mm:ss'"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'UTC'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "timestamp_parse",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "tstr"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'yyyy-MM-dd HH:mm:ss'"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'America/Los_Angeles'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "timestamp_format",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "t"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'yyyy-MM-dd HH:mm:ss'"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'UTC'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "timestamp_format",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "t"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'yyyy-MM-dd HH:mm:ss'"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'America/Los_Angeles'")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "timestamp_extract",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "t"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'QUARTER'"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'UTC'")
            )
        ),
        1L
    );

    testHelper.testExpression(
        SqlStdOperatorTable.EXTRACT,
        ImmutableList.of(
            testHelper.makeFlag(TimeUnitRange.DAY),
            testHelper.makeInputRef("t")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "timestamp_extract",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "t"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'DAY'"),
                DruidExpression.ofLiteral(ColumnType.STRING, "'UTC'")
            )
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
  public void testRight()
  {
    testHelper.testExpression(
        new RightOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(1)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "right",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.LONG, "1")
            )
        ),
        "o"
    );

    testHelper.testExpression(
        new RightOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(2)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "right",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.LONG, "2")
            )
        ),
        "oo"
    );

    testHelper.testExpression(
        new RightOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(3)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "right",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.LONG, "3")
            )
        ),
        "foo"
    );

    testHelper.testExpression(
        new RightOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(4)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "right",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.LONG, "4")
            )
        ),
        "foo"
    );

    testHelper.testExpression(
        new RightOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("tstr"),
            testHelper.makeLiteral(5)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "right",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "tstr"),
                DruidExpression.ofLiteral(ColumnType.LONG, "5")
            )
        ),
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
            DruidExpression.ofFunctionCall(
                ColumnType.STRING,
                "right",
                ImmutableList.of(
                    DruidExpression.ofColumn(ColumnType.STRING, "s"),
                    DruidExpression.ofLiteral(ColumnType.LONG, "-1")
                )
            ),
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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "left",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.LONG, "1")
            )
        ),
        "f"
    );

    testHelper.testExpression(
        new LeftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(2)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "left",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.LONG, "2")
            )
        ),
        "fo"
    );

    testHelper.testExpression(
        new LeftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(3)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "left",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.LONG, "3")
            )
        ),
        "foo"
    );

    testHelper.testExpression(
        new LeftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(4)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "left",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.LONG, "4")
            )
        ),
        "foo"
    );

    testHelper.testExpression(
        new LeftOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("tstr"),
            testHelper.makeLiteral(10)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "left",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "tstr"),
                DruidExpression.ofLiteral(ColumnType.LONG, "10")
            )
        ),
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
            DruidExpression.ofFunctionCall(
                ColumnType.STRING,
                "left",
                ImmutableList.of(
                    DruidExpression.ofColumn(ColumnType.STRING, "s"),
                    DruidExpression.ofLiteral(ColumnType.LONG, "-1")
                )
            ),
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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "repeat",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.LONG, "1")
            )
        ),
        "foo"
    );

    testHelper.testExpression(
        new RepeatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(3)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "repeat",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.LONG, "3")
            )
        ),
        "foofoofoo"
    );

    testHelper.testExpression(
        new RepeatOperatorConversion().calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeLiteral(-1)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "repeat",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofLiteral(ColumnType.LONG, "-1")
            )
        ),
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "bitwiseComplement",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "a")
            )
        ),
        -11L
    );

    testHelper.testExpression(
        OperatorConversions.druidUnaryLongFn("BITWISE_COMPLEMENT", "bitwiseComplement").calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("x")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "bitwiseComplement",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.FLOAT, "x")
            )
        ),
        -3L
    );

    testHelper.testExpression(
        OperatorConversions.druidUnaryLongFn("BITWISE_COMPLEMENT", "bitwiseComplement").calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "bitwiseComplement",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s")
            )
        ),
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
        DruidExpression.ofFunctionCall(
            ColumnType.DOUBLE,
            "bitwiseConvertLongBitsToDouble",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "a")
            )
        ),
        4.9E-323
    );

    testHelper.testExpression(
        OperatorConversions.druidUnaryDoubleFn("BITWISE_CONVERT_LONG_BITS_TO_DOUBLE", "bitwiseConvertLongBitsToDouble")
                           .calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("x")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.DOUBLE,
            "bitwiseConvertLongBitsToDouble",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.FLOAT, "x")
            )
        ),
        1.0E-323
    );

    testHelper.testExpression(
        OperatorConversions.druidUnaryDoubleFn("BITWISE_CONVERT_LONG_BITS_TO_DOUBLE", "bitwiseConvertLongBitsToDouble")
                           .calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.DOUBLE,
            "bitwiseConvertLongBitsToDouble",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "bitwiseAnd",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "a"),
                DruidExpression.ofColumn(ColumnType.LONG, "b")
            )
        ),
        8L
    );

    testHelper.testExpression(
        OperatorConversions.druidBinaryLongFn("BITWISE_AND", "bitwiseAnd").calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("x"),
            testHelper.makeInputRef("y")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "bitwiseAnd",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.FLOAT, "x"),
                DruidExpression.ofColumn(ColumnType.LONG, "y")
            )
        ),
        2L
    );

    testHelper.testExpression(
        OperatorConversions.druidBinaryLongFn("BITWISE_AND", "bitwiseAnd").calciteOperator(),
        ImmutableList.of(
            testHelper.makeInputRef("s"),
            testHelper.makeInputRef("s")
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.LONG,
            "bitwiseAnd",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.STRING, "s"),
                DruidExpression.ofColumn(ColumnType.STRING, "s")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "human_readable_binary_byte_format",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.LONG, "1000")
            )
        ),
        "1000 B"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(1024)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "human_readable_binary_byte_format",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.LONG, "1024")
            )
        ),
        "1.00 KiB"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(Long.MAX_VALUE)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "human_readable_binary_byte_format",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.LONG, "9223372036854775807")
            )
        ),
        "8.00 EiB"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(Long.MIN_VALUE)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "human_readable_binary_byte_format",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.LONG, "-9223372036854775808")
            )
        ),
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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "human_readable_binary_byte_format",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "b"),
                DruidExpression.ofColumn(ColumnType.LONG, "p")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "human_readable_binary_byte_format",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.LONG, "45000"),
                DruidExpression.ofLiteral(ColumnType.LONG, "0")
            )
        ),
        "44 KiB"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45000),
            //precision 1
            testHelper.makeLiteral(1)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "human_readable_binary_byte_format",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.LONG, "45000"),
                DruidExpression.ofLiteral(ColumnType.LONG, "1")
            )
        ),
        "43.9 KiB"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45000),
            //precision 2
            testHelper.makeLiteral(2)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "human_readable_binary_byte_format",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.LONG, "45000"),
                DruidExpression.ofLiteral(ColumnType.LONG, "2")
            )
        ),
        "43.95 KiB"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.BINARY_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45000),
            //precision 3
            testHelper.makeLiteral(3)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "human_readable_binary_byte_format",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.LONG, "45000"),
                DruidExpression.ofLiteral(ColumnType.LONG, "3")
            )
        ),
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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "human_readable_decimal_byte_format",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.LONG, "999")
            )
        ),
        "999 B"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(1024)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "human_readable_decimal_byte_format",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.LONG, "1024")
            )
        ),
        "1.02 KB"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(Long.MAX_VALUE)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "human_readable_decimal_byte_format",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.LONG, "9223372036854775807")
            )
        ),
        "9.22 EB"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(Long.MIN_VALUE)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "human_readable_decimal_byte_format",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.LONG, "-9223372036854775808")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "human_readable_decimal_byte_format",
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.LONG, "b"),
                DruidExpression.ofColumn(ColumnType.LONG, "p")
            )
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
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "human_readable_decimal_byte_format",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.LONG, "45678"),
                DruidExpression.ofLiteral(ColumnType.LONG, "0")
            )
        ),
        "46 KB"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45678),
            //precision 1
            testHelper.makeLiteral(1)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "human_readable_decimal_byte_format",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.LONG, "45678"),
                DruidExpression.ofLiteral(ColumnType.LONG, "1")
            )
        ),
        "45.7 KB"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45678),
            //precision 2
            testHelper.makeLiteral(2)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "human_readable_decimal_byte_format",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.LONG, "45678"),
                DruidExpression.ofLiteral(ColumnType.LONG, "2")
            )
        ),
        "45.68 KB"
    );
    testHelper.testExpression(
        HumanReadableFormatOperatorConversion.DECIMAL_BYTE_FORMAT.calciteOperator(),
        ImmutableList.of(
            testHelper.makeLiteral(45678),
            //precision 3
            testHelper.makeLiteral(3)
        ),
        DruidExpression.ofFunctionCall(
            ColumnType.STRING,
            "human_readable_decimal_byte_format",
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.LONG, "45678"),
                DruidExpression.ofLiteral(ColumnType.LONG, "3")
            )
        ),
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
