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

package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

import java.math.BigDecimal;

/**
 * Workaround for a Calcite 1.37.0 bug in which {@code INTERVAL n WEEK} is
 * incorrectly converted to {@code n} hours instead of {@code n * 7} days.
 *
 * <p>The bug is in {@code SqlIntervalQualifier.evaluateIntervalLiteralAsWeek},
 * which packages the WEEK value into a year-month shaped int array
 * ({@code [sign, 0, week]}) even though {@code SqlIntervalQualifier.typeName()}
 * reports WEEK intervals as {@link org.apache.calcite.sql.type.SqlTypeName#INTERVAL_DAY}.
 * Downstream, {@code SqlParserUtil.intervalToMillis} interprets that array as
 * {@code [sign, day, hour, minute, second, ms]}, so the week value lands in
 * the hour slot. The bug was fixed upstream in Calcite 1.38.0
 * (<a href="https://issues.apache.org/jira/browse/CALCITE-6391">CALCITE-6391</a>),
 * but Druid is still on Calcite 1.37.0, so we work around it by rewriting any
 * {@code WEEK} interval in the parsed {@link SqlNode} tree to an equivalent
 * {@code DAY} interval whose value has been multiplied by seven.
 *
 * <p>Two parsed forms reach this shuttle:
 * <ul>
 *   <li>Quoted: {@code INTERVAL '1' WEEK} parses to a {@link SqlIntervalLiteral}
 *       with a WEEK qualifier. We replace it with a {@link SqlIntervalLiteral}
 *       containing the original numeric value multiplied by seven and a
 *       {@link TimeUnit#DAY} qualifier.</li>
 *   <li>Unquoted: {@code INTERVAL 1 WEEK} parses to
 *       {@code SqlStdOperatorTable.INTERVAL.createCall(pos, n, qualifier)}.
 *       We rewrite the call so the numeric operand becomes
 *       {@code MULTIPLY(n, 7)} and the qualifier becomes {@code DAY}.</li>
 * </ul>
 *
 * <p>This rewrite preserves the semantics of {@code TimeUnitRange.WEEK} while
 * avoiding the buggy code path inside Calcite. Druid still treats WEEK as a
 * day-time (millisecond) interval in arithmetic, which matches the expectation
 * documented in the issue: {@code INTERVAL '1' WEEK} should add seven days.
 */
public class SqlIntervalWeekRewriteShuttle extends SqlShuttle
{
  private static final BigDecimal SEVEN = BigDecimal.valueOf(7);

  @Override
  public SqlNode visit(SqlLiteral literal)
  {
    if (literal instanceof SqlIntervalLiteral) {
      final SqlIntervalLiteral intervalLiteral = (SqlIntervalLiteral) literal;
      final SqlIntervalLiteral.IntervalValue value =
          (SqlIntervalLiteral.IntervalValue) intervalLiteral.getValue();
      if (value != null && isPlainWeek(value.getIntervalQualifier())) {
        final String multiplied = multiplyByWeek(value.getIntervalLiteral());
        if (multiplied != null) {
          return SqlLiteral.createInterval(
              value.getSign(),
              multiplied,
              new SqlIntervalQualifier(TimeUnit.DAY, null, value.getIntervalQualifier().getParserPosition()),
              literal.getParserPosition()
          );
        }
      }
    }
    return literal;
  }

  @Override
  public SqlNode visit(SqlCall call)
  {
    if (call.getOperator() == SqlStdOperatorTable.INTERVAL && call.operandCount() == 2) {
      final SqlNode qualifierNode = call.operand(1);
      if (qualifierNode instanceof SqlIntervalQualifier
          && isPlainWeek((SqlIntervalQualifier) qualifierNode)) {
        // First, recurse into the numeric operand so any nested rewrites still happen.
        final SqlNode rewrittenNumeric = call.operand(0).accept(this);
        final SqlNode numeric = rewrittenNumeric == null ? call.operand(0) : rewrittenNumeric;
        final SqlParserPos pos = call.getParserPosition();
        final SqlNode multipliedNumeric = SqlStdOperatorTable.MULTIPLY.createCall(
            pos,
            numeric,
            SqlLiteral.createExactNumeric("7", pos)
        );
        final SqlIntervalQualifier dayQualifier =
            new SqlIntervalQualifier(TimeUnit.DAY, null, qualifierNode.getParserPosition());
        return SqlStdOperatorTable.INTERVAL.createCall(pos, multipliedNumeric, dayQualifier);
      }
    }
    return super.visit(call);
  }

  private static boolean isPlainWeek(SqlIntervalQualifier qualifier)
  {
    // Only the bare WEEK qualifier is affected. WEEK(SUNDAY)..WEEK(SATURDAY) and
    // ISOWEEK use a non-null timeFrameName and are handled differently in Calcite.
    return qualifier != null
           && qualifier.timeUnitRange == TimeUnitRange.WEEK
           && qualifier.timeFrameName == null;
  }

  /**
   * Multiplies the integer string value of a WEEK interval literal by seven.
   * Returns null if the value is not a plain non-negative integer literal,
   * which means it does not match Calcite's WEEK interval grammar and would
   * fail validation anyway.
   */
  private static String multiplyByWeek(String intervalStr)
  {
    if (intervalStr == null || intervalStr.isEmpty()) {
      return null;
    }
    for (int i = 0; i < intervalStr.length(); i++) {
      final char c = intervalStr.charAt(i);
      if (c < '0' || c > '9') {
        return null;
      }
    }
    return new BigDecimal(intervalStr).multiply(SEVEN).toString();
  }
}
