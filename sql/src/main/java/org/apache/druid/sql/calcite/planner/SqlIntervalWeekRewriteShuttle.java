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
 * Workaround for Calcite 1.37.0 bugs in which {@code INTERVAL n WEEK} is
 * incorrectly converted to {@code n} hours instead of {@code n * 7} days,
 * and {@code INTERVAL n QUARTER} is incorrectly converted to {@code n} months
 * instead of {@code n * 3} months.
 *
 * <p>Both bugs live in {@code SqlIntervalQualifier}:
 * <ul>
 *   <li>{@code evaluateIntervalLiteralAsWeek} packages the WEEK value into a
 *       year-month shaped int array ({@code [sign, 0, week]}) even though
 *       {@code SqlIntervalQualifier.typeName()} reports WEEK intervals as
 *       {@link org.apache.calcite.sql.type.SqlTypeName#INTERVAL_DAY}. Downstream,
 *       {@code SqlParserUtil.intervalToMillis} interprets that array as
 *       {@code [sign, day, hour, minute, second, ms]}, so the week value lands
 *       in the hour slot.</li>
 *   <li>{@code evaluateIntervalLiteralAsQuarter} packages the QUARTER value
 *       into a year-month shaped int array ({@code [sign, 0, quarter]}) without
 *       multiplying by three, so {@code intervalToMonths} reads
 *       {@code 12*0 + 1*quarter = quarter} months when it should read
 *       {@code 3*quarter} months.</li>
 * </ul>
 *
 * <p>Both bugs were fixed upstream in Calcite 1.38.0
 * (<a href="https://issues.apache.org/jira/browse/CALCITE-6581">CALCITE-6581</a>),
 * but Druid is still on Calcite 1.37.0, so we work around them by rewriting any
 * affected interval in the parsed {@link SqlNode} tree to an equivalent
 * interval in a unit that Calcite 1.37 handles correctly:
 * <ul>
 *   <li>{@code WEEK} is rewritten to {@code DAY} with the value multiplied by seven.</li>
 *   <li>{@code QUARTER} is rewritten to {@code MONTH} with the value multiplied by three.</li>
 * </ul>
 *
 * <p>Two parsed forms reach this shuttle for each affected unit:
 * <ul>
 *   <li>Quoted: {@code INTERVAL '1' WEEK} parses to a {@link SqlIntervalLiteral}
 *       with a WEEK qualifier. We replace it with a {@link SqlIntervalLiteral}
 *       containing the original numeric value multiplied by the appropriate
 *       factor and a {@link TimeUnit#DAY} (or {@link TimeUnit#MONTH}) qualifier.</li>
 *   <li>Unquoted: {@code INTERVAL 1 WEEK} parses to
 *       {@code SqlStdOperatorTable.INTERVAL.createCall(pos, n, qualifier)}.
 *       We rewrite the call so the numeric operand becomes
 *       {@code MULTIPLY(n, factor)} and the qualifier becomes the appropriate
 *       base unit.</li>
 * </ul>
 *
 * <p>This rewrite preserves the semantics of {@code TimeUnitRange.WEEK} and
 * {@code TimeUnitRange.QUARTER} while avoiding the buggy code paths inside
 * Calcite. {@code WEEK(SUNDAY)..WEEK(SATURDAY)} and {@code ISOWEEK} qualifiers
 * carry a non-null {@code timeFrameName} and are handled differently in
 * Calcite, so the shuttle leaves them alone and only touches the bare
 * {@code WEEK} form that is broken in 1.37.
 */
public class SqlIntervalWeekRewriteShuttle extends SqlShuttle
{
  private static final BigDecimal SEVEN = BigDecimal.valueOf(7);
  private static final BigDecimal THREE = BigDecimal.valueOf(3);

  @Override
  public SqlNode visit(SqlLiteral literal)
  {
    if (literal instanceof SqlIntervalLiteral) {
      final SqlIntervalLiteral intervalLiteral = (SqlIntervalLiteral) literal;
      final SqlIntervalLiteral.IntervalValue value =
          (SqlIntervalLiteral.IntervalValue) intervalLiteral.getValue();
      if (value != null) {
        final SqlIntervalQualifier qualifier = value.getIntervalQualifier();
        if (isPlainWeek(qualifier)) {
          final String multiplied = multiplyLiteral(value.getIntervalLiteral(), SEVEN);
          if (multiplied != null) {
            return SqlLiteral.createInterval(
                value.getSign(),
                multiplied,
                new SqlIntervalQualifier(TimeUnit.DAY, null, qualifier.getParserPosition()),
                literal.getParserPosition()
            );
          }
        } else if (isPlainQuarter(qualifier)) {
          final String multiplied = multiplyLiteral(value.getIntervalLiteral(), THREE);
          if (multiplied != null) {
            return SqlLiteral.createInterval(
                value.getSign(),
                multiplied,
                new SqlIntervalQualifier(TimeUnit.MONTH, null, qualifier.getParserPosition()),
                literal.getParserPosition()
            );
          }
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
      if (qualifierNode instanceof SqlIntervalQualifier) {
        final SqlIntervalQualifier qualifier = (SqlIntervalQualifier) qualifierNode;
        if (isPlainWeek(qualifier)) {
          return rewriteIntervalCall(call, qualifier, "7", TimeUnit.DAY);
        } else if (isPlainQuarter(qualifier)) {
          return rewriteIntervalCall(call, qualifier, "3", TimeUnit.MONTH);
        }
      }
    }
    return super.visit(call);
  }

  private SqlNode rewriteIntervalCall(
      SqlCall call,
      SqlIntervalQualifier qualifier,
      String factor,
      TimeUnit rewrittenUnit
  )
  {
    // First, recurse into the numeric operand so any nested rewrites still happen.
    final SqlNode rewrittenNumeric = call.operand(0).accept(this);
    final SqlNode numeric = rewrittenNumeric == null ? call.operand(0) : rewrittenNumeric;
    final SqlParserPos pos = call.getParserPosition();
    final SqlNode multipliedNumeric = SqlStdOperatorTable.MULTIPLY.createCall(
        pos,
        numeric,
        SqlLiteral.createExactNumeric(factor, pos)
    );
    final SqlIntervalQualifier rewrittenQualifier =
        new SqlIntervalQualifier(rewrittenUnit, null, qualifier.getParserPosition());
    return SqlStdOperatorTable.INTERVAL.createCall(pos, multipliedNumeric, rewrittenQualifier);
  }

  private static boolean isPlainWeek(SqlIntervalQualifier qualifier)
  {
    // Only the bare WEEK qualifier is affected. WEEK(SUNDAY)..WEEK(SATURDAY) and
    // ISOWEEK use a non-null timeFrameName and are handled differently in Calcite.
    return qualifier != null
           && qualifier.timeUnitRange == TimeUnitRange.WEEK
           && qualifier.timeFrameName == null;
  }

  private static boolean isPlainQuarter(SqlIntervalQualifier qualifier)
  {
    // Only the bare QUARTER qualifier is affected.
    return qualifier != null
           && qualifier.timeUnitRange == TimeUnitRange.QUARTER
           && qualifier.timeFrameName == null;
  }

  /**
   * Multiplies the integer string value of an interval literal by a factor.
   * Returns null if the value is not a plain non-negative integer literal,
   * which means it does not match Calcite's WEEK/QUARTER interval grammar and
   * would fail validation anyway.
   */
  private static String multiplyLiteral(String intervalStr, BigDecimal factor)
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
    return new BigDecimal(intervalStr).multiply(factor).toString();
  }
}
