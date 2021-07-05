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

package org.apache.druid.query.expression;

import com.google.common.base.Preconditions;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.math.expr.Expr;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;

import javax.annotation.Nullable;

public class ExprUtils
{
  private static final Expr.ObjectBinding NIL_BINDINGS = name -> null;

  public static Expr.ObjectBinding nilBindings()
  {
    return NIL_BINDINGS;
  }

  static DateTimeZone toTimeZone(final Expr timeZoneArg)
  {
    if (!timeZoneArg.isLiteral()) {
      throw new IAE("Time zone must be a literal");
    }

    final Object literalValue = timeZoneArg.getLiteralValue();
    return literalValue == null ? DateTimeZone.UTC : DateTimes.inferTzFromString((String) literalValue);
  }

  static PeriodGranularity toPeriodGranularity(
      final Expr periodArg,
      @Nullable final Expr originArg,
      @Nullable final Expr timeZoneArg,
      final Expr.ObjectBinding bindings
  )
  {
    final Period period = new Period(periodArg.eval(bindings).asString());
    final DateTime origin;
    final DateTimeZone timeZone;

    if (timeZoneArg == null) {
      timeZone = null;
    } else {
      final String value = timeZoneArg.eval(bindings).asString();
      timeZone = value != null ? DateTimes.inferTzFromString(value) : null;
    }

    if (originArg == null) {
      origin = null;
    } else {
      Chronology chronology = timeZone == null ? ISOChronology.getInstanceUTC() : ISOChronology.getInstance(timeZone);
      final Object value = originArg.eval(bindings).value();
      if (value instanceof String && NullHandling.isNullOrEquivalent((String) value)) {
        // We get a blank string here, when sql compatible null handling is enabled
        // and expression contains empty string for for origin
        // e.g timestamp_floor(\"__time\",'PT1M','','UTC')
        origin = null;
      } else {
        origin = value != null ? new DateTime(value, chronology) : null;
      }
    }

    return new PeriodGranularity(period, origin, timeZone);
  }

  static String createErrMsg(String functionName, String msg)
  {
    String prefix = "Function[" + functionName + "] ";
    return prefix + msg;
  }

  static void checkLiteralArgument(String functionName, Expr arg, String argName)
  {
    Preconditions.checkArgument(arg.isLiteral(), createErrMsg(functionName, argName + " arg must be a literal"));
  }

  /**
   * True if Expr is a string literal.
   *
   * In non-SQL-compliant null handling mode, this method will return true for null literals as well (because they are
   * treated equivalently to empty strings, and we cannot tell the difference.)
   *
   * In SQL-compliant null handling mode, this method will return true for actual strings only, not nulls.
   */
  static boolean isStringLiteral(final Expr expr)
  {
    return (expr.isLiteral() && expr.getLiteralValue() instanceof String)
           || (NullHandling.replaceWithDefault() && isNullLiteral(expr));
  }

  /**
   * True if Expr is a null literal.
   *
   * In non-SQL-compliant null handling mode, this method will return true for either a null literal or an empty string
   * literal (they are treated equivalently and we cannot tell the difference).
   *
   * In SQL-compliant null handling mode, this method will only return true for an actual null literal.
   */
  static boolean isNullLiteral(final Expr expr)
  {
    return expr.isLiteral() && expr.getLiteralValue() == null;
  }
}
