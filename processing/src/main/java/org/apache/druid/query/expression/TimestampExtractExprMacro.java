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

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public class TimestampExtractExprMacro implements ExprMacroTable.ExprMacro
{
  private static final String FN_NAME = "timestamp_extract";

  public enum Unit
  {
    EPOCH,
    MICROSECOND,
    MILLISECOND,
    SECOND,
    MINUTE,
    HOUR,
    DAY,
    DOW,
    ISODOW,
    DOY,
    WEEK,
    MONTH,
    QUARTER,
    YEAR,
    ISOYEAR,
    DECADE,
    CENTURY,
    MILLENNIUM
  }

  @Override
  public String name()
  {
    return FN_NAME;
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    if (args.size() < 2 || args.size() > 3) {
      throw new IAE("Function[%s] must have 2 to 3 arguments", name());
    }

    if (!args.get(1).isLiteral() || args.get(1).getLiteralValue() == null) {
      throw new IAE("Function[%s] unit arg must be literal", name());
    }

    if (args.size() > 2 && !args.get(2).isLiteral()) {
      throw new IAE("Function[%s] timezone arg must be literal", name());
    }

    final Expr arg = args.get(0);
    final Unit unit = Unit.valueOf(StringUtils.toUpperCase((String) args.get(1).getLiteralValue()));
    final DateTimeZone timeZone;

    if (args.size() > 2) {
      timeZone = ExprUtils.toTimeZone(args.get(2));
    } else {
      timeZone = DateTimeZone.UTC;
    }

    final ISOChronology chronology = ISOChronology.getInstance(timeZone);

    class TimestampExtractExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      private TimestampExtractExpr(Expr arg)
      {
        super(FN_NAME, arg);
      }

      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        Object val = arg.eval(bindings).value();
        if (val == null) {
          // Return null if the argument if null.
          return ExprEval.of(null);
        }
        final DateTime dateTime = new DateTime(val, chronology);
        long epoch = dateTime.getMillis() / 1000;

        switch (unit) {
          case EPOCH:
            return ExprEval.of(epoch);
          case MICROSECOND:
            return ExprEval.of(epoch / 1000);
          case MILLISECOND:
            return ExprEval.of(dateTime.millisOfSecond().get());
          case SECOND:
            return ExprEval.of(dateTime.secondOfMinute().get());
          case MINUTE:
            return ExprEval.of(dateTime.minuteOfHour().get());
          case HOUR:
            return ExprEval.of(dateTime.hourOfDay().get());
          case DAY:
            return ExprEval.of(dateTime.dayOfMonth().get());
          case DOW:
            return ExprEval.of(dateTime.dayOfWeek().get());
          case ISODOW:
            return ExprEval.of(dateTime.dayOfWeek().get());
          case DOY:
            return ExprEval.of(dateTime.dayOfYear().get());
          case WEEK:
            return ExprEval.of(dateTime.weekOfWeekyear().get());
          case MONTH:
            return ExprEval.of(dateTime.monthOfYear().get());
          case QUARTER:
            return ExprEval.of((dateTime.monthOfYear().get() - 1) / 3 + 1);
          case YEAR:
            return ExprEval.of(dateTime.year().get());
          case ISOYEAR:
            return ExprEval.of(dateTime.year().get());
          case DECADE:
            // The year field divided by 10, See https://www.postgresql.org/docs/10/functions-datetime.html
            return ExprEval.of(dateTime.year().get() / 10);
          case CENTURY:
            return ExprEval.of(Math.ceil((double) dateTime.year().get() / 100));
          case MILLENNIUM:
            // Years in the 1900s are in the second millennium. The third millennium started January 1, 2001.
            // See https://www.postgresql.org/docs/10/functions-datetime.html
            return ExprEval.of(Math.ceil((double) dateTime.year().get() / 1000));
          default:
            throw new ISE("Unhandled unit[%s]", unit);
        }
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        Expr newArg = arg.visit(shuttle);
        return shuttle.visit(new TimestampExtractExpr(newArg));
      }

      @Nullable
      @Override
      public ExprType getOutputType(InputBindingInspector inspector)
      {
        switch (unit) {
          case CENTURY:
          case MILLENNIUM:
            return ExprType.DOUBLE;
          default:
            return ExprType.LONG;
        }
      }

      @Override
      public String stringify()
      {
        if (args.size() > 2) {
          return StringUtils.format(
              "%s(%s, %s, %s)",
              FN_NAME,
              arg.stringify(),
              args.get(1).stringify(),
              args.get(2).stringify()
          );
        }
        return StringUtils.format("%s(%s, %s)", FN_NAME, arg.stringify(), args.get(1).stringify());
      }
    }

    return new TimestampExtractExpr(arg);
  }
}
