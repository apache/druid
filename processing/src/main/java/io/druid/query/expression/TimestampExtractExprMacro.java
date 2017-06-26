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

package io.druid.query.expression;

import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.ExprMacroTable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import javax.annotation.Nonnull;
import java.util.List;

public class TimestampExtractExprMacro implements ExprMacroTable.ExprMacro
{
  public enum Unit
  {
    EPOCH,
    SECOND,
    MINUTE,
    HOUR,
    DAY,
    DOW,
    DOY,
    WEEK,
    MONTH,
    QUARTER,
    YEAR
  }

  @Override
  public String name()
  {
    return "timestamp_extract";
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
    final Unit unit = Unit.valueOf(((String) args.get(1).getLiteralValue()).toUpperCase());
    final DateTimeZone timeZone;

    if (args.size() > 2) {
      timeZone = ExprUtils.toTimeZone(args.get(2));
    } else {
      timeZone = DateTimeZone.UTC;
    }

    final ISOChronology chronology = ISOChronology.getInstance(timeZone);

    class TimestampExtractExpr implements Expr
    {
      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        final DateTime dateTime = new DateTime(arg.eval(bindings).asLong()).withChronology(chronology);
        switch (unit) {
          case EPOCH:
            return ExprEval.of(dateTime.getMillis());
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
          default:
            throw new ISE("Unhandled unit[%s]", unit);
        }
      }

      @Override
      public void visit(final Visitor visitor)
      {
        arg.visit(visitor);
        visitor.visit(this);
      }
    }

    return new TimestampExtractExpr();
  }
}
