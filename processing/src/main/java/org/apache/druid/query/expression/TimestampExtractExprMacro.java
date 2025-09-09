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

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.vector.DoubleUnivariateLongFunctionVectorProcessor;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.LongUnivariateLongFunctionVectorProcessor;
import org.joda.time.Chronology;
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

  private long evalAsLong(final DateTime dateTime, final Unit unit)
  {
    switch (unit) {
      case EPOCH:
        return dateTime.getMillis() / 1000;
      case MILLISECOND:
        return dateTime.millisOfSecond().get();
      case SECOND:
        return dateTime.secondOfMinute().get();
      case MINUTE:
        return dateTime.minuteOfHour().get();
      case HOUR:
        return dateTime.hourOfDay().get();
      case DAY:
        return dateTime.dayOfMonth().get();
      case DOW:
      case ISODOW:
        return dateTime.dayOfWeek().get();
      case DOY:
        return dateTime.dayOfYear().get();
      case WEEK:
        return dateTime.weekOfWeekyear().get();
      case MONTH:
        return dateTime.monthOfYear().get();
      case QUARTER:
        return (dateTime.monthOfYear().get() - 1) / 3 + 1;
      case YEAR:
      case ISOYEAR:
        return dateTime.year().get();
      case DECADE:
        // The year field divided by 10, See https://www.postgresql.org/docs/10/functions-datetime.html
        return dateTime.year().get() / 10;
      default:
        throw TimestampExtractExprMacro.this.validationFailed("unhandled unit[%s]", unit);
    }
  }

  private double evalAsDouble(final DateTime dateTime, final Unit unit)
  {
    switch (unit) {
      case CENTURY:
        return Math.ceil((double) dateTime.year().get() / 100);
      case MILLENNIUM:
        // Years in the 1900s are in the second millennium. The third millennium started January 1, 2001.
        // See https://www.postgresql.org/docs/10/functions-datetime.html
        return Math.ceil((double) dateTime.year().get() / 1000);
      default:
        throw TimestampExtractExprMacro.this.validationFailed("unhandled unit[%s]", unit);
    }
  }

  private static ExpressionType getOutputExpressionType(final Unit unit)
  {
    switch (unit) {
      case CENTURY:
      case MILLENNIUM:
        return ExpressionType.DOUBLE;
      default:
        return ExpressionType.LONG;
    }
  }

  private static ISOChronology computeChronology(final List<Expr> args, final Expr.ObjectBinding bindings)
  {
    String timeZoneVal = (String) args.get(2).eval(bindings).value();
    return timeZoneVal != null
           ? ISOChronology.getInstance(DateTimes.inferTzFromString(timeZoneVal))
           : ISOChronology.getInstanceUTC();
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    validationHelperCheckArgumentRange(args, 2, 3);

    if (!args.get(1).isLiteral() || args.get(1).getLiteralValue() == null) {
      throw validationFailed("unit arg must be literal");
    }

    final Unit unit = Unit.valueOf(StringUtils.toUpperCase((String) args.get(1).getLiteralValue()));

    if (args.size() > 2) {
      if (args.get(2).isLiteral()) {
        DateTimeZone timeZone = ExprUtils.toTimeZone(args.get(2));
        ISOChronology chronology = ISOChronology.getInstance(timeZone);
        return new TimestampExtractExpr(args, unit, chronology);
      } else {
        return new TimestampExtractDynamicExpr(args, unit);
      }
    }
    return new TimestampExtractExpr(args, unit, ISOChronology.getInstanceUTC());
  }

  public class TimestampExtractExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
  {
    private final ISOChronology chronology;
    private final Unit unit;

    private TimestampExtractExpr(final List<Expr> args, final Unit unit, final ISOChronology chronology)
    {
      super(TimestampExtractExprMacro.this, args);
      this.unit = unit;
      this.chronology = chronology;
    }

    @Nonnull
    @Override
    public ExprEval eval(final ObjectBinding bindings)
    {
      final ExprEval<?> eval = args.get(0).eval(bindings);
      if (eval.value() == null) {
        // Return null if the argument if null.
        return ExprEval.of(null);
      }
      final DateTime dateTime = new DateTime(eval.value(), chronology);
      switch (getOutputExpressionType(unit).getType()) {
        case LONG:
          return ExprEval.of(evalAsLong(dateTime, unit));
        case DOUBLE:
          return ExprEval.of(evalAsDouble(dateTime, unit));
        default:
          throw DruidException.defensive("Unexpected type[%s]", getOutputExpressionType(unit).getType());
      }
    }

    @Nullable
    @Override
    public ExpressionType getOutputType(InputBindingInspector inspector)
    {
      return getOutputExpressionType(unit);
    }

    @Override
    public boolean canVectorize(InputBindingInspector inspector)
    {
      return args.get(0).canVectorize(inspector);
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(VectorInputBindingInspector inspector)
    {
      final ExprVectorProcessor<?> processor;

      if (getOutputExpressionType(unit).is(ExprType.DOUBLE)) {
        processor = new DoubleUnivariateLongFunctionVectorProcessor(
            args.get(0).asVectorProcessor(inspector),
            input -> evalAsDouble(new DateTime(input, chronology), unit)
        );
      } else {
        processor = new LongUnivariateLongFunctionVectorProcessor(
            args.get(0).asVectorProcessor(inspector),
            input -> evalAsLong(new DateTime(input, chronology), unit)
        );
      }

      return (ExprVectorProcessor<T>) processor;
    }
  }

  public class TimestampExtractDynamicExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
  {
    private final Unit unit;

    private TimestampExtractDynamicExpr(final List<Expr> args, final Unit unit)
    {
      super(TimestampExtractExprMacro.this, args);
      this.unit = unit;
    }

    @Nonnull
    @Override
    public ExprEval eval(final ObjectBinding bindings)
    {
      final ExprEval<?> eval = args.get(0).eval(bindings);
      if (eval.value() == null) {
        // Return null if the argument if null.
        return ExprEval.of(null);
      }
      final Chronology chronology = computeChronology(args, bindings);
      final DateTime dateTime = new DateTime(eval.value(), chronology);
      switch (getOutputExpressionType(unit).getType()) {
        case LONG:
          return ExprEval.of(evalAsLong(dateTime, unit));
        case DOUBLE:
          return ExprEval.of(evalAsDouble(dateTime, unit));
        default:
          throw DruidException.defensive("Unexpected type[%s]", getOutputExpressionType(unit).getType());
      }
    }

    @Nullable
    @Override
    public ExpressionType getOutputType(InputBindingInspector inspector)
    {
      return getOutputExpressionType(unit);
    }
  }
}
