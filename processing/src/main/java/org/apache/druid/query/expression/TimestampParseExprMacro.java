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

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nonnull;
import java.util.List;

public class TimestampParseExprMacro implements ExprMacroTable.ExprMacro
{
  @Override
  public String name()
  {
    return "timestamp_parse";
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    if (args.size() < 1 || args.size() > 3) {
      throw new IAE("Function[%s] must have 1 to 3 arguments", name());
    }

    final Expr arg = args.get(0);
    final String formatString = args.size() > 1 ? (String) args.get(1).getLiteralValue() : null;
    final DateTimeZone timeZone;

    if (args.size() > 2 && args.get(2).getLiteralValue() != null) {
      timeZone = DateTimes.inferTzFromString((String) args.get(2).getLiteralValue());
    } else {
      timeZone = DateTimeZone.UTC;
    }

    final DateTimes.UtcFormatter formatter =
        formatString == null
        ? createDefaultParser(timeZone)
        : DateTimes.wrapFormatter(DateTimeFormat.forPattern(formatString).withZone(timeZone));

    class TimestampParseExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      private TimestampParseExpr(Expr arg)
      {
        super(arg);
      }

      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        final String value = arg.eval(bindings).asString();
        if (value == null) {
          return ExprEval.of(null);
        }

        try {
          return ExprEval.of(formatter.parse(value).getMillis());
        }
        catch (IllegalArgumentException e) {
          // Catch exceptions potentially thrown by formatter.parseDateTime. Our docs say that unparseable timestamps
          // are returned as nulls.
          return ExprEval.of(null);
        }
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        Expr newArg = arg.visit(shuttle);
        return shuttle.visit(new TimestampParseExpr(newArg));
      }
    }

    return new TimestampParseExpr(arg);
  }

  /**
   * Default formatter that parses according to the docs for this method: "If the pattern is not provided, this parses
   * time strings in either ISO8601 or SQL format."
   */
  private static DateTimes.UtcFormatter createDefaultParser(final DateTimeZone timeZone)
  {
    final DateTimeFormatter offsetElement = new DateTimeFormatterBuilder()
        .appendTimeZoneOffset("Z", true, 2, 4)
        .toFormatter();

    DateTimeParser timeOrOffset = new DateTimeFormatterBuilder()
        .append(
            null,
            new DateTimeParser[]{
                new DateTimeFormatterBuilder().appendLiteral('T').toParser(),
                new DateTimeFormatterBuilder().appendLiteral(' ').toParser()
            }
        )
        .appendOptional(ISODateTimeFormat.timeElementParser().getParser())
        .appendOptional(offsetElement.getParser())
        .toParser();

    return DateTimes.wrapFormatter(
        new DateTimeFormatterBuilder()
            .append(ISODateTimeFormat.dateElementParser())
            .appendOptional(timeOrOffset)
            .toFormatter()
            .withZone(timeZone)
    );
  }
}
