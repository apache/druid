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
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.ExprMacroTable;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
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
      timeZone = DateTimeZone.forID((String) args.get(2).getLiteralValue());
    } else {
      timeZone = DateTimeZone.UTC;
    }

    final DateTimeFormatter formatter = formatString == null
                                        ? ISODateTimeFormat.dateTimeParser()
                                        : DateTimeFormat.forPattern(formatString).withZone(timeZone);

    class TimestampParseExpr implements Expr
    {
      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        try {
          return ExprEval.of(formatter.parseDateTime(arg.eval(bindings).asString()).getMillis());
        }
        catch (IllegalArgumentException e) {
          // Catch exceptions potentially thrown by formatter.parseDateTime. Our docs say that unparseable timestamps
          // are returned as nulls.
          return ExprEval.of(null);
        }
      }

      @Override
      public void visit(final Visitor visitor)
      {
        arg.visit(visitor);
        visitor.visit(this);
      }
    }

    return new TimestampParseExpr();
  }
}
