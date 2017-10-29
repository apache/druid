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

import com.google.common.base.Preconditions;
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

public class TimestampFormatExprMacro implements ExprMacroTable.ExprMacro
{
  @Override
  public String name()
  {
    return "timestamp_format";
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    if (args.size() < 1 || args.size() > 3) {
      throw new IAE("Function[%s] must have 1 to 3 arguments", name());
    }

    final Expr arg = args.get(0);
    final String formatString;
    final DateTimeZone timeZone;

    if (args.size() > 1) {
      Preconditions.checkArgument(args.get(1).isLiteral(), "Function[%s] format arg must be a literal", name());
      formatString = (String) args.get(1).getLiteralValue();
    } else {
      formatString = null;
    }

    if (args.size() > 2) {
      timeZone = ExprUtils.toTimeZone(args.get(2));
    } else {
      timeZone = DateTimeZone.UTC;
    }

    final DateTimeFormatter formatter = formatString == null
                                        ? ISODateTimeFormat.dateTime()
                                        : DateTimeFormat.forPattern(formatString).withZone(timeZone);

    class TimestampFormatExpr implements Expr
    {
      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        return ExprEval.of(formatter.print(arg.eval(bindings).asLong()));
      }

      @Override
      public void visit(final Visitor visitor)
      {
        arg.visit(visitor);
        visitor.visit(this);
      }
    }

    return new TimestampFormatExpr();
  }
}
