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

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public class TimestampFormatExprMacro implements ExprMacroTable.ExprMacro
{
  private static final String FN_NAME = "timestamp_format";

  @Override
  public String name()
  {
    return FN_NAME;
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    validationHelperCheckArgumentRange(args, 1, 3);

    final Expr arg = args.get(0);
    final String formatString;
    final DateTimeZone timeZone;

    if (args.size() > 1) {
      validationHelperCheckArgIsLiteral(args.get(1), "format");
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
                                        ? ISODateTimeFormat.dateTime().withZone(timeZone)
                                        : DateTimeFormat.forPattern(formatString).withZone(timeZone);

    class TimestampFormatExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {
      private TimestampFormatExpr(List<Expr> args)
      {
        super(TimestampFormatExprMacro.this, args);
      }

      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        ExprEval eval = arg.eval(bindings);
        if (eval.isNumericNull()) {
          // Return null if the argument if null.
          return ExprEval.of(null);
        }
        return ExprEval.of(formatter.print(arg.eval(bindings).asLong()));
      }

      @Nullable
      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        return ExpressionType.STRING;
      }
    }

    return new TimestampFormatExpr(args);
  }
}
