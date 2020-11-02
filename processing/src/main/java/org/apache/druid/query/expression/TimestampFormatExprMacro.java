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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
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
                                        ? ISODateTimeFormat.dateTime().withZone(timeZone)
                                        : DateTimeFormat.forPattern(formatString).withZone(timeZone);

    class TimestampFormatExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      private TimestampFormatExpr(Expr arg)
      {
        super(FN_NAME, arg);
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

      @Override
      public Expr visit(Shuttle shuttle)
      {
        Expr newArg = arg.visit(shuttle);
        return shuttle.visit(new TimestampFormatExpr(newArg));
      }

      @Nullable
      @Override
      public ExprType getOutputType(InputBindingInspector inspector)
      {
        return ExprType.STRING;
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
        if (args.size() > 1) {
          return StringUtils.format("%s(%s, %s)", FN_NAME, arg.stringify(), args.get(1).stringify());
        }
        return super.stringify();
      }
    }

    return new TimestampFormatExpr(arg);
  }
}
