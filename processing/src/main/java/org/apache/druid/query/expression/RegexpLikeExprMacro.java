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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexpLikeExprMacro implements ExprMacroTable.ExprMacro
{
  private static final String FN_NAME = "regexp_like";

  @Override
  public String name()
  {
    return FN_NAME;
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    if (args.size() != 2) {
      throw new IAE("Function[%s] must have 2 arguments", name());
    }

    final Expr arg = args.get(0);
    final Expr patternExpr = args.get(1);

    if (!ExprUtils.isStringLiteral(patternExpr)) {
      throw new IAE("Function[%s] pattern must be a string literal", name());
    }

    // Precompile the pattern.
    final Pattern pattern = Pattern.compile(
        StringUtils.nullToEmptyNonDruidDataString((String) patternExpr.getLiteralValue())
    );

    class RegexpLikeExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      private RegexpLikeExpr(Expr arg)
      {
        super(FN_NAME, arg);
      }

      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        final String s = NullHandling.nullToEmptyIfNeeded(arg.eval(bindings).asString());

        if (s == null) {
          // True nulls do not match anything. Note: this branch only executes in SQL-compatible null handling mode.
          return ExprEval.of(false, ExprType.LONG);
        } else {
          final Matcher matcher = pattern.matcher(s);
          return ExprEval.of(matcher.find(), ExprType.LONG);
        }
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        Expr newArg = arg.visit(shuttle);
        return shuttle.visit(new RegexpLikeExpr(newArg));
      }

      @Override
      public String stringify()
      {
        return StringUtils.format("%s(%s, %s)", FN_NAME, arg.stringify(), patternExpr.stringify());
      }
    }
    return new RegexpLikeExpr(arg);
  }
}
