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

import javax.annotation.Nonnull;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexpExtractExprMacro implements ExprMacroTable.ExprMacro
{
  private static final String FN_NAME = "regexp_extract";

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

    final Expr arg = args.get(0);
    final Expr patternExpr = args.get(1);
    final Expr indexExpr = args.size() > 2 ? args.get(2) : null;

    if (!ExprUtils.isStringLiteral(patternExpr)) {
      throw new IAE("Function[%s] pattern must be a string literal", name());
    }

    if (indexExpr != null && (!indexExpr.isLiteral() || !(indexExpr.getLiteralValue() instanceof Number))) {
      throw new IAE("Function[%s] index must be a numeric literal", name());
    }

    // Precompile the pattern.
    final Pattern pattern = Pattern.compile(
        StringUtils.nullToEmptyNonDruidDataString((String) patternExpr.getLiteralValue())
    );

    final int index = indexExpr == null ? 0 : ((Number) indexExpr.getLiteralValue()).intValue();

    class RegexpExtractExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      private RegexpExtractExpr(Expr arg)
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
          return ExprEval.of(null);
        } else {
          final Matcher matcher = pattern.matcher(NullHandling.nullToEmptyIfNeeded(s));
          final String retVal = matcher.find() ? matcher.group(index) : null;
          return ExprEval.of(retVal);
        }
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        Expr newArg = arg.visit(shuttle);
        return shuttle.visit(new RegexpExtractExpr(newArg));
      }

      @Override
      public String stringify()
      {
        if (indexExpr != null) {
          return StringUtils.format(
              "%s(%s, %s, %s)",
              FN_NAME,
              arg.stringify(),
              patternExpr.stringify(),
              indexExpr.stringify()
          );
        }
        return StringUtils.format("%s(%s, %s)", FN_NAME, arg.stringify(), patternExpr.stringify());
      }
    }
    return new RegexpExtractExpr(arg);
  }
}
