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

import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

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
    validationHelperCheckAnyOfArgumentCount(args, 2, 3);

    final Expr arg = args.get(0);
    final Expr patternExpr = args.get(1);
    final Expr indexExpr = args.size() > 2 ? args.get(2) : null;

    if (!ExprUtils.isStringLiteral(patternExpr)) {
      throw validationFailed("pattern must be a string literal");
    }

    if (indexExpr != null && (!indexExpr.isLiteral() || !(indexExpr.getLiteralValue() instanceof Number))) {
      throw validationFailed("index must be a numeric literal");
    }

    // Precompile the pattern.
    final Pattern pattern = compilePattern((String) patternExpr.getLiteralValue());

    final int index = indexExpr == null ? 0 : ((Number) indexExpr.getLiteralValue()).intValue();

    class RegexpExtractExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {
      private RegexpExtractExpr(List<Expr> args)
      {
        super(RegexpExtractExprMacro.this, args);
      }

      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        final String s = arg.eval(bindings).asString();

        if (s == null) {
          // True nulls do not match anything.
          return ExprEval.of(null);
        } else {
          final Matcher matcher = pattern.matcher(s);
          final String retVal = matcher.find() ? matcher.group(index) : null;
          return ExprEval.of(retVal);
        }
      }

      @Nullable
      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        return ExpressionType.STRING;
      }
    }
    return new RegexpExtractExpr(args);
  }

  /**
   * Compile the provided pattern, or provide a nice error message if it cannot be compiled.
   */
  private static Pattern compilePattern(@Nullable String pattern)
  {
    try {
      return Pattern.compile(StringUtils.nullToEmptyNonDruidDataString(pattern));
    }
    catch (PatternSyntaxException e) {
      throw InvalidInput.exception(
          e,
          StringUtils.format(
              "An invalid pattern [%s] was provided for the %s function, error: [%s]",
              e.getPattern(),
              FN_NAME,
              e.getMessage()
          )
      );
    }
  }
}
