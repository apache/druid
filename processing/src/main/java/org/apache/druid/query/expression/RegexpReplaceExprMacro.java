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
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexpReplaceExprMacro implements ExprMacroTable.ExprMacro
{
  private static final String FN_NAME = "regexp_replace";

  @Override
  public String name()
  {
    return FN_NAME;
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    validationHelperCheckArgumentCount(args, 3);

    if (args.stream().skip(1).allMatch(Expr::isLiteral)) {
      return new RegexpReplaceExpr(args);
    } else {
      return new RegexpReplaceDynamicExpr(args);
    }
  }

  abstract class BaseRegexpReplaceExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
  {
    public BaseRegexpReplaceExpr(final List<Expr> args)
    {
      super(RegexpReplaceExprMacro.this, args);
    }

    @Nullable
    @Override
    public ExpressionType getOutputType(InputBindingInspector inspector)
    {
      return ExpressionType.STRING;
    }
  }

  /**
   * Expr when pattern and replacement are literals.
   */
  class RegexpReplaceExpr extends BaseRegexpReplaceExpr
  {
    private final Expr arg;
    private final Pattern pattern;
    private final String replacement;

    private RegexpReplaceExpr(List<Expr> args)
    {
      super(args);

      final Expr patternExpr = args.get(1);
      final Expr replacementExpr = args.get(2);

      if (!ExprUtils.isStringLiteral(patternExpr)
          && !(patternExpr.isLiteral() && patternExpr.getLiteralValue() == null)) {
        throw validationFailed("pattern must be a string literal");
      }

      if (!ExprUtils.isStringLiteral(replacementExpr)
          && !(replacementExpr.isLiteral() && replacementExpr.getLiteralValue() == null)) {
        throw validationFailed("replacement must be a string literal");
      }

      final String patternString = NullHandling.nullToEmptyIfNeeded((String) patternExpr.getLiteralValue());

      this.arg = args.get(0);
      this.pattern = patternString != null ? Pattern.compile(patternString) : null;
      this.replacement = NullHandling.nullToEmptyIfNeeded((String) replacementExpr.getLiteralValue());
    }

    @Nonnull
    @Override
    public ExprEval<?> eval(final ObjectBinding bindings)
    {
      if (pattern == null || replacement == null) {
        return ExprEval.of(null);
      }

      final String s = NullHandling.nullToEmptyIfNeeded(arg.eval(bindings).asString());

      if (s == null) {
        return ExprEval.of(null);
      } else {
        final Matcher matcher = pattern.matcher(s);
        final String retVal = matcher.replaceAll(replacement);
        return ExprEval.of(retVal);
      }
    }
  }

  /**
   * Expr when pattern and replacement are dynamic (not literals).
   */
  class RegexpReplaceDynamicExpr extends BaseRegexpReplaceExpr
  {
    private RegexpReplaceDynamicExpr(List<Expr> args)
    {
      super(args);
    }

    @Nonnull
    @Override
    public ExprEval<?> eval(final ObjectBinding bindings)
    {
      final String s = NullHandling.nullToEmptyIfNeeded(args.get(0).eval(bindings).asString());
      final String pattern = NullHandling.nullToEmptyIfNeeded(args.get(1).eval(bindings).asString());
      final String replacement = NullHandling.nullToEmptyIfNeeded(args.get(2).eval(bindings).asString());

      if (s == null || pattern == null || replacement == null) {
        return ExprEval.of(null);
      } else {
        final Matcher matcher = Pattern.compile(pattern).matcher(s);
        final String retVal = matcher.replaceAll(replacement);
        return ExprEval.of(retVal);
      }
    }
  }
}
