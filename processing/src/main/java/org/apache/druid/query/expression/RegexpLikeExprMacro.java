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
    validationHelperCheckArgumentCount(args, 2);
    if (ExprUtils.isStringLiteral(args.get(1))) {
      return new RegexpLikeExpr(args);
    } else {
      return new RegexpLikeDynamicExpr(args);
    }
  }

  abstract class BaseRegexpLikeExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
  {
    public BaseRegexpLikeExpr(final List<Expr> args)
    {
      super(RegexpLikeExprMacro.this, args);
    }

    @Nullable
    @Override
    public ExpressionType getOutputType(InputBindingInspector inspector)
    {
      return ExpressionType.LONG;
    }
  }

  /**
   * Expr when pattern is a literal.
   */
  class RegexpLikeExpr extends BaseRegexpLikeExpr
  {
    private final Expr arg;
    private final Pattern pattern;

    private RegexpLikeExpr(List<Expr> args)
    {
      super(args);

      final Expr patternExpr = args.get(1);
      if (!ExprUtils.isStringLiteral(patternExpr)) {
        throw validationFailed("pattern must be a STRING literal");
      }

      final String patternString = (String) patternExpr.getLiteralValue();
      this.arg = args.get(0);
      this.pattern = Pattern.compile(StringUtils.nullToEmptyNonDruidDataString(patternString));
    }

    @Nonnull
    @Override
    public ExprEval<?> eval(final ObjectBinding bindings)
    {
      final String s = arg.eval(bindings).asString();
      if (s == null) {
        return ExprEval.ofLong(null);
      } else {
        final Matcher matcher = pattern.matcher(s);
        return ExprEval.ofLongBoolean(matcher.find());
      }
    }
  }

  /**
   * Expr when pattern is dynamic (not literal).
   */
  class RegexpLikeDynamicExpr extends BaseRegexpLikeExpr
  {
    private RegexpLikeDynamicExpr(List<Expr> args)
    {
      super(args);
    }

    @Nonnull
    @Override
    public ExprEval<?> eval(final ObjectBinding bindings)
    {
      final Expr patternExpr = args.get(1).eval(bindings).toExpr();
      if (!ExprUtils.isStringLiteral(patternExpr)) {
        throw validationFailed("pattern must be a STRING literal");
      }

      final String s = args.get(0).eval(bindings).asString();
      if (s == null) {
        return ExprEval.ofLong(null);
      }

      final String patternString = (String) patternExpr.getLiteralValue();
      final Pattern pattern = Pattern.compile(StringUtils.nullToEmptyNonDruidDataString(patternString));
      final Matcher matcher = pattern.matcher(s);
      return ExprEval.ofLongBoolean(matcher.find());
    }
  }
}
