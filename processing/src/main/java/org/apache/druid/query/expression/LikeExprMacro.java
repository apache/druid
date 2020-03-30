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
import org.apache.druid.query.filter.LikeDimFilter;

import javax.annotation.Nonnull;
import java.util.List;

public class LikeExprMacro implements ExprMacroTable.ExprMacro
{
  private static final String FN_NAME = "like";

  @Override
  public String name()
  {
    return FN_NAME;
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    if (args.size() < 2 || args.size() > 3) {
      throw new IAE("Function[%s] must have 2 or 3 arguments", name());
    }

    final Expr arg = args.get(0);
    final Expr patternExpr = args.get(1);
    final Expr escapeExpr = args.size() > 2 ? args.get(2) : null;

    if (!patternExpr.isLiteral() || (escapeExpr != null && !escapeExpr.isLiteral())) {
      throw new IAE("pattern and escape must be literals");
    }

    final String escape = escapeExpr == null ? null : (String) escapeExpr.getLiteralValue();
    final Character escapeChar;

    if (escape != null && escape.length() != 1) {
      throw new IllegalArgumentException("Escape must be null or a single character");
    } else {
      escapeChar = escape == null ? null : escape.charAt(0);
    }

    final LikeDimFilter.LikeMatcher likeMatcher = LikeDimFilter.LikeMatcher.from(
        NullHandling.nullToEmptyIfNeeded((String) patternExpr.getLiteralValue()),
        escapeChar
    );

    class LikeExtractExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      private LikeExtractExpr(Expr arg)
      {
        super(FN_NAME, arg);
      }

      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        return ExprEval.of(likeMatcher.matches(arg.eval(bindings).asString()), ExprType.LONG);
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        Expr newArg = arg.visit(shuttle);
        return shuttle.visit(new LikeExtractExpr(newArg));
      }

      @Override
      public String stringify()
      {
        if (escapeExpr != null) {
          return StringUtils.format(
              "%s(%s, %s, %s)",
              FN_NAME,
              arg.stringify(),
              patternExpr.stringify(),
              escapeExpr.stringify()
          );
        }
        return StringUtils.format("%s(%s, %s)", FN_NAME, arg.stringify(), patternExpr.stringify());
      }
    }
    return new LikeExtractExpr(arg);
  }
}

