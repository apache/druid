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
import org.apache.druid.query.filter.DruidPredicateMatch;
import org.apache.druid.query.filter.LikeDimFilter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
    validationHelperCheckAnyOfArgumentCount(args, 2, 3);

    final Expr arg = args.get(0);
    final Expr patternExpr = args.get(1);
    final Expr escapeExpr = args.size() > 2 ? args.get(2) : null;

    validationHelperCheckArgIsLiteral(patternExpr, "pattern");
    if (escapeExpr != null) {
      validationHelperCheckArgIsLiteral(escapeExpr, "escape");
    }

    final String escape = escapeExpr == null ? null : (String) escapeExpr.getLiteralValue();
    final Character escapeChar;

    if (escape != null && escape.length() != 1) {
      throw validationFailed("escape must be null or a single character");
    } else {
      escapeChar = escape == null ? null : escape.charAt(0);
    }

    final LikeDimFilter.LikeMatcher likeMatcher = LikeDimFilter.LikeMatcher.from(
        NullHandling.nullToEmptyIfNeeded((String) patternExpr.getLiteralValue()),
        escapeChar
    );

    class LikeExtractExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {
      private LikeExtractExpr(List<Expr> args)
      {
        super(LikeExprMacro.this, args);
      }

      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        final DruidPredicateMatch match = likeMatcher.matches(arg.eval(bindings).asString());
        if (match == DruidPredicateMatch.UNKNOWN) {
          return ExprEval.ofLong(null);
        }
        return ExprEval.ofLongBoolean(match.matches(false));
      }

      @Nullable
      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        return ExpressionType.LONG;
      }
    }
    return new LikeExtractExpr(args);
  }
}
