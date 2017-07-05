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

import com.google.common.base.Strings;
import io.druid.java.util.common.IAE;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.ExprMacroTable;
import io.druid.math.expr.ExprType;
import io.druid.query.filter.LikeDimFilter;

import javax.annotation.Nonnull;
import java.util.List;

public class LikeExprMacro implements ExprMacroTable.ExprMacro
{
  @Override
  public String name()
  {
    return "like";
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
        Strings.nullToEmpty((String) patternExpr.getLiteralValue()),
        escapeChar
    );

    class LikeExtractExpr implements Expr
    {
      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        return ExprEval.of(likeMatcher.matches(arg.eval(bindings).asString()), ExprType.LONG);
      }

      @Override
      public void visit(final Visitor visitor)
      {
        arg.visit(visitor);
        visitor.visit(this);
      }
    }
    return new LikeExtractExpr();
  }
}

