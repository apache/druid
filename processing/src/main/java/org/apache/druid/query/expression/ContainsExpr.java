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
import org.apache.druid.math.expr.ExpressionType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Function;

/**
 * {@link Expr} class returned by {@link ContainsExprMacro} and {@link CaseInsensitiveContainsExprMacro} for
 * evaluating the expression.
 */
class ContainsExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
{
  private final Function<String, Boolean> searchFunction;
  private final Expr searchStrExpr;
  private final Function<Shuttle, Expr> visitFunction;

  ContainsExpr(
      final String functionName,
      final Expr arg,
      final Expr searchStrExpr,
      final boolean caseSensitive,
      final Function<Shuttle, Expr> visitFunction
  )
  {
    this(functionName, arg, searchStrExpr, createFunction(searchStrExpr, caseSensitive), visitFunction);
  }

  private ContainsExpr(
      final String functionName,
      final Expr arg,
      final Expr searchStrExpr,
      final Function<String, Boolean> searchFunction,
      final Function<Shuttle, Expr> visitFunction
  )
  {
    super(functionName, arg);
    this.searchFunction = searchFunction;
    this.searchStrExpr = validateSearchExpr(searchStrExpr, functionName);
    this.visitFunction = visitFunction;
  }

  @Nonnull
  @Override
  public ExprEval eval(final Expr.ObjectBinding bindings)
  {
    final String s = NullHandling.nullToEmptyIfNeeded(arg.eval(bindings).asString());

    if (s == null) {
      // same behavior as regexp_like.
      return ExprEval.ofLongBoolean(false);
    } else {
      final boolean doesContain = searchFunction.apply(s);
      return ExprEval.ofLongBoolean(doesContain);
    }
  }

  @Nullable
  @Override
  public ExpressionType getOutputType(InputBindingInspector inspector)
  {
    return ExpressionType.LONG;
  }

  @Override
  public Expr visit(Expr.Shuttle shuttle)
  {
    return visitFunction.apply(shuttle);
  }

  @Override
  public String stringify()
  {
    return StringUtils.format("%s(%s, %s)", name, arg.stringify(), searchStrExpr.stringify());
  }

  private Expr validateSearchExpr(Expr searchExpr, String functioName)
  {
    if (!ExprUtils.isStringLiteral(searchExpr)) {
      throw new IAE("Function[%s] substring must be a string literal", functioName);
    }
    return searchExpr;
  }

  private static Function<String, Boolean> createFunction(Expr searchStrExpr, boolean caseSensitive)
  {
    String searchStr = StringUtils.nullToEmptyNonDruidDataString((String) searchStrExpr.getLiteralValue());
    if (caseSensitive) {
      return s -> s.contains(searchStr);
    }
    return s -> org.apache.commons.lang.StringUtils.containsIgnoreCase(s, searchStr);
  }
}
