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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * {@link Expr} class returned by {@link ContainsExprMacro} and {@link CaseInsensitiveContainsExprMacro} for
 * evaluating the expression.
 */
class ContainsExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
{
  private final Function<String, Boolean> searchFunction;
  private final Expr searchStrExpr;

  ContainsExpr(String functioName, Expr arg, Expr searchStrExpr, boolean caseSensitive)
  {
    super(functioName, arg);
    this.searchStrExpr = searchStrExpr;
    // Creates the function eagerly to avoid branching in eval.
    this.searchFunction = createFunction(searchStrExpr, caseSensitive);
  }

  private ContainsExpr(String functioName, Expr arg, Expr searchStrExpr, Function<String, Boolean> searchFunction)
  {
    super(functioName, arg);
    this.searchFunction = searchFunction;
    this.searchStrExpr = searchStrExpr;
  }

  @Nonnull
  @Override
  public ExprEval eval(final Expr.ObjectBinding bindings)
  {
    final String s = NullHandling.nullToEmptyIfNeeded(arg.eval(bindings).asString());

    if (s == null) {
      // same behavior as regexp_like.
      return ExprEval.of(false, ExprType.LONG);
    } else {
      final boolean doesContain = searchFunction.apply(s);
      return ExprEval.of(doesContain, ExprType.LONG);
    }
  }

  @Override
  public Expr visit(Expr.Shuttle shuttle)
  {
    Expr newArg = arg.visit(shuttle);
    return shuttle.visit(new ContainsExpr(name, newArg, searchStrExpr, searchFunction));
  }

  @Override
  public String stringify()
  {
    return StringUtils.format("%s(%s, %s)", name, arg.stringify(), searchStrExpr.stringify());
  }

  private Function<String, Boolean> createFunction(Expr searchStrExpr, boolean caseSensitive)
  {
    String searchStr = (String) searchStrExpr.getLiteralValue();
    return caseSensitive ?
           (s -> s.contains(searchStr)) :
           (s -> org.apache.commons.lang3.StringUtils.containsIgnoreCase(s, searchStr));
  }
}
