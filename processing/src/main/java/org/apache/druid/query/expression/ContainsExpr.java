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

import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * {@link Expr} class returned by {@link ContainsExprMacro} and {@link CaseInsensitiveContainsExprMacro} for
 * evaluating the expression.
 */
class ContainsExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
{
  private final Expr arg;
  private final Function<String, Boolean> searchFunction;

  ContainsExpr(
      final ExprMacroTable.ExprMacro macro,
      final Expr arg,
      final Expr searchStrExpr,
      final boolean caseSensitive
  )
  {
    this(macro, arg, searchStrExpr, createFunction(getSearchString(searchStrExpr, macro.name()), caseSensitive));
  }

  private ContainsExpr(
      final ExprMacroTable.ExprMacro macro,
      final Expr arg,
      final Expr searchStrExpr,
      final Function<String, Boolean> searchFunction
  )
  {
    super(macro, ImmutableList.of(arg, searchStrExpr));
    this.arg = arg;
    this.searchFunction = searchFunction;
    getSearchString(searchStrExpr, macro.name());
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

  @Override
  public ExpressionType getOutputType(InputBindingInspector inspector)
  {
    return ExpressionType.LONG;
  }

  private static String getSearchString(Expr searchExpr, String functioName)
  {
    if (!ExprUtils.isStringLiteral(searchExpr)) {
      throw new IAE("Function[%s] substring must be a string literal", functioName);
    }
    return StringUtils.nullToEmptyNonDruidDataString((String) searchExpr.getLiteralValue());
  }

  private static Function<String, Boolean> createFunction(String searchString, boolean caseSensitive)
  {
    if (caseSensitive) {
      return s -> s.contains(searchString);
    } else {
      return s -> org.apache.commons.lang.StringUtils.containsIgnoreCase(s, searchString);
    }
  }
}
