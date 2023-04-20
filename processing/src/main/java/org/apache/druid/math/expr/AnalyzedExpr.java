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

package org.apache.druid.math.expr;

import com.google.common.base.Suppliers;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Wrapper around {@link Expr} and {@link Expr.BindingAnalysis}. Often cached to improve performance.
 *
 * Create using {@link Parser#parseAndAnalyze(String, ExprMacroTable)} or {@link Parser#lazyParseAndAnalyze(String, ExprMacroTable)}.
 */
public class AnalyzedExpr
{
  private final Expr expr;
  private final Supplier<Expr.BindingAnalysis> analysis;

  private AnalyzedExpr(Expr expr, Supplier<Expr.BindingAnalysis> analysis)
  {
    this.expr = expr;
    this.analysis = analysis;
  }

  public static AnalyzedExpr wrap(final Expr expr)
  {
    return new AnalyzedExpr(expr, Suppliers.memoize(expr::analyzeInputs)::get);
  }

  /**
   * Returns the parsed expression.
   *
   * Do not call {@link Expr#analyzeInputs()} on the parsed expression; instead, use {@link #analyzeInputs()} on
   * this class, in order to leverage caching.
   */
  public Expr expr()
  {
    return expr;
  }

  /**
   * Returns a memoized result of {@link Expr#analyzeInputs()}.
   */
  public Expr.BindingAnalysis analyzeInputs()
  {
    return analysis.get();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AnalyzedExpr that = (AnalyzedExpr) o;
    return Objects.equals(expr, that.expr) && Objects.equals(analysis, that.analysis);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(expr, analysis);
  }

  @Override
  public String toString()
  {
    return expr.toString();
  }
}
