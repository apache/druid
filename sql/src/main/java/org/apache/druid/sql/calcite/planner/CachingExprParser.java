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

package org.apache.druid.sql.calcite.planner;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/**
 * An expression parser that caches expressions in memory.
 * This parser is created per SQL query and used by {@link DruidPlanner}.
 */
public class CachingExprParser
{
  private final ExprMacroTable macroTable;
  private final boolean enabled;
  private final Map<String, Expr> exprCache = new HashMap<>();

  public CachingExprParser(ExprMacroTable macroTable, PlannerConfig plannerConfig)
  {
    this.macroTable = macroTable;
    this.enabled = plannerConfig.isUseParsedExprCache();
  }

  /**
   * Parses the given expression and caches the result if caching is enabled.
   * Returns null only when the given expression is null.
   */
  @Nullable
  public Expr parse(@Nullable String expression)
  {
    if (expression == null) {
      return null;
    }
    if (enabled) {
      return exprCache.computeIfAbsent(expression, k -> Parser.parse(k, macroTable));
    } else {
      return Parser.parse(expression, macroTable);
    }
  }

  /**
   * Returns an {@link Expr} supplier that lazily parses the given expression.
   * The supplier can return null if the given expression is null.
   */
  public Supplier<Expr> lazyParse(@Nullable String expression)
  {
    return () -> parse(expression);
  }

  @VisibleForTesting
  public Map<String, Expr> getExprCache()
  {
    return exprCache;
  }
}
