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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;

/**
 * Member of {@link PlannerToolbox} that caches the result of {@link Parser#parse}.
 */
public class ExpressionParserImpl implements ExpressionParser
{
  /**
   * Maximum total expression length stored in the {@link #cache}, measured in characters of unparsed expressions.
   */
  private static final int MAX_EXPRESSION_WEIGHT = 1_000_000;

  private final Cache<String, Expr> cache;
  private final ExprMacroTable macroTable;

  public ExpressionParserImpl(final ExprMacroTable macroTable)
  {
    this.cache = Caffeine.newBuilder()
                         .maximumWeight(MAX_EXPRESSION_WEIGHT)
                         .weigher((Weigher<String, Expr>) (key, value) -> key.length())
                         .build();
    this.macroTable = macroTable;
  }

  @Override
  public Expr parse(final String expression)
  {
    return cache.get(expression, k -> Parser.parse(expression, macroTable));
  }
}
