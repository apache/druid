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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

public class CachingExprParserTest extends InitializedNullHandlingTest
{
  private static final ExprMacroTable MACRO_TABLE = ExprMacroTable.nil();
  private static final PlannerConfig CACHE_DISABLING_CONFIG = new PlannerConfig();
  private static final PlannerConfig CACHE_ENABLING_CONFIG = new PlannerConfig().withOverrides(
      ImmutableMap.of(PlannerConfig.CTX_KEY_USE_PARSED_EXPR_CACHE, true)
  );

  @Test
  public void testParseNullReturnNull()
  {
    CachingExprParser parser = new CachingExprParser(MACRO_TABLE, CACHE_DISABLING_CONFIG);
    Assert.assertNull(parser.parse(null));
    Assert.assertTrue(parser.getExprCache().isEmpty());

    parser = new CachingExprParser(MACRO_TABLE, CACHE_ENABLING_CONFIG);
    Assert.assertNull(parser.parse(null));
    Assert.assertTrue(parser.getExprCache().isEmpty());
  }

  @Test
  public void testLazyParseNullReturnSupplierOfNull()
  {
    CachingExprParser parser = new CachingExprParser(MACRO_TABLE, CACHE_DISABLING_CONFIG);
    Supplier<Expr> exprSupplier = parser.lazyParse(null);
    Assert.assertNull(exprSupplier.get());
    Assert.assertTrue(parser.getExprCache().isEmpty());

    parser = new CachingExprParser(MACRO_TABLE, CACHE_ENABLING_CONFIG);
    exprSupplier = parser.lazyParse(null);
    Assert.assertNull(exprSupplier.get());
    Assert.assertTrue(parser.getExprCache().isEmpty());
  }

  @Test
  public void testParseWithCacheEnabled()
  {
    final CachingExprParser parser = new CachingExprParser(MACRO_TABLE, CACHE_ENABLING_CONFIG);
    final String strExpr = "x + y";
    final Expr expr = parser.parse(strExpr);
    Assert.assertNotNull(expr);
    Assert.assertSame(expr, parser.parse(strExpr));
    Assert.assertSame(expr, parser.getExprCache().get(strExpr));
  }

  @Test
  public void testParseWithCacheDisabled()
  {
    final CachingExprParser parser = new CachingExprParser(MACRO_TABLE, CACHE_DISABLING_CONFIG);
    final String strExpr = "x + y";
    final Expr expr = parser.parse(strExpr);
    Assert.assertNotNull(expr);
    Assert.assertNotSame(expr, parser.parse(strExpr));
    Assert.assertTrue(parser.getExprCache().isEmpty());
  }

  @Test
  public void testLazyParseWithCacheEnabled()
  {
    final CachingExprParser parser = new CachingExprParser(MACRO_TABLE, CACHE_ENABLING_CONFIG);
    final String strExpr = "x + y";
    final Supplier<Expr> exprSupplier = parser.lazyParse(strExpr);
    Assert.assertSame(exprSupplier.get(), parser.parse(strExpr));
    Assert.assertSame(exprSupplier.get(), parser.getExprCache().get(strExpr));
  }

  @Test
  public void testLazyParseWithCacheDisabled()
  {
    final CachingExprParser parser = new CachingExprParser(MACRO_TABLE, CACHE_DISABLING_CONFIG);
    final String strExpr = "x + y";
    final Supplier<Expr> exprSupplier = parser.lazyParse(strExpr);
    Assert.assertNotSame(exprSupplier.get(), parser.parse(strExpr));
    Assert.assertTrue(parser.getExprCache().isEmpty());
  }
}
