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

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class UrlEncodeExprMacroTest extends MacroTestBase
{

  public UrlEncodeExprMacroTest()
  {
    super(new UrlEncodeExprMacro());
  }

  @Test
  public void testNull()
  {
    Assert.assertNull(eval(null));
  }

  @Test
  public void testEmpty()
  {
    Assert.assertNull(eval(""));
  }

  @Test
  public void testBlank()
  {
    Assert.assertEquals("+", eval(" "));
  }

  @Test
  public void testPercent()
  {
    Assert.assertEquals("%25", eval("%"));
    Assert.assertEquals("%25%25", eval("%%"));
  }

  @Test
  public void testUrls()
  {
    Assert.assertEquals("http%3A%2F%2Fdruid.apache.org", eval("http://druid.apache.org"));
    Assert.assertEquals("http%3A%2F%2Fdruid.apache.org%2Fa+b%3Fc%3D%25", eval("http://druid.apache.org/a b?c=%"));
    Assert.assertEquals("a+b", eval("a b"));
  }


  private Object eval(String url)
  {
    Expr expr = apply(Collections.singletonList(toExpr(url)));
    ExprEval eval = expr.eval(ExprUtils.nilBindings());
    return eval.value();
  }

  private Expr toExpr(String url)
  {
    return ExprEval.of(url).toExpr();
  }

}
