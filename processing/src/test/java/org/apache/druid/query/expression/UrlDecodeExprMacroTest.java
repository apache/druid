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

public class UrlDecodeExprMacroTest extends MacroTestBase
{

  public UrlDecodeExprMacroTest()
  {
    super(new UrlDecodeExprMacro());
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
    Assert.assertEquals(" ", eval(" "));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPercent()
  {
    eval("%");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPercentAndOne()
  {
    eval("%2");
  }

  @Test
  public void testValid()
  {
    Assert.assertEquals("druid", eval("druid"));
    Assert.assertEquals("http://druid.apache.org", eval("http://druid.apache.org"));
    Assert.assertEquals("http://druid.apache.org/a/b/c", eval("http://druid.apache.org/a/b/c"));
    Assert.assertEquals("http://druid.apache.org/a/b/c c", eval("http://druid.apache.org/a/b/c%20c"));
    Assert.assertEquals("http://druid.apache.org/a/b/c", eval("http:%2F%2Fdruid.apache.org/a/b/c"));
    Assert.assertEquals("http://druid.apache.org/a/b/c", eval("http%3A%2F%2Fdruid.apache.org/a/b/c"));
    Assert.assertEquals("%", eval("%25"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalid()
  {
    Assert.assertEquals("http://druid.apache.org/", eval("http://druid.apache.org/%"));
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
