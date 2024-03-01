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

import com.google.common.collect.ImmutableList;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.List;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class ConstantExprTest extends InitializedNullHandlingTest
{

  private Expr expr;

  @Parameters
  public static List<Object[]> getParamteres()
  {

    return ImmutableList.of(
        new Object[] {new LongExpr(13L)},
        new Object[] {new DoubleExpr(1.3)}
    );

  }

  public ConstantExprTest(Expr expr)
  {
    this.expr = expr;
  }

  @Test
  public void testSingleThreaded()
  {
    assumeTrue(expr instanceof Expr.SingleThreaded);
    assertNotSame(expr.eval(null), expr.eval(null));
    Expr s = Expr.SingleThreaded.make(expr);
    assertSame(s.eval(null), s.eval(null));

  }
}
