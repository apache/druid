/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.math.expr;

import com.google.common.base.Supplier;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class EvalTest
{
  private Supplier<Number> constantSupplier(final Number number)
  {
    return new Supplier<Number>()
    {
      @Override
      public Number get()
      {
        return number;
      }
    };
  }

  @Test
  public void testDoubleEval()
  {
    Map<String, Supplier<Number>> bindings = new HashMap<>();
    bindings.put( "x", constantSupplier(2.0d));

    Assert.assertEquals(2.0, evaluate("x", bindings).doubleValue(), 0.0001);

    Assert.assertFalse(evaluate("1.0 && 0.0", bindings).doubleValue() > 0.0);
    Assert.assertTrue(evaluate("1.0 && 2.0", bindings).doubleValue() > 0.0);

    Assert.assertTrue(evaluate("1.0 || 0.0", bindings).doubleValue() > 0.0);
    Assert.assertFalse(evaluate("0.0 || 0.0", bindings).doubleValue() > 0.0);

    Assert.assertTrue(evaluate("2.0 > 1.0", bindings).doubleValue() > 0.0);
    Assert.assertTrue(evaluate("2.0 >= 2.0", bindings).doubleValue() > 0.0);
    Assert.assertTrue(evaluate("1.0 < 2.0", bindings).doubleValue() > 0.0);
    Assert.assertTrue(evaluate("2.0 <= 2.0", bindings).doubleValue() > 0.0);
    Assert.assertTrue(evaluate("2.0 == 2.0", bindings).doubleValue() > 0.0);
    Assert.assertTrue(evaluate("2.0 != 1.0", bindings).doubleValue() > 0.0);

    Assert.assertEquals(3.5, evaluate("2.0 + 1.5", bindings).doubleValue(), 0.0001);
    Assert.assertEquals(0.5, evaluate("2.0 - 1.5", bindings).doubleValue(), 0.0001);
    Assert.assertEquals(3.0, evaluate("2.0 * 1.5", bindings).doubleValue(), 0.0001);
    Assert.assertEquals(4.0, evaluate("2.0 / 0.5", bindings).doubleValue(), 0.0001);
    Assert.assertEquals(0.2, evaluate("2.0 % 0.3", bindings).doubleValue(), 0.0001);
    Assert.assertEquals(8.0, evaluate("2.0 ^ 3.0", bindings).doubleValue(), 0.0001);
    Assert.assertEquals(-1.5, evaluate("-1.5", bindings).doubleValue(), 0.0001);

    Assert.assertTrue(evaluate("!-1.0", bindings).doubleValue() > 0.0);
    Assert.assertTrue(evaluate("!0.0", bindings).doubleValue() > 0.0);
    Assert.assertFalse(evaluate("!2.0", bindings).doubleValue() > 0.0);

    Assert.assertEquals(2.0, evaluate("sqrt(4.0)", bindings).doubleValue(), 0.0001);
    Assert.assertEquals(2.0, evaluate("if(1.0, 2.0, 3.0)", bindings).doubleValue(), 0.0001);
    Assert.assertEquals(3.0, evaluate("if(0.0, 2.0, 3.0)", bindings).doubleValue(), 0.0001);
  }

  private Number evaluate(String in, Map<String, Supplier<Number>> bindings) {
    return Parser.parse(in).eval(Parser.withSuppliers(bindings));
  }

  @Test
  public void testLongEval()
  {
    Map<String, Supplier<Number>> bindings = new HashMap<>();
    bindings.put("x", constantSupplier(9223372036854775807L));

    Assert.assertEquals(9223372036854775807L, evaluate("x", bindings).longValue());

    Assert.assertFalse(evaluate("9223372036854775807 && 0", bindings).longValue() > 0);
    Assert.assertTrue(evaluate("9223372036854775807 && 9223372036854775806", bindings).longValue() > 0);

    Assert.assertTrue(evaluate("9223372036854775807 || 0", bindings).longValue() > 0);
    Assert.assertFalse(evaluate("-9223372036854775807 || -9223372036854775807", bindings).longValue() > 0);
    Assert.assertTrue(evaluate("-9223372036854775807 || 9223372036854775807", bindings).longValue() > 0);
    Assert.assertFalse(evaluate("0 || 0", bindings).longValue() > 0);

    Assert.assertTrue(evaluate("9223372036854775807 > 9223372036854775806", bindings).longValue() > 0);
    Assert.assertTrue(evaluate("9223372036854775807 >= 9223372036854775807", bindings).longValue() > 0);
    Assert.assertTrue(evaluate("9223372036854775806 < 9223372036854775807", bindings).longValue() > 0);
    Assert.assertTrue(evaluate("9223372036854775807 <= 9223372036854775807", bindings).longValue() > 0);
    Assert.assertTrue(evaluate("9223372036854775807 == 9223372036854775807", bindings).longValue() > 0);
    Assert.assertTrue(evaluate("9223372036854775807 != 9223372036854775806", bindings).longValue() > 0);

    Assert.assertEquals(9223372036854775807L, evaluate("9223372036854775806 + 1", bindings).longValue());
    Assert.assertEquals(9223372036854775806L, evaluate("9223372036854775807 - 1", bindings).longValue());
    Assert.assertEquals(9223372036854775806L, evaluate("4611686018427387903 * 2", bindings).longValue());
    Assert.assertEquals(4611686018427387903L, evaluate("9223372036854775806 / 2", bindings).longValue());
    Assert.assertEquals(7L, evaluate("9223372036854775807 % 9223372036854775800", bindings).longValue());
    Assert.assertEquals( 9223372030926249001L, evaluate("3037000499 ^ 2", bindings).longValue());
    Assert.assertEquals(-9223372036854775807L, evaluate("-9223372036854775807", bindings).longValue());

    Assert.assertTrue(evaluate("!-9223372036854775807", bindings).longValue() > 0);
    Assert.assertTrue(evaluate("!0", bindings).longValue() > 0);
    Assert.assertFalse(evaluate("!9223372036854775807", bindings).longValue() > 0);

    Assert.assertEquals(3037000499L, evaluate("sqrt(9223372036854775807)", bindings).longValue());
    Assert.assertEquals(9223372036854775807L, evaluate(
        "if(9223372036854775807, 9223372036854775807, 9223372036854775806)",
        bindings
    ).longValue());
    Assert.assertEquals(9223372036854775806L, evaluate(
        "if(0, 9223372036854775807, 9223372036854775806)",
        bindings
    ).longValue());
  }
}
