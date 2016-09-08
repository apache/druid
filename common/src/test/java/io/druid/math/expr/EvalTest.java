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

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class EvalTest
{
  @Test
  public void testDoubleEval()
  {
    Map<String, Number> bindings = new HashMap<>();
    bindings.put("x", 2.0d);

    Assert.assertEquals(2.0, Parser.parse("x").eval(bindings).doubleValue(), 0.0001);

    Assert.assertFalse(Parser.parse("1.0 && 0.0").eval(bindings).doubleValue() > 0.0);
    Assert.assertTrue(Parser.parse("1.0 && 2.0").eval(bindings).doubleValue() > 0.0);

    Assert.assertTrue(Parser.parse("1.0 || 0.0").eval(bindings).doubleValue() > 0.0);
    Assert.assertFalse(Parser.parse("0.0 || 0.0").eval(bindings).doubleValue() > 0.0);

    Assert.assertTrue(Parser.parse("2.0 > 1.0").eval(bindings).doubleValue() > 0.0);
    Assert.assertTrue(Parser.parse("2.0 >= 2.0").eval(bindings).doubleValue() > 0.0);
    Assert.assertTrue(Parser.parse("1.0 < 2.0").eval(bindings).doubleValue() > 0.0);
    Assert.assertTrue(Parser.parse("2.0 <= 2.0").eval(bindings).doubleValue() > 0.0);
    Assert.assertTrue(Parser.parse("2.0 == 2.0").eval(bindings).doubleValue() > 0.0);
    Assert.assertTrue(Parser.parse("2.0 != 1.0").eval(bindings).doubleValue() > 0.0);

    Assert.assertEquals(3.5, Parser.parse("2.0 + 1.5").eval(bindings).doubleValue(), 0.0001);
    Assert.assertEquals(0.5, Parser.parse("2.0 - 1.5").eval(bindings).doubleValue(), 0.0001);
    Assert.assertEquals(3.0, Parser.parse("2.0 * 1.5").eval(bindings).doubleValue(), 0.0001);
    Assert.assertEquals(4.0, Parser.parse("2.0 / 0.5").eval(bindings).doubleValue(), 0.0001);
    Assert.assertEquals(0.2, Parser.parse("2.0 % 0.3").eval(bindings).doubleValue(), 0.0001);
    Assert.assertEquals(8.0, Parser.parse("2.0 ^ 3.0").eval(bindings).doubleValue(), 0.0001);
    Assert.assertEquals(-1.5, Parser.parse("-1.5").eval(bindings).doubleValue(), 0.0001);

    Assert.assertTrue(Parser.parse("!-1.0").eval(bindings).doubleValue() > 0.0);
    Assert.assertTrue(Parser.parse("!0.0").eval(bindings).doubleValue() > 0.0);
    Assert.assertFalse(Parser.parse("!2.0").eval(bindings).doubleValue() > 0.0);

    Assert.assertEquals(2.0, Parser.parse("sqrt(4.0)").eval(bindings).doubleValue(), 0.0001);
    Assert.assertEquals(2.0, Parser.parse("if(1.0, 2.0, 3.0)").eval(bindings).doubleValue(), 0.0001);
    Assert.assertEquals(3.0, Parser.parse("if(0.0, 2.0, 3.0)").eval(bindings).doubleValue(), 0.0001);
  }

  @Test
  public void testLongEval()
  {
    Map<String, Number> bindings = new HashMap<>();
    bindings.put("x", 9223372036854775807L);

    Assert.assertEquals(9223372036854775807L, Parser.parse("x").eval(bindings).longValue());

    Assert.assertFalse(Parser.parse("9223372036854775807 && 0").eval(bindings).longValue() > 0);
    Assert.assertTrue(Parser.parse("9223372036854775807 && 9223372036854775806").eval(bindings).longValue() > 0);

    Assert.assertTrue(Parser.parse("9223372036854775807 || 0").eval(bindings).longValue() > 0);
    Assert.assertFalse(Parser.parse("-9223372036854775807 || -9223372036854775807").eval(bindings).longValue() > 0);
    Assert.assertTrue(Parser.parse("-9223372036854775807 || 9223372036854775807").eval(bindings).longValue() > 0);
    Assert.assertFalse(Parser.parse("0 || 0").eval(bindings).longValue() > 0);

    Assert.assertTrue(Parser.parse("9223372036854775807 > 9223372036854775806").eval(bindings).longValue() > 0);
    Assert.assertTrue(Parser.parse("9223372036854775807 >= 9223372036854775807").eval(bindings).longValue() > 0);
    Assert.assertTrue(Parser.parse("9223372036854775806 < 9223372036854775807").eval(bindings).longValue() > 0);
    Assert.assertTrue(Parser.parse("9223372036854775807 <= 9223372036854775807").eval(bindings).longValue() > 0);
    Assert.assertTrue(Parser.parse("9223372036854775807 == 9223372036854775807").eval(bindings).longValue() > 0);
    Assert.assertTrue(Parser.parse("9223372036854775807 != 9223372036854775806").eval(bindings).longValue() > 0);

    Assert.assertEquals(9223372036854775807L, Parser.parse("9223372036854775806 + 1").eval(bindings).longValue());
    Assert.assertEquals(9223372036854775806L, Parser.parse("9223372036854775807 - 1").eval(bindings).longValue());
    Assert.assertEquals(9223372036854775806L, Parser.parse("4611686018427387903 * 2").eval(bindings).longValue());
    Assert.assertEquals(4611686018427387903L, Parser.parse("9223372036854775806 / 2").eval(bindings).longValue());
    Assert.assertEquals(7L, Parser.parse("9223372036854775807 % 9223372036854775800").eval(bindings).longValue());
    Assert.assertEquals( 9223372030926249001L, Parser.parse("3037000499 ^ 2").eval(bindings).longValue());
    Assert.assertEquals(-9223372036854775807L, Parser.parse("-9223372036854775807").eval(bindings).longValue());

    Assert.assertTrue(Parser.parse("!-9223372036854775807").eval(bindings).longValue() > 0);
    Assert.assertTrue(Parser.parse("!0").eval(bindings).longValue() > 0);
    Assert.assertFalse(Parser.parse("!9223372036854775807").eval(bindings).longValue() > 0);

    Assert.assertEquals(3037000499L, Parser.parse("sqrt(9223372036854775807)").eval(bindings).longValue());
    Assert.assertEquals(9223372036854775807L, Parser.parse("if(9223372036854775807, 9223372036854775807, 9223372036854775806)").eval(bindings).longValue());
    Assert.assertEquals(9223372036854775806L, Parser.parse("if(0, 9223372036854775807, 9223372036854775806)").eval(bindings).longValue());
  }
}
