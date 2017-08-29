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

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class EvalTest
{
  private long evalLong(String x, Expr.ObjectBinding bindings)
  {
    ExprEval ret = eval(x, bindings);
    Assert.assertEquals(ExprType.LONG, ret.type());
    return ret.asLong();
  }

  private double evalDouble(String x, Expr.ObjectBinding bindings)
  {
    ExprEval ret = eval(x, bindings);
    Assert.assertEquals(ExprType.DOUBLE, ret.type());
    return ret.asDouble();
  }

  private ExprEval eval(String x, Expr.ObjectBinding bindings)
  {
    return Parser.parse(x, ExprMacroTable.nil()).eval(bindings);
  }

  @Test
  public void testDoubleEval()
  {
    Expr.ObjectBinding bindings = Parser.withMap(ImmutableMap.of("x", 2.0d));
    Assert.assertEquals(2.0, evalDouble("x", bindings), 0.0001);
    Assert.assertEquals(2.0, evalDouble("\"x\"", bindings), 0.0001);
    Assert.assertEquals(304.0, evalDouble("300 + \"x\" * 2", bindings), 0.0001);

    Assert.assertFalse(evalDouble("1.0 && 0.0", bindings) > 0.0);
    Assert.assertTrue(evalDouble("1.0 && 2.0", bindings) > 0.0);

    Assert.assertTrue(evalDouble("1.0 || 0.0", bindings) > 0.0);
    Assert.assertFalse(evalDouble("0.0 || 0.0", bindings) > 0.0);

    Assert.assertTrue(evalDouble("2.0 > 1.0", bindings) > 0.0);
    Assert.assertTrue(evalDouble("2.0 >= 2.0", bindings) > 0.0);
    Assert.assertTrue(evalDouble("1.0 < 2.0", bindings) > 0.0);
    Assert.assertTrue(evalDouble("2.0 <= 2.0", bindings) > 0.0);
    Assert.assertTrue(evalDouble("2.0 == 2.0", bindings) > 0.0);
    Assert.assertTrue(evalDouble("2.0 != 1.0", bindings) > 0.0);

    Assert.assertEquals(3.5, evalDouble("2.0 + 1.5", bindings), 0.0001);
    Assert.assertEquals(0.5, evalDouble("2.0 - 1.5", bindings), 0.0001);
    Assert.assertEquals(3.0, evalDouble("2.0 * 1.5", bindings), 0.0001);
    Assert.assertEquals(4.0, evalDouble("2.0 / 0.5", bindings), 0.0001);
    Assert.assertEquals(0.2, evalDouble("2.0 % 0.3", bindings), 0.0001);
    Assert.assertEquals(8.0, evalDouble("2.0 ^ 3.0", bindings), 0.0001);
    Assert.assertEquals(-1.5, evalDouble("-1.5", bindings), 0.0001);

    Assert.assertTrue(evalDouble("!-1.0", bindings) > 0.0);
    Assert.assertTrue(evalDouble("!0.0", bindings) > 0.0);
    Assert.assertFalse(evalDouble("!2.0", bindings) > 0.0);

    Assert.assertEquals(2.0, evalDouble("sqrt(4.0)", bindings), 0.0001);
    Assert.assertEquals(2.0, evalDouble("if(1.0, 2.0, 3.0)", bindings), 0.0001);
    Assert.assertEquals(3.0, evalDouble("if(0.0, 2.0, 3.0)", bindings), 0.0001);
  }

  @Test
  public void testLongEval()
  {
    Expr.ObjectBinding bindings = Parser.withMap(ImmutableMap.of("x", 9223372036854775807L));

    Assert.assertEquals(9223372036854775807L, evalLong("x", bindings));
    Assert.assertEquals(9223372036854775807L, evalLong("\"x\"", bindings));
    Assert.assertEquals(92233720368547759L, evalLong("\"x\" / 100 + 1", bindings));

    Assert.assertFalse(evalLong("9223372036854775807 && 0", bindings) > 0);
    Assert.assertTrue(evalLong("9223372036854775807 && 9223372036854775806", bindings) > 0);

    Assert.assertTrue(evalLong("9223372036854775807 || 0", bindings) > 0);
    Assert.assertFalse(evalLong("-9223372036854775807 || -9223372036854775807", bindings) > 0);
    Assert.assertTrue(evalLong("-9223372036854775807 || 9223372036854775807", bindings) > 0);
    Assert.assertFalse(evalLong("0 || 0", bindings) > 0);

    Assert.assertTrue(evalLong("9223372036854775807 > 9223372036854775806", bindings) > 0);
    Assert.assertTrue(evalLong("9223372036854775807 >= 9223372036854775807", bindings) > 0);
    Assert.assertTrue(evalLong("9223372036854775806 < 9223372036854775807", bindings) > 0);
    Assert.assertTrue(evalLong("9223372036854775807 <= 9223372036854775807", bindings) > 0);
    Assert.assertTrue(evalLong("9223372036854775807 == 9223372036854775807", bindings) > 0);
    Assert.assertTrue(evalLong("9223372036854775807 != 9223372036854775806", bindings) > 0);

    Assert.assertEquals(9223372036854775807L, evalLong("9223372036854775806 + 1", bindings));
    Assert.assertEquals(9223372036854775806L, evalLong("9223372036854775807 - 1", bindings));
    Assert.assertEquals(9223372036854775806L, evalLong("4611686018427387903 * 2", bindings));
    Assert.assertEquals(4611686018427387903L, evalLong("9223372036854775806 / 2", bindings));
    Assert.assertEquals(7L, evalLong("9223372036854775807 % 9223372036854775800", bindings));
    Assert.assertEquals(9223372030926249001L, evalLong("3037000499 ^ 2", bindings));
    Assert.assertEquals(-9223372036854775807L, evalLong("-9223372036854775807", bindings));

    Assert.assertTrue(evalLong("!-9223372036854775807", bindings) > 0);
    Assert.assertTrue(evalLong("!0", bindings) > 0);
    Assert.assertFalse(evalLong("!9223372036854775807", bindings) > 0);

    Assert.assertEquals(3037000499L, evalLong("cast(sqrt(9223372036854775807), 'long')", bindings));
    Assert.assertEquals(
        1L, evalLong("if(x == 9223372036854775807, 1, 0)", bindings)
    );
    Assert.assertEquals(
        0L, evalLong("if(x - 1 == 9223372036854775807, 1, 0)", bindings)
    );

    Assert.assertEquals(1271030400000L, evalLong("timestamp('2010-04-12')", bindings));
    Assert.assertEquals(1270998000000L, evalLong("timestamp('2010-04-12T+09:00')", bindings));
    Assert.assertEquals(1271055781000L, evalLong("timestamp('2010-04-12T07:03:01')", bindings));
    Assert.assertEquals(1271023381000L, evalLong("timestamp('2010-04-12T07:03:01+09:00')", bindings));
    Assert.assertEquals(1271023381419L, evalLong("timestamp('2010-04-12T07:03:01.419+09:00')", bindings));

    Assert.assertEquals(1271030400L, evalLong("unix_timestamp('2010-04-12')", bindings));
    Assert.assertEquals(1270998000L, evalLong("unix_timestamp('2010-04-12T+09:00')", bindings));
    Assert.assertEquals(1271055781L, evalLong("unix_timestamp('2010-04-12T07:03:01')", bindings));
    Assert.assertEquals(1271023381L, evalLong("unix_timestamp('2010-04-12T07:03:01+09:00')", bindings));
    Assert.assertEquals(1271023381L, evalLong("unix_timestamp('2010-04-12T07:03:01.419+09:00')", bindings));

    Assert.assertEquals("NULL", eval("nvl(if(x == 9223372036854775807, '', 'x'), 'NULL')", bindings).asString());
    Assert.assertEquals("x", eval("nvl(if(x == 9223372036854775806, '', 'x'), 'NULL')", bindings).asString());
  }

  @Test
  public void testBooleanReturn()
  {
    Expr.ObjectBinding bindings = Parser.withMap(
        ImmutableMap.of("x", 100L, "y", 100L, "z", 100D, "w", 100D)
    );
    ExprEval eval = Parser.parse("x==y", ExprMacroTable.nil()).eval(bindings);
    Assert.assertTrue(eval.asBoolean());
    Assert.assertEquals(ExprType.LONG, eval.type());

    eval = Parser.parse("x!=y", ExprMacroTable.nil()).eval(bindings);
    Assert.assertFalse(eval.asBoolean());
    Assert.assertEquals(ExprType.LONG, eval.type());

    eval = Parser.parse("x==z", ExprMacroTable.nil()).eval(bindings);
    Assert.assertTrue(eval.asBoolean());
    Assert.assertEquals(ExprType.DOUBLE, eval.type());

    eval = Parser.parse("x!=z", ExprMacroTable.nil()).eval(bindings);
    Assert.assertFalse(eval.asBoolean());
    Assert.assertEquals(ExprType.DOUBLE, eval.type());

    eval = Parser.parse("z==w", ExprMacroTable.nil()).eval(bindings);
    Assert.assertTrue(eval.asBoolean());
    Assert.assertEquals(ExprType.DOUBLE, eval.type());

    eval = Parser.parse("z!=w", ExprMacroTable.nil()).eval(bindings);
    Assert.assertFalse(eval.asBoolean());
    Assert.assertEquals(ExprType.DOUBLE, eval.type());
  }
}
