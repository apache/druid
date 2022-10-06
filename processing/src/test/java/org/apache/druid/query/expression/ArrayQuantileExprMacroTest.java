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

import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.doubles.DoubleLists;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

public class ArrayQuantileExprMacroTest extends InitializedNullHandlingTest
{
  @Test
  public void test_apply_longArray()
  {
    final Expr result = new ArrayQuantileExprMacro().apply(
        ImmutableList.of(
            ExprEval.ofLongArray(new Object[]{1L, 3L, 2L}).toExpr(),
            ExprEval.ofDouble(0.5).toExpr()
        )
    );

    Assert.assertEquals(
        2.0,
        result.eval(InputBindings.nilBindings()).asDouble(),
        0.0
    );
  }

  @Test
  public void test_apply_longArrayWithNulls()
  {
    final Expr result = new ArrayQuantileExprMacro().apply(
        ImmutableList.of(
            ExprEval.ofLongArray(new Object[]{1L, 3L, null, null, null, 2L}).toExpr(),
            ExprEval.ofDouble(0.5).toExpr()
        )
    );

    Assert.assertEquals(
        2.0,
        result.eval(InputBindings.nilBindings()).asDouble(),
        0.0
    );
  }

  @Test
  public void test_apply_doubleArray()
  {
    final Expr result = new ArrayQuantileExprMacro().apply(
        ImmutableList.of(
            ExprEval.ofDoubleArray(new Object[]{1.0, 3.0, 2.0}).toExpr(),
            ExprEval.ofDouble(0.5).toExpr()
        )
    );

    Assert.assertEquals(
        2.0,
        result.eval(InputBindings.nilBindings()).asDouble(),
        0.0
    );
  }

  @Test
  public void test_apply_doubleArrayWithNulls()
  {
    final Expr result = new ArrayQuantileExprMacro().apply(
        ImmutableList.of(
            ExprEval.ofDoubleArray(new Object[]{1.0, null, null, null, 3.0, 2.0}).toExpr(),
            ExprEval.ofDouble(0.5).toExpr()
        )
    );

    Assert.assertEquals(
        2.0,
        result.eval(InputBindings.nilBindings()).asDouble(),
        0.0
    );
  }

  @Test
  public void test_apply_stringArray()
  {
    final Expr result = new ArrayQuantileExprMacro().apply(
        ImmutableList.of(
            ExprEval.ofStringArray(new Object[]{"1.0", "3.0", "2.0"}).toExpr(),
            ExprEval.ofDouble(0.5).toExpr()
        )
    );

    if (NullHandling.sqlCompatible()) {
      Assert.assertTrue(result.eval(InputBindings.nilBindings()).isNumericNull());
    } else {
      Assert.assertFalse(result.eval(InputBindings.nilBindings()).isNumericNull());
      Assert.assertEquals(0, result.eval(InputBindings.nilBindings()).asDouble(), 0);
    }
  }

  @Test
  public void test_apply_null()
  {
    final Expr result = new ArrayQuantileExprMacro().apply(
        ImmutableList.of(
            ExprEval.ofLongArray(null).toExpr(),
            ExprEval.ofDouble(0.5).toExpr()
        )
    );

    if (NullHandling.sqlCompatible()) {
      Assert.assertTrue(result.eval(InputBindings.nilBindings()).isNumericNull());
    } else {
      Assert.assertFalse(result.eval(InputBindings.nilBindings()).isNumericNull());
      Assert.assertEquals(0, result.eval(InputBindings.nilBindings()).asDouble(), 0);
    }
  }

  @Test
  public void test_quantileFromSortedArray()
  {
    final DoubleList doubles = DoubleList.of(
        1.74894566717352,
        2.45877596678213,
        6.84501873459025,
        18.05541572400960,
        18.46552908786640,
        21.67577450542990,
        28.27502148905920,
        29.38150656294550,
        31.51777607091530,
        35.07176789407870,
        35.44337813640110,
        36.00285458859680,
        38.02930138807480,
        38.91193281665990,
        39.10448180900530,
        41.73995751226990,
        44.09796685057930,
        44.97457148479690,
        69.52896057856050,
        74.77683331911330,
        77.96955453249100,
        80.79983221039570,
        83.32696453924490,
        87.71915087266120,
        90.18343512171780,
        96.84202159588680
    );

    Assert.assertEquals(Double.NaN, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, -0.1), 0.0000001);
    Assert.assertEquals(1.748945667, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0), 0.0000001);
    Assert.assertEquals(1.748963413, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.000001), 0.0000001);
    Assert.assertEquals(12.45021723, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.1), 0.0000001);
    Assert.assertEquals(21.67577451, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.2), 0.0000001);
    Assert.assertEquals(30.44964132, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.3), 0.0000001);
    Assert.assertEquals(35.44337814, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.4), 0.0000001);
    Assert.assertEquals(38.4706171, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.5), 0.0000001);
    Assert.assertEquals(41.73995751, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.6), 0.0000001);
    Assert.assertEquals(57.25176603, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.7), 0.0000001);
    Assert.assertEquals(77.96955453, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.8), 0.0000001);
    Assert.assertEquals(85.52305771, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.9), 0.0000001);
    Assert.assertEquals(96.84185513, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.999999), 0.0000001);
    Assert.assertEquals(96.8420216, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 1), 0.0000001);
    Assert.assertEquals(Double.NaN, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 1.1), 0.0000001);
  }

  @Test
  public void test_quantileFromSortedArray_singleElement()
  {
    final DoubleList doubles = DoubleList.of(1.748945667);

    Assert.assertEquals(Double.NaN, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, -0.1), 0);
    Assert.assertEquals(1.748945667, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0), 0);
    Assert.assertEquals(1.748945667, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.000001), 0);
    Assert.assertEquals(1.748945667, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.1), 0);
    Assert.assertEquals(1.748945667, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.2), 0);
    Assert.assertEquals(1.748945667, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.3), 0);
    Assert.assertEquals(1.748945667, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.4), 0);
    Assert.assertEquals(1.748945667, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.5), 0);
    Assert.assertEquals(1.748945667, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.6), 0);
    Assert.assertEquals(1.748945667, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.7), 0);
    Assert.assertEquals(1.748945667, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.8), 0);
    Assert.assertEquals(1.748945667, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.9), 0);
    Assert.assertEquals(1.748945667, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.999999), 0);
    Assert.assertEquals(1.748945667, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 1), 0);
    Assert.assertEquals(Double.NaN, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 1.1), 0);
  }

  @Test
  public void test_quantileFromSortedArray_noElements()
  {
    final DoubleList doubles = DoubleLists.emptyList();

    Assert.assertEquals(Double.NaN, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, -0.1), 0);
    Assert.assertEquals(Double.NaN, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0), 0);
    Assert.assertEquals(Double.NaN, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.000001), 0);
    Assert.assertEquals(Double.NaN, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.1), 0);
    Assert.assertEquals(Double.NaN, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.2), 0);
    Assert.assertEquals(Double.NaN, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.3), 0);
    Assert.assertEquals(Double.NaN, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.4), 0);
    Assert.assertEquals(Double.NaN, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.5), 0);
    Assert.assertEquals(Double.NaN, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.6), 0);
    Assert.assertEquals(Double.NaN, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.7), 0);
    Assert.assertEquals(Double.NaN, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.8), 0);
    Assert.assertEquals(Double.NaN, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.9), 0);
    Assert.assertEquals(Double.NaN, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 0.999999), 0);
    Assert.assertEquals(Double.NaN, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 1), 0);
    Assert.assertEquals(Double.NaN, ArrayQuantileExprMacro.quantileFromSortedArray(doubles, 1.1), 0);
  }
}
