/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.aggregation;


import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.metamx.druid.processing.FloatMetricSelector;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class JavaScriptAggregatorTest
{
  protected static final String sumLogATimesBPlusTen =
      "function aggregate(current, a, b) { return current + (Math.log(a) * b) }"
    + "function combine(a,b)             { return a + b }"
    + "function reset()                  { return 10 }";

  protected static final String scriptDoubleSum =
      "function aggregate(current, a) { return current + a }"
    + "function combine(a,b)          { return a + b }"
    + "function reset()               { return 0 }";

  private static void aggregate(TestFloatMetricSelector selector1, TestFloatMetricSelector selector2, Aggregator agg)
  {
    agg.aggregate();
    selector1.increment();
    selector2.increment();
  }

  private void aggregateBuffer(TestFloatMetricSelector selector1,
                               TestFloatMetricSelector selector2,
                               BufferAggregator agg,
                               ByteBuffer buf, int position)
  {
    agg.aggregate(buf, position);
    selector1.increment();
    selector2.increment();
  }

  private static void aggregate(TestFloatMetricSelector selector, Aggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  @Test
  public void testAggregate()
  {
    final TestFloatMetricSelector selector1 = new TestFloatMetricSelector(new float[]{42.12f, 9f});
    final TestFloatMetricSelector selector2 = new TestFloatMetricSelector(new float[]{2f, 3f});

    JavaScriptAggregator agg = new JavaScriptAggregator(
      "billy",
      Arrays.<FloatMetricSelector>asList(selector1, selector2),
      JavaScriptAggregatorFactory.compileScript(sumLogATimesBPlusTen)
    );

    agg.reset();

    Assert.assertEquals("billy", agg.getName());

    double val = 10.;
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());
    aggregate(selector1, selector2, agg);

    val += Math.log(42.12f) * 2f;
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());

    aggregate(selector1, selector2, agg);
    val += Math.log(9f) * 3f;
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());
  }

  @Test
  public void testBufferAggregate()
  {
    final TestFloatMetricSelector selector1 = new TestFloatMetricSelector(new float[]{42.12f, 9f});
    final TestFloatMetricSelector selector2 = new TestFloatMetricSelector(new float[]{2f, 3f});

    JavaScriptBufferAggregator agg = new JavaScriptBufferAggregator(
      Arrays.<FloatMetricSelector>asList(selector1, selector2),
      JavaScriptAggregatorFactory.compileScript(sumLogATimesBPlusTen)
    );

    ByteBuffer buf = ByteBuffer.allocateDirect(32);
    final int position = 4;
    agg.init(buf, position);

    double val = 10.;
    Assert.assertEquals(val, agg.get(buf, position));
    Assert.assertEquals(val, agg.get(buf, position));
    Assert.assertEquals(val, agg.get(buf, position));
    aggregateBuffer(selector1, selector2, agg, buf, position);

    val += Math.log(42.12f) * 2f;
    Assert.assertEquals(val, agg.get(buf, position));
    Assert.assertEquals(val, agg.get(buf, position));
    Assert.assertEquals(val, agg.get(buf, position));

    aggregateBuffer(selector1, selector2, agg, buf, position);
    val += Math.log(9f) * 3f;
    Assert.assertEquals(val, agg.get(buf, position));
    Assert.assertEquals(val, agg.get(buf, position));
    Assert.assertEquals(val, agg.get(buf, position));
  }

  public static void main(String... args) throws Exception {
    final LoopingFloatMetricSelector selector = new LoopingFloatMetricSelector(new float[]{42.12f, 9f});

    /* memory usage test
    List<JavaScriptAggregator> aggs = Lists.newLinkedList();

    for(int i = 0; i < 100000; ++i) {
        JavaScriptAggregator a = new JavaScriptAggregator(
          "billy",
          Lists.asList(selector, new FloatMetricSelector[]{}),
          JavaScriptAggregatorFactory.compileScript(scriptDoubleSum)
        );
        //aggs.add(a);
        a.aggregate();
        a.aggregate();
        a.aggregate();
        if(i % 1000 == 0) System.out.println(String.format("Query object %d", i));
    }
    */


    JavaScriptAggregator aggRhino = new JavaScriptAggregator(
      "billy",
      Lists.asList(selector, new FloatMetricSelector[]{}),
      JavaScriptAggregatorFactory.compileScript(scriptDoubleSum)
    );

    DoubleSumAggregator doubleAgg = new DoubleSumAggregator("billy", selector);

    // warmup
    int i = 0;
    long t = 0;
    while(i < 10000) {
      aggregate(selector, aggRhino);
      ++i;
    }
    i = 0;
    while(i < 10000) {
      aggregate(selector, doubleAgg);
      ++i;
    }


    t = System.currentTimeMillis();
    i = 0;
    while(i < 500000000) {
      aggregate(selector, aggRhino);
      ++i;
    }
    long t1 = System.currentTimeMillis() - t;
    System.out.println(String.format("JavaScript aggregator == %,f: %d ms", aggRhino.get(), t1));

    t = System.currentTimeMillis();
    i = 0;
    while(i < 500000000) {
      aggregate(selector, doubleAgg);
      ++i;
    }
    long t2 = System.currentTimeMillis() - t;
    System.out.println(String.format("DoubleSum  aggregator == %,f: %d ms", doubleAgg.get(), t2));

    System.out.println(String.format("JavaScript is %2.1fx slower", (double)t1 / t2));
  }

  static class LoopingFloatMetricSelector extends TestFloatMetricSelector
  {
    private final float[] floats;
    private long index = 0;

    public LoopingFloatMetricSelector(float[] floats)
    {
      super(floats);
      this.floats = floats;
    }

    @Override
    public float get()
    {
      return floats[(int)(index % floats.length)];
    }

    public void increment()
    {
      ++index;
      if (index < 0) {
        index = 0;
      }
    }
  }
}
