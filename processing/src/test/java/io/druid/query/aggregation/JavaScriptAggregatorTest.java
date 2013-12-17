/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.query.aggregation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.segment.ObjectColumnSelector;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class JavaScriptAggregatorTest
{
  protected static final Map<String, String> sumLogATimesBPlusTen = Maps.newHashMap();
  protected static final Map<String, String> scriptDoubleSum = Maps.newHashMap();

  static {
    sumLogATimesBPlusTen.put("fnAggregate", "function aggregate(current, a, b) { return current + (Math.log(a) * b) }");
    sumLogATimesBPlusTen.put("fnReset", "function reset()                  { return 10 }");
    sumLogATimesBPlusTen.put("fnCombine", "function combine(a,b)             { return a + b }");

    scriptDoubleSum.put("fnAggregate", "function aggregate(current, a) { return current + a }");
    scriptDoubleSum.put("fnReset", "function reset()               { return 0 }");
    scriptDoubleSum.put("fnCombine", "function combine(a,b)          { return a + b }");
  }

  private static void aggregate(TestFloatColumnSelector selector1, TestFloatColumnSelector selector2, Aggregator agg)
  {
    agg.aggregate();
    selector1.increment();
    selector2.increment();
  }

  private void aggregateBuffer(TestFloatColumnSelector selector1,
                               TestFloatColumnSelector selector2,
                               BufferAggregator agg,
                               ByteBuffer buf, int position)
  {
    agg.aggregate(buf, position);
    selector1.increment();
    selector2.increment();
  }

  private static void aggregate(TestFloatColumnSelector selector, Aggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  @Test
  public void testAggregate()
  {
    final TestFloatColumnSelector selector1 = new TestFloatColumnSelector(new float[]{42.12f, 9f});
    final TestFloatColumnSelector selector2 = new TestFloatColumnSelector(new float[]{2f, 3f});

    Map<String, String> script = sumLogATimesBPlusTen;

    JavaScriptAggregator agg = new JavaScriptAggregator(
      "billy",
      Arrays.<ObjectColumnSelector>asList(MetricSelectorUtils.wrap(selector1), MetricSelectorUtils.wrap(selector2)),
      JavaScriptAggregatorFactory.compileScript(script.get("fnAggregate"),
                                                script.get("fnReset"),
                                                script.get("fnCombine"))
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
    final TestFloatColumnSelector selector1 = new TestFloatColumnSelector(new float[]{42.12f, 9f});
    final TestFloatColumnSelector selector2 = new TestFloatColumnSelector(new float[]{2f, 3f});

    Map<String, String> script = sumLogATimesBPlusTen;
    JavaScriptBufferAggregator agg = new JavaScriptBufferAggregator(
      Arrays.<ObjectColumnSelector>asList(MetricSelectorUtils.wrap(selector1), MetricSelectorUtils.wrap(selector2)),
      JavaScriptAggregatorFactory.compileScript(script.get("fnAggregate"),
                                                script.get("fnReset"),
                                                script.get("fnCombine"))
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

  @Test
  public void testAggregateMissingColumn()
  {
    Map<String, String> script = scriptDoubleSum;

    JavaScriptAggregator agg = new JavaScriptAggregator(
        "billy",
        Collections.<ObjectColumnSelector>singletonList(null),
        JavaScriptAggregatorFactory.compileScript(script.get("fnAggregate"),
                                                  script.get("fnReset"),
                                                  script.get("fnCombine"))
    );

    final double val = 0;

    Assert.assertEquals("billy", agg.getName());

    agg.reset();
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());

    agg.aggregate();
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());

    agg.aggregate();
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());
  }

  public static void main(String... args) throws Exception {
    final LoopingFloatColumnSelector selector = new LoopingFloatColumnSelector(new float[]{42.12f, 9f});

    /* memory usage test
    List<JavaScriptAggregator> aggs = Lists.newLinkedList();

    for(int i = 0; i < 100000; ++i) {
        JavaScriptAggregator a = new JavaScriptAggregator(
          "billy",
          Lists.asList(selector, new FloatColumnSelector[]{}),
          JavaScriptAggregatorFactory.compileScript(scriptDoubleSum)
        );
        //aggs.add(a);
        a.aggregate();
        a.aggregate();
        a.aggregate();
        if(i % 1000 == 0) System.out.println(String.format("Query object %d", i));
    }
    */

    Map<String, String> script = scriptDoubleSum;
    JavaScriptAggregator aggRhino = new JavaScriptAggregator(
      "billy",
      Lists.asList(MetricSelectorUtils.wrap(selector), new ObjectColumnSelector[]{}),
      JavaScriptAggregatorFactory.compileScript(script.get("fnAggregate"),
                                                script.get("fnReset"),
                                                script.get("fnCombine"))
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

  static class LoopingFloatColumnSelector extends TestFloatColumnSelector
  {
    private final float[] floats;
    private long index = 0;

    public LoopingFloatColumnSelector(float[] floats)
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
