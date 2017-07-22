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

package io.druid.query.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.java.util.common.StringUtils;
import io.druid.js.JavaScriptConfig;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.ColumnCapabilities;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class JavaScriptAggregatorTest
{
  protected static final Map<String, String> sumLogATimesBPlusTen = Maps.newHashMap();
  protected static final Map<String, String> scriptDoubleSum = Maps.newHashMap();

  final ColumnSelectorFactory DUMMY_COLUMN_SELECTOR_FACTORY = new ColumnSelectorFactory()
  {
    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return null;
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      return null;
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      return null;
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      return null;
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      return null;
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      return null;
    }
  };

  static {
    sumLogATimesBPlusTen.put("fnAggregate", "function aggregate(current, a, b) { return current + (Math.log(a) * b) }");
    sumLogATimesBPlusTen.put("fnReset", "function reset()                  { return 10 }");
    sumLogATimesBPlusTen.put("fnCombine", "function combine(a,b)             { return a + b }");

    scriptDoubleSum.put("fnAggregate", "function aggregate(current, a) { return current + a }");
    scriptDoubleSum.put("fnReset", "function reset()               { return 0 }");
    scriptDoubleSum.put("fnCombine", "function combine(a,b)          { return a + b }");
  }

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private static void aggregate(TestDoubleColumnSelectorImpl selector1, TestDoubleColumnSelectorImpl selector2, Aggregator agg)
  {
    agg.aggregate();
    selector1.increment();
    selector2.increment();
  }

  private void aggregateBuffer(
      TestFloatColumnSelector selector1,
      TestFloatColumnSelector selector2,
      BufferAggregator agg,
      ByteBuffer buf, int position
  )
  {
    agg.aggregate(buf, position);
    selector1.increment();
    selector2.increment();
  }

  private static void aggregate(TestDoubleColumnSelectorImpl selector, Aggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  private static void aggregate(TestObjectColumnSelector selector, Aggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  @Test
  public void testAggregate()
  {
    final TestDoubleColumnSelectorImpl selector1 = new TestDoubleColumnSelectorImpl(new double[]{42.12d, 9d});
    final TestDoubleColumnSelectorImpl selector2 = new TestDoubleColumnSelectorImpl(new double[]{2d, 3d});

    Map<String, String> script = sumLogATimesBPlusTen;

    JavaScriptAggregator agg = new JavaScriptAggregator(
        Arrays.<ObjectColumnSelector>asList(MetricSelectorUtils.wrap(selector1), MetricSelectorUtils.wrap(selector2)),
        JavaScriptAggregatorFactory.compileScript(
            script.get("fnAggregate"),
            script.get("fnReset"),
            script.get("fnCombine")
        )
    );

    agg.reset();

    double val = 10.;
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());
    aggregate(selector1, selector2, agg);

    val += Math.log(42.12d) * 2d;
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());

    aggregate(selector1, selector2, agg);
    val += Math.log(9d) * 3d;
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
        JavaScriptAggregatorFactory.compileScript(
            script.get("fnAggregate"),
            script.get("fnReset"),
            script.get("fnCombine")
        )
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
        Collections.<ObjectColumnSelector>singletonList(null),
        JavaScriptAggregatorFactory.compileScript(
            script.get("fnAggregate"),
            script.get("fnReset"),
            script.get("fnCombine")
        )
    );

    final double val = 0;

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

  @Test
  public void testAggregateStrings()
  {
    final TestObjectColumnSelector ocs = new TestObjectColumnSelector("what", null, new String[]{"hey", "there"});
    final JavaScriptAggregator agg = new JavaScriptAggregator(
        Collections.<ObjectColumnSelector>singletonList(ocs),
        JavaScriptAggregatorFactory.compileScript(
            "function aggregate(current, a) { if (Array.isArray(a)) { return current + a.length; } else if (typeof a === 'string') { return current + 1; } else { return current; } }",
            scriptDoubleSum.get("fnReset"),
            scriptDoubleSum.get("fnCombine")
        )
    );

    agg.reset();

    double val = 0.;
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());
    aggregate(ocs, agg);

    val += 1;
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());
    aggregate(ocs, agg);

    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());
    aggregate(ocs, agg);

    val += 2;
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());
    Assert.assertEquals(val, agg.get());
  }

  @Test
  public void testJavaScriptDisabledFactorize()
  {
    final JavaScriptAggregatorFactory factory = new JavaScriptAggregatorFactory(
        "foo",
        ImmutableList.of("foo"),
        scriptDoubleSum.get("fnAggregate"),
        scriptDoubleSum.get("fnReset"),
        scriptDoubleSum.get("fnCombine"),
        new JavaScriptConfig(false)
    );

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("JavaScript is disabled");
    factory.factorize(DUMMY_COLUMN_SELECTOR_FACTORY);
    Assert.assertTrue(false);
  }

  @Test
  public void testJavaScriptDisabledFactorizeBuffered()
  {
    final JavaScriptAggregatorFactory factory = new JavaScriptAggregatorFactory(
        "foo",
        ImmutableList.of("foo"),
        scriptDoubleSum.get("fnAggregate"),
        scriptDoubleSum.get("fnReset"),
        scriptDoubleSum.get("fnCombine"),
        new JavaScriptConfig(false)
    );

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("JavaScript is disabled");
    factory.factorizeBuffered(DUMMY_COLUMN_SELECTOR_FACTORY);
    Assert.assertTrue(false);
  }

  public static void main(String... args) throws Exception
  {
    final JavaScriptAggregatorBenchmark.LoopingDoubleColumnSelector selector = new JavaScriptAggregatorBenchmark.LoopingDoubleColumnSelector(
        new double[]{42.12d, 9d});

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
        if(i % 1000 == 0) System.out.println(StringUtils.format("Query object %d", i));
    }
    */

    Map<String, String> script = scriptDoubleSum;
    JavaScriptAggregator aggRhino = new JavaScriptAggregator(
        Lists.asList(MetricSelectorUtils.wrap(selector), new ObjectColumnSelector[]{}),
        JavaScriptAggregatorFactory.compileScript(
            script.get("fnAggregate"),
            script.get("fnReset"),
            script.get("fnCombine")
        )
    );

    DoubleSumAggregator doubleAgg = new DoubleSumAggregator(selector);

    // warmup
    int i = 0;
    long t = 0;
    while (i < 10000) {
      aggregate(selector, aggRhino);
      ++i;
    }
    i = 0;
    while (i < 10000) {
      aggregate(selector, doubleAgg);
      ++i;
    }

    t = System.currentTimeMillis();
    i = 0;
    while (i < 500000000) {
      aggregate(selector, aggRhino);
      ++i;
    }
    long t1 = System.currentTimeMillis() - t;
    System.out.println(StringUtils.format("JavaScript aggregator == %,f: %d ms", aggRhino.getFloat(), t1));

    t = System.currentTimeMillis();
    i = 0;
    while (i < 500000000) {
      aggregate(selector, doubleAgg);
      ++i;
    }
    long t2 = System.currentTimeMillis() - t;
    System.out.println(StringUtils.format("DoubleSum  aggregator == %,f: %d ms", doubleAgg.getFloat(), t2));

    System.out.println(StringUtils.format("JavaScript is %2.1fx slower", (double) t1 / t2));
  }
}
