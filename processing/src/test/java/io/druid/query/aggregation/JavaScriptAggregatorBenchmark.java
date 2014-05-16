/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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

import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.segment.ObjectColumnSelector;

import java.util.Map;

public class JavaScriptAggregatorBenchmark extends SimpleBenchmark
{

  protected static final Map<String, String> scriptDoubleSum = Maps.newHashMap();
  static {
    scriptDoubleSum.put("fnAggregate", "function aggregate(current, a) { return current + a }");
    scriptDoubleSum.put("fnReset", "function reset() { return 0 }");
    scriptDoubleSum.put("fnCombine", "function combine(a,b) { return a + b }");
  }

  private static void aggregate(TestFloatColumnSelector selector, Aggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  private JavaScriptAggregator jsAggregator;
  private DoubleSumAggregator doubleAgg;
  final LoopingFloatColumnSelector selector = new LoopingFloatColumnSelector(new float[]{42.12f, 9f});

  @Override
  protected void setUp() throws Exception
  {
    Map<String, String> script = scriptDoubleSum;

    jsAggregator = new JavaScriptAggregator(
        "billy",
        Lists.asList(MetricSelectorUtils.wrap(selector), new ObjectColumnSelector[]{}),
        JavaScriptAggregatorFactory.compileScript(
            script.get("fnAggregate"),
            script.get("fnReset"),
            script.get("fnCombine")
        )
    );

    doubleAgg = new DoubleSumAggregator("billy", selector);
  }

  public double timeJavaScriptDoubleSum(int reps)
  {
    double val = 0;
    for(int i = 0; i < reps; ++i) {
      aggregate(selector, jsAggregator);
    }
    return val;
  }

  public double timeNativeDoubleSum(int reps)
  {
    double val = 0;
    for(int i = 0; i < reps; ++i) {
      aggregate(selector, doubleAgg);
    }
    return val;
  }

  public static void main(String[] args) throws Exception
  {
    Runner.main(JavaScriptAggregatorBenchmark.class, args);
  }

  protected static class LoopingFloatColumnSelector extends TestFloatColumnSelector
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
      return floats[(int) (index % floats.length)];
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
