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
