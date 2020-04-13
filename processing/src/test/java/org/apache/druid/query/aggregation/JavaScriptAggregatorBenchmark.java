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

package org.apache.druid.query.aggregation;

import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO rewrite to use JMH and move to benchmarks project
 */
public class JavaScriptAggregatorBenchmark extends SimpleBenchmark
{

  protected static final Map<String, String> SCRIPT_DOUBLE_SUM = new HashMap<>();

  static {
    SCRIPT_DOUBLE_SUM.put("fnAggregate", "function aggregate(current, a) { return current + a }");
    SCRIPT_DOUBLE_SUM.put("fnReset", "function reset() { return 0 }");
    SCRIPT_DOUBLE_SUM.put("fnCombine", "function combine(a,b) { return a + b }");
  }

  private static void aggregate(TestDoubleColumnSelectorImpl selector, Aggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  private JavaScriptAggregator jsAggregator;
  private DoubleSumAggregator doubleAgg;
  final LoopingDoubleColumnSelector selector = new LoopingDoubleColumnSelector(new double[]{42.12d, 9d});

  @Override
  protected void setUp()
  {
    Map<String, String> script = SCRIPT_DOUBLE_SUM;

    jsAggregator = new JavaScriptAggregator(
        Collections.singletonList(selector),
        JavaScriptAggregatorFactory.compileScript(
            script.get("fnAggregate"),
            script.get("fnReset"),
            script.get("fnCombine")
        )
    );

    doubleAgg = new DoubleSumAggregator(selector);
  }

  @SuppressWarnings("unused") // Supposedly called by Caliper
  public double timeJavaScriptDoubleSum(int reps)
  {
    double val = 0;
    for (int i = 0; i < reps; ++i) {
      aggregate(selector, jsAggregator);
    }
    return val;
  }

  @SuppressWarnings("unused") // Supposedly called by Caliper
  public double timeNativeDoubleSum(int reps)
  {
    double val = 0;
    for (int i = 0; i < reps; ++i) {
      aggregate(selector, doubleAgg);
    }
    return val;
  }

  public static void main(String[] args)
  {
    Runner.main(JavaScriptAggregatorBenchmark.class, args);
  }

  protected static class LoopingDoubleColumnSelector extends TestDoubleColumnSelectorImpl
  {
    private final double[] doubles;
    private long index = 0;

    public LoopingDoubleColumnSelector(double[] doubles)
    {
      super(doubles);
      this.doubles = doubles;
    }

    @Override
    public double getDouble()
    {
      return doubles[(int) (index % doubles.length)];
    }

    @Override
    public void increment()
    {
      ++index;
      if (index < 0) {
        index = 0;
      }
    }
  }
}
