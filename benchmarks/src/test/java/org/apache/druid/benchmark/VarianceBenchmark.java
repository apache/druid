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

package org.apache.druid.benchmark;

import org.apache.druid.query.aggregation.variance.VarianceAggregatorCollector;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class VarianceBenchmark
{
  @Param({"128", "256", "512", "1024"})
  int vectorSize;

  private float[] randomValues;

  @Setup
  public void setup()
  {
    randomValues = new float[vectorSize];
    Random r = ThreadLocalRandom.current();
    for (int i = 0; i < vectorSize; i++) {
      randomValues[i] = r.nextFloat();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void collectVarianceOneByOne(Blackhole blackhole)
  {
    VarianceAggregatorCollector collector = new VarianceAggregatorCollector();
    for (float v : randomValues) {
      collector.add(v);
    }
    blackhole.consume(collector);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void collectVarianceInBatch(Blackhole blackhole)
  {
    double sum = 0, nvariance = 0;
    for (float v : randomValues) {
      sum += v;
    }
    double mean = sum / randomValues.length;
    for (float v : randomValues) {
      nvariance += (v - mean) * (v - mean);
    }
    VarianceAggregatorCollector collector = new VarianceAggregatorCollector(randomValues.length, sum, nvariance);
    blackhole.consume(collector);
  }
}
