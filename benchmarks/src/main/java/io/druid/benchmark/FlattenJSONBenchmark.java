/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import io.druid.java.util.common.parsers.Parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class FlattenJSONBenchmark
{
  private static final int numEvents = 1000000;

  List<String> flatInputs;
  List<String> nestedInputs;
  Parser flatParser;
  Parser nestedParser;
  Parser fieldDiscoveryParser;
  Parser forcedPathParser;
  int flatCounter = 0;
  int nestedCounter = 0;

  @Setup
  public void prepare() throws Exception
  {
    FlattenJSONBenchmarkUtil gen = new FlattenJSONBenchmarkUtil();
    flatInputs = new ArrayList<String>();
    for (int i = 0; i < numEvents; i++) {
      flatInputs.add(gen.generateFlatEvent());
    }
    nestedInputs = new ArrayList<String>();
    for (int i = 0; i < numEvents; i++) {
      nestedInputs.add(gen.generateNestedEvent());
    }

    flatParser = gen.getFlatParser();
    nestedParser = gen.getNestedParser();
    fieldDiscoveryParser = gen.getFieldDiscoveryParser();
    forcedPathParser = gen.getForcedPathParser();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public Map<String, Object> baseline()
  {
    Map<String, Object> parsed = flatParser.parse(flatInputs.get(flatCounter));
    flatCounter = (flatCounter + 1) % numEvents;
    return parsed;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public Map<String, Object> flatten()
  {
    Map<String, Object> parsed = nestedParser.parse(nestedInputs.get(nestedCounter));
    nestedCounter = (nestedCounter + 1) % numEvents;
    return parsed;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public Map<String, Object> preflattenNestedParser()
  {
    Map<String, Object> parsed = fieldDiscoveryParser.parse(flatInputs.get(nestedCounter));
    nestedCounter = (nestedCounter + 1) % numEvents;
    return parsed;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public Map<String, Object> forcedRootPaths()
  {
    Map<String, Object> parsed = forcedPathParser.parse(flatInputs.get(nestedCounter));
    nestedCounter = (nestedCounter + 1) % numEvents;
    return parsed;
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(FlattenJSONBenchmark.class.getSimpleName())
        .warmupIterations(1)
        .measurementIterations(25)
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}

