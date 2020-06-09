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

import org.apache.druid.java.util.common.parsers.Parser;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
@Fork(value = 1)
public class FlattenJSONBenchmark
{
  private static final int NUM_EVENTS = 100000;

  List<String> flatInputs;
  List<String> nestedInputs;
  List<String> jqInputs;
  Parser flatParser;
  Parser nestedParser;
  Parser jqParser;
  Parser fieldDiscoveryParser;
  Parser forcedPathParser;
  int flatCounter = 0;
  int nestedCounter = 0;
  int jqCounter = 0;

  @Setup
  public void prepare() throws Exception
  {
    FlattenJSONBenchmarkUtil gen = new FlattenJSONBenchmarkUtil();
    flatInputs = new ArrayList<String>();
    for (int i = 0; i < NUM_EVENTS; i++) {
      flatInputs.add(gen.generateFlatEvent());
    }
    nestedInputs = new ArrayList<String>();
    for (int i = 0; i < NUM_EVENTS; i++) {
      nestedInputs.add(gen.generateNestedEvent());
    }
    jqInputs = new ArrayList<String>();
    for (int i = 0; i < NUM_EVENTS; i++) {
      jqInputs.add(gen.generateNestedEvent()); // reuse the same event as "nested"
    }

    flatParser = gen.getFlatParser();
    nestedParser = gen.getNestedParser();
    jqParser = gen.getJqParser();
    fieldDiscoveryParser = gen.getFieldDiscoveryParser();
    forcedPathParser = gen.getForcedPathParser();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public Map<String, Object> baseline(final Blackhole blackhole)
  {
    Map<String, Object> parsed = flatParser.parseToMap(flatInputs.get(flatCounter));
    for (String s : parsed.keySet()) {
      blackhole.consume(parsed.get(s));
    }
    flatCounter = (flatCounter + 1) % NUM_EVENTS;
    return parsed;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public Map<String, Object> flatten(final Blackhole blackhole)
  {
    Map<String, Object> parsed = nestedParser.parseToMap(nestedInputs.get(nestedCounter));
    for (String s : parsed.keySet()) {
      blackhole.consume(parsed.get(s));
    }
    nestedCounter = (nestedCounter + 1) % NUM_EVENTS;
    return parsed;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public Map<String, Object> jqflatten(final Blackhole blackhole)
  {
    Map<String, Object> parsed = jqParser.parseToMap(jqInputs.get(jqCounter));
    for (String s : parsed.keySet()) {
      blackhole.consume(parsed.get(s));
    }
    jqCounter = (jqCounter + 1) % NUM_EVENTS;
    return parsed;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public Map<String, Object> preflattenNestedParser(final Blackhole blackhole)
  {
    Map<String, Object> parsed = fieldDiscoveryParser.parseToMap(flatInputs.get(nestedCounter));
    for (String s : parsed.keySet()) {
      blackhole.consume(parsed.get(s));
    }
    nestedCounter = (nestedCounter + 1) % NUM_EVENTS;
    return parsed;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public Map<String, Object> forcedRootPaths(final Blackhole blackhole)
  {
    Map<String, Object> parsed = forcedPathParser.parseToMap(flatInputs.get(nestedCounter));
    for (String s : parsed.keySet()) {
      blackhole.consume(parsed.get(s));
    }
    nestedCounter = (nestedCounter + 1) % NUM_EVENTS;
    return parsed;
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(FlattenJSONBenchmark.class.getSimpleName())
        .build();

    new Runner(opt).run();
  }
}
