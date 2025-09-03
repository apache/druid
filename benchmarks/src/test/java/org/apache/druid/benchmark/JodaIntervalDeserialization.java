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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.joda.time.Interval;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class JodaIntervalDeserialization
{
  @Param({"20000"})
  public int numValues;
  private ObjectMapper legacyMapper;
  private ObjectMapper optimizedMapper;
  private List<String> intervalJsonValues;
  private List<String> fallbackIntervalJsonValues;

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(JodaIntervalDeserialization.class.getSimpleName())
        .forks(1)
        .build();
    new Runner(opt).run();
  }

  @Setup
  public void setUp()
  {
    SimpleModule legacyModule = new SimpleModule()
        .addDeserializer(
            Interval.class,
            new StdDeserializer<>(Interval.class)
            {
              @Override
              public Interval deserialize(JsonParser jsonParser, DeserializationContext ctx) throws IOException
              {
                return Intervals.of(jsonParser.getText());
              }
            }
        );

    optimizedMapper = new DefaultObjectMapper();
    legacyMapper = new DefaultObjectMapper().registerModule(legacyModule);

    intervalJsonValues = new ArrayList<>(numValues);
    fallbackIntervalJsonValues = new ArrayList<>(numValues);

    // Use a small set of valid ISO UTC interval strings that hit the optimized fast path.
    final String[] samples = new String[]{
        "\"2022-09-16T00:00:00.000Z/2022-09-17T00:00:00.000Z\"",
        "\"2021-01-01T12:34:56.789Z/2021-01-02T12:34:56.789Z\"",
        "\"2010-06-30T23:59:59.000Z/2010-07-01T23:59:59.000Z\"",
        "\"1999-12-31T00:00:00.123Z/2000-01-01T00:00:00.123Z\""
    };

    final String[] fallbackSamples = new String[]{
        "\"2022-01-01T00:00:00.000Z/P1D\"",
        "\"2022-01-01T12:00:00Z/PT6H\"",
        "\"2022-01-01T00:00:00Z/P2DT3H4M5S\"",
        "\"P2DT3H4M5S/2022-01-03T03:04:05Z\""
    };

    for (int i = 0; i < numValues; i++) {
      intervalJsonValues.add(samples[i % samples.length]);
      fallbackIntervalJsonValues.add(fallbackSamples[i % fallbackSamples.length]);
    }
  }

  @Benchmark
  public void deserializeOptimized(Blackhole blackhole) throws Exception
  {
    for (String json : intervalJsonValues) {
      blackhole.consume(optimizedMapper.readValue(json, Interval.class));
    }
  }

  @Benchmark
  public void deserializeLegacy(Blackhole blackhole) throws Exception
  {
    for (String json : intervalJsonValues) {
      blackhole.consume(legacyMapper.readValue(json, Interval.class));
    }
  }

  @Benchmark
  public void deserializeOptimizedFallback(Blackhole blackhole) throws Exception
  {
    for (String json : fallbackIntervalJsonValues) {
      blackhole.consume(optimizedMapper.readValue(json, Interval.class));
    }
  }

  @Benchmark
  public void deserializeLegacyFallback(Blackhole blackhole) throws Exception
  {
    for (String json : fallbackIntervalJsonValues) {
      blackhole.consume(legacyMapper.readValue(json, Interval.class));
    }
  }
}


