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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class RegexMatchBenchmark
{
  @Param({"100000"})
  private int numPatterns;

  private ObjectMapper jsonMapper;

  private List<String> uuids;

  private String granularityPathRegex = "^.*[Yy]=(\\d{4})/(?:[Mm]=(\\d{2})/(?:[Dd]=(\\d{2})/(?:[Hh]=(\\d{2})/(?:[Mm]=(\\d{2})/(?:[Ss]=(\\d{2})/)?)?)?)?)?.*$";
  private String uuidRegex = "[\\w]{8}-[\\w]{4}-[\\w]{4}-[\\w]{4}-[\\w]{12}";
  private Pattern uuidPattern = Pattern.compile(uuidRegex);
  private Pattern granularityPathPattern = Pattern.compile(granularityPathRegex);
  private byte[] uuidPatternBytes;
  private byte[] granularityPathPatternBytes;
  private String randomUUID = UUID.randomUUID().toString();

  @Setup
  public void setup() throws IOException
  {
    jsonMapper = new DefaultObjectMapper();

    uuids = new ArrayList<>();
    for (int i = 0; i < numPatterns; i++) {
      UUID uuid = UUID.randomUUID();
      uuids.add(uuid.toString());
    }

    uuidPatternBytes = jsonMapper.writeValueAsBytes(uuidPattern);
    granularityPathPatternBytes = jsonMapper.writeValueAsBytes(granularityPathPattern);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void compileUUIDsAsRegex(final Blackhole blackhole)
  {
    for (String uuid : uuids) {
      Pattern pattern = Pattern.compile(uuid);
      blackhole.consume(pattern);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void compileUUIDsAsRegexAndMatchRandomUUID(final Blackhole blackhole)
  {
    for (String uuid : uuids) {
      Pattern pattern = Pattern.compile(uuid);
      Matcher matcher = pattern.matcher(randomUUID);
      blackhole.consume(matcher.matches());
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void compileGranularityPathRegex(final Blackhole blackhole)
  {
    for (int i = 0; i < numPatterns; i++) {
      Pattern pattern = Pattern.compile(granularityPathRegex);
      blackhole.consume(pattern);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void deserializeGranularityPathRegex(final Blackhole blackhole) throws IOException
  {
    for (int i = 0; i < numPatterns; i++) {
      Pattern pattern = jsonMapper.readValue(granularityPathPatternBytes, Pattern.class);
      blackhole.consume(pattern);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void compileUUIDRegex(final Blackhole blackhole)
  {
    for (int i = 0; i < numPatterns; i++) {
      Pattern pattern = Pattern.compile(uuidRegex);
      blackhole.consume(pattern);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void deserializeUUIDRegex(final Blackhole blackhole) throws IOException
  {
    for (int i = 0; i < numPatterns; i++) {
      Pattern pattern = jsonMapper.readValue(uuidPatternBytes, Pattern.class);
      blackhole.consume(pattern);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void compileUUIDRegexAndMatch(final Blackhole blackhole)
  {
    for (String uuid : uuids) {
      Pattern pattern = Pattern.compile(uuidRegex);
      Matcher matcher = pattern.matcher(uuid);
      blackhole.consume(matcher.matches());
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void compileGranularityPathRegexAndMatch(final Blackhole blackhole)
  {
    for (String uuid : uuids) {
      Pattern pattern = Pattern.compile(granularityPathRegex);
      Matcher matcher = pattern.matcher(uuid);
      blackhole.consume(matcher.matches());
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void precompileUUIDRegexAndMatch(final Blackhole blackhole)
  {
    for (String uuid : uuids) {
      Matcher matcher = uuidPattern.matcher(uuid);
      blackhole.consume(matcher.matches());
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void precompileGranularityPathRegexAndMatch(final Blackhole blackhole)
  {
    for (String uuid : uuids) {
      Matcher matcher = granularityPathPattern.matcher(uuid);
      blackhole.consume(matcher.matches());
    }
  }
}

