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

package org.apache.druid.java.util.common;

import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Compares {@link DateTimes#of(String)} (with the fast canonical-timestamp path added for #20927) against a frozen
 * copy of the pre-optimization implementation, for both the canonical {@code used_status_last_updated} format and
 * inputs that must fall back to the original lenient parser, to confirm the fallback is not slower than before.
 */
@State(Scope.Benchmark)
@Fork(value = 3)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class DateTimesOfBenchmark
{
  private List<String> canonicalTimestamps;
  private List<String> nonCanonicalOffsetTimestamps;
  private List<String> epochMillisTimestamps;

  @Setup(Level.Trial)
  public void setup()
  {
    canonicalTimestamps = new ArrayList<>();
    nonCanonicalOffsetTimestamps = new ArrayList<>();
    epochMillisTimestamps = new ArrayList<>();

    final DateTime start = DateTimes.of("2025-01-01T00:00:00.000Z");
    for (int i = 0; i < 1000; i++) {
      final DateTime t = start.plusSeconds(i);
      canonicalTimestamps.add(t.toString());
      nonCanonicalOffsetTimestamps.add(t.withZone(DateTimes.inferTzFromString("+05:30")).toString());
      epochMillisTimestamps.add(String.valueOf(t.getMillis()));
    }
    canonicalTimestamps = Collections.unmodifiableList(canonicalTimestamps);
    nonCanonicalOffsetTimestamps = Collections.unmodifiableList(nonCanonicalOffsetTimestamps);
    epochMillisTimestamps = Collections.unmodifiableList(epochMillisTimestamps);
  }

  @Benchmark
  public void current_canonicalFormat(Blackhole blackhole)
  {
    for (String s : canonicalTimestamps) {
      blackhole.consume(DateTimes.of(s));
    }
  }

  @Benchmark
  public void legacy_canonicalFormat(Blackhole blackhole)
  {
    for (String s : canonicalTimestamps) {
      blackhole.consume(legacyOf(s));
    }
  }

  @Benchmark
  public void current_nonCanonicalOffsetFallback(Blackhole blackhole)
  {
    for (String s : nonCanonicalOffsetTimestamps) {
      blackhole.consume(DateTimes.of(s));
    }
  }

  @Benchmark
  public void legacy_nonCanonicalOffsetFallback(Blackhole blackhole)
  {
    for (String s : nonCanonicalOffsetTimestamps) {
      blackhole.consume(legacyOf(s));
    }
  }

  @Benchmark
  public void current_epochMillisFallback(Blackhole blackhole)
  {
    for (String s : epochMillisTimestamps) {
      blackhole.consume(DateTimes.of(s));
    }
  }

  @Benchmark
  public void legacy_epochMillisFallback(Blackhole blackhole)
  {
    for (String s : epochMillisTimestamps) {
      blackhole.consume(legacyOf(s));
    }
  }

  /**
   * Frozen copy of {@link DateTimes#of(String)} as it existed before #20927: identical except that it never
   * attempts the fast canonical-timestamp path. Kept here only so this benchmark has a stable "before" baseline.
   */
  private static DateTime legacyOf(String instant)
  {
    try {
      return new DateTime(instant, ISOChronology.getInstanceUTC());
    }
    catch (IllegalArgumentException ex) {
      try {
        return new DateTime(Long.valueOf(instant), ISOChronology.getInstanceUTC());
      }
      catch (IllegalArgumentException ex2) {
        throw ex;
      }
    }
  }
}
