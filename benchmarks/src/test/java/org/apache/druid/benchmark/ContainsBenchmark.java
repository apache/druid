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

import it.unimi.dsi.fastutil.longs.LongArraySet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
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

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(value = 1)
public class ContainsBenchmark
{
  private static final long[] LONGS;
  private static final long[] SORTED_LONGS;
  private static final LongOpenHashSet LONG_HASH_SET;
  private static final LongArraySet LONG_ARRAY_SET;

  private long worstSearchValue;
  private long worstSearchValueBin;

  static {
    LONGS = new long[16];
    for (int i = 0; i < LONGS.length; i++) {
      LONGS[i] = ThreadLocalRandom.current().nextInt(Short.MAX_VALUE);
    }

    LONG_HASH_SET = new LongOpenHashSet(LONGS);
    LONG_ARRAY_SET = new LongArraySet(LONGS);
    SORTED_LONGS = Arrays.copyOf(LONGS, LONGS.length);

    Arrays.sort(SORTED_LONGS);

  }

  @Setup
  public void setUp()
  {
    worstSearchValue = LONGS[LONGS.length - 1];
    worstSearchValueBin = SORTED_LONGS[(SORTED_LONGS.length - 1) >>> 1];
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void linearSearch(Blackhole blackhole)
  {
    boolean found = LONG_ARRAY_SET.contains(worstSearchValue);
    blackhole.consume(found);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void hashSetSearch(Blackhole blackhole)
  {

    boolean found = LONG_HASH_SET.contains(worstSearchValueBin);
    blackhole.consume(found);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void binarySearch(Blackhole blackhole)
  {
    boolean found = Arrays.binarySearch(SORTED_LONGS, worstSearchValueBin) >= 0;
    blackhole.consume(found);
  }
}
