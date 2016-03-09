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

import com.google.common.collect.Ordering;
import com.metamx.common.ISE;
import io.druid.query.ordering.StringComparators;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.text.Collator;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class LexicographicBenchmark
{
  private static final String STRING_COMPARE = UUID.randomUUID().toString();
  private static final Ordering<String> NATIVE_STRING = Ordering.natural();
  private static final Ordering<String> LEXICOGRAPHIC = Ordering.from(StringComparators.LEXICOGRAPHIC);
  private static final Collator COLLATOR_US = Collator.getInstance(Locale.US);
  private static final Ordering<String> COLLATOR = Ordering.from(
      new Comparator<String>()
      {
        @Override
        public int compare(String o1, String o2)
        {
          return COLLATOR_US.compare(o1, o2);
        }
      }
  );

  @Setup(Level.Iteration)
  public void setUp()
  {

  }

  @TearDown(Level.Iteration)
  public void tearDown()
  {

  }

  @State(Scope.Benchmark)
  public static class StringHolder implements Iterable<String>
  {
    final static int size = 1_000_000;
    final static List<String> vals = new ArrayList<>(size);

    static {
      for (int i = 0; i < size; ++i) {
        vals.add(UUID.randomUUID().toString());
      }
    }

    @Override
    public Iterator<String> iterator()
    {
      return vals.iterator();
    }
  }

  // LexicographicBenchmark.nativeCompare         avgt  200   18033.279 ±  355.628  us/op
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void nativeCompare(StringHolder pool, Blackhole blackhole)
  {
    for (final String other : pool) {
      blackhole.consume(NATIVE_STRING.compare(STRING_COMPARE, other));
    }
  }

  // LexicographicBenchmark.lexicographicCompare  avgt  200  129211.256 ± 2018.940  us/op
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void lexicographicCompare(StringHolder pool, Blackhole blackhole)
  {
    for (final String other : pool) {
      blackhole.consume(LEXICOGRAPHIC.compare(STRING_COMPARE, other));
    }
  }


  // LexicographicBenchmark.collatorCompare       avgt  200  218471.717 ± 1536.769  us/op
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void collatorCompare(StringHolder pool, Blackhole blackhole)
  {
    for (final String other : pool) {
      blackhole.consume(COLLATOR_US.compare(STRING_COMPARE, other));
    }
  }

  //
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void validate(StringHolder pool, Blackhole blackhole)
  {
    for (final String other : pool) {
      if (LEXICOGRAPHIC.compare(STRING_COMPARE, other) != NATIVE_STRING.compare(STRING_COMPARE, other)) {
        throw new ISE("Differ between [%s] and [%s]", STRING_COMPARE, other);
      }
      blackhole.consume(LEXICOGRAPHIC.compare(STRING_COMPARE, other));
    }
  }
}
