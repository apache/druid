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

package org.apache.druid.benchmark.lookup;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.lookup.LookupExtractor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for various important operations on {@link LookupExtractor}.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
public class LookupExtractorBenchmark
{
  static {
    NullHandling.initializeForTests();
  }

  /**
   * Type of lookup to benchmark. All are members of enum {@link LookupBenchmarkUtil.LookupType}.
   */
  @Param({"reversible"})
  private String lookupType;

  /**
   * Number of keys in the lookup table.
   */
  @Param({"1000000"})
  private int numKeys;

  /**
   * Average number of keys that map to each value.
   */
  @Param({"1", "1000"})
  private int keysPerValue;

  private LookupExtractor lookup;
  private String oneValue;
  private Set<String> oneThousandValues;

  @Setup(Level.Trial)
  public void setup()
  {
    final int numValues = Math.max(1, numKeys / keysPerValue);
    lookup = LookupBenchmarkUtil.makeLookupExtractor(
        LookupBenchmarkUtil.LookupType.valueOf(StringUtils.toUpperCase(lookupType)),
        numKeys,
        numValues
    );

    Preconditions.checkArgument(lookup.asMap().size() == numKeys);

    // Values to unapply for the benchmark lookupUnapplyOne.
    oneValue = LookupBenchmarkUtil.makeKeyOrValue(0);

    // Set of values to unapply for the benchmark lookupUnapplyOneThousand.
    oneThousandValues = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      oneThousandValues.add(LookupBenchmarkUtil.makeKeyOrValue(i));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  public void lookupApply(Blackhole blackhole)
  {
    blackhole.consume(lookup.apply("0"));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void lookupUnapplyOne()
  {
    final int numKeys = Iterators.size(lookup.unapplyAll(Collections.singleton(oneValue)));
    if (numKeys != keysPerValue) {
      throw new ISE("Expected [%s] keys, got[%s]", keysPerValue, numKeys);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void lookupUnapplyOneThousand()
  {
    final int numKeys = Iterators.size(lookup.unapplyAll(oneThousandValues));
    if (numKeys != keysPerValue * 1000) {
      throw new ISE("Expected [%s] keys, got[%s]", keysPerValue * 1000, numKeys);
    }
  }
}
