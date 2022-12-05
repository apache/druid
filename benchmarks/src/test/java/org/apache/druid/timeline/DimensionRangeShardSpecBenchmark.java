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

package org.apache.druid.timeline;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.junit.Assert;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = {"-Xms128m", "-Xmx128m", "-XX:+UseG1GC"})
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@OperationsPerInvocation(3) // 3 shard specs per invocation
@SuppressWarnings("UnstableApiUsage") // Range, RangeSet are unstable APIs
public class DimensionRangeShardSpecBenchmark
{
  private final Map<String, RangeSet<String>> domainSinglePrunable =
      ImmutableMap.<String, RangeSet<String>>builder()
                  .put("country", ImmutableRangeSet.of(Range.singleton("India")))
                  .build();

  // Initialized in @Setup method.
  private Map<String, RangeSet<String>> domain5kPrunable;

  private final Map<String, RangeSet<String>> domainSingleNonPrunable =
      ImmutableMap.<String, RangeSet<String>>builder()
                  .put("country", ImmutableRangeSet.of(Range.singleton("Shanghai")))
                  .build();

  // Initial segment (null -> values)
  private final DimensionRangeShardSpec shardSpec0 = new DimensionRangeShardSpec(
      Arrays.asList("country", "city"),
      new StringTuple(new String[]{null, null}),
      new StringTuple(new String[]{"Germany", "Munich"}),
      0,
      4
  );

  // Middle segment (values -> other values)
  private final DimensionRangeShardSpec shardSpec1 = new DimensionRangeShardSpec(
      Arrays.asList("country", "city"),
      new StringTuple(new String[]{"Germany", "Munich"}),
      new StringTuple(new String[]{"United States", "New York"}),
      1,
      4
  );

  // End segment (values -> null)
  private final DimensionRangeShardSpec shardSpec2 = new DimensionRangeShardSpec(
      Arrays.asList("country", "city"),
      new StringTuple(new String[]{"United States", "New York"}),
      new StringTuple(new String[]{null, null}),
      2,
      4
  );

  @Setup
  public void setUp()
  {
    NullHandling.initializeForTests();

    final Set<String> strings5k = new HashSet<>();

    final Random random = new Random(0); // Random... ish.
    for (int i = 0; i < 5000; i++) {
      // Uppercase so the strings end up in different shardSpecs
      final String s = StringUtils.format(
          "%s%s",
          String.valueOf((char) ('A' + random.nextInt(26))),
          String.valueOf(random.nextInt())
      );
      strings5k.add(s);
    }

    final RangeSet<String> rangeSet5k = new InDimFilter("country", strings5k).getDimensionRangeSet("country");
    domain5kPrunable = ImmutableMap.of("country", rangeSet5k);
  }

  @Benchmark
  public void benchSinglePrunable()
  {
    Assert.assertFalse(shardSpec0.possibleInDomain(domainSinglePrunable));
    Assert.assertTrue(shardSpec1.possibleInDomain(domainSinglePrunable));
    Assert.assertFalse(shardSpec2.possibleInDomain(domainSinglePrunable));
  }

  @Benchmark
  public void bench5kPrunable()
  {
    Assert.assertTrue(shardSpec0.possibleInDomain(domain5kPrunable));
    Assert.assertTrue(shardSpec1.possibleInDomain(domain5kPrunable));
    Assert.assertTrue(shardSpec2.possibleInDomain(domain5kPrunable));
  }

  @Benchmark
  public void benchSingleNonPrunable()
  {
    Assert.assertTrue(shardSpec0.possibleInDomain(domainSingleNonPrunable));
    Assert.assertTrue(shardSpec1.possibleInDomain(domainSingleNonPrunable));
    Assert.assertTrue(shardSpec2.possibleInDomain(domainSingleNonPrunable));
  }
}
