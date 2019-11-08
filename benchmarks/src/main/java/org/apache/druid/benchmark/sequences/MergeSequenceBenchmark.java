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

package org.apache.druid.benchmark.sequences;

import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.guava.MergeSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class MergeSequenceBenchmark
{

  // Number of Sequences to Merge
  @Param({"1000"})
  int count;

  // Number of elements in each sequence
  @Param({"1000", "10000"})
  int sequenceLength;

  // Number of sequences to merge at once
  @Param({"10", "100"})
  int mergeAtOnce;

  private List<Sequence<Integer>> sequences;

  @Setup
  public void setup()
  {
    Random rand = ThreadLocalRandom.current();
    sequences = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      int[] sequence = new int[sequenceLength];
      for (int j = 0; j < sequenceLength; j++) {
        sequence[j] = rand.nextInt();
      }
      sequences.add(Sequences.simple(Ints.asList(sequence)));
    }

  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void mergeHierarchical(Blackhole blackhole)
  {
    Iterator<Sequence<Integer>> iterator = sequences.iterator();
    List<Sequence<Integer>> partialMerged = new ArrayList<Sequence<Integer>>();
    List<Sequence<Integer>> toMerge = new ArrayList<Sequence<Integer>>();

    while (iterator.hasNext()) {
      toMerge.add(iterator.next());
      if (toMerge.size() == mergeAtOnce) {
        partialMerged.add(new MergeSequence<Integer>(Ordering.natural(), Sequences.simple(toMerge)));
        toMerge = new ArrayList<Sequence<Integer>>();
      }
    }

    if (!toMerge.isEmpty()) {
      partialMerged.add(new MergeSequence<>(Ordering.natural(), Sequences.simple(toMerge)));
    }
    MergeSequence<Integer> mergeSequence = new MergeSequence<>(
        Ordering.natural(),
        Sequences.simple(partialMerged)
    );
    Integer accumulate = mergeSequence.accumulate(0, Integer::sum);
    blackhole.consume(accumulate);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void mergeFlat(final Blackhole blackhole)
  {
    MergeSequence<Integer> mergeSequence = new MergeSequence<>(Ordering.natural(), Sequences.simple(sequences));
    Integer accumulate = mergeSequence.accumulate(0, Integer::sum);
    blackhole.consume(accumulate);
  }
}
