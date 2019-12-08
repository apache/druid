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

import org.apache.druid.common.guava.CombiningSequence;
import org.apache.druid.java.util.common.guava.MergeSequence;
import org.apache.druid.java.util.common.guava.ParallelMergeCombiningSequence;
import org.apache.druid.java.util.common.guava.ParallelMergeCombiningSequenceTest;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;

@State(Scope.Benchmark)
public class BaseParallelMergeCombiningSequenceBenchmark
{
  private static final Logger log = new Logger(ParallelMergeCombiningSequenceBenchmark.class);
  // default merge FJP size
  static final ForkJoinPool MERGE_POOL = new ForkJoinPool(
      (int) Math.ceil(Runtime.getRuntime().availableProcessors() * 0.75),
      ForkJoinPool.defaultForkJoinWorkerThreadFactory,
      (t, e) -> log.error(e, "Unhandled exception in thread [%s]", t),
      true
  );

  // note: parameters are broken down one per line to allow easily commenting out lines to mix and match which
  // benchmarks to run
  // also note: don't really run this like it is unless you have days to spare
  @Param({
      "8",
      "16",
      "32",
      "64"
  })
  int numSequences;

  /**
   * Strategy encodes the type of sequence and configuration parameters for that sequence.
   *
   * Strategies of the form: 'parallelism-{parallelism}-{targetTime}ms-{batchSize}-{yieldAfter}'
   * encode the parameters for a {@link ParallelMergeCombiningSequence}.
   *
   * A strategy of: 'combiningMergeSequence-same-thread' (or an unrecognized value) will use a
   * {@link CombiningSequence} that wraps a {@link MergeSequence}
   */
  @Param({
      "combiningMergeSequence-same-thread",
      "parallelism-1-10ms-256-1024",
      "parallelism-4-10ms-256-1024",
      "parallelism-8-10ms-256-1024",
      "parallelism-16-10ms-256-1024",
      "parallelism-1-100ms-1024-4096",
      "parallelism-4-100ms-1024-4096",
      "parallelism-8-100ms-1024-4096",
      "parallelism-16-100ms-1024-4096",
      "parallelism-1-100ms-4096-16384",
      "parallelism-4-100ms-4096-16384",
      "parallelism-8-100ms-4096-16384",
      "parallelism-16-100ms-4096-16384"
  })
  String strategy;

  private int parallelism;
  private int targetTaskTimeMillis;
  private int batchSize;
  private int yieldAfter;

  private Function<List<Sequence<ParallelMergeCombiningSequenceTest.IntPair>>, Sequence<ParallelMergeCombiningSequenceTest.IntPair>> outputSequenceFactory;

  @Setup(Level.Trial)
  public void setup()
  {
    setupOutputSequence();
  }

  void setupOutputSequence()
  {
    String[] strategySplit = strategy.split("-");
    if ("parallelism".equals(strategySplit[0])) {
      // "parallelism-{parallelism}-{targetTime}ms-{batchSize}-{yieldAfter}"
      parallelism = Integer.parseInt(strategySplit[1]);
      targetTaskTimeMillis = Integer.parseInt(strategySplit[2].substring(0, strategySplit[2].length() - 2));
      batchSize = Integer.parseInt(strategySplit[3]);
      yieldAfter = Integer.parseInt(strategySplit[4]);
      outputSequenceFactory = this::createParallelSequence;
    } else {
      outputSequenceFactory = this::createCombiningMergeSequence;
    }
  }


  Sequence<ParallelMergeCombiningSequenceTest.IntPair> createParallelSequence(
      List<Sequence<ParallelMergeCombiningSequenceTest.IntPair>> inputSequences
  )
  {
    return new ParallelMergeCombiningSequence<>(
        MERGE_POOL,
        inputSequences,
        ParallelMergeCombiningSequenceTest.INT_PAIR_ORDERING,
        ParallelMergeCombiningSequenceTest.INT_PAIR_MERGE_FN,
        false,
        0,
        0,
        parallelism,
        yieldAfter,
        batchSize,
        targetTaskTimeMillis,
        null
    );
  }

  Sequence<ParallelMergeCombiningSequenceTest.IntPair> createCombiningMergeSequence(
      List<Sequence<ParallelMergeCombiningSequenceTest.IntPair>> inputSequences
  )
  {
    return CombiningSequence.create(
        new MergeSequence<>(
            ParallelMergeCombiningSequenceTest.INT_PAIR_ORDERING,
            Sequences.simple(inputSequences)
        ),
        ParallelMergeCombiningSequenceTest.INT_PAIR_ORDERING,
        ParallelMergeCombiningSequenceTest.INT_PAIR_MERGE_FN
    );
  }

  void consumeSequence(Blackhole blackhole, Supplier<Sequence<ParallelMergeCombiningSequenceTest.IntPair>> supplier)
  {
    try {
      Yielder<ParallelMergeCombiningSequenceTest.IntPair> yielder =
          Yielders.each(outputSequenceFactory.apply(createInputSequences(supplier)));

      ParallelMergeCombiningSequenceTest.IntPair prev;
      while (!yielder.isDone()) {
        prev = yielder.get();
        blackhole.consume(prev);
        yielder = yielder.next(yielder.get());
      }
    }
    catch (Exception anyException) {
      log.error(anyException, "benchmark failed");
      throw new RuntimeException(anyException);
    }
  }

  List<Sequence<ParallelMergeCombiningSequenceTest.IntPair>> createInputSequences(
      Supplier<Sequence<ParallelMergeCombiningSequenceTest.IntPair>> supplier
  )
  {
    List<Sequence<ParallelMergeCombiningSequenceTest.IntPair>> inputSequences = new ArrayList<>(numSequences);
    for (int j = 0; j < numSequences; j++) {
      inputSequences.add(supplier.get());
    }
    return inputSequences;
  }

  Sequence<ParallelMergeCombiningSequenceTest.IntPair> generateSmallSequence()
  {
    return ParallelMergeCombiningSequenceTest.blockingSequence(
        ThreadLocalRandom.current().nextInt(500, 10000),
        50,
        200,
        -1,
        0,
        true
    );
  }

  Sequence<ParallelMergeCombiningSequenceTest.IntPair> generateModeratelyLargeSequence()
  {
    return ParallelMergeCombiningSequenceTest.blockingSequence(
        ThreadLocalRandom.current().nextInt(50_000, 75_000),
        1000,
        2500,
        -1,
        0,
        true
    );
  }

  Sequence<ParallelMergeCombiningSequenceTest.IntPair> generateLargeSequence()
  {
    final int numRows = ThreadLocalRandom.current().nextInt(1_500_000, 10_000_000);
    final int frequency = numRows / 5;
    return ParallelMergeCombiningSequenceTest.blockingSequence(
        numRows,
        5000,
        10000,
        frequency,
        10,
        true
    );
  }
}
