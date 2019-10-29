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

  @Param({
      "1000",
      "75000",
      "10000000"
  })
  int rowsPerSequence;

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


  @Param({
      "non-blocking-sequence",
      // note: beware when using the blocking sequences for a direct comparison between strategies
      // at minimum they are useful for gauging behavior when sequences block, but because the sequences are not stable
      // between strategies or number of sequences, much less between iterations of the same strategy, compensation in
      // the form of running a lot of iterations could potentially make them more directly comparable
      "initially-blocking-sequence-100-500ms",
      "initially-blocking-sequence-4000-5000ms",
      "blocking-sequence-10-100ms-10-1ms"
  })
  String inputSequenceType;

  private int parallelism;
  private int targetTaskTimeMillis;
  private int batchSize;
  private int yieldAfter;

  private Supplier<Sequence<ParallelMergeCombiningSequenceTest.IntPair>> inputSequenceFactory;
  private Supplier<Sequence<ParallelMergeCombiningSequenceTest.IntPair>> outputSequenceFactory;

  @Setup(Level.Trial)
  public void setup()
  {
    String[] inputSequenceTypeSplit = inputSequenceType.split("-");
    if ("initially".equals(inputSequenceTypeSplit[0])) {
      // e.g. "initially-blocking-sequence-{startDelayStart}-{startDelayEnd}ms"
      final int startDelayStartMillis = Integer.parseInt(inputSequenceTypeSplit[3]);
      final int startDelayEndMillis = Integer.parseInt(
          inputSequenceTypeSplit[4].substring(0, inputSequenceTypeSplit[4].length() - 2)
      );
      inputSequenceFactory = () ->
          ParallelMergeCombiningSequenceTest.blockingSequence(
              rowsPerSequence,
              startDelayStartMillis,
              startDelayEndMillis,
              -1,
              0,
              true
          );
    } else if ("blocking".equals(inputSequenceTypeSplit[0])) {
      // e.g. "blocking-sequence-{startDelayStart}-{startDelayEnd}ms-{numberOfTimesToBlock}-{frequencyDelay}ms"
      final int startDelayStartMillis = Integer.parseInt(inputSequenceTypeSplit[2]);
      final int startDelayEndMillis = Integer.parseInt(
          inputSequenceTypeSplit[3].substring(0, inputSequenceTypeSplit[3].length() - 2)
      );
      final int numberOfTimesToBlock = Integer.parseInt(inputSequenceTypeSplit[4]);
      final int maxIterationDelayMillis = Integer.parseInt(
          inputSequenceTypeSplit[5].substring(0, inputSequenceTypeSplit[5].length() - 2)
      );
      final int frequency = rowsPerSequence / numberOfTimesToBlock;
      inputSequenceFactory = () ->
          ParallelMergeCombiningSequenceTest.blockingSequence(
              rowsPerSequence,
              startDelayStartMillis,
              startDelayEndMillis,
              frequency,
              maxIterationDelayMillis,
              true
          );
    } else { // non-blocking sequence
      inputSequenceFactory = () ->
          ParallelMergeCombiningSequenceTest.nonBlockingSequence(rowsPerSequence, true);
    }

    String[] strategySplit = strategy.split("-");
    if ("parallelism".equals(strategySplit[0])) {
      // "parallelism-{parallelism}-{targetTime}ms-{batchSize}-{yieldAfter}"
      parallelism = Integer.parseInt(strategySplit[1]);
      targetTaskTimeMillis = Integer.parseInt(strategySplit[2].substring(0, strategySplit[2].length() - 2));
      batchSize = Integer.parseInt(strategySplit[3]);
      yieldAfter = Integer.parseInt(strategySplit[4]);
      outputSequenceFactory = () -> createParallelSequence();
    } else {
      outputSequenceFactory = () -> createCombiningMergeSequence();
    }
  }


  List<Sequence<ParallelMergeCombiningSequenceTest.IntPair>> createInputSequences()
  {
    List<Sequence<ParallelMergeCombiningSequenceTest.IntPair>> inputSequences = new ArrayList<>(numSequences);
    for (int j = 0; j < numSequences; j++) {
      inputSequences.add(inputSequenceFactory.get());
    }
    return inputSequences;
  }

  Sequence<ParallelMergeCombiningSequenceTest.IntPair> createParallelSequence()
  {
    return new ParallelMergeCombiningSequence<>(
        MERGE_POOL,
        createInputSequences(),
        ParallelMergeCombiningSequenceTest.INT_PAIR_ORDERING,
        ParallelMergeCombiningSequenceTest.INT_PAIR_MERGE_FN,
        false,
        0,
        0,
        parallelism,
        yieldAfter,
        batchSize,
        targetTaskTimeMillis
    );
  }

  Sequence<ParallelMergeCombiningSequenceTest.IntPair> createCombiningMergeSequence()
  {
    return CombiningSequence.create(
        new MergeSequence<>(
            ParallelMergeCombiningSequenceTest.INT_PAIR_ORDERING,
            Sequences.simple(createInputSequences())
        ),
        ParallelMergeCombiningSequenceTest.INT_PAIR_ORDERING,
        ParallelMergeCombiningSequenceTest.INT_PAIR_MERGE_FN
    );
  }

  void consumeSequence(Blackhole blackhole)
  {
    try {
      Yielder<ParallelMergeCombiningSequenceTest.IntPair> yielder = Yielders.each(outputSequenceFactory.get());

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
}
