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


  /**
   * This encodes the type of input sequences and parameters that control their behavior.
   * 'non-blocking-sequence-{numRows}' uses {@link ParallelMergeCombiningSequenceTest#nonBlockingSequence} to as you
   * might expect create an input sequence that is lazily generated and will not block while being consumed.
   *
   * 'initially-blocking-sequence-{numRows}-{startDelayStart}-{startDelayEnd}ms' uses
   * {@link ParallelMergeCombiningSequenceTest#blockingSequence} to create a lazily generated input sequence that will
   * initially block for a random time within the range specified in the parameter, and will not perform any additional
   * blocking during further processing.
   *
   * 'blocking-sequence-{numRows}-{startDelayStart}-{startDelayEnd}ms-{numberOfTimesToBlock}-{frequencyDelay}ms' uses
   * {@link ParallelMergeCombiningSequenceTest#blockingSequence} to create a lazily generated input sequence that will
   * initially block for a random time within the range specified in the parameter, and additionally will randomly block
   * up to the number of occurrences for up to the delay encoded in the parameter.
   *
   * 'typical-distribution-sequence' will randomly produce a 'class' of input sequences at the following rates:
   * - 80% probability of a small result set which has a short initial delay on the order of tens to hundreds of millis
   *   and input row counts of up to a few thousand
   * - 20% probability produce a moderately large result set which has an initial delay in the range of a few seconds
   *   and input sequence row counts in the 50k-75k range
   * This input sequence is only useful when testing a large number of concurrent threads
   *
   * note: beware when using the blocking sequences for a direct comparison between strategies
   * at minimum they are useful for gauging behavior when sequences block, but because the sequences are not stable
   * between strategies or number of sequences, much less between iterations of the same strategy, compensation in
   * the form of running a lot of iterations could potentially make them more directly comparable
   */
  @Param({
      "non-blocking-sequence-1000",
      "non-blocking-sequence-75000",
      "non-blocking-sequence-10000000",
      "initially-blocking-sequence-1000-100-500ms",
      "initially-blocking-sequence-75000-100-500ms",
      "initially-blocking-sequence-10000000-100-500ms",
      "initially-blocking-sequence-1000-4000-5000ms",
      "initially-blocking-sequence-75000-4000-5000ms",
      "initially-blocking-sequence-10000000-4000-5000ms",
      "blocking-sequence-1000-10-500ms-10-1ms",
      "blocking-sequence-75000-10-500ms-10-1ms",
      "blocking-sequence-10000000-10-500ms-10-1ms",
      "typical-distribution-sequence"
  })
  String inputSequenceType;

  private int parallelism;
  private int targetTaskTimeMillis;
  private int batchSize;
  private int yieldAfter;

  private Supplier<Sequence<ParallelMergeCombiningSequenceTest.IntPair>> homogeneousInputSequenceFactory;
  private Function<Double, Sequence<ParallelMergeCombiningSequenceTest.IntPair>> heterogeneousInputSequenceFactory;
  private Supplier<Sequence<ParallelMergeCombiningSequenceTest.IntPair>> outputSequenceFactory;

  @Setup(Level.Trial)
  public void setup()
  {
    String[] inputSequenceTypeSplit = inputSequenceType.split("-");
    if ("initially".equals(inputSequenceTypeSplit[0])) {
      // e.g. "initially-blocking-sequence-{numRows}-{startDelayStart}-{startDelayEnd}ms"
      final int numRows = Integer.parseInt(inputSequenceTypeSplit[3]);
      final int startDelayStartMillis = Integer.parseInt(inputSequenceTypeSplit[4]);
      final int startDelayEndMillis = Integer.parseInt(
          inputSequenceTypeSplit[5].substring(0, inputSequenceTypeSplit[5].length() - 2)
      );
      homogeneousInputSequenceFactory = () ->
          ParallelMergeCombiningSequenceTest.blockingSequence(
              numRows,
              startDelayStartMillis,
              startDelayEndMillis,
              -1,
              0,
              true
          );
    } else if ("blocking".equals(inputSequenceTypeSplit[0])) {
      // e.g. "blocking-sequence-{numRows}-{startDelayStart}-{startDelayEnd}ms-{numberOfTimesToBlock}-{frequencyDelay}ms"
      final int numRows = Integer.parseInt(inputSequenceTypeSplit[2]);
      final int startDelayStartMillis = Integer.parseInt(inputSequenceTypeSplit[3]);
      final int startDelayEndMillis = Integer.parseInt(
          inputSequenceTypeSplit[4].substring(0, inputSequenceTypeSplit[4].length() - 2)
      );
      final int numberOfTimesToBlock = Integer.parseInt(inputSequenceTypeSplit[5]);
      final int maxIterationDelayMillis = Integer.parseInt(
          inputSequenceTypeSplit[6].substring(0, inputSequenceTypeSplit[6].length() - 2)
      );
      final int frequency = numRows / numberOfTimesToBlock;
      homogeneousInputSequenceFactory = () ->
          ParallelMergeCombiningSequenceTest.blockingSequence(
              numRows,
              startDelayStartMillis,
              startDelayEndMillis,
              frequency,
              maxIterationDelayMillis,
              true
          );
    } else if ("non".equals(inputSequenceTypeSplit[0])) {
      // e.g. "non-blocking-sequence-{numRows}"
      final int numRows = Integer.parseInt(inputSequenceTypeSplit[3]);
      homogeneousInputSequenceFactory = () ->
          ParallelMergeCombiningSequenceTest.nonBlockingSequence(numRows, true);
    } else { // "typical distribution" input sequence
      // approximately 80% of threads will merge/combine small result sets between 500-10k results per input sequence
      // blocking for 50-200 ms before initial results are yielded
      // approximately 20% of threads will merge/combine moderate sized result sets between 50k-75k per input
      // sequence, blocking for 1000-2500 ms before initial results are yielded
      heterogeneousInputSequenceFactory = (d) -> {
        if (d < 0.80) { // small queries
          return ParallelMergeCombiningSequenceTest.blockingSequence(
              ThreadLocalRandom.current().nextInt(500, 10000),
              50,
              200,
              -1,
              0,
              true
          );
        } else { // moderately large queries
          return ParallelMergeCombiningSequenceTest.blockingSequence(
              ThreadLocalRandom.current().nextInt(50_000, 75_000),
              1000,
              2500,
              -1,
              0,
              true
          );
        }
      };
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


  private List<Sequence<ParallelMergeCombiningSequenceTest.IntPair>> createInputSequences()
  {
    List<Sequence<ParallelMergeCombiningSequenceTest.IntPair>> inputSequences = new ArrayList<>(numSequences);
    final double d = ThreadLocalRandom.current().nextDouble(0.0, 1.0);
    for (int j = 0; j < numSequences; j++) {
      if (heterogeneousInputSequenceFactory != null) {
        inputSequences.add(heterogeneousInputSequenceFactory.apply(d));
      } else {
        inputSequences.add(homogeneousInputSequenceFactory.get());
      }
    }
    return inputSequences;
  }

  private Sequence<ParallelMergeCombiningSequenceTest.IntPair> createParallelSequence()
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

  private Sequence<ParallelMergeCombiningSequenceTest.IntPair> createCombiningMergeSequence()
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
