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

import org.apache.druid.common.guava.CombiningSequence;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.MergeSequence;
import org.apache.druid.java.util.common.guava.ParallelMergeCombiningSequence;
import org.apache.druid.java.util.common.guava.ParallelMergeCombiningSequenceTest;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
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
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = "-XX:+UseG1GC")
@Warmup(iterations = 10)
@Measurement(iterations = 30)
public class ParallelMergeCombiningSequenceBenchmark
{
  private static final Logger log = new Logger(ParallelMergeCombiningSequenceBenchmark.class);
  private static final ForkJoinPool mergePool = new ForkJoinPool(
      (int) Math.ceil(Runtime.getRuntime().availableProcessors() * 1.5),
      ForkJoinPool.defaultForkJoinWorkerThreadFactory,
      (t, e) -> log.error(e, "Unhandled exception in thread [%s]", t),
      true
  );

  private static final ExecutorService consumer = Execs.multiThreaded(40, "mock-http-thread");

  @Param({"8", "16", "32", "64"})
  private int numSequences;

  @Param({"75000"})
  private int rowsPerSequence;

  @Param({"1", "2", "4", "8", "16", "32", "64"})
  private int concurrentSequenceConsumers;

  @Param({
      "combiningMergeSequence-same-thread",
      "parallelism-1-10ms-256-1024",
      "parallelism-4-10ms-256-1024",
      "parallelism-8-10ms-256-1024",
      "parallelism-16-10ms-256-1024",
      "parallelism-32-10ms-256-1024",
      "parallelism-1-100ms-512-4096",
      "parallelism-4-100ms-512-4096",
      "parallelism-8-100ms-512-4096",
      "parallelism-16-100ms-512-4096",
      "parallelism-32-100ms-512-4096",
      "parallelism-1-100ms-1024-4096",
      "parallelism-4-100ms-1024-4096",
      "parallelism-8-100ms-1024-4096",
      "parallelism-16-100ms-1024-4096",
      "parallelism-32-100ms-1024-4096",
      "parallelism-1-100ms-1024-16384",
      "parallelism-4-100ms-1024-16384",
      "parallelism-8-100ms-1024-16384",
      "parallelism-16-100ms-1024-16384",
      "parallelism-32-100ms-1024-16384",
      "parallelism-1-100ms-4096-16384",
      "parallelism-4-100ms-4096-16384",
      "parallelism-8-100ms-4096-16384",
      "parallelism-16-100ms-4096-16384",
      "parallelism-32-100ms-4096-16384",
  })
  private String strategy;

  @Param({
      "non-blocking",
      "initial-block-random-500ms",
      "initial-block-random-5000ms",
      "slow-sequence-random-100ms-1ms"
  })
  private String inputSequenceType;

  private List<Sequence<ParallelMergeCombiningSequenceTest.IntPair>> inputSequences;

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
    if ("initial".equals(inputSequenceTypeSplit[0])) {
      // e.g. "initial-block-random-{startDelay}ms"
      final int delayMillis = Integer.parseInt(inputSequenceTypeSplit[3].substring(0, inputSequenceTypeSplit[3].length() - 2));
      inputSequenceFactory = () ->
          ParallelMergeCombiningSequenceTest.initialDelaySequence(rowsPerSequence, delayMillis);
    } else if("slow".equals(inputSequenceTypeSplit[0])) {
      // e.g. "slow-sequence-random-{startDelay}ms-{frequencyDelay}ms"
      final int startDelayMillis = Integer.parseInt(inputSequenceTypeSplit[3].substring(0,inputSequenceTypeSplit[3].length() - 2));
      final int delayMillis = Integer.parseInt(inputSequenceTypeSplit[4].substring(0, inputSequenceTypeSplit[4].length() - 2));
      final int frequency = rowsPerSequence / 10;
      inputSequenceFactory = () ->
          ParallelMergeCombiningSequenceTest.slowSequence(rowsPerSequence, frequency, startDelayMillis, delayMillis);
    } else { // non-blocking sequence
      inputSequenceFactory = () ->
          ParallelMergeCombiningSequenceTest.generateOrderedPairsSequence(rowsPerSequence);
    }

    inputSequences = new ArrayList<>(numSequences);
    for (int i = 0; i < numSequences; i++) {
      inputSequences.add(inputSequenceFactory.get());
    }

    String[] strategySplit = strategy.split("-");
    if ("parallelism".equals(strategySplit[0])) {
      // "parallelism-{parallelism}-{targetTime}ms-{batchSize}-{yieldAfter}"
      parallelism = Integer.parseInt(strategySplit[1]);
      targetTaskTimeMillis = Integer.parseInt(strategySplit[2].substring(0,strategySplit[2].length() - 2));
      batchSize = Integer.parseInt(strategySplit[3]);
      yieldAfter = Integer.parseInt(strategySplit[4]);
      outputSequenceFactory = () -> createParallelSequence();
    } else {
      outputSequenceFactory = () -> createCombiningMergeSequence();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void exec(Blackhole blackhole) throws Exception
  {
    List<Future> futures = new ArrayList<>(concurrentSequenceConsumers);
    for (int i = 0; i < concurrentSequenceConsumers; i++) {
      futures.add(
          consumer.submit(() -> {
            try {
              consumeSequence(blackhole);
            } catch (Exception anyException) {
              log.error(anyException, "benchmark failed");
            }
          })
      );
    }

    for (int i = 0; i < concurrentSequenceConsumers; i++) {
      blackhole.consume(futures.get(i).get());
    }
    blackhole.consume(futures);
  }

  private Sequence<ParallelMergeCombiningSequenceTest.IntPair> createParallelSequence()
  {
    return new ParallelMergeCombiningSequence<>(
        mergePool,
        inputSequences,
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
        new MergeSequence<>(ParallelMergeCombiningSequenceTest.INT_PAIR_ORDERING, Sequences.simple(inputSequences)),
        ParallelMergeCombiningSequenceTest.INT_PAIR_ORDERING,
        ParallelMergeCombiningSequenceTest.INT_PAIR_MERGE_FN
    );
  }

  private void consumeSequence(Blackhole blackhole)
  {
    Yielder<ParallelMergeCombiningSequenceTest.IntPair> yielder = Yielders.each(outputSequenceFactory.get());

    ParallelMergeCombiningSequenceTest.IntPair prev;
    while (!yielder.isDone()) {
      prev = yielder.get();
      blackhole.consume(prev);
      yielder = yielder.next(yielder.get());
    }
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(ParallelMergeCombiningSequenceBenchmark.class.getSimpleName())
        .forks(1)
        .syncIterations(true)
        .resultFormat(ResultFormatType.CSV)
        .result("parallel-merge-combining-sequence.csv")
        .build();

    new Runner(opt).run();
  }
}
