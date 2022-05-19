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

import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.ParallelMergeCombiningSequenceTest;
import org.apache.druid.java.util.common.guava.Sequence;
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
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;


@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = "-XX:+UseG1GC")
@Warmup(iterations = 5)
@Measurement(iterations = 25)
public class ParallelMergeCombiningSequenceBenchmark extends BaseParallelMergeCombiningSequenceBenchmark
{
  private static final Logger log = new Logger(ParallelMergeCombiningSequenceBenchmark.class);
  // this should be as large as the largest value of concurrentSequenceConsumers
  private static final ExecutorService CONSUMER_POOL = Execs.multiThreaded(64, "mock-http-thread");

  /**
   * Number of threads to run on {@link #CONSUMER_POOL}, each running {@link #consumeSequence}
   */
  @Param({
      "1",
      "2",
      "4",
      "8",
      "16",
      "32",
      "64"
  })
  private int concurrentSequenceConsumers;

  /**
   * Offset to start each thread of {@link #concurrentSequenceConsumers}
   */
  @Param({
      "0",
      "10",
      "100",
      "500",
      "1000"
  })
  private int concurrentConsumerDelayMillis;

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

  private Supplier<Sequence<ParallelMergeCombiningSequenceTest.IntPair>> homogeneousInputSequenceFactory;
  private Function<Double, Sequence<ParallelMergeCombiningSequenceTest.IntPair>> heterogeneousInputSequenceFactory;

  @Setup(Level.Trial)
  public void setupInputSequenceGenerator()
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
          return generateSmallSequence();
        } else { // moderately large queries
          return generateModeratelyLargeSequence();
        }
      };
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void exec(Blackhole blackhole) throws Exception
  {
    List<Future> futures = createConsumers(blackhole, concurrentSequenceConsumers, concurrentConsumerDelayMillis);

    for (int i = 0; i < concurrentSequenceConsumers; i++) {
      blackhole.consume(futures.get(i).get());
    }
    blackhole.consume(futures);
  }

  private List<Future> createConsumers(Blackhole blackhole, int consumers, int delayMillis) throws Exception
  {
    List<Future> futures = new ArrayList<>(consumers);
    for (int i = 0; i < consumers; i++) {
      if (delayMillis > 0) {
        Thread.sleep(delayMillis);
      }
      if (heterogeneousInputSequenceFactory != null) {
        double d = ThreadLocalRandom.current().nextDouble(0.0, 1.0);
        futures.add(
            CONSUMER_POOL.submit(() -> consumeSequence(blackhole, () -> heterogeneousInputSequenceFactory.apply(d)))
        );
      } else {
        futures.add(CONSUMER_POOL.submit(() -> consumeSequence(blackhole, homogeneousInputSequenceFactory)));
      }
    }

    return futures;
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
