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
import org.apache.druid.java.util.common.logger.Logger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
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
import java.util.concurrent.TimeUnit;


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
      futures.add(CONSUMER_POOL.submit(() -> consumeSequence(blackhole)));
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
