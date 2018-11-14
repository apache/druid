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

package org.apache.druid.benchmark.indexing;

import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.apache.druid.benchmark.datagen.BenchmarkDataGenerator;
import org.apache.druid.benchmark.datagen.BenchmarkSchemaInfo;
import org.apache.druid.benchmark.datagen.BenchmarkSchemas;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.hll.HyperLogLogHash;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
@Threads(8)
public class IndexIngestionMultiThreadedBenchmark
{
  @Param({"10000"})
  private int rowsPerSegmentPerThread;

  @Param({"basic"})
  private String schema;

  @Param({"true", "false"})
  private boolean rollup;

  @Param({"oak", "onheap"})
  private String indexType;

  private static final Logger log = new Logger(IndexIngestionMultiThreadedBenchmark.class);
  private static final int RNG_SEED = 9999;

  private IncrementalIndex incIndex;
  private ArrayList<InputRow> rows;
  private BenchmarkSchemaInfo schemaInfo;
  private AtomicInteger startIndexdistributer = new AtomicInteger(0);

  @Setup
  public void setup(BenchmarkParams params)
  {
    int threads = params.getThreads();
    ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde(HyperLogLogHash.getDefault()));

    rows = new ArrayList<InputRow>();
    schemaInfo = BenchmarkSchemas.SCHEMA_MAP.get(schema);

    BenchmarkDataGenerator gen = new BenchmarkDataGenerator(
            schemaInfo.getColumnSchemas(),
            RNG_SEED,
            schemaInfo.getDataInterval(),
            rowsPerSegmentPerThread * threads
    );

    for (int i = 0; i < rowsPerSegmentPerThread * threads; i++) {
      InputRow row = gen.nextRow();
      if (i % 10000 == 0) {
        log.info(i + " rows generated.");
      }
      rows.add(row);
    }
  }

  @TearDown(Level.Iteration)
  public void tearDown()
  {
    incIndex.close();
  }

  @State(Scope.Thread)
  public static class ThreadState
  {
    int startIndex;

    @Setup
    public void setup(IndexIngestionMultiThreadedBenchmark state)
    {
      startIndex = state.startIndexdistributer.getAndIncrement();
    }
  }


  @Setup(Level.Iteration)
  public void setup2(BenchmarkParams params)
  {
    incIndex = makeIncIndex(params.getThreads() * rowsPerSegmentPerThread * 2);
  }

  private IncrementalIndex makeIncIndex(int maxRowCount)
  {
    switch (indexType) {
      case "oak":
      {
        return new IncrementalIndex.Builder()
                .setIndexSchema(
                        new IncrementalIndexSchema.Builder()
                                .withMetrics(schemaInfo.getAggsArray())
                                .withRollup(rollup)
                                .build()
                )
                .setReportParseExceptions(false)
                .setMaxRowCount(maxRowCount)
                .buildOffheapOak();
      }
      case "onheap": {
        return new IncrementalIndex.Builder()
                .setIndexSchema(
                        new IncrementalIndexSchema.Builder()
                                .withMetrics(schemaInfo.getAggsArray())
                                .withRollup(rollup)
                                .build()
                )
                .setReportParseExceptions(false)
                .setMaxRowCount(maxRowCount)
                .buildOnheap();
      }
      default:
        return null;
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void addRows(Blackhole blackhole, ThreadState threadState) throws Exception
  {
    for (int i = 0; i < rowsPerSegmentPerThread; i++) {
      InputRow row = rows.get(i + threadState.startIndex);
      int rv = incIndex.add(row).getRowCount();
      blackhole.consume(rv);
    }
  }


  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
            .include(IndexIngestionMultiThreadedBenchmark.class.getSimpleName())
            .threads(8)
            .forks(0)
            .build();

    new Runner(opt).run();
  }

}
