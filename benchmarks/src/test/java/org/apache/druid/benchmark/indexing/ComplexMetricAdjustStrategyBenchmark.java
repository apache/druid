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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchModule;
import org.apache.druid.query.aggregation.datasketches.theta.SketchModule;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.segment.generator.DataGenerator;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.serde.ComplexMetrics;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class ComplexMetricAdjustStrategyBenchmark
{
  private static final Logger log = new Logger(ComplexMetricAdjustStrategyBenchmark.class);
  private static final int RNG_SEED = 9999;

  static {
    NullHandling.initializeForTests();
  }

  @Param({"20000000000"})
  private long maxBytesInMemory;
  @Param({"75000"})
  private int rowsPerSegment;
  @Param({"rollupHigh", "rollupModerate", "rollupLow"})
  private String schema;
  @Param({"true"})
  private boolean rollup;
  @Param({"true", "false"})
  private boolean adjustFlag;
  @Param({"1000", "10000"})
  private int adjustMaxRows;
  @Param({"1000"})
  private int adjustMaxTimeMs;

  private IncrementalIndex incIndex;
  private ArrayList<InputRow> rows;
  private GeneratorSchemaInfo schemaInfo;

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(ComplexMetricAdjustStrategyBenchmark.class.getSimpleName())
        .forks(1)
        .build();

    new Runner(opt).run();
  }

  @Setup
  public void setup()
  {
    ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde());
    SketchModule.registerSerde();
    DoublesSketchModule.registerSerde();
  }

  @Setup(Level.Invocation)
  public void setup2()
  {
    schemaInfo = GeneratorComplexSchemas.SCHEMA_MAP.get(schema);
    incIndex = makeIncIndex();

    rows = new ArrayList<InputRow>();

    DataGenerator gen = new DataGenerator(
        schemaInfo.getColumnSchemas(),
        RNG_SEED,
        schemaInfo.getDataInterval(),
        rowsPerSegment
    );

    for (int i = 0; i < rowsPerSegment; i++) {
      InputRow row = gen.nextRow();
      if (i % 10000 == 0) {
        log.info(i + " rows generated.");
      }
      rows.add(row);
    }
  }

  private IncrementalIndex makeIncIndex()
  {
    return new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withMetrics(schemaInfo.getAggsArray())
                .withRollup(rollup)
                .build()
        )
        .setMaxRowCount(rowsPerSegment * 10)
        .setMaxBytesInMemory(maxBytesInMemory * 10)
        .setAdjustmentBytesInMemoryFlag(adjustFlag)
        .setAdjustmentBytesInMemoryMaxRollupRows(adjustMaxRows)
        .setadjustmentBytesInMemoryMaxTimeMs(adjustMaxTimeMs)
        .buildOnheap();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void addRows(Blackhole blackhole) throws Exception
  {
    for (int i = 0; i < rowsPerSegment; i++) {
      InputRow row = rows.get(i);
      int rv = incIndex.add(row).getRowCount();
      blackhole.consume(rv);
    }
  }

  @TearDown(Level.Iteration)
  public void teardown()
  {
    incIndex.close();
    incIndex = null;
  }
}
