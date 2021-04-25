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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.segment.generator.DataGenerator;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexCreator;
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

import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class IndexIngestionBenchmark
{
  @Param({"75000"})
  private int rowsPerSegment;

  @Param({"basic"})
  private String schema;

  @Param({"true", "false"})
  private boolean rollup;

  @Param({"none", "moderate", "high"})
  private String rollupOpportunity;

  @Param({"onheap", "offheap"})
  private String indexType;

  private static final Logger log = new Logger(IndexIngestionBenchmark.class);
  private static final int RNG_SEED = 9999;

  static {
    NullHandling.initializeForTests();
  }

  private AppendableIndexSpec appendableIndexSpec;
  private IncrementalIndex<?> incIndex;
  private List<InputRow> rows;
  private GeneratorSchemaInfo schemaInfo;

  @Setup
  public void setup() throws JsonProcessingException
  {
    ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde());

    schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get(schema);

    // Creates an AppendableIndexSpec that corresponds to the indexType parametrization.
    // It is used in {@code makeIncIndex()} to instanciate an incremental-index of the specified type.
    appendableIndexSpec = IncrementalIndexCreator.parseIndexType(indexType);

    DataGenerator gen = new DataGenerator(
        schemaInfo.getColumnSchemas(),
        RNG_SEED,
        schemaInfo.getDataInterval().getStartMillis(),
        IndexPersistBenchmark.getValuesPerTimestamp(rollupOpportunity),
        1000.0
    );

    rows = gen.toList(rowsPerSegment);
  }

  @Setup(Level.Invocation)
  public void setup2()
  {
    incIndex = makeIncIndex();
  }

  @TearDown(Level.Invocation)
  public void tearDown()
  {
    if (incIndex != null) {
      incIndex.close();
    }
  }

  private IncrementalIndex<?> makeIncIndex()
  {
    return appendableIndexSpec.builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withMetrics(schemaInfo.getAggsArray())
                .withRollup(rollup)
                .build()
        )
        .setMaxRowCount(rowsPerSegment * 2)
        .build();
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
}
