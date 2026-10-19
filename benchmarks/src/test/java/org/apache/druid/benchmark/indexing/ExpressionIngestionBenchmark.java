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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Measures on-heap incremental ingestion of 100,000 rows into 25,000 rollup keys using three expression metrics.
 * Input-row construction is performed once per trial and excluded from the measured invocation.
 */
@State(Scope.Benchmark)
@Fork(3)
@Warmup(iterations = 3)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ExpressionIngestionBenchmark
{
  private static final int ROW_COUNT = 100_000;
  private static final int ROLLUP_KEY_COUNT = 25_000;

  private List<InputRow> rows;
  private AggregatorFactory[] metrics;

  @Setup(Level.Trial)
  public void setup()
  {
    ExpressionProcessing.initializeForTests();
    rows = makeRows();

    final ExprMacroTable macroTable = ExprMacroTable.nil();
    metrics = new AggregatorFactory[]{
        new DoubleSumAggregatorFactory(
            "exprDouble1",
            null,
            "if(x > 0, x * y + z, y - z)",
            macroTable
        ),
        new DoubleSumAggregatorFactory(
            "exprDouble2",
            null,
            "x + y * z",
            macroTable
        ),
        new LongSumAggregatorFactory(
            "exprLong",
            null,
            "if(x > 0, x + y, z)",
            macroTable
        )
    };
  }

  @Benchmark
  public int ingest()
  {
    try (
        final OnheapIncrementalIndex index = (OnheapIncrementalIndex) new OnheapIncrementalIndex.Builder()
            .setIndexSchema(
                new IncrementalIndexSchema.Builder()
                    .withDimensionsSpec(
                        new DimensionsSpec(Collections.singletonList(new StringDimensionSchema("key")))
                    )
                    .withMetrics(metrics)
                    .withRollup(true)
                    .build()
            )
            .setMaxRowCount(ROW_COUNT + 1)
            .build()
    ) {
      for (final InputRow row : rows) {
        index.add(row);
      }
      return index.numRows();
    }
  }

  private static List<InputRow> makeRows()
  {
    final List<InputRow> rows = new ArrayList<>(ROW_COUNT);
    for (int rowNumber = 0; rowNumber < ROW_COUNT; rowNumber++) {
      final Map<String, Object> event = new HashMap<>();
      event.put("key", "key-" + rowNumber % ROLLUP_KEY_COUNT);
      event.put("x", (long) (rowNumber % 1_000) - 500L);
      event.put("y", (long) (rowNumber % 100));
      event.put("z", (long) (rowNumber % 17));
      rows.add(new MapBasedInputRow(0L, Collections.singletonList("key"), event));
    }
    return rows;
  }
}
