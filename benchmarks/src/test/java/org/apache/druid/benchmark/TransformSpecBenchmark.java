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

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.transform.BaseTransformer;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.ScanTransformSpec;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Measures per-row throughput of the ingestion-time transform pipeline for the three shapes
 * streaming ingestion actually hits:
 *   NONE              — TransformSpec.NONE, baseline overhead of the transform wrapper.
 *   EXPRESSION        — TransformSpec with a single expression transform (upper(user)).
 *   SCAN_PASSTHROUGH  — ScanTransformSpec with no unnest (scan-query baseline over a 1-row cursor).
 *   SCAN_VC           — ScanTransformSpec with one scan-query virtual column (upper(user)), equivalent
 *                       work to EXPRESSION but via the scan pipeline.
 *   SCAN_UNNEST       — ScanTransformSpec that unnests an array of {@code unnestArraySize} elements.
 *
 * Output is nanoseconds per input row. For SCAN_UNNEST, use {@link #totalOutputRowsPerInput()} to
 * translate to ns/output-row.
 *
 * The input row is a {@link MapBasedInputRow} matching what Kafka JSON ingestion produces.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@OperationsPerInvocation(TransformSpecBenchmark.NUM_ROWS)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(value = 1)
public class TransformSpecBenchmark extends InitializedNullHandlingTest
{
  public static final int NUM_ROWS = 10_000;
  private static final long TIMESTAMP = DateTimes.of("2024-01-01").getMillis();

  public enum Mode
  {
    NONE {
      @Override
      BaseTransformer makeTransformer()
      {
        return TransformSpec.NONE.toTransformer();
      }
    },
    EXPRESSION {
      @Override
      BaseTransformer makeTransformer()
      {
        return new TransformSpec(
            null,
            ImmutableList.of(
                new ExpressionTransform("upper_user", "upper(\"user\")", TestExprMacroTable.INSTANCE)
            )
        ).toTransformer();
      }
    },
    SCAN_PASSTHROUGH {
      @Override
      BaseTransformer makeTransformer()
      {
        return new ScanTransformSpec(
            Druids.newScanQueryBuilder()
                  .dataSource(new TableDataSource("__input__"))
                  .eternityInterval()
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
                  .build()
        ).toTransformer();
      }
    },
    SCAN_VC {
      @Override
      BaseTransformer makeTransformer()
      {
        return new ScanTransformSpec(
            Druids.newScanQueryBuilder()
                  .dataSource(new TableDataSource("__input__"))
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "upper_user",
                          "upper(\"user\")",
                          ColumnType.STRING,
                          TestExprMacroTable.INSTANCE
                      )
                  )
                  .eternityInterval()
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
                  .build()
        ).toTransformer();
      }
    },
    SCAN_UNNEST {
      @Override
      BaseTransformer makeTransformer()
      {
        return new ScanTransformSpec(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource("__input__"),
                      new ExpressionVirtualColumn(
                          "tag",
                          "\"tags\"",
                          ColumnType.STRING,
                          ExprMacroTable.nil()
                      ),
                      null
                  ))
                  .eternityInterval()
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
                  .build()
        ).toTransformer();
      }
    };

    abstract BaseTransformer makeTransformer();
  }

  @Param({"NONE", "EXPRESSION", "SCAN_PASSTHROUGH", "SCAN_VC", "SCAN_UNNEST"})
  private Mode mode;

  /** Only affects {@link Mode#SCAN_UNNEST}; ignored by every other mode. */
  @Param({"5"})
  private int unnestArraySize;

  private BaseTransformer transformer;
  private List<InputRow> rows;

  @Setup
  public void setUp()
  {
    transformer = mode.makeTransformer();
    rows = new ArrayList<>(NUM_ROWS);
    final List<String> tags = new ArrayList<>(unnestArraySize);
    for (int i = 0; i < unnestArraySize; i++) {
      tags.add("tag" + i);
    }
    for (int i = 0; i < NUM_ROWS; i++) {
      final Map<String, Object> event = new LinkedHashMap<>();
      event.put("user", "user" + (i % 100));
      event.put("tags", tags);
      event.put("bytes_sent", (long) (i & 0xff));
      rows.add(new MapBasedInputRow(TIMESTAMP, ImmutableList.of("user", "tags", "bytes_sent"), event));
    }
  }

  @Benchmark
  public void transformRows(final Blackhole blackhole)
  {
    if (transformer.hasMultiRowTransform()) {
      for (int i = 0; i < NUM_ROWS; i++) {
        final List<InputRow> out = transformer.transformToList(rows.get(i));
        for (int j = 0; j < out.size(); j++) {
          blackhole.consume(out.get(j));
        }
      }
    } else {
      for (int i = 0; i < NUM_ROWS; i++) {
        blackhole.consume(transformer.transform(rows.get(i)));
      }
    }
  }

  /** Useful when translating ns/input-row → ns/output-row for unnest modes. */
  public int totalOutputRowsPerInput()
  {
    return mode == Mode.SCAN_UNNEST ? unnestArraySize : 1;
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(TransformSpecBenchmark.class.getSimpleName())
        .build();
    new Runner(opt).run();
  }
}
