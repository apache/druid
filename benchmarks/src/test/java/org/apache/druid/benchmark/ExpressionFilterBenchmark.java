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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.generator.GeneratorColumnSchema;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 15)
@Measurement(iterations = 30)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ExpressionFilterBenchmark
{
  static {
    NullHandling.initializeForTests();
    ExpressionProcessing.initializeForTests();
  }

  @Param({"1000000"})
  private int rowsPerSegment;

  private QueryableIndex index;
  private Closer closer;

  private ExpressionDimFilter expressionFilter;
  private DimFilter nativeFilter;

  @Setup(Level.Trial)
  public void setup()
  {
    this.closer = Closer.create();

    final GeneratorSchemaInfo schemaInfo = new GeneratorSchemaInfo(
        ImmutableList.of(
            GeneratorColumnSchema.makeEnumerated(
                "x",
                ValueType.STRING,
                false,
                3,
                null,
                Arrays.asList("Apple", "Orange", "Xylophone", "Corundum", null),
                Arrays.asList(0.2, 0.25, 0.15, 0.10, 0.3)
            ),
            GeneratorColumnSchema.makeEnumerated(
                "y",
                ValueType.STRING,
                false,
                4,
                null,
                Arrays.asList("Hello", "World", "Foo", "Bar", "Baz"),
                Arrays.asList(0.2, 0.25, 0.15, 0.10, 0.3)
            )
        ),
        ImmutableList.of(),
        Intervals.of("2000/P1D"),
        false
    );

    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource("foo")
                                               .interval(schemaInfo.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .size(0)
                                               .build();

    final SegmentGenerator segmentGenerator = closer.register(new SegmentGenerator());
    this.index = closer.register(
        segmentGenerator.generate(dataSegment, schemaInfo, Granularities.NONE, rowsPerSegment)
    );

    expressionFilter = new ExpressionDimFilter(
        "array_contains(x, ['Orange', 'Xylophone'])",
        TestExprMacroTable.INSTANCE
    );
    nativeFilter = new AndDimFilter(
        new SelectorDimFilter("x", "Orange", null),
        new SelectorDimFilter("x", "Xylophone", null)
    );
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    closer.close();
  }

  @Benchmark
  public void expressionFilter(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = new QueryableIndexStorageAdapter(index).makeCursors(
        expressionFilter.toFilter(),
        index.getDataInterval(),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );
    final List<?> results = cursors
        .map(cursor -> {
          final ColumnValueSelector selector = cursor.getColumnSelectorFactory().makeColumnValueSelector("x");
          consumeString(cursor, selector, blackhole);
          return null;
        })
        .toList();

    blackhole.consume(results);
  }

  @Benchmark
  public void nativeFilter(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = new QueryableIndexStorageAdapter(index).makeCursors(
        nativeFilter.toFilter(),
        index.getDataInterval(),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );
    final List<?> results = cursors
        .map(cursor -> {
          final ColumnValueSelector selector = cursor.getColumnSelectorFactory().makeColumnValueSelector("x");
          consumeString(cursor, selector, blackhole);
          return null;
        })
        .toList();

    blackhole.consume(results);
  }

  private void consumeString(final Cursor cursor, final ColumnValueSelector selector, final Blackhole blackhole)
  {
    while (!cursor.isDone()) {
      blackhole.consume(selector.getLong());
      cursor.advance();
    }
  }
}
