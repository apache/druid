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
import org.apache.druid.benchmark.datagen.BenchmarkColumnSchema;
import org.apache.druid.benchmark.datagen.BenchmarkSchemaInfo;
import org.apache.druid.benchmark.datagen.SegmentGenerator;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.extraction.StrlenExtractionFn;
import org.apache.druid.query.extraction.TimeFormatExtractionFn;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
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

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 15)
@Measurement(iterations = 30)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ExpressionSelectorBenchmark
{
  @Param({"1000000"})
  private int rowsPerSegment;

  private QueryableIndex index;
  private Closer closer;

  @Setup(Level.Trial)
  public void setup()
  {
    this.closer = Closer.create();

    final BenchmarkSchemaInfo schemaInfo = new BenchmarkSchemaInfo(
        ImmutableList.of(
            BenchmarkColumnSchema.makeZipf(
                "n",
                ValueType.LONG,
                false,
                1,
                0d,
                1000,
                10000,
                3d
            ),
            BenchmarkColumnSchema.makeZipf(
                "s",
                ValueType.STRING,
                false,
                1,
                0d,
                1000,
                10000,
                3d
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
                                               .build();

    final SegmentGenerator segmentGenerator = closer.register(new SegmentGenerator());
    this.index = closer.register(
        segmentGenerator.generate(dataSegment, schemaInfo, Granularities.HOUR, rowsPerSegment)
    );
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    closer.close();
  }

  @Benchmark
  public void timeFloorUsingExpression(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = new QueryableIndexStorageAdapter(index).makeCursors(
        null,
        index.getDataInterval(),
        VirtualColumns.create(
            ImmutableList.of(
                new ExpressionVirtualColumn(
                    "v",
                    "timestamp_floor(__time, 'PT1H')",
                    ValueType.LONG,
                    TestExprMacroTable.INSTANCE
                )
            )
        ),
        Granularities.ALL,
        false,
        null
    );

    final List<?> results = cursors
        .map(cursor -> {
          final ColumnValueSelector selector = cursor.getColumnSelectorFactory().makeColumnValueSelector("v");
          consumeLong(cursor, selector, blackhole);
          return null;
        })
        .toList();

    blackhole.consume(results);
  }

  @Benchmark
  public void timeFloorUsingExtractionFn(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = new QueryableIndexStorageAdapter(index).makeCursors(
        null,
        index.getDataInterval(),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    final List<?> results = cursors
        .map(cursor -> {
          final DimensionSelector selector = cursor
              .getColumnSelectorFactory()
              .makeDimensionSelector(
                  new ExtractionDimensionSpec(
                      ColumnHolder.TIME_COLUMN_NAME,
                      "v",
                      new TimeFormatExtractionFn(null, null, null, Granularities.HOUR, true)
                  )
              );
          consumeDimension(cursor, selector, blackhole);
          return null;
        })
        .toList();

    blackhole.consume(results);
  }

  @Benchmark
  public void timeFloorUsingCursor(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = new QueryableIndexStorageAdapter(index).makeCursors(
        null,
        index.getDataInterval(),
        VirtualColumns.EMPTY,
        Granularities.HOUR,
        false,
        null
    );

    final List<Long> results = cursors
        .map(cursor -> {
          long count = 0L;
          while (!cursor.isDone()) {
            count++;
            cursor.advance();
          }
          return count;
        })
        .toList();

    long count = 0L;
    for (Long result : results) {
      count += result;
    }

    blackhole.consume(count);
  }

  @Benchmark
  public void timeFormatUsingExpression(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = new QueryableIndexStorageAdapter(index).makeCursors(
        null,
        index.getDataInterval(),
        VirtualColumns.create(
            ImmutableList.of(
                new ExpressionVirtualColumn(
                    "v",
                    "timestamp_format(__time, 'yyyy-MM-dd')",
                    ValueType.STRING,
                    TestExprMacroTable.INSTANCE
                )
            )
        ),
        Granularities.ALL,
        false,
        null
    );

    final List<?> results = cursors
        .map(cursor -> {
          final DimensionSelector selector = cursor.getColumnSelectorFactory().makeDimensionSelector(
              DefaultDimensionSpec.of("v")
          );
          consumeDimension(cursor, selector, blackhole);
          return null;
        })
        .toList();

    blackhole.consume(results);
  }

  @Benchmark
  public void timeFormatUsingExtractionFn(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = new QueryableIndexStorageAdapter(index).makeCursors(
        null,
        index.getDataInterval(),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    final List<?> results = cursors
        .map(cursor -> {
          final DimensionSelector selector = cursor
              .getColumnSelectorFactory()
              .makeDimensionSelector(
                  new ExtractionDimensionSpec(
                      ColumnHolder.TIME_COLUMN_NAME,
                      "v",
                      new TimeFormatExtractionFn("yyyy-MM-dd", null, null, null, false)
                  )
              );
          consumeDimension(cursor, selector, blackhole);
          return null;
        })
        .toList();

    blackhole.consume(results);
  }

  @Benchmark
  public void strlenUsingExpressionAsLong(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = new QueryableIndexStorageAdapter(index).makeCursors(
        null,
        index.getDataInterval(),
        VirtualColumns.create(
            ImmutableList.of(
                new ExpressionVirtualColumn(
                    "v",
                    "strlen(s)",
                    ValueType.STRING,
                    TestExprMacroTable.INSTANCE
                )
            )
        ),
        Granularities.ALL,
        false,
        null
    );

    final List<?> results = cursors
        .map(cursor -> {
          final ColumnValueSelector selector = cursor.getColumnSelectorFactory().makeColumnValueSelector("v");
          consumeLong(cursor, selector, blackhole);
          return null;
        })
        .toList();

    blackhole.consume(results);
  }

  @Benchmark
  public void strlenUsingExpressionAsString(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = new QueryableIndexStorageAdapter(index).makeCursors(
        null,
        index.getDataInterval(),
        VirtualColumns.create(
            ImmutableList.of(
                new ExpressionVirtualColumn(
                    "v",
                    "strlen(s)",
                    ValueType.STRING,
                    TestExprMacroTable.INSTANCE
                )
            )
        ),
        Granularities.ALL,
        false,
        null
    );

    final List<?> results = cursors
        .map(cursor -> {
          final DimensionSelector selector = cursor
              .getColumnSelectorFactory()
              .makeDimensionSelector(new DefaultDimensionSpec("v", "v", ValueType.STRING));

          consumeDimension(cursor, selector, blackhole);
          return null;
        })
        .toList();

    blackhole.consume(results);
  }

  @Benchmark
  public void strlenUsingExtractionFn(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = new QueryableIndexStorageAdapter(index).makeCursors(
        null,
        index.getDataInterval(),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    final List<?> results = cursors
        .map(cursor -> {
          final DimensionSelector selector = cursor
              .getColumnSelectorFactory()
              .makeDimensionSelector(new ExtractionDimensionSpec("x", "v", StrlenExtractionFn.instance()));

          consumeDimension(cursor, selector, blackhole);
          return null;
        })
        .toList();

    blackhole.consume(results);
  }

  @Benchmark
  public void arithmeticOnLong(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = new QueryableIndexStorageAdapter(index).makeCursors(
        null,
        index.getDataInterval(),
        VirtualColumns.create(
            ImmutableList.of(
                new ExpressionVirtualColumn(
                    "v",
                    "n + 1",
                    ValueType.LONG,
                    TestExprMacroTable.INSTANCE
                )
            )
        ),
        Granularities.ALL,
        false,
        null
    );

    final List<?> results = cursors
        .map(cursor -> {
          final ColumnValueSelector selector = cursor.getColumnSelectorFactory().makeColumnValueSelector("v");
          consumeLong(cursor, selector, blackhole);
          return null;
        })
        .toList();

    blackhole.consume(results);
  }

  @Benchmark
  public void stringConcatAndCompareOnLong(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = new QueryableIndexStorageAdapter(index).makeCursors(
        null,
        index.getDataInterval(),
        VirtualColumns.create(
            ImmutableList.of(
                new ExpressionVirtualColumn(
                    "v",
                    "concat(n, ' is my favorite number') == '3 is my favorite number'",
                    ValueType.LONG,
                    TestExprMacroTable.INSTANCE
                )
            )
        ),
        Granularities.ALL,
        false,
        null
    );

    final List<?> results = cursors
        .map(cursor -> {
          final ColumnValueSelector selector = cursor.getColumnSelectorFactory().makeColumnValueSelector("v");
          consumeLong(cursor, selector, blackhole);
          return null;
        })
        .toList();

    blackhole.consume(results);
  }

  private void consumeDimension(final Cursor cursor, final DimensionSelector selector, final Blackhole blackhole)
  {
    if (selector.getValueCardinality() >= 0) {
      // Read all IDs and then lookup all names.
      final BitSet values = new BitSet();

      while (!cursor.isDone()) {
        final int value = selector.getRow().get(0);
        values.set(value);
        cursor.advance();
      }

      for (int i = values.nextSetBit(0); i >= 0; i = values.nextSetBit(i + 1)) {
        blackhole.consume(selector.lookupName(i));
      }
    } else {
      // Lookup names as we go.
      while (!cursor.isDone()) {
        final int value = selector.getRow().get(0);
        blackhole.consume(selector.lookupName(value));
        cursor.advance();
      }
    }
  }

  private void consumeLong(final Cursor cursor, final ColumnValueSelector selector, final Blackhole blackhole)
  {
    while (!cursor.isDone()) {
      blackhole.consume(selector.getLong());
      cursor.advance();
    }
  }
}
