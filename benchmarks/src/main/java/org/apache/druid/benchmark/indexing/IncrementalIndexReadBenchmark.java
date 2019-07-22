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

import org.apache.druid.benchmark.datagen.BenchmarkDataGenerator;
import org.apache.druid.benchmark.datagen.BenchmarkSchemaInfo;
import org.apache.druid.benchmark.datagen.BenchmarkSchemas;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.JavaScriptDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.RegexDimFilter;
import org.apache.druid.query.filter.SearchQueryDimFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.search.ContainsSearchQuerySpec;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class IncrementalIndexReadBenchmark
{
  @Param({"750000"})
  private int rowsPerSegment;

  @Param({"basic"})
  private String schema;

  @Param({"true", "false"})
  private boolean rollup;

  private static final Logger log = new Logger(IncrementalIndexReadBenchmark.class);
  private static final int RNG_SEED = 9999;
  private IncrementalIndex incIndex;

  private BenchmarkSchemaInfo schemaInfo;

  @Setup
  public void setup() throws IOException
  {
    log.info("SETUP CALLED AT " + +System.currentTimeMillis());

    ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde());

    schemaInfo = BenchmarkSchemas.SCHEMA_MAP.get(schema);

    BenchmarkDataGenerator gen = new BenchmarkDataGenerator(
        schemaInfo.getColumnSchemas(),
        RNG_SEED,
        schemaInfo.getDataInterval(),
        rowsPerSegment
    );

    incIndex = makeIncIndex();

    for (int j = 0; j < rowsPerSegment; j++) {
      InputRow row = gen.nextRow();
      if (j % 10000 == 0) {
        log.info(j + " rows generated.");
      }
      incIndex.add(row);
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
        .setReportParseExceptions(false)
        .setMaxRowCount(rowsPerSegment)
        .buildOnheap();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void read(Blackhole blackhole)
  {
    IncrementalIndexStorageAdapter sa = new IncrementalIndexStorageAdapter(incIndex);
    Sequence<Cursor> cursors = makeCursors(sa, null);
    Cursor cursor = cursors.limit(1).toList().get(0);

    List<DimensionSelector> selectors = new ArrayList<>();
    selectors.add(makeDimensionSelector(cursor, "dimSequential"));
    selectors.add(makeDimensionSelector(cursor, "dimZipf"));
    selectors.add(makeDimensionSelector(cursor, "dimUniform"));
    selectors.add(makeDimensionSelector(cursor, "dimSequentialHalfNull"));

    cursor.reset();
    while (!cursor.isDone()) {
      for (DimensionSelector selector : selectors) {
        IndexedInts row = selector.getRow();
        blackhole.consume(selector.lookupName(row.get(0)));
      }
      cursor.advance();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void readWithFilters(Blackhole blackhole)
  {
    DimFilter filter = new OrDimFilter(
        Arrays.asList(
            new BoundDimFilter("dimSequential", "-1", "-1", true, true, null, null, StringComparators.ALPHANUMERIC),
            new JavaScriptDimFilter("dimSequential", "function(x) { return false }", null, JavaScriptConfig.getEnabledInstance()),
            new RegexDimFilter("dimSequential", "X", null),
            new SearchQueryDimFilter("dimSequential", new ContainsSearchQuerySpec("X", false), null),
            new InDimFilter("dimSequential", Collections.singletonList("X"), null)
        )
    );

    IncrementalIndexStorageAdapter sa = new IncrementalIndexStorageAdapter(incIndex);
    Sequence<Cursor> cursors = makeCursors(sa, filter);
    Cursor cursor = cursors.limit(1).toList().get(0);

    List<DimensionSelector> selectors = new ArrayList<>();
    selectors.add(makeDimensionSelector(cursor, "dimSequential"));
    selectors.add(makeDimensionSelector(cursor, "dimZipf"));
    selectors.add(makeDimensionSelector(cursor, "dimUniform"));
    selectors.add(makeDimensionSelector(cursor, "dimSequentialHalfNull"));

    cursor.reset();
    while (!cursor.isDone()) {
      for (DimensionSelector selector : selectors) {
        IndexedInts row = selector.getRow();
        blackhole.consume(selector.lookupName(row.get(0)));
      }
      cursor.advance();
    }
  }

  private Sequence<Cursor> makeCursors(IncrementalIndexStorageAdapter sa, DimFilter filter)
  {
    return sa.makeCursors(
        filter.toFilter(),
        schemaInfo.getDataInterval(),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );
  }

  private static DimensionSelector makeDimensionSelector(Cursor cursor, String name)
  {
    return cursor.getColumnSelectorFactory().makeDimensionSelector(new DefaultDimensionSpec(name, null));
  }
}
