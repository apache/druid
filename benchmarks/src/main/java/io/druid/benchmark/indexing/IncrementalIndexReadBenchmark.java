/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.benchmark.indexing;

import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;

import io.druid.benchmark.datagen.BenchmarkDataGenerator;
import io.druid.benchmark.datagen.BenchmarkSchemaInfo;
import io.druid.benchmark.datagen.BenchmarkSchemas;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.js.JavaScriptConfig;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.JavaScriptDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.RegexDimFilter;
import io.druid.query.filter.SearchQueryDimFilter;
import io.druid.query.ordering.StringComparators;
import io.druid.query.search.search.ContainsSearchQuerySpec;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.segment.serde.ComplexMetrics;
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
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(jvmArgsPrepend = "-server", value = 1)
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

    if (ComplexMetrics.getSerdeForType("hyperUnique") == null) {
      ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde(Hashing.murmur3_128()));
    }

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
    return new OnheapIncrementalIndex(
        new IncrementalIndexSchema.Builder()
            .withQueryGranularity(QueryGranularities.NONE)
            .withMetrics(schemaInfo.getAggsArray())
            .withDimensionsSpec(new DimensionsSpec(null, null, null))
            .withRollup(rollup)
            .build(),
        true,
        false,
        true,
        rowsPerSegment
    );
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void read(Blackhole blackhole) throws Exception
  {
    IncrementalIndexStorageAdapter sa = new IncrementalIndexStorageAdapter(incIndex);
    Sequence<Cursor> cursors = sa.makeCursors(null, schemaInfo.getDataInterval(), QueryGranularities.ALL, false);
    Cursor cursor = Sequences.toList(Sequences.limit(cursors, 1), Lists.<Cursor>newArrayList()).get(0);

    List<DimensionSelector> selectors = new ArrayList<>();
    selectors.add(cursor.makeDimensionSelector(new DefaultDimensionSpec("dimSequential", null)));
    selectors.add(cursor.makeDimensionSelector(new DefaultDimensionSpec("dimZipf", null)));
    selectors.add(cursor.makeDimensionSelector(new DefaultDimensionSpec("dimUniform", null)));
    selectors.add(cursor.makeDimensionSelector(new DefaultDimensionSpec("dimSequentialHalfNull", null)));

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
  public void readWithFilters(Blackhole blackhole) throws Exception
  {
    DimFilter filter = new OrDimFilter(
        Arrays.asList(
            new BoundDimFilter("dimSequential", "-1", "-1", true, true, null, null, StringComparators.ALPHANUMERIC),
            new JavaScriptDimFilter("dimSequential", "function(x) { return false }", null, JavaScriptConfig.getDefault()),
            new RegexDimFilter("dimSequential", "X", null),
            new SearchQueryDimFilter("dimSequential", new ContainsSearchQuerySpec("X", false), null),
            new InDimFilter("dimSequential", Arrays.asList("X"), null)
        )
    );

    IncrementalIndexStorageAdapter sa = new IncrementalIndexStorageAdapter(incIndex);
    Sequence<Cursor> cursors = sa.makeCursors(filter.toFilter(), schemaInfo.getDataInterval(), QueryGranularities.ALL, false);
    Cursor cursor = Sequences.toList(Sequences.limit(cursors, 1), Lists.<Cursor>newArrayList()).get(0);

    List<DimensionSelector> selectors = new ArrayList<>();
    selectors.add(cursor.makeDimensionSelector(new DefaultDimensionSpec("dimSequential", null)));
    selectors.add(cursor.makeDimensionSelector(new DefaultDimensionSpec("dimZipf", null)));
    selectors.add(cursor.makeDimensionSelector(new DefaultDimensionSpec("dimUniform", null)));
    selectors.add(cursor.makeDimensionSelector(new DefaultDimensionSpec("dimSequentialHalfNull", null)));

    cursor.reset();
    while (!cursor.isDone()) {
      for (DimensionSelector selector : selectors) {
        IndexedInts row = selector.getRow();
        blackhole.consume(selector.lookupName(row.get(0)));
      }
      cursor.advance();
    }
  }
}
