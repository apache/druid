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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.JavaScriptExtractionFn;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.BoundFilter;
import org.apache.druid.segment.filter.DimensionPredicateFilter;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.segment.filter.cnf.CNFFilterExplosionException;
import org.apache.druid.segment.generator.DataGenerator;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.joda.time.Interval;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class FilterPartitionBenchmark
{
  static {
    NullHandling.initializeForTests();
  }

  @Param({"750000"})
  private int rowsPerSegment;

  @Param({"basic"})
  private String schema;

  private static final Logger log = new Logger(FilterPartitionBenchmark.class);
  private static final int RNG_SEED = 9999;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;
  private IncrementalIndex incIndex;
  private QueryableIndex qIndex;
  private File indexFile;
  private File tmpDir;

  private Filter timeFilterNone;
  private Filter timeFilterHalf;
  private Filter timeFilterAll;

  private GeneratorSchemaInfo schemaInfo;

  private static String JS_FN = "function(str) { return 'super-' + str; }";
  private static ExtractionFn JS_EXTRACTION_FN = new JavaScriptExtractionFn(JS_FN, false, JavaScriptConfig.getEnabledInstance());

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    INDEX_IO = new IndexIO(
        JSON_MAPPER,
        new ColumnConfig()
        {
        }
    );
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO, OffHeapMemorySegmentWriteOutMediumFactory.instance());
  }

  @Setup
  public void setup() throws IOException
  {
    log.info("SETUP CALLED AT " + System.currentTimeMillis());

    ComplexMetrics.registerSerde(HyperUniquesSerde.TYPE_NAME, new HyperUniquesSerde());

    schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get(schema);

    DataGenerator gen = new DataGenerator(
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

    tmpDir = FileUtils.createTempDir();
    log.info("Using temp dir: " + tmpDir.getAbsolutePath());

    indexFile = INDEX_MERGER_V9.persist(
        incIndex,
        tmpDir,
        IndexSpec.DEFAULT,
        null
    );
    qIndex = INDEX_IO.loadIndex(indexFile);

    Interval interval = schemaInfo.getDataInterval();
    timeFilterNone = new BoundFilter(new BoundDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        String.valueOf(Long.MAX_VALUE),
        String.valueOf(Long.MAX_VALUE),
        true,
        true,
        null,
        null,
        StringComparators.ALPHANUMERIC
    ));

    long halfEnd = (interval.getEndMillis() + interval.getStartMillis()) / 2;
    timeFilterHalf = new BoundFilter(new BoundDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        String.valueOf(interval.getStartMillis()),
        String.valueOf(halfEnd),
        true,
        true,
        null,
        null,
        StringComparators.ALPHANUMERIC
    ));

    timeFilterAll = new BoundFilter(new BoundDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        String.valueOf(interval.getStartMillis()),
        String.valueOf(interval.getEndMillis()),
        true,
        true,
        null,
        null,
        StringComparators.ALPHANUMERIC
    ));
  }

  @TearDown
  public void tearDown() throws IOException
  {
    FileUtils.deleteDirectory(tmpDir);
  }

  private IncrementalIndex makeIncIndex()
  {
    return new OnheapIncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(schemaInfo.getAggsArray())
        .setMaxRowCount(rowsPerSegment)
        .build();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void stringRead(Blackhole blackhole)
  {
    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = makeCursors(sa, null);
    readCursors(cursors, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void longRead(Blackhole blackhole)
  {
    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = makeCursors(sa, null);

    readCursorsLong(cursors, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void timeFilterNone(Blackhole blackhole)
  {
    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = makeCursors(sa, timeFilterNone);

    readCursorsLong(cursors, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void timeFilterHalf(Blackhole blackhole)
  {
    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = makeCursors(sa, timeFilterHalf);

    readCursorsLong(cursors, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void timeFilterAll(Blackhole blackhole)
  {
    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = makeCursors(sa, timeFilterAll);

    readCursorsLong(cursors, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void readWithPreFilter(Blackhole blackhole)
  {
    Filter filter = new SelectorFilter("dimSequential", "199");

    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = makeCursors(sa, filter);
    readCursors(cursors, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void readWithPostFilter(Blackhole blackhole)
  {
    Filter filter = new NoBitmapSelectorFilter("dimSequential", "199");

    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = makeCursors(sa, filter);
    readCursors(cursors, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void readWithExFnPreFilter(Blackhole blackhole)
  {
    Filter filter = new SelectorDimFilter("dimSequential", "super-199", JS_EXTRACTION_FN).toFilter();

    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = makeCursors(sa, filter);
    readCursors(cursors, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void readWithExFnPostFilter(Blackhole blackhole)
  {
    Filter filter = new NoBitmapSelectorDimFilter("dimSequential", "super-199", JS_EXTRACTION_FN).toFilter();

    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = makeCursors(sa, filter);
    readCursors(cursors, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void readAndFilter(Blackhole blackhole)
  {
    Filter andFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("dimUniform", "199"),
            new NoBitmapSelectorDimFilter("dimUniform", "super-199", JS_EXTRACTION_FN).toFilter()
        )
    );

    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = makeCursors(sa, andFilter);
    readCursors(cursors, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void readOrFilter(Blackhole blackhole)
  {
    Filter filter = new NoBitmapSelectorFilter("dimSequential", "199");
    Filter filter2 = new AndFilter(Arrays.asList(new SelectorFilter("dimMultivalEnumerated2", "Corundum"), new NoBitmapSelectorFilter("dimMultivalEnumerated", "Bar")));
    Filter orFilter = new OrFilter(Arrays.asList(filter, filter2));

    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = makeCursors(sa, orFilter);
    readCursors(cursors, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void readOrFilterCNF(Blackhole blackhole) throws CNFFilterExplosionException
  {
    Filter filter = new NoBitmapSelectorFilter("dimSequential", "199");
    Filter filter2 = new AndFilter(Arrays.asList(new SelectorFilter("dimMultivalEnumerated2", "Corundum"), new NoBitmapSelectorFilter("dimMultivalEnumerated", "Bar")));
    Filter orFilter = new OrFilter(Arrays.asList(filter, filter2));

    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = makeCursors(sa, Filters.toCnf(orFilter));
    readCursors(cursors, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void readComplexOrFilter(Blackhole blackhole)
  {
    DimFilter dimFilter1 = new OrDimFilter(Arrays.asList(
        new SelectorDimFilter("dimSequential", "199", null),
        new AndDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dimMultivalEnumerated2", "Corundum", null),
            new SelectorDimFilter("dimMultivalEnumerated", "Bar", null)
        )
        ))
    );
    DimFilter dimFilter2 = new OrDimFilter(Arrays.asList(
        new SelectorDimFilter("dimSequential", "299", null),
        new SelectorDimFilter("dimSequential", "399", null),
        new AndDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dimMultivalEnumerated2", "Xylophone", null),
            new SelectorDimFilter("dimMultivalEnumerated", "Foo", null)
        )
        ))
    );
    DimFilter dimFilter3 = new OrDimFilter(Arrays.asList(
        dimFilter1,
        dimFilter2,
        new AndDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dimMultivalEnumerated2", "Orange", null),
            new SelectorDimFilter("dimMultivalEnumerated", "World", null)
        )
        ))
    );

    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = makeCursors(sa, dimFilter3.toFilter());
    readCursors(cursors, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void readComplexOrFilterCNF(Blackhole blackhole) throws CNFFilterExplosionException
  {
    DimFilter dimFilter1 = new OrDimFilter(Arrays.asList(
        new SelectorDimFilter("dimSequential", "199", null),
        new AndDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dimMultivalEnumerated2", "Corundum", null),
            new SelectorDimFilter("dimMultivalEnumerated", "Bar", null)
        )
        ))
    );
    DimFilter dimFilter2 = new OrDimFilter(Arrays.asList(
        new SelectorDimFilter("dimSequential", "299", null),
        new SelectorDimFilter("dimSequential", "399", null),
        new AndDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dimMultivalEnumerated2", "Xylophone", null),
            new SelectorDimFilter("dimMultivalEnumerated", "Foo", null)
        )
        ))
    );
    DimFilter dimFilter3 = new OrDimFilter(Arrays.asList(
        dimFilter1,
        dimFilter2,
        new AndDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dimMultivalEnumerated2", "Orange", null),
            new SelectorDimFilter("dimMultivalEnumerated", "World", null)
        )
        ))
    );

    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = makeCursors(sa, Filters.toCnf(dimFilter3.toFilter()));
    readCursors(cursors, blackhole);
  }

  private Sequence<Cursor> makeCursors(StorageAdapter sa, Filter filter)
  {
    return sa.makeCursors(filter, schemaInfo.getDataInterval(), VirtualColumns.EMPTY, Granularities.ALL, false, null);
  }

  private void readCursors(Sequence<Cursor> cursors, Blackhole blackhole)
  {
    final Sequence<Void> voids = Sequences.map(
        cursors,
        input -> {
          List<DimensionSelector> selectors = new ArrayList<>();
          selectors.add(
              input.getColumnSelectorFactory().makeDimensionSelector(new DefaultDimensionSpec("dimSequential", null))
          );
          while (!input.isDone()) {
            for (DimensionSelector selector : selectors) {
              IndexedInts row = selector.getRow();
              blackhole.consume(selector.lookupName(row.get(0)));
            }
            input.advance();
          }
          return null;
        }
    );

    blackhole.consume(voids.toList());
  }

  private void readCursorsLong(Sequence<Cursor> cursors, final Blackhole blackhole)
  {
    final Sequence<Void> voids = Sequences.map(
        cursors,
        input -> {
          BaseLongColumnValueSelector selector = input.getColumnSelectorFactory()
                                                      .makeColumnValueSelector("sumLongSequential");
          while (!input.isDone()) {
            long rowval = selector.getLong();
            blackhole.consume(rowval);
            input.advance();
          }
          return null;
        }
    );

    blackhole.consume(voids.toList());
  }

  private static class NoBitmapSelectorFilter extends SelectorFilter
  {
    public NoBitmapSelectorFilter(
        String dimension,
        String value
    )
    {
      super(dimension, value);
    }

  }

  private static class NoBitmapDimensionPredicateFilter extends DimensionPredicateFilter
  {
    public NoBitmapDimensionPredicateFilter(
        final String dimension,
        final DruidPredicateFactory predicateFactory,
        final ExtractionFn extractionFn
    )
    {
      super(dimension, predicateFactory, extractionFn);
    }

  }

  private static class NoBitmapSelectorDimFilter extends SelectorDimFilter
  {
    NoBitmapSelectorDimFilter(String dimension, String value, ExtractionFn extractionFn)
    {
      super(dimension, value, extractionFn);
    }

    @Override
    public Filter toFilter()
    {
      ExtractionFn extractionFn = getExtractionFn();
      String dimension = getDimension();
      final String value = getValue();
      if (extractionFn == null) {
        return new NoBitmapSelectorFilter(dimension, value);
      } else {
        final String valueOrNull = NullHandling.emptyToNullIfNeeded(value);

        final DruidPredicateFactory predicateFactory = new DruidPredicateFactory()
        {
          @Override
          public DruidObjectPredicate<String> makeStringPredicate()
          {
            return valueOrNull == null ? DruidObjectPredicate.isNull() : DruidObjectPredicate.equalTo(valueOrNull);
          }

          @Override
          public DruidLongPredicate makeLongPredicate()
          {
            return DruidLongPredicate.ALWAYS_FALSE_WITH_NULL_UNKNOWN;
          }

          @Override
          public DruidFloatPredicate makeFloatPredicate()
          {
            return DruidFloatPredicate.ALWAYS_FALSE_WITH_NULL_UNKNOWN;
          }

          @Override
          public DruidDoublePredicate makeDoublePredicate()
          {
            return DruidDoublePredicate.ALWAYS_FALSE_WITH_NULL_UNKNOWN;
          }
        };

        return new NoBitmapDimensionPredicateFilter(dimension, predicateFactory, extractionFn);
      }
    }
  }
}
