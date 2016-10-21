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

package io.druid.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;

import io.druid.benchmark.datagen.BenchmarkDataGenerator;
import io.druid.benchmark.datagen.BenchmarkSchemaInfo;
import io.druid.benchmark.datagen.BenchmarkSchemas;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.js.JavaScriptConfig;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.JavaScriptExtractionFn;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DruidLongPredicate;
import io.druid.query.filter.DruidPredicateFactory;
import io.druid.query.filter.Filter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexStorageAdapter;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.AndFilter;
import io.druid.segment.filter.BoundFilter;
import io.druid.segment.filter.DimensionPredicateFilter;
import io.druid.segment.filter.Filters;
import io.druid.segment.filter.OrFilter;
import io.druid.segment.filter.SelectorFilter;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.segment.serde.ComplexMetrics;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(jvmArgsPrepend = "-server", value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class FilterPartitionBenchmark
{
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

  private Filter timeFilterNone;
  private Filter timeFilterHalf;
  private Filter timeFilterAll;

  private BenchmarkSchemaInfo schemaInfo;

  private static String JS_FN = "function(str) { return 'super-' + str; }";
  private static ExtractionFn JS_EXTRACTION_FN = new JavaScriptExtractionFn(JS_FN, false, JavaScriptConfig.getDefault());

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    INDEX_IO = new IndexIO(
        JSON_MAPPER,
        new ColumnConfig()
        {
          @Override
          public int columnCacheSizeBytes()
          {
            return 0;
          }
        }
    );
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO);
  }

  @Setup
  public void setup() throws IOException
  {
    log.info("SETUP CALLED AT " + System.currentTimeMillis());

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

    File tmpFile = Files.createTempDir();
    log.info("Using temp dir: " + tmpFile.getAbsolutePath());
    tmpFile.deleteOnExit();

    indexFile = INDEX_MERGER_V9.persist(
        incIndex,
        tmpFile,
        new IndexSpec()
    );
    qIndex = INDEX_IO.loadIndex(indexFile);

    Interval interval = schemaInfo.getDataInterval();
    timeFilterNone = new BoundFilter(new BoundDimFilter(
        Column.TIME_COLUMN_NAME,
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
        Column.TIME_COLUMN_NAME,
        String.valueOf(interval.getStartMillis()),
        String.valueOf(halfEnd),
        true,
        true,
        null,
        null,
        StringComparators.ALPHANUMERIC
    ));

    timeFilterAll = new BoundFilter(new BoundDimFilter(
        Column.TIME_COLUMN_NAME,
        String.valueOf(interval.getStartMillis()),
        String.valueOf(interval.getEndMillis()),
        true,
        true,
        null,
        null,
        StringComparators.ALPHANUMERIC
    ));
  }

  private IncrementalIndex makeIncIndex()
  {
    return new OnheapIncrementalIndex(
        new IncrementalIndexSchema.Builder()
            .withQueryGranularity(QueryGranularities.NONE)
            .withMetrics(schemaInfo.getAggsArray())
            .withDimensionsSpec(new DimensionsSpec(null, null, null))
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
  public void stringRead(Blackhole blackhole) throws Exception
  {
    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = sa.makeCursors(null, schemaInfo.getDataInterval(), QueryGranularities.ALL, false);

    Sequence<List<String>> stringListSeq = readCursors(cursors, blackhole);
    List<String> strings = Sequences.toList(Sequences.limit(stringListSeq, 1), Lists.<List<String>>newArrayList()).get(0);
    for (String st : strings) {
      blackhole.consume(st);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void longRead(Blackhole blackhole) throws Exception
  {
    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = sa.makeCursors(null, schemaInfo.getDataInterval(), QueryGranularities.ALL, false);

    Sequence<List<Long>> longListSeq = readCursorsLong(cursors, blackhole);
    List<Long> strings = Sequences.toList(Sequences.limit(longListSeq, 1), Lists.<List<Long>>newArrayList()).get(0);
    for (Long st : strings) {
      blackhole.consume(st);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void timeFilterNone(Blackhole blackhole) throws Exception
  {
    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = sa.makeCursors(timeFilterNone, schemaInfo.getDataInterval(), QueryGranularities.ALL, false);

    Sequence<List<Long>> longListSeq = readCursorsLong(cursors, blackhole);
    List<Long> strings = Sequences.toList(Sequences.limit(longListSeq, 1), Lists.<List<Long>>newArrayList()).get(0);
    for (Long st : strings) {
      blackhole.consume(st);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void timeFilterHalf(Blackhole blackhole) throws Exception
  {
    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = sa.makeCursors(timeFilterHalf, schemaInfo.getDataInterval(), QueryGranularities.ALL, false);

    Sequence<List<Long>> longListSeq = readCursorsLong(cursors, blackhole);
    List<Long> strings = Sequences.toList(Sequences.limit(longListSeq, 1), Lists.<List<Long>>newArrayList()).get(0);
    for (Long st : strings) {
      blackhole.consume(st);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void timeFilterAll(Blackhole blackhole) throws Exception
  {
    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = sa.makeCursors(timeFilterAll, schemaInfo.getDataInterval(), QueryGranularities.ALL, false);

    Sequence<List<Long>> longListSeq = readCursorsLong(cursors, blackhole);
    List<Long> strings = Sequences.toList(Sequences.limit(longListSeq, 1), Lists.<List<Long>>newArrayList()).get(0);
    for (Long st : strings) {
      blackhole.consume(st);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void readWithPreFilter(Blackhole blackhole) throws Exception
  {
    Filter filter = new SelectorFilter("dimSequential", "199");

    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = sa.makeCursors(filter, schemaInfo.getDataInterval(), QueryGranularities.ALL, false);

    Sequence<List<String>> stringListSeq = readCursors(cursors, blackhole);
    List<String> strings = Sequences.toList(Sequences.limit(stringListSeq, 1), Lists.<List<String>>newArrayList()).get(0);
    for (String st : strings) {
      blackhole.consume(st);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void readWithPostFilter(Blackhole blackhole) throws Exception
  {
    Filter filter = new NoBitmapSelectorFilter("dimSequential", "199");

    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = sa.makeCursors(filter, schemaInfo.getDataInterval(), QueryGranularities.ALL, false);

    Sequence<List<String>> stringListSeq = readCursors(cursors, blackhole);
    List<String> strings = Sequences.toList(Sequences.limit(stringListSeq, 1), Lists.<List<String>>newArrayList()).get(0);
    for (String st : strings) {
      blackhole.consume(st);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void readWithExFnPreFilter(Blackhole blackhole) throws Exception
  {
    Filter filter = new SelectorDimFilter("dimSequential", "super-199", JS_EXTRACTION_FN).toFilter();

    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = sa.makeCursors(filter, schemaInfo.getDataInterval(), QueryGranularities.ALL, false);

    Sequence<List<String>> stringListSeq = readCursors(cursors, blackhole);
    List<String> strings = Sequences.toList(Sequences.limit(stringListSeq, 1), Lists.<List<String>>newArrayList()).get(0);
    for (String st : strings) {
      blackhole.consume(st);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void readWithExFnPostFilter(Blackhole blackhole) throws Exception
  {
    Filter filter = new NoBitmapSelectorDimFilter("dimSequential", "super-199", JS_EXTRACTION_FN).toFilter();

    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = sa.makeCursors(filter, schemaInfo.getDataInterval(), QueryGranularities.ALL, false);

    Sequence<List<String>> stringListSeq = readCursors(cursors, blackhole);
    List<String> strings = Sequences.toList(Sequences.limit(stringListSeq, 1), Lists.<List<String>>newArrayList()).get(0);
    for (String st : strings) {
      blackhole.consume(st);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void readOrFilter(Blackhole blackhole) throws Exception
  {
    Filter filter = new NoBitmapSelectorFilter("dimSequential", "199");
    Filter filter2 = new AndFilter(Arrays.<Filter>asList(new SelectorFilter("dimMultivalEnumerated2", "Corundum"), new NoBitmapSelectorFilter("dimMultivalEnumerated", "Bar")));
    Filter orFilter = new OrFilter(Arrays.<Filter>asList(filter, filter2));

    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = sa.makeCursors(orFilter, schemaInfo.getDataInterval(), QueryGranularities.ALL, false);

    Sequence<List<String>> stringListSeq = readCursors(cursors, blackhole);
    List<String> strings = Sequences.toList(Sequences.limit(stringListSeq, 1), Lists.<List<String>>newArrayList()).get(0);
    for (String st : strings) {
      blackhole.consume(st);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void readOrFilterCNF(Blackhole blackhole) throws Exception
  {
    Filter filter = new NoBitmapSelectorFilter("dimSequential", "199");
    Filter filter2 = new AndFilter(Arrays.<Filter>asList(new SelectorFilter("dimMultivalEnumerated2", "Corundum"), new NoBitmapSelectorFilter("dimMultivalEnumerated", "Bar")));
    Filter orFilter = new OrFilter(Arrays.<Filter>asList(filter, filter2));

    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = sa.makeCursors(Filters.convertToCNF(orFilter), schemaInfo.getDataInterval(), QueryGranularities.ALL, false);

    Sequence<List<String>> stringListSeq = readCursors(cursors, blackhole);
    List<String> strings = Sequences.toList(Sequences.limit(stringListSeq, 1), Lists.<List<String>>newArrayList()).get(0);
    for (String st : strings) {
      blackhole.consume(st);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void readComplexOrFilter(Blackhole blackhole) throws Exception
  {
    DimFilter dimFilter1 = new OrDimFilter(Arrays.<DimFilter>asList(
        new SelectorDimFilter("dimSequential", "199", null),
        new AndDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dimMultivalEnumerated2", "Corundum", null),
            new SelectorDimFilter("dimMultivalEnumerated", "Bar", null)
        )
        ))
    );
    DimFilter dimFilter2 = new OrDimFilter(Arrays.<DimFilter>asList(
        new SelectorDimFilter("dimSequential", "299", null),
        new SelectorDimFilter("dimSequential", "399", null),
        new AndDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dimMultivalEnumerated2", "Xylophone", null),
            new SelectorDimFilter("dimMultivalEnumerated", "Foo", null)
        )
        ))
    );
    DimFilter dimFilter3 = new OrDimFilter(Arrays.<DimFilter>asList(
        dimFilter1,
        dimFilter2,
        new AndDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dimMultivalEnumerated2", "Orange", null),
            new SelectorDimFilter("dimMultivalEnumerated", "World", null)
        )
        ))
    );

    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = sa.makeCursors(dimFilter3.toFilter(), schemaInfo.getDataInterval(), QueryGranularities.ALL, false);

    Sequence<List<String>> stringListSeq = readCursors(cursors, blackhole);
    List<String> strings = Sequences.toList(Sequences.limit(stringListSeq, 1), Lists.<List<String>>newArrayList()).get(0);
    for (String st : strings) {
      blackhole.consume(st);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void readComplexOrFilterCNF(Blackhole blackhole) throws Exception
  {
    DimFilter dimFilter1 = new OrDimFilter(Arrays.<DimFilter>asList(
        new SelectorDimFilter("dimSequential", "199", null),
        new AndDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dimMultivalEnumerated2", "Corundum", null),
            new SelectorDimFilter("dimMultivalEnumerated", "Bar", null)
        )
        ))
    );
    DimFilter dimFilter2 = new OrDimFilter(Arrays.<DimFilter>asList(
        new SelectorDimFilter("dimSequential", "299", null),
        new SelectorDimFilter("dimSequential", "399", null),
        new AndDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dimMultivalEnumerated2", "Xylophone", null),
            new SelectorDimFilter("dimMultivalEnumerated", "Foo", null)
        )
        ))
    );
    DimFilter dimFilter3 = new OrDimFilter(Arrays.<DimFilter>asList(
        dimFilter1,
        dimFilter2,
        new AndDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dimMultivalEnumerated2", "Orange", null),
            new SelectorDimFilter("dimMultivalEnumerated", "World", null)
        )
        ))
    );

    StorageAdapter sa = new QueryableIndexStorageAdapter(qIndex);
    Sequence<Cursor> cursors = sa.makeCursors(Filters.convertToCNF(dimFilter3.toFilter()), schemaInfo.getDataInterval(), QueryGranularities.ALL, false);

    Sequence<List<String>> stringListSeq = readCursors(cursors, blackhole);
    List<String> strings = Sequences.toList(Sequences.limit(stringListSeq, 1), Lists.<List<String>>newArrayList()).get(0);
    for (String st : strings) {
      blackhole.consume(st);
    }
  }

  private Sequence<List<String>> readCursors(Sequence<Cursor> cursors, final Blackhole blackhole)
  {
    return Sequences.map(
        cursors,
        new Function<Cursor, List<String>>()
        {
          @Override
          public List<String> apply(Cursor input)
          {
            List<String> strings = new ArrayList<String>();
            List<DimensionSelector> selectors = new ArrayList<>();
            selectors.add(input.makeDimensionSelector(new DefaultDimensionSpec("dimSequential", null)));
            //selectors.add(input.makeDimensionSelector(new DefaultDimensionSpec("dimB", null)));
            while (!input.isDone()) {
              for (DimensionSelector selector : selectors) {
                IndexedInts row = selector.getRow();
                blackhole.consume(selector.lookupName(row.get(0)));
                //strings.add(selector.lookupName(row.get(0)));
              }
              input.advance();
            }
            return strings;
          }
        }
    );
  }

  private Sequence<List<Long>> readCursorsLong(Sequence<Cursor> cursors, final Blackhole blackhole)
  {
    return Sequences.map(
        cursors,
        new Function<Cursor, List<Long>>()
        {
          @Override
          public List<Long> apply(Cursor input)
          {
            List<Long> longvals = new ArrayList<Long>();
            LongColumnSelector selector = input.makeLongColumnSelector("sumLongSequential");
            while (!input.isDone()) {
              long rowval = selector.get();
              blackhole.consume(rowval);
              input.advance();
            }
            return longvals;
          }
        }
    );
  }

  private class NoBitmapSelectorFilter extends SelectorFilter
  {
    public NoBitmapSelectorFilter(
        String dimension,
        String value
    )
    {
      super(dimension, value);
    }

    @Override
    public boolean supportsBitmapIndex(BitmapIndexSelector selector)
    {
      return false;
    }
  }

  private class NoBitmapDimensionPredicateFilter extends DimensionPredicateFilter
  {
    public NoBitmapDimensionPredicateFilter(
        final String dimension,
        final DruidPredicateFactory predicateFactory,
        final ExtractionFn extractionFn
    )
    {
      super(dimension, predicateFactory, extractionFn);
    }

    @Override
    public boolean supportsBitmapIndex(BitmapIndexSelector selector)
    {
      return false;
    }
  }

  private class NoBitmapSelectorDimFilter extends SelectorDimFilter
  {
    public NoBitmapSelectorDimFilter(
        String dimension,
        String value,
        ExtractionFn extractionFn
    )
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
        final String valueOrNull = Strings.emptyToNull(value);

        final DruidPredicateFactory predicateFactory = new DruidPredicateFactory()
        {
          @Override
          public Predicate<String> makeStringPredicate()
          {
            return new Predicate<String>()
            {
              @Override
              public boolean apply(String input)
              {
                return Objects.equals(valueOrNull, input);
              }
            };
          }

          @Override
          public DruidLongPredicate makeLongPredicate()
          {
            return new DruidLongPredicate()
            {
              @Override
              public boolean applyLong(long input)
              {
                return false;
              }
            };
          }
        };

        return new NoBitmapDimensionPredicateFilter(dimension, predicateFactory, extractionFn);
      }
    }
  }
}
