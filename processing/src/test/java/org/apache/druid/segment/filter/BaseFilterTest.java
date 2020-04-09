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

package org.apache.druid.segment.filter;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.BitmapIndexSelector;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.RowBasedStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.ConciseBitmapSerdeFactory;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class BaseFilterTest extends InitializedNullHandlingTest
{
  static final String TIMESTAMP_COLUMN = "timestamp";

  static final VirtualColumns VIRTUAL_COLUMNS = VirtualColumns.create(
      ImmutableList.of(
          new ExpressionVirtualColumn("expr", "1.0 + 0.1", ValueType.FLOAT, TestExprMacroTable.INSTANCE),
          new ExpressionVirtualColumn("exprDouble", "1.0 + 1.1", ValueType.DOUBLE, TestExprMacroTable.INSTANCE),
          new ExpressionVirtualColumn("exprLong", "1 + 2", ValueType.LONG, TestExprMacroTable.INSTANCE)
      )
  );

  static final TimestampSpec DEFAULT_TIMESTAMP_SPEC = new TimestampSpec(TIMESTAMP_COLUMN, "iso", DateTimes.of("2000"));
  static final DimensionsSpec DEFAULT_DIM_SPEC = new DimensionsSpec(
      ImmutableList.<DimensionSchema>builder()
          .addAll(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim0", "dim1", "dim2", "dim3", "timeDim")))
          .add(new DoubleDimensionSchema("d0"))
          .add(new FloatDimensionSchema("f0"))
          .add(new LongDimensionSchema("l0"))
          .build(),
      null,
      null
  );

  static final InputRowParser<Map<String, Object>> DEFAULT_PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          DEFAULT_TIMESTAMP_SPEC,
          DEFAULT_DIM_SPEC
      )
  );

  static final List<InputRow> DEFAULT_ROWS = ImmutableList.of(
      makeDefaultSchemaRow("0", "", ImmutableList.of("a", "b"), "2017-07-25", 0.0, 0.0f, 0L),
      makeDefaultSchemaRow("1", "10", ImmutableList.of(), "2017-07-25", 10.1, 10.1f, 100L),
      makeDefaultSchemaRow("2", "2", ImmutableList.of(""), "2017-05-25", null, 5.5f, 40L),
      makeDefaultSchemaRow("3", "1", ImmutableList.of("a"), "2020-01-25", 120.0245, 110.0f, null),
      makeDefaultSchemaRow("4", "abdef", ImmutableList.of("c"), null, 60.0, null, 9001L),
      makeDefaultSchemaRow("5", "abc", null, "2020-01-25", 765.432, 123.45f, 12345L)
  );

  static final IncrementalIndexSchema DEFAULT_INDEX_SCHEMA = new IncrementalIndexSchema.Builder()
      .withDimensionsSpec(DEFAULT_DIM_SPEC)
      .withMetrics(new CountAggregatorFactory("count"))
      .build();

  static InputRow makeDefaultSchemaRow(
      @Nullable String dim0,
      @Nullable String dim1,
      @Nullable List<String> dim2,
      @Nullable String timeDim,
      @Nullable Double d0,
      @Nullable Float f0,
      @Nullable Long l0
  )
  {
    // for row selector to work correctly as part of the test matrix, default value coercion needs to happen to columns
    Map<String, Object> mapRow = Maps.newHashMapWithExpectedSize(6);
    mapRow.put("dim0", NullHandling.nullToEmptyIfNeeded(dim0));
    mapRow.put("dim1", NullHandling.nullToEmptyIfNeeded(dim1));
    mapRow.put("dim2", dim2 != null ? dim2 : NullHandling.defaultStringValue());
    mapRow.put("timeDim", NullHandling.nullToEmptyIfNeeded(timeDim));
    mapRow.put("d0", d0 != null ? d0 : NullHandling.defaultDoubleValue());
    mapRow.put("f0", f0 != null ? f0 : NullHandling.defaultFloatValue());
    mapRow.put("l0", l0 != null ? l0 : NullHandling.defaultLongValue());
    return DEFAULT_PARSER.parseBatch(mapRow).get(0);
  }


  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final List<InputRow> rows;

  protected final IndexBuilder indexBuilder;
  protected final Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher;
  protected final boolean cnf;
  protected final boolean optimize;
  protected final String testName;

  protected StorageAdapter adapter;

  // JUnit creates a new test instance for every test method call.
  // For filter tests, the test setup creates a segment.
  // Creating a new segment for every test method call is pretty slow, so cache the StorageAdapters.
  // Each thread gets its own map.
  private static ThreadLocal<Map<String, Map<String, Pair<StorageAdapter, Closeable>>>> adapterCache =
      ThreadLocal.withInitial(HashMap::new);

  public BaseFilterTest(
      String testName,
      List<InputRow> rows,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean cnf,
      boolean optimize
  )
  {
    this.testName = testName;
    this.rows = rows;
    this.indexBuilder = indexBuilder;
    this.finisher = finisher;
    this.cnf = cnf;
    this.optimize = optimize;
  }

  @Before
  public void setUp() throws Exception
  {
    String className = getClass().getName();
    Map<String, Pair<StorageAdapter, Closeable>> adaptersForClass = adapterCache.get().get(className);
    if (adaptersForClass == null) {
      adaptersForClass = new HashMap<>();
      adapterCache.get().put(className, adaptersForClass);
    }

    Pair<StorageAdapter, Closeable> pair = adaptersForClass.get(testName);
    if (pair == null) {
      pair = finisher.apply(
          indexBuilder.tmpDir(temporaryFolder.newFolder()).rows(rows)
      );
      adaptersForClass.put(testName, pair);
    }

    this.adapter = pair.lhs;

  }

  public static void tearDown(String className) throws Exception
  {
    Map<String, Pair<StorageAdapter, Closeable>> adaptersForClass = adapterCache.get().get(className);

    if (adaptersForClass != null) {
      for (Map.Entry<String, Pair<StorageAdapter, Closeable>> entry : adaptersForClass.entrySet()) {
        Closeable closeable = entry.getValue().rhs;
        closeable.close();
      }
      adapterCache.get().put(className, null);
    }
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder()
  {
    return makeConstructors();
  }

  public static Collection<Object[]> makeConstructors()
  {
    final List<Object[]> constructors = new ArrayList<>();

    final Map<String, BitmapSerdeFactory> bitmapSerdeFactories = ImmutableMap.of(
        "concise", new ConciseBitmapSerdeFactory(),
        "roaring", new RoaringBitmapSerdeFactory(true)
    );

    final Map<String, SegmentWriteOutMediumFactory> segmentWriteOutMediumFactories = ImmutableMap.of(
        "tmpFile segment write-out medium", TmpFileSegmentWriteOutMediumFactory.instance(),
        "off-heap memory segment write-out medium", OffHeapMemorySegmentWriteOutMediumFactory.instance()
    );

    final Map<String, Function<IndexBuilder, Pair<StorageAdapter, Closeable>>> finishers =
        ImmutableMap.<String, Function<IndexBuilder, Pair<StorageAdapter, Closeable>>>builder()
            .put(
                "incremental",
                input -> {
                  final IncrementalIndex index = input.buildIncrementalIndex();
                  return Pair.of(new IncrementalIndexStorageAdapter(index), index);
                }
            )
            .put(
                "mmapped",
                input -> {
                  final QueryableIndex index = input.buildMMappedIndex();
                  return Pair.of(new QueryableIndexStorageAdapter(index), index);
                }
            )
            .put(
                "mmappedMerged",
                input -> {
                  final QueryableIndex index = input.buildMMappedMergedIndex();
                  return Pair.of(new QueryableIndexStorageAdapter(index), index);
                }
            )
            .put(
                "rowBasedWithoutTypeSignature",
                input -> Pair.of(input.buildRowBasedSegmentWithoutTypeSignature().asStorageAdapter(), () -> {})
            )
            .put(
                "rowBasedWithTypeSignature",
                input -> Pair.of(input.buildRowBasedSegmentWithTypeSignature().asStorageAdapter(), () -> {})
            )
            .build();

    for (Map.Entry<String, BitmapSerdeFactory> bitmapSerdeFactoryEntry : bitmapSerdeFactories.entrySet()) {
      for (Map.Entry<String, SegmentWriteOutMediumFactory> segmentWriteOutMediumFactoryEntry :
          segmentWriteOutMediumFactories.entrySet()) {
        for (Map.Entry<String, Function<IndexBuilder, Pair<StorageAdapter, Closeable>>> finisherEntry :
            finishers.entrySet()) {
          for (boolean cnf : ImmutableList.of(false, true)) {
            for (boolean optimize : ImmutableList.of(false, true)) {
              final String testName = StringUtils.format(
                  "bitmaps[%s], indexMerger[%s], finisher[%s], cnf[%s], optimize[%s]",
                  bitmapSerdeFactoryEntry.getKey(),
                  segmentWriteOutMediumFactoryEntry.getKey(),
                  finisherEntry.getKey(),
                  cnf,
                  optimize
              );
              final IndexBuilder indexBuilder = IndexBuilder
                  .create()
                  .schema(DEFAULT_INDEX_SCHEMA)
                  .indexSpec(new IndexSpec(bitmapSerdeFactoryEntry.getValue(), null, null, null))
                  .segmentWriteOutMediumFactory(segmentWriteOutMediumFactoryEntry.getValue());
              constructors.add(new Object[]{testName, indexBuilder, finisherEntry.getValue(), cnf, optimize});
            }
          }
        }
      }
    }

    return constructors;
  }

  private Filter makeFilter(final DimFilter dimFilter)
  {
    if (dimFilter == null) {
      return null;
    }

    final DimFilter maybeOptimized = optimize ? dimFilter.optimize() : dimFilter;
    final Filter filter = maybeOptimized.toFilter();
    return cnf ? Filters.toCnf(filter) : filter;
  }

  private DimFilter maybeOptimize(final DimFilter dimFilter)
  {
    if (dimFilter == null) {
      return null;
    }
    return optimize ? dimFilter.optimize() : dimFilter;
  }

  private Sequence<Cursor> makeCursorSequence(final Filter filter)
  {
    return adapter.makeCursors(
        filter,
        Intervals.ETERNITY,
        VIRTUAL_COLUMNS,
        Granularities.ALL,
        false,
        null
    );
  }

  private VectorCursor makeVectorCursor(final Filter filter)
  {
    return adapter.makeVectorCursor(
        filter,
        Intervals.ETERNITY,
        // VirtualColumns do not support vectorization yet. Avoid passing them in, and any tests that need virtual
        // columns should skip vectorization tests.
        VirtualColumns.EMPTY,
        false,
        3, // Vector size smaller than the number of rows, to ensure we use more than one.
        null
    );
  }

  /**
   * Selects elements from "selectColumn" from rows matching a filter. selectColumn must be a single valued dimension.
   */
  private List<String> selectColumnValuesMatchingFilter(final DimFilter filter, final String selectColumn)
  {
    final Sequence<Cursor> cursors = makeCursorSequence(makeFilter(filter));
    Sequence<List<String>> seq = Sequences.map(
        cursors,
        cursor -> {
          final DimensionSelector selector = cursor
              .getColumnSelectorFactory()
              .makeDimensionSelector(new DefaultDimensionSpec(selectColumn, selectColumn));

          final List<String> values = new ArrayList<>();

          while (!cursor.isDone()) {
            IndexedInts row = selector.getRow();
            Preconditions.checkState(row.size() == 1);
            values.add(selector.lookupName(row.get(0)));
            cursor.advance();
          }

          return values;
        }
    );
    return seq.toList().get(0);
  }

  private long selectCountUsingFilteredAggregator(final DimFilter filter)
  {
    final Sequence<Cursor> cursors = makeCursorSequence(null);
    Sequence<Aggregator> aggSeq = Sequences.map(
        cursors,
        cursor -> {
          Aggregator agg = new FilteredAggregatorFactory(
              new CountAggregatorFactory("count"),
              maybeOptimize(filter)
          ).factorize(cursor.getColumnSelectorFactory());

          for (; !cursor.isDone(); cursor.advance()) {
            agg.aggregate();
          }

          return agg;
        }
    );
    return aggSeq.toList().get(0).getLong();
  }

  private long selectCountUsingVectorizedFilteredAggregator(final DimFilter dimFilter)
  {
    Preconditions.checkState(makeFilter(dimFilter).canVectorizeMatcher(), "Cannot vectorize filter: %s", dimFilter);

    try (final VectorCursor cursor = makeVectorCursor(null)) {
      final FilteredAggregatorFactory aggregatorFactory = new FilteredAggregatorFactory(
          new CountAggregatorFactory("count"),
          maybeOptimize(dimFilter)
      );
      final VectorAggregator aggregator = aggregatorFactory.factorizeVector(cursor.getColumnSelectorFactory());
      final ByteBuffer buf = ByteBuffer.allocate(aggregatorFactory.getMaxIntermediateSizeWithNulls() * 2);

      // Use two slots: one for each form of aggregate.
      aggregator.init(buf, 0);
      aggregator.init(buf, aggregatorFactory.getMaxIntermediateSizeWithNulls());

      for (; !cursor.isDone(); cursor.advance()) {
        aggregator.aggregate(buf, 0, 0, cursor.getCurrentVectorSize());

        final int[] positions = new int[cursor.getCurrentVectorSize()];
        Arrays.fill(positions, aggregatorFactory.getMaxIntermediateSizeWithNulls());

        final int[] allRows = new int[cursor.getCurrentVectorSize()];
        for (int i = 0; i < allRows.length; i++) {
          allRows[i] = i;
        }

        aggregator.aggregate(buf, cursor.getCurrentVectorSize(), positions, allRows, 0);
      }

      final long val1 = (long) aggregator.get(buf, 0);
      final long val2 = (long) aggregator.get(buf, aggregatorFactory.getMaxIntermediateSizeWithNulls());

      if (val1 != val2) {
        throw new ISE("Oh no, val1[%d] != val2[%d]", val1, val2);
      }

      return val1;
    }
  }

  private List<String> selectColumnValuesMatchingFilterUsingPostFiltering(
      final DimFilter filter,
      final String selectColumn
  )
  {
    final Filter theFilter = makeFilter(filter);
    final Filter postFilteringFilter = new Filter()
    {
      @Override
      public <T> T getBitmapResult(BitmapIndexSelector selector, BitmapResultFactory<T> bitmapResultFactory)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
      {
        return theFilter.makeMatcher(factory);
      }

      @Override
      public boolean supportsBitmapIndex(BitmapIndexSelector selector)
      {
        return false;
      }

      @Override
      public boolean shouldUseBitmapIndex(BitmapIndexSelector selector)
      {
        return false;
      }

      @Override
      public boolean supportsSelectivityEstimation(ColumnSelector columnSelector, BitmapIndexSelector indexSelector)
      {
        return false;
      }

      @Override
      public Set<String> getRequiredColumns()
      {
        return Collections.emptySet();
      }

      @Override
      public double estimateSelectivity(BitmapIndexSelector indexSelector)
      {
        return 1.0;
      }
    };

    final Sequence<Cursor> cursors = makeCursorSequence(postFilteringFilter);
    Sequence<List<String>> seq = Sequences.map(
        cursors,
        cursor -> {
          final DimensionSelector selector = cursor
              .getColumnSelectorFactory()
              .makeDimensionSelector(new DefaultDimensionSpec(selectColumn, selectColumn));

          final List<String> values = new ArrayList<>();

          while (!cursor.isDone()) {
            IndexedInts row = selector.getRow();
            Preconditions.checkState(row.size() == 1);
            values.add(selector.lookupName(row.get(0)));
            cursor.advance();
          }

          return values;
        }
    );
    return seq.toList().get(0);
  }

  private List<String> selectColumnValuesMatchingFilterUsingVectorizedPostFiltering(
      final DimFilter filter,
      final String selectColumn
  )
  {
    final Filter theFilter = makeFilter(filter);
    final Filter postFilteringFilter = new Filter()
    {
      @Override
      public <T> T getBitmapResult(BitmapIndexSelector selector, BitmapResultFactory<T> bitmapResultFactory)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
      {
        return theFilter.makeMatcher(factory);
      }

      @Override
      public boolean supportsBitmapIndex(BitmapIndexSelector selector)
      {
        return false;
      }

      @Override
      public boolean shouldUseBitmapIndex(BitmapIndexSelector selector)
      {
        return false;
      }

      @Override
      public VectorValueMatcher makeVectorMatcher(VectorColumnSelectorFactory factory)
      {
        return theFilter.makeVectorMatcher(factory);
      }

      @Override
      public boolean canVectorizeMatcher()
      {
        return theFilter.canVectorizeMatcher();
      }

      @Override
      public Set<String> getRequiredColumns()
      {
        return null;
      }

      @Override
      public boolean supportsSelectivityEstimation(ColumnSelector columnSelector, BitmapIndexSelector indexSelector)
      {
        return false;
      }

      @Override
      public double estimateSelectivity(BitmapIndexSelector indexSelector)
      {
        return 1.0;
      }
    };

    try (final VectorCursor cursor = makeVectorCursor(postFilteringFilter)) {
      final SingleValueDimensionVectorSelector selector = cursor
          .getColumnSelectorFactory()
          .makeSingleValueDimensionSelector(new DefaultDimensionSpec(selectColumn, selectColumn));

      final List<String> values = new ArrayList<>();

      while (!cursor.isDone()) {
        final int[] rowVector = selector.getRowVector();
        for (int i = 0; i < cursor.getCurrentVectorSize(); i++) {
          values.add(selector.lookupName(rowVector[i]));
        }
        cursor.advance();
      }

      return values;
    }
  }

  private List<String> selectColumnValuesMatchingFilterUsingVectorCursor(
      final DimFilter filter,
      final String selectColumn
  )
  {
    try (final VectorCursor cursor = makeVectorCursor(makeFilter(filter))) {
      final SingleValueDimensionVectorSelector selector = cursor
          .getColumnSelectorFactory()
          .makeSingleValueDimensionSelector(new DefaultDimensionSpec(selectColumn, selectColumn));

      final List<String> values = new ArrayList<>();

      while (!cursor.isDone()) {
        final int[] rowVector = selector.getRowVector();
        for (int i = 0; i < cursor.getCurrentVectorSize(); i++) {
          values.add(selector.lookupName(rowVector[i]));
        }
        cursor.advance();
      }

      return values;
    }
  }

  private List<String> selectColumnValuesMatchingFilterUsingRowBasedColumnSelectorFactory(
      final DimFilter filter,
      final String selectColumn
  )
  {
    // Generate rowSignature
    final RowSignature.Builder rowSignatureBuilder = RowSignature.builder();
    for (String columnName : Iterables.concat(adapter.getAvailableDimensions(), adapter.getAvailableMetrics())) {
      rowSignatureBuilder.add(columnName, adapter.getColumnCapabilities(columnName).getType());
    }

    // Perform test
    final SettableSupplier<InputRow> rowSupplier = new SettableSupplier<>();
    final ValueMatcher matcher = makeFilter(filter).makeMatcher(
        VIRTUAL_COLUMNS.wrap(
            RowBasedColumnSelectorFactory.create(
                RowAdapters.standardRow(),
                rowSupplier::get,
                rowSignatureBuilder.build(),
                false
            )
        )
    );
    final List<String> values = new ArrayList<>();
    for (InputRow row : rows) {
      rowSupplier.set(row);
      if (matcher.matches()) {
        values.add((String) row.getRaw(selectColumn));
      }
    }
    return values;
  }

  protected void assertFilterMatches(
      final DimFilter filter,
      final List<String> expectedRows
  )
  {
    // IncrementalIndex and RowBasedSegment cannot ever vectorize.
    final boolean testVectorized =
        !(adapter instanceof IncrementalIndexStorageAdapter) && !(adapter instanceof RowBasedStorageAdapter);
    assertFilterMatches(filter, expectedRows, testVectorized);
  }

  protected void assertFilterMatchesSkipVectorize(
      final DimFilter filter,
      final List<String> expectedRows
  )
  {
    assertFilterMatches(filter, expectedRows, false);
  }

  private void assertFilterMatches(
      final DimFilter filter,
      final List<String> expectedRows,
      final boolean testVectorized
  )
  {
    Assert.assertEquals(
        "Cursor: " + filter,
        expectedRows,
        selectColumnValuesMatchingFilter(filter, "dim0")
    );

    if (testVectorized) {
      Assert.assertEquals(
          "Cursor (vectorized): " + filter,
          expectedRows,
          selectColumnValuesMatchingFilterUsingVectorCursor(filter, "dim0")
      );
    }

    Assert.assertEquals(
        "Cursor with postFiltering: " + filter,
        expectedRows,
        selectColumnValuesMatchingFilterUsingPostFiltering(filter, "dim0")
    );

    if (testVectorized) {
      Assert.assertEquals(
          "Cursor with postFiltering (vectorized): " + filter,
          expectedRows,
          selectColumnValuesMatchingFilterUsingVectorizedPostFiltering(filter, "dim0")
      );
    }

    Assert.assertEquals(
        "Filtered aggregator: " + filter,
        expectedRows.size(),
        selectCountUsingFilteredAggregator(filter)
    );

    if (testVectorized) {
      Assert.assertEquals(
          "Filtered aggregator (vectorized): " + filter,
          expectedRows.size(),
          selectCountUsingVectorizedFilteredAggregator(filter)
      );
    }

    Assert.assertEquals(
        "RowBasedColumnSelectorFactory: " + filter,
        expectedRows,
        selectColumnValuesMatchingFilterUsingRowBasedColumnSelectorFactory(filter, "dim0")
    );
  }
}
