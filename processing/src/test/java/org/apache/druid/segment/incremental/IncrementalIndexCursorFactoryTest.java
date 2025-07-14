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

package org.apache.druid.segment.incremental;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.collections.CloseableDefaultBlockingPool;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.JavaScriptAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.DimFilters;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByResourcesReservationPool;
import org.apache.druid.query.groupby.GroupByStatsProvider;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.topn.TopNQueryEngine;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.CloserRule;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.Cursors;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.IncrementalIndexTimeBoundaryInspector;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.segment.index.AllTrueBitmapColumnIndex;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 *
 */
@RunWith(Parameterized.class)
public class IncrementalIndexCursorFactoryTest extends InitializedNullHandlingTest
{
  public final IncrementalIndexCreator indexCreator;

  private final GroupingEngine groupingEngine;
  private final TopNQueryEngine topnQueryEngine;

  private final NonBlockingPool<ByteBuffer> nonBlockingPool;


  /**
   * If true, sort by [billy, __time]. If false, sort by [__time].
   */
  public final boolean sortByDim;

  @Rule
  public final CloserRule closer = new CloserRule(false);

  public IncrementalIndexCursorFactoryTest(String indexType, boolean sortByDim)
      throws JsonProcessingException
  {
    BuiltInTypesModule.registerHandlersAndSerde();
    this.sortByDim = sortByDim;
    this.indexCreator = closer.closeLater(
        new IncrementalIndexCreator(
            indexType,
            (builder, args) -> {
              final DimensionsSpec dimensionsSpec;

              if (sortByDim) {
                dimensionsSpec =
                    DimensionsSpec.builder()
                                  .setDimensions(Collections.singletonList(new StringDimensionSchema("billy")))
                                  .setForceSegmentSortByTime(false)
                                  .setIncludeAllDimensions(true)
                                  .build();
              } else {
                dimensionsSpec = DimensionsSpec.EMPTY;
              }

              return builder
                  .setIndexSchema(
                      IncrementalIndexSchema
                          .builder()
                          .withDimensionsSpec(dimensionsSpec)
                          .withMetrics(new CountAggregatorFactory("cnt"))
                          .build()
                  )
                  .setMaxRowCount(1_000)
                  .build();
            }
        )
    );

    nonBlockingPool = closer.closeLater(
        new CloseableStupidPool<>(
            "GroupByQueryEngine-bufferPool",
            () -> ByteBuffer.allocate(50000)
        )
    );
    groupingEngine = new GroupingEngine(
        new DruidProcessingConfig(),
        GroupByQueryConfig::new,
        new GroupByResourcesReservationPool(
            closer.closeLater(
                new CloseableDefaultBlockingPool<>(
                    () -> ByteBuffer.allocate(50000),
                    5
                )
            ),
            new GroupByQueryConfig()
        ),
        TestHelper.makeJsonMapper(),
        TestHelper.makeSmileMapper(),
        (query, future) -> {
        },
        new GroupByStatsProvider()
    );
    topnQueryEngine = new TopNQueryEngine(nonBlockingPool);
  }

  @Parameterized.Parameters(name = "{index}: {0}, sortByDim: {1}")
  public static Collection<?> constructorFeeder()
  {
    return IncrementalIndexCreator.indexTypeCartesianProduct(
        ImmutableList.of(true, false) // sortByDim
    );
  }

  @Test
  public void testSanity() throws Exception
  {
    IncrementalIndex index = indexCreator.createIndex();
    index.add(
        new MapBasedInputRow(
            System.currentTimeMillis() - 1,
            Collections.singletonList("billy"),
            ImmutableMap.of("billy", "hi")
        )
    );
    index.add(
        new MapBasedInputRow(
            System.currentTimeMillis() - 1,
            Collections.singletonList("sally"),
            ImmutableMap.of("sally", "bo")
        )
    );


    final GroupByQuery query = GroupByQuery.builder()
                                           .setDataSource("test")
                                           .setGranularity(Granularities.ALL)
                                           .setInterval(new Interval(DateTimes.EPOCH, DateTimes.nowUtc()))
                                           .addDimension("billy")
                                           .addDimension("sally")
                                           .addAggregator(new LongSumAggregatorFactory("cnt", "cnt"))
                                           .addOrderByColumn("billy")
                                           .build();
    final IncrementalIndexCursorFactory cursorFactory = new IncrementalIndexCursorFactory(index);
    final Sequence<ResultRow> rows = groupingEngine.process(
        query,
        cursorFactory,
        new IncrementalIndexTimeBoundaryInspector(index),
        nonBlockingPool,
        null
    );

    final List<ResultRow> results = rows.toList();

    Assert.assertEquals(2, results.size());

    ResultRow row = results.get(0);
    Assert.assertArrayEquals(new Object[]{null, "bo", 1L}, row.getArray());

    row = results.get(1);
    Assert.assertArrayEquals(new Object[]{"hi", null, 1L}, row.getArray());
  }

  @Test
  public void testObjectColumnSelectorOnVaryingColumnSchema() throws Exception
  {
    IncrementalIndex index = indexCreator.createIndex();
    index.add(
        new MapBasedInputRow(
            DateTimes.of("2014-09-01T00:00:00"),
            Collections.singletonList("billy"),
            ImmutableMap.of("billy", "hi")
        )
    );
    index.add(
        new MapBasedInputRow(
            DateTimes.of("2014-09-01T01:00:00"),
            Lists.newArrayList("billy", "sally"),
            ImmutableMap.of(
                "billy", "hip",
                "sally", "hop"
            )
        )
    );

    final GroupByQuery query = GroupByQuery.builder()
                                           .setDataSource("test")
                                           .setGranularity(Granularities.ALL)
                                           .setInterval(new Interval(DateTimes.EPOCH, DateTimes.nowUtc()))
                                           .addDimension("billy")
                                           .addDimension("sally")
                                           .addAggregator(
                                               new LongSumAggregatorFactory("cnt", "cnt")
                                           )
                                           .addAggregator(
                                               new JavaScriptAggregatorFactory(
                                                   "fieldLength",
                                                   Arrays.asList("sally", "billy"),
                                                   "function(current, s, b) { return current + (s == null ? 0 : s.length) + (b == null ? 0 : b.length); }",
                                                   "function() { return 0; }",
                                                   "function(a,b) { return a + b; }",
                                                   JavaScriptConfig.getEnabledInstance()
                                               )
                                           )
                                           .addOrderByColumn("billy")
                                           .build();
    final IncrementalIndexCursorFactory cursorFactory = new IncrementalIndexCursorFactory(index);
    final Sequence<ResultRow> rows = groupingEngine.process(
        query,
        cursorFactory,
        new IncrementalIndexTimeBoundaryInspector(index),
        nonBlockingPool,
        null
    );

    final List<ResultRow> results = rows.toList();

    Assert.assertEquals(2, results.size());

    ResultRow row = results.get(0);
    Assert.assertArrayEquals(new Object[]{"hi", null, 1L, 2.0}, row.getArray());

    row = results.get(1);
    Assert.assertArrayEquals(
        new Object[]{"hip", "hop", 1L, 6.0},
        row.getArray()
    );
  }

  @Test
  public void testResetSanity() throws IOException
  {
    // Test is only valid when sortByDim = false, due to usage of Granularities.NONE.
    Assume.assumeFalse(sortByDim);

    IncrementalIndex index = indexCreator.createIndex();
    DateTime t = DateTimes.nowUtc();
    Interval interval = new Interval(t.minusMinutes(1), t.plusMinutes(1));

    index.add(
        new MapBasedInputRow(
            t.minus(1).getMillis(),
            Collections.singletonList("billy"),
            ImmutableMap.of("billy", "hi")
        )
    );
    index.add(
        new MapBasedInputRow(
            t.minus(1).getMillis(),
            Collections.singletonList("sally"),
            ImmutableMap.of("sally", "bo")
        )
    );

    IncrementalIndexCursorFactory cursorFactory = new IncrementalIndexCursorFactory(index);

    for (boolean descending : Arrays.asList(false, true)) {
      final CursorBuildSpec buildSpec = CursorBuildSpec
          .builder()
          .setFilter(new SelectorFilter("sally", "bo"))
          .setInterval(interval)
          .setPreferredOrdering(descending ? Cursors.descendingTimeOrder() : Cursors.ascendingTimeOrder())
          .build();

      try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(buildSpec)) {
        Cursor cursor = cursorHolder.asCursor();
        DimensionSelector dimSelector;

        dimSelector = cursor
            .getColumnSelectorFactory()
            .makeDimensionSelector(new DefaultDimensionSpec("sally", "sally"));
        Assert.assertEquals("bo", dimSelector.lookupName(dimSelector.getRow().get(0)));

        index.add(
            new MapBasedInputRow(
                t.minus(1).getMillis(),
                Collections.singletonList("sally"),
                ImmutableMap.of("sally", "ah")
            )
        );

        // Cursor reset should not be affected by out of order values
        cursor.reset();

        dimSelector = cursor
            .getColumnSelectorFactory()
            .makeDimensionSelector(new DefaultDimensionSpec("sally", "sally"));
        Assert.assertEquals("bo", dimSelector.lookupName(dimSelector.getRow().get(0)));
      }
    }
  }

  @Test
  public void testSingleValueTopN() throws IOException
  {
    IncrementalIndex index = indexCreator.createIndex();
    DateTime t = DateTimes.nowUtc();
    index.add(
        new MapBasedInputRow(
            t.minus(1).getMillis(),
            Collections.singletonList("sally"),
            ImmutableMap.of("sally", "bo")
        )
    );

    final Iterable<Result<TopNResultValue>> results = topnQueryEngine
        .query(
            new TopNQueryBuilder()
                .dataSource("test")
                .granularity(Granularities.ALL)
                .intervals(Collections.singletonList(new Interval(DateTimes.EPOCH, DateTimes.nowUtc())))
                .dimension("sally")
                .metric("cnt")
                .threshold(10)
                .aggregators(new LongSumAggregatorFactory("cnt", "cnt"))
                .build(),
            new IncrementalIndexSegment(index, SegmentId.dummy("test")),
            null
        )
        .toList();

    Assert.assertEquals(1, Iterables.size(results));
    Assert.assertEquals(1, results.iterator().next().getValue().getValue().size());
  }

  @Test
  public void testFilterByNull() throws Exception
  {
    IncrementalIndex index = indexCreator.createIndex();
    index.add(
        new MapBasedInputRow(
            System.currentTimeMillis() - 1,
            Collections.singletonList("billy"),
            ImmutableMap.of("billy", "hi")
        )
    );
    index.add(
        new MapBasedInputRow(
            System.currentTimeMillis() - 1,
            Collections.singletonList("sally"),
            ImmutableMap.of("sally", "bo")
        )
    );


    final GroupByQuery query = GroupByQuery.builder()
                                           .setDataSource("test")
                                           .setGranularity(Granularities.ALL)
                                           .setInterval(new Interval(DateTimes.EPOCH, DateTimes.nowUtc()))
                                           .addDimension("billy")
                                           .addDimension("sally")
                                           .addAggregator(new LongSumAggregatorFactory("cnt", "cnt"))
                                           .setDimFilter(DimFilters.dimEquals("sally", (String) null))
                                           .build();
    final IncrementalIndexCursorFactory cursorFactory = new IncrementalIndexCursorFactory(index);

    final Sequence<ResultRow> rows = groupingEngine.process(
        query,
        cursorFactory,
        new IncrementalIndexTimeBoundaryInspector(index),
        nonBlockingPool,
        null
    );

    final List<ResultRow> results = rows.toList();

    Assert.assertEquals(1, results.size());

    ResultRow row = results.get(0);
    Assert.assertArrayEquals(new Object[]{"hi", null, 1L}, row.getArray());
  }

  @Test
  public void testCursoringAndIndexUpdationInterleaving() throws Exception
  {
    final IncrementalIndex index = indexCreator.createIndex();
    final long timestamp = System.currentTimeMillis();

    for (int i = 0; i < 2; i++) {
      index.add(
          new MapBasedInputRow(
              timestamp,
              Collections.singletonList("billy"),
              ImmutableMap.of("billy", "v1" + i)
          )
      );
    }

    final CursorFactory cursorFactory = new IncrementalIndexCursorFactory(index);

    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                     .setInterval(Intervals.utc(timestamp - 60_000, timestamp + 60_000))
                                                     .build();
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(buildSpec)) {
      Cursor cursor = cursorHolder.asCursor();
      DimensionSelector dimSelector = cursor
          .getColumnSelectorFactory()
          .makeDimensionSelector(new DefaultDimensionSpec("billy", "billy"));
      int cardinality = dimSelector.getValueCardinality();

      //index gets more rows at this point, while other thread is iterating over the cursor
      try {
        for (int i = 0; i < 1; i++) {
          index.add(new MapBasedInputRow(
              timestamp,
              Collections.singletonList("billy"),
              ImmutableMap.of("billy", "v2" + i)
          ));
        }
      }
      catch (Exception ex) {
        throw new RuntimeException(ex);
      }

      int rowNumInCursor = 0;
      // and then, cursoring continues in the other thread
      while (!cursor.isDone()) {
        IndexedInts row = dimSelector.getRow();
        row.forEach(i -> Assert.assertTrue(i < cardinality));
        cursor.advance();
        rowNumInCursor++;
      }
      Assert.assertEquals(2, rowNumInCursor);
    }
  }

  @Test
  public void testCursorDictionaryRaceConditionFix() throws Exception
  {
    // Tests the dictionary ID race condition bug described at https://github.com/apache/druid/pull/6340

    final IncrementalIndex index = indexCreator.createIndex();
    final long timestamp = System.currentTimeMillis();

    for (int i = 0; i < 5; i++) {
      index.add(
          new MapBasedInputRow(
              timestamp,
              Collections.singletonList("billy"),
              ImmutableMap.of("billy", "v1" + i)
          )
      );
    }

    final CursorFactory cursorFactory = new IncrementalIndexCursorFactory(index);

    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                     .setFilter(new DictionaryRaceTestFilter(index, timestamp))
                                                     .setInterval(Intervals.utc(timestamp - 60_000, timestamp + 60_000))
                                                     .build();
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(buildSpec)) {
      Cursor cursor = cursorHolder.asCursor();
      DimensionSelector dimSelector = cursor
          .getColumnSelectorFactory()
          .makeDimensionSelector(new DefaultDimensionSpec("billy", "billy"));
      int cardinality = dimSelector.getValueCardinality();

      int rowNumInCursor = 0;
      while (!cursor.isDone()) {
        IndexedInts row = dimSelector.getRow();
        row.forEach(i -> Assert.assertTrue(i < cardinality));
        cursor.advance();
        rowNumInCursor++;
      }
      Assert.assertEquals(5, rowNumInCursor);
    }
  }

  @Test
  public void testCursoringAndSnapshot() throws Exception
  {
    final IncrementalIndex index = indexCreator.createIndex();
    final long timestamp = System.currentTimeMillis();

    final List<InputRow> rows = ImmutableList.of(
        new MapBasedInputRow(timestamp, Collections.singletonList("billy"), ImmutableMap.of("billy", "v00")),
        new MapBasedInputRow(timestamp, Collections.singletonList("billy"), ImmutableMap.of("billy", "v01")),
        new MapBasedInputRow(timestamp, Collections.singletonList("billy"), ImmutableMap.of("billy", "v1")),
        new MapBasedInputRow(timestamp, Collections.singletonList("billy"), ImmutableMap.of("billy", "v2")),
        new MapBasedInputRow(timestamp, Collections.singletonList("billy2"), ImmutableMap.of("billy2", "v3")),
        new MapBasedInputRow(timestamp, Collections.singletonList("billy"), ImmutableMap.of("billy", "v3")),
        new MapBasedInputRow(timestamp, Collections.singletonList("billy3"), ImmutableMap.of("billy3", ""))
    );

    // Add first two rows.
    for (int i = 0; i < 2; i++) {
      index.add(rows.get(i));
    }

    final CursorFactory cursorFactory = new IncrementalIndexCursorFactory(index);

    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                     .setInterval(Intervals.utc(timestamp - 60_000, timestamp + 60_000))
                                                     .build();
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(buildSpec)) {
      Cursor cursor = cursorHolder.asCursor();

      DimensionSelector dimSelector1A = cursor
          .getColumnSelectorFactory()
          .makeDimensionSelector(new DefaultDimensionSpec("billy", "billy"));
      int cardinalityA = dimSelector1A.getValueCardinality();

      //index gets more rows at this point, while other thread is iterating over the cursor
      try {
        index.add(rows.get(2));
      }
      catch (Exception ex) {
        throw new RuntimeException(ex);
      }

      DimensionSelector dimSelector1B = cursor
          .getColumnSelectorFactory()
          .makeDimensionSelector(new DefaultDimensionSpec("billy", "billy"));
      //index gets more rows at this point, while other thread is iterating over the cursor
      try {
        index.add(rows.get(3));
        index.add(rows.get(4));
      }
      catch (Exception ex) {
        throw new RuntimeException(ex);
      }

      DimensionSelector dimSelector1C = cursor
          .getColumnSelectorFactory()
          .makeDimensionSelector(new DefaultDimensionSpec("billy", "billy"));

      DimensionSelector dimSelector2D = cursor
          .getColumnSelectorFactory()
          .makeDimensionSelector(new DefaultDimensionSpec("billy2", "billy2"));
      //index gets more rows at this point, while other thread is iterating over the cursor
      try {
        index.add(rows.get(5));
        index.add(rows.get(6));
      }
      catch (Exception ex) {
        throw new RuntimeException(ex);
      }

      DimensionSelector dimSelector3E = cursor
          .getColumnSelectorFactory()
          .makeDimensionSelector(new DefaultDimensionSpec("billy3", "billy3"));

      int rowNumInCursor = 0;
      // and then, cursoring continues in the other thread
      while (!cursor.isDone()) {
        IndexedInts rowA = dimSelector1A.getRow();
        rowA.forEach(i -> Assert.assertTrue(i < cardinalityA));
        IndexedInts rowB = dimSelector1B.getRow();
        rowB.forEach(i -> Assert.assertTrue(i < cardinalityA));
        IndexedInts rowC = dimSelector1C.getRow();
        rowC.forEach(i -> Assert.assertTrue(i < cardinalityA));
        IndexedInts rowD = dimSelector2D.getRow();
        Assert.assertEquals(1, rowD.size());
        Assert.assertEquals(1, rowD.get(0));
        IndexedInts rowE = dimSelector3E.getRow();
        Assert.assertEquals(1, rowE.size());
        Assert.assertEquals(1, rowE.get(0));
        cursor.advance();
        rowNumInCursor++;
      }
      Assert.assertEquals(2, rowNumInCursor);
    }
  }

  private static class DictionaryRaceTestFilter implements Filter
  {
    private final IncrementalIndex index;
    private final long timestamp;

    private DictionaryRaceTestFilter(
        IncrementalIndex index,
        long timestamp
    )
    {
      this.index = index;
      this.timestamp = timestamp;
    }

    @Nullable
    @Override
    public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
    {
      return new AllTrueBitmapColumnIndex(selector);
    }

    @Override
    public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
    {
      return Filters.makeValueMatcher(
          factory,
          "billy",
          new DictionaryRaceTestFilterDruidPredicateFactory()
      );
    }

    @Override
    public Set<String> getRequiredColumns()
    {
      return Collections.emptySet();
    }

    @Override
    public int hashCode()
    {
      // Test code, hashcode and equals isn't important
      return super.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
      // Test code, hashcode and equals isn't important
      return super.equals(obj);
    }

    private class DictionaryRaceTestFilterDruidPredicateFactory implements DruidPredicateFactory
    {
      @Override
      public DruidObjectPredicate<String> makeStringPredicate()
      {
        index.add(
            new MapBasedInputRow(
                timestamp,
                Collections.singletonList("billy"),
                ImmutableMap.of("billy", "v31234")
            )
        );

        return DruidObjectPredicate.alwaysTrue();
      }

      @Override
      public DruidLongPredicate makeLongPredicate()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public DruidFloatPredicate makeFloatPredicate()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public DruidDoublePredicate makeDoublePredicate()
      {
        throw new UnsupportedOperationException();
      }
    }
  }
}
