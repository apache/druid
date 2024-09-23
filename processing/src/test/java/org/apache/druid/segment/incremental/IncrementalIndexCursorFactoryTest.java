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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.ListBasedInputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.JavaScriptAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
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
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.segment.index.AllTrueBitmapColumnIndex;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
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
  public final IncrementalIndexCursorFactory projectionsCursorFactory;
  public final IncrementalIndexTimeBoundaryInspector projectionsTimeBoundaryInspector;

  private final GroupingEngine groupingEngine;
  private final TopNQueryEngine topnQueryEngine;

  private final NonBlockingPool<ByteBuffer> nonBlockingPool;

  private final DateTime timestamp = Granularities.DAY.bucket(DateTimes.nowUtc()).getStart();

  final RowSignature projectionBaseSignature = RowSignature.builder()
                                                           .add("a", ColumnType.STRING)
                                                           .add("b", ColumnType.STRING)
                                                           .add("c", ColumnType.LONG)
                                                           .add("d", ColumnType.DOUBLE)
                                                           .build();
  final List<InputRow> projectionBaseRows = Arrays.asList(
      new ListBasedInputRow(
          projectionBaseSignature,
          timestamp,
          projectionBaseSignature.getColumnNames(),
          Arrays.asList("a", "aa", 1L, 1.0)
      ),
      new ListBasedInputRow(
          projectionBaseSignature,
          timestamp.plusMinutes(2),
          projectionBaseSignature.getColumnNames(),
          Arrays.asList("a", "bb", 1L, 1.1, 1.1f)
      ),
      new ListBasedInputRow(
          projectionBaseSignature,
          timestamp.plusMinutes(4),
          projectionBaseSignature.getColumnNames(),
          Arrays.asList("a", "cc", 2L, 2.2, 2.2f)
      ),
      new ListBasedInputRow(
          projectionBaseSignature,
          timestamp.plusMinutes(6),
          projectionBaseSignature.getColumnNames(),
          Arrays.asList("b", "aa", 3L, 3.3, 3.3f)
      ),
      new ListBasedInputRow(
          projectionBaseSignature,
          timestamp.plusMinutes(8),
          projectionBaseSignature.getColumnNames(),
          Arrays.asList("b", "aa", 4L, 4.4, 4.4f)
      ),
      new ListBasedInputRow(
          projectionBaseSignature,
          timestamp.plusMinutes(10),
          projectionBaseSignature.getColumnNames(),
          Arrays.asList("b", "bb", 5L, 5.5, 5.5f)
      ),
      new ListBasedInputRow(
          projectionBaseSignature,
          timestamp.plusHours(1),
          projectionBaseSignature.getColumnNames(),
          Arrays.asList("a", "aa", 1L, 1.1, 1.1f)
      ),
      new ListBasedInputRow(
          projectionBaseSignature,
          timestamp.plusHours(1).plusMinutes(1),
          projectionBaseSignature.getColumnNames(),
          Arrays.asList("a", "dd", 2L, 2.2, 2.2f)
      )
  );
  /**
   * If true, sort by [billy, __time]. If false, sort by [__time].
   */
  public final boolean sortByDim;

  @Rule
  public final CloserRule closer = new CloserRule(false);

  public IncrementalIndexCursorFactoryTest(String indexType, boolean sortByDim)
      throws JsonProcessingException, IndexSizeExceededException
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

    final IncrementalIndexCreator projectionsCreator = closer.closeLater(
        new IncrementalIndexCreator(
            indexType,
            (builder, args) -> {
              DimensionsSpec.Builder bob = DimensionsSpec.builder()
                                                         .setDimensions(
                                                             Arrays.asList(
                                                                 new StringDimensionSchema("a"),
                                                                 new StringDimensionSchema("b"),
                                                                 new LongDimensionSchema("c"),
                                                                 new DoubleDimensionSchema("d"),
                                                                 new FloatDimensionSchema("e")
                                                             )
                                                         );
              if (sortByDim) {
                bob.setForceSegmentSortByTime(false);
              }

              return builder
                  .setIndexSchema(
                      IncrementalIndexSchema
                          .builder()
                          .withRollup(false)
                          .withMinTimestamp(timestamp.getMillis())
                          .withDimensionsSpec(bob.build())
                          .withProjections(
                              Arrays.asList(
                                  new AggregateProjectionSpec(
                                      "ab_hourly_cd_sum",
                                      Arrays.asList(
                                          new StringDimensionSchema("a"),
                                          new StringDimensionSchema("b"),
                                          new LongDimensionSchema("__gran")
                                      ),
                                      VirtualColumns.create(
                                          Granularities.toVirtualColumn(Granularities.HOUR, "__gran")
                                      ),
                                      new AggregatorFactory[]{
                                          new LongSumAggregatorFactory("_c_sum", "c"),
                                          new DoubleSumAggregatorFactory("d", "d")
                                      }
                                  ),
                                  new AggregateProjectionSpec(
                                      "a_hourly_c_sum_with_count",
                                      Arrays.asList(
                                          new LongDimensionSchema("__gran"),
                                          new StringDimensionSchema("a")
                                      ),
                                      VirtualColumns.create(
                                          Granularities.toVirtualColumn(Granularities.HOUR, "__gran")
                                      ),
                                      new AggregatorFactory[]{
                                          new CountAggregatorFactory("chocula"),
                                          new LongSumAggregatorFactory("_c_sum", "c")
                                      }
                                  ),
                                  new AggregateProjectionSpec(
                                      "bf_daily_c_sum",
                                      Arrays.asList(
                                          new LongDimensionSchema("__gran"),
                                          new StringDimensionSchema("b"),
                                          new FloatDimensionSchema("e")
                                      ),
                                      VirtualColumns.create(
                                          Granularities.toVirtualColumn(Granularities.DAY, "__gran")
                                      ),
                                      new AggregatorFactory[]{
                                          new LongSumAggregatorFactory("_c_sum", "c")
                                      }
                                  ),
                                  new AggregateProjectionSpec(
                                      "ab_daily",
                                      Arrays.asList(
                                          new StringDimensionSchema("a"),
                                          new StringDimensionSchema("b")
                                      ),
                                      null,
                                      null
                                  ),
                                  new AggregateProjectionSpec(
                                      "abfoo_daily",
                                      Arrays.asList(
                                          new StringDimensionSchema("a"),
                                          new StringDimensionSchema("bfoo")
                                      ),
                                      VirtualColumns.create(
                                          new ExpressionVirtualColumn(
                                              "bfoo",
                                              "concat(b, 'foo')",
                                              ColumnType.STRING,
                                              TestExprMacroTable.INSTANCE
                                          )
                                      ),
                                      null
                                  )
                              )
                          )
                          .build()
                  )
                  .setMaxRowCount(1_000)
                  .build();
            }
        )
    );

    final IncrementalIndex projectionsIndex = projectionsCreator.createIndex();

    for (InputRow row : projectionBaseRows) {
      projectionsIndex.add(row);
    }

    projectionsCursorFactory = new IncrementalIndexCursorFactory(projectionsIndex);
    projectionsTimeBoundaryInspector = new IncrementalIndexTimeBoundaryInspector(projectionsIndex);

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
        }
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
    Assert.assertArrayEquals(new Object[]{NullHandling.defaultStringValue(), "bo", 1L}, row.getArray());

    row = results.get(1);
    Assert.assertArrayEquals(new Object[]{"hi", NullHandling.defaultStringValue(), 1L}, row.getArray());
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
    Assert.assertArrayEquals(new Object[]{"hi", NullHandling.defaultStringValue(), 1L, 2.0}, row.getArray());

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
    Assert.assertArrayEquals(new Object[]{"hi", NullHandling.defaultStringValue(), 1L}, row.getArray());
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
        // no null id, so should get empty dims array
        Assert.assertEquals(0, rowD.size());
        IndexedInts rowE = dimSelector3E.getRow();
        if (NullHandling.replaceWithDefault()) {
          Assert.assertEquals(1, rowE.size());
          // the null id
          Assert.assertEquals(0, rowE.get(0));
        } else {
          Assert.assertEquals(0, rowE.size());
        }
        cursor.advance();
        rowNumInCursor++;
      }
      Assert.assertEquals(2, rowNumInCursor);
    }
  }

  @Test
  public void testProjectionSelectionTwoDims()
  {
    // this query can use the projection with 2 dims, which has 7 rows instead of the total of 8
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("a")
                    .addDimension("b")
                    .build();

    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, null);

    try (final CursorHolder cursorHolder = projectionsCursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      int rowCount = 0;
      while (!cursor.isDone()) {
        rowCount++;
        cursor.advance();
      }
      Assert.assertEquals(6, rowCount);
    }

    final Sequence<ResultRow> resultRows = groupingEngine.process(
        query,
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        nonBlockingPool,
        null
    );

    final List<ResultRow> results = resultRows.toList();
    Assert.assertEquals(6, results.size());
    Assert.assertArrayEquals(new Object[]{"a", "aa"}, results.get(0).getArray());
    Assert.assertArrayEquals(new Object[]{"a", "bb"}, results.get(1).getArray());
    Assert.assertArrayEquals(new Object[]{"a", "cc"}, results.get(2).getArray());
    Assert.assertArrayEquals(new Object[]{"a", "dd"}, results.get(3).getArray());
    Assert.assertArrayEquals(new Object[]{"b", "aa"}, results.get(4).getArray());
    Assert.assertArrayEquals(new Object[]{"b", "bb"}, results.get(5).getArray());
  }

  @Test
  public void testProjectionSelectionTwoDimsVirtual()
  {
    // this query can use the projection with 2 dims, which has 7 rows instead of the total of 8
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("a")
                    .addDimension("v0")
                    .setVirtualColumns(
                        new ExpressionVirtualColumn(
                            "v0",
                            "concat(b, 'foo')",
                            ColumnType.STRING,
                            TestExprMacroTable.INSTANCE
                        )
                    )
                    .setContext(ImmutableMap.of(QueryContexts.CTX_USE_PROJECTION, "abfoo_daily"))
                    .build();

    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, null);

    try (final CursorHolder cursorHolder = projectionsCursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      int rowCount = 0;
      while (!cursor.isDone()) {
        rowCount++;
        cursor.advance();
      }
      Assert.assertEquals(6, rowCount);
    }

    final Sequence<ResultRow> resultRows = groupingEngine.process(
        query,
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        nonBlockingPool,
        null
    );

    final List<ResultRow> results = resultRows.toList();
    Assert.assertEquals(6, results.size());
    Assert.assertArrayEquals(new Object[]{"a", "aafoo"}, results.get(0).getArray());
    Assert.assertArrayEquals(new Object[]{"a", "bbfoo"}, results.get(1).getArray());
    Assert.assertArrayEquals(new Object[]{"a", "ccfoo"}, results.get(2).getArray());
    Assert.assertArrayEquals(new Object[]{"a", "ddfoo"}, results.get(3).getArray());
    Assert.assertArrayEquals(new Object[]{"b", "aafoo"}, results.get(4).getArray());
    Assert.assertArrayEquals(new Object[]{"b", "bbfoo"}, results.get(5).getArray());
  }

  @Test
  public void testProjectionSelectionTwoDimsCount()
  {
    // this query can use the projection with 2 dims, which has 7 rows instead of the total of 8
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("a")
                    .addDimension("b")
                    .addAggregator(new CountAggregatorFactory("count"))
                    .build();

    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, null);

    try (final CursorHolder cursorHolder = projectionsCursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      int rowCount = 0;
      while (!cursor.isDone()) {
        rowCount++;
        cursor.advance();
      }
      Assert.assertEquals(6, rowCount);
    }

    final Sequence<ResultRow> resultRows = groupingEngine.process(
        query,
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        nonBlockingPool,
        null
    );

    final List<ResultRow> results = resultRows.toList();
    Assert.assertEquals(6, results.size());
    Assert.assertArrayEquals(new Object[]{"a", "aa", 2L}, results.get(0).getArray());
    Assert.assertArrayEquals(new Object[]{"a", "bb", 1L}, results.get(1).getArray());
    Assert.assertArrayEquals(new Object[]{"a", "cc", 1L}, results.get(2).getArray());
    Assert.assertArrayEquals(new Object[]{"a", "dd", 1L}, results.get(3).getArray());
    Assert.assertArrayEquals(new Object[]{"b", "aa", 2L}, results.get(4).getArray());
    Assert.assertArrayEquals(new Object[]{"b", "bb", 1L}, results.get(5).getArray());
  }

  @Test
  public void testProjectionSkipContext()
  {
    // setting context flag to skip projection
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("a")
                    .addDimension("b")
                    .setContext(ImmutableMap.of(QueryContexts.CTX_NO_PROJECTION, true))
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"))
                    .build();
    CursorBuildSpec buildSpecSkip = GroupingEngine.makeCursorBuildSpec(query, null);
    try (final CursorHolder cursorHolder = projectionsCursorFactory.makeCursorHolder(buildSpecSkip)) {
      final Cursor cursor = cursorHolder.asCursor();
      int rowCount = 0;
      while (!cursor.isDone()) {
        rowCount++;
        cursor.advance();
      }
      // has to scan full 8 rows because context ensures projections not used
      Assert.assertEquals(8, rowCount);
    }
  }

  @Test
  public void testProjectionSingleDim()
  {
    // test can use the single dimension projection
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("a")
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"))
                    .build();
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, null);
    try (final CursorHolder cursorHolder = projectionsCursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      int rowCount = 0;
      while (!cursor.isDone()) {
        rowCount++;
        cursor.advance();
      }
      Assert.assertEquals(3, rowCount);
    }
    final Sequence<ResultRow> resultRows = groupingEngine.process(
        query,
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        nonBlockingPool,
        null
    );
    final List<ResultRow> results = resultRows.toList();
    Assert.assertEquals(2, results.size());
    Assert.assertArrayEquals(new Object[]{"a", 7L}, results.get(0).getArray());
    Assert.assertArrayEquals(new Object[]{"b", 12L}, results.get(1).getArray());
  }

  @Test
  public void testProjectionSingleDimCount()
  {
    // test can use the single dimension projection
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("a")
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"))
                    .addAggregator(new CountAggregatorFactory("cnt"))
                    .build();
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, null);
    try (final CursorHolder cursorHolder = projectionsCursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      int rowCount = 0;
      while (!cursor.isDone()) {
        rowCount++;
        cursor.advance();
      }
      Assert.assertEquals(3, rowCount);
    }
    final Sequence<ResultRow> resultRows = groupingEngine.process(
        query,
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        nonBlockingPool,
        null
    );
    final List<ResultRow> results = resultRows.toList();
    Assert.assertEquals(2, results.size());
    Assert.assertArrayEquals(new Object[]{"a", 7L, 5L}, results.get(0).getArray());
    Assert.assertArrayEquals(new Object[]{"b", 12L, 3L}, results.get(1).getArray());
  }

  @Test
  public void testQueryGranularityFinerThanProjectionGranularity()
  {
    final GroupByQuery.Builder queryBuilder =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setInterval(Intervals.ETERNITY)
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"));
    if (sortByDim) {
      queryBuilder.setVirtualColumns(Granularities.toVirtualColumn(Granularities.MINUTE, "__gran"))
                  .setDimensions(
                      DefaultDimensionSpec.of("__gran", ColumnType.LONG),
                      DefaultDimensionSpec.of("a")
                  )
                  .setGranularity(Granularities.ALL);
    } else {
      queryBuilder.addDimension("a")
                  .setGranularity(Granularities.MINUTE);
    }
    final GroupByQuery query = queryBuilder.build();
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, null);
    try (final CursorHolder cursorHolder = projectionsCursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      int rowCount = 0;
      while (!cursor.isDone()) {
        rowCount++;
        cursor.advance();
      }
      Assert.assertEquals(8, rowCount);
    }

    final Sequence<ResultRow> resultRows = groupingEngine.process(
        query,
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        nonBlockingPool,
        null
    );

    final List<ResultRow> results = resultRows.toList();
    Assert.assertEquals(8, results.size());

    Assert.assertArrayEquals(
        new Object[]{projectionBaseRows.get(0).getTimestamp().getMillis(), "a", 1L},
        results.get(0).getArray()
    );
    Assert.assertArrayEquals(
        new Object[]{projectionBaseRows.get(1).getTimestamp().getMillis(), "a", 1L},
        results.get(1).getArray()
    );
    Assert.assertArrayEquals(
        new Object[]{projectionBaseRows.get(2).getTimestamp().getMillis(), "a", 2L},
        results.get(2).getArray()
    );
    Assert.assertArrayEquals(
        new Object[]{projectionBaseRows.get(3).getTimestamp().getMillis(), "b", 3L},
        results.get(3).getArray()
    );
    Assert.assertArrayEquals(
        new Object[]{projectionBaseRows.get(4).getTimestamp().getMillis(), "b", 4L},
        results.get(4).getArray()
    );
    Assert.assertArrayEquals(
        new Object[]{projectionBaseRows.get(5).getTimestamp().getMillis(), "b", 5L},
        results.get(5).getArray()
    );
    Assert.assertArrayEquals(
        new Object[]{projectionBaseRows.get(6).getTimestamp().getMillis(), "a", 1L},
        results.get(6).getArray()
    );
    Assert.assertArrayEquals(
        new Object[]{projectionBaseRows.get(7).getTimestamp().getMillis(), "a", 2L},
        results.get(7).getArray()
    );
  }

  @Test
  public void testQueryGranularityFitsProjectionGranularity()
  {
    final GroupByQuery.Builder queryBuilder =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setInterval(Intervals.ETERNITY)
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"));
    if (sortByDim) {
      queryBuilder.setGranularity(Granularities.ALL)
                  .setDimensions(
                      DefaultDimensionSpec.of("__gran", ColumnType.LONG),
                      DefaultDimensionSpec.of("a")
                  )
                  .setVirtualColumns(Granularities.toVirtualColumn(Granularities.HOUR, "__gran"));
    } else {
      queryBuilder.addDimension("a")
                  .setGranularity(Granularities.HOUR);
    }
    final GroupByQuery query = queryBuilder.build();
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, null);
    try (final CursorHolder cursorHolder = projectionsCursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      int rowCount = 0;
      while (!cursor.isDone()) {
        rowCount++;
        cursor.advance();
      }
      Assert.assertEquals(3, rowCount);
    }

    final Sequence<ResultRow> resultRows = groupingEngine.process(
        query,
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        nonBlockingPool,
        null
    );

    final List<ResultRow> results = resultRows.toList();
    Assert.assertEquals(3, results.size());
    Assert.assertArrayEquals(new Object[]{timestamp.getMillis(), "a", 4L}, results.get(0).getArray());
    Assert.assertArrayEquals(new Object[]{timestamp.getMillis(), "b", 12L}, results.get(1).getArray());
    Assert.assertArrayEquals(new Object[]{timestamp.plusHours(1).getMillis(), "a", 3L}, results.get(2).getArray());
  }

  @Test
  public void testProjectionSelectionMissingAggregatorButHasAggregatorInput()
  {
    // d is present as a column on the projection, but since its an aggregate projection we cannot use it
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("b")
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"))
                    .addAggregator(new FloatSumAggregatorFactory("e_sum", "e"))
                    .build();
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, null);
    try (final CursorHolder cursorHolder = projectionsCursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      int rowCount = 0;
      while (!cursor.isDone()) {
        rowCount++;
        cursor.advance();
      }
      Assert.assertEquals(8, rowCount);
    }

    final Sequence<ResultRow> resultRows = groupingEngine.process(
        query,
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        nonBlockingPool,
        null
    );

    final List<ResultRow> results = resultRows.toList();
    Assert.assertEquals(4, results.size());
    Assert.assertArrayEquals(new Object[]{"aa", 9L, NullHandling.defaultFloatValue()}, results.get(0).getArray());
    Assert.assertArrayEquals(new Object[]{"bb", 6L, NullHandling.defaultFloatValue()}, results.get(1).getArray());
    Assert.assertArrayEquals(new Object[]{"cc", 2L, NullHandling.defaultFloatValue()}, results.get(2).getArray());
    Assert.assertArrayEquals(new Object[]{"dd", 2L, NullHandling.defaultFloatValue()  }, results.get(3).getArray());
  }

  @Test
  public void testProjectionSelectionMissingAggregatorAndAggregatorInput()
  {
    // since d isn't present on the smaller projection, cant use it, but can still use the larger projection
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("a")
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"))
                    .addAggregator(new DoubleSumAggregatorFactory("d_sum", "d"))
                    .build();
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, null);
    try (final CursorHolder cursorHolder = projectionsCursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      int rowCount = 0;
      while (!cursor.isDone()) {
        rowCount++;
        cursor.advance();
      }
      Assert.assertEquals(7, rowCount);
    }

    final Sequence<ResultRow> resultRows = groupingEngine.process(
        query,
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        nonBlockingPool,
        null
    );

    final List<ResultRow> results = resultRows.toList();
    Assert.assertEquals(2, results.size());
    Assert.assertArrayEquals(new Object[]{"a", 7L, 7.6000000000000005}, results.get(0).getArray());
    Assert.assertArrayEquals(new Object[]{"b", 12L, 13.2}, results.get(1).getArray());
  }

  @Test
  public void testProjectionSelectionMissingColumnOnBaseTableToo()
  {
    // since d isn't present on the smaller projection, cant use it, but can still use the larger projection
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("a")
                    .addDimension("z")
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"))
                    .addAggregator(new DoubleSumAggregatorFactory("d_sum", "d"))
                    .build();
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, null);
    try (final CursorHolder cursorHolder = projectionsCursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      int rowCount = 0;
      while (!cursor.isDone()) {
        rowCount++;
        cursor.advance();
      }
      Assert.assertEquals(7, rowCount);
    }

    final Sequence<ResultRow> resultRows = groupingEngine.process(
        query,
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        nonBlockingPool,
        null
    );

    final List<ResultRow> results = resultRows.toList();
    Assert.assertEquals(2, results.size());
    Assert.assertArrayEquals(new Object[]{"a", null, 7L, 7.6000000000000005}, results.get(0).getArray());
    Assert.assertArrayEquals(new Object[]{"b", null, 12L, 13.2}, results.get(1).getArray());
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
        try {
          index.add(
              new MapBasedInputRow(
                  timestamp,
                  Collections.singletonList("billy"),
                  ImmutableMap.of("billy", "v31234")
              )
          );
        }
        catch (IndexSizeExceededException isee) {
          throw new RuntimeException(isee);
        }

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
