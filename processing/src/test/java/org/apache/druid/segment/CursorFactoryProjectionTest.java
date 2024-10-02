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

package org.apache.druid.segment;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.collections.CloseableDefaultBlockingPool;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.ListBasedInputRow;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.firstlast.last.LongLastAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByResourcesReservationPool;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexCursorFactory;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class CursorFactoryProjectionTest extends InitializedNullHandlingTest
{
  private static final Closer CLOSER = Closer.create();
  private static final DateTime TIMESTAMP = Granularities.DAY.bucket(DateTimes.nowUtc()).getStart();

  private static final RowSignature ROW_SIGNATURE = RowSignature.builder()
                                                                .add("a", ColumnType.STRING)
                                                                .add("b", ColumnType.STRING)
                                                                .add("c", ColumnType.LONG)
                                                                .add("d", ColumnType.DOUBLE)
                                                                .build();
  private static final List<InputRow> ROWS = Arrays.asList(
      new ListBasedInputRow(
          ROW_SIGNATURE,
          TIMESTAMP,
          ROW_SIGNATURE.getColumnNames(),
          Arrays.asList("a", "aa", 1L, 1.0)
      ),
      new ListBasedInputRow(
          ROW_SIGNATURE,
          TIMESTAMP.plusMinutes(2),
          ROW_SIGNATURE.getColumnNames(),
          Arrays.asList("a", "bb", 1L, 1.1, 1.1f)
      ),
      new ListBasedInputRow(
          ROW_SIGNATURE,
          TIMESTAMP.plusMinutes(4),
          ROW_SIGNATURE.getColumnNames(),
          Arrays.asList("a", "cc", 2L, 2.2, 2.2f)
      ),
      new ListBasedInputRow(
          ROW_SIGNATURE,
          TIMESTAMP.plusMinutes(6),
          ROW_SIGNATURE.getColumnNames(),
          Arrays.asList("b", "aa", 3L, 3.3, 3.3f)
      ),
      new ListBasedInputRow(
          ROW_SIGNATURE,
          TIMESTAMP.plusMinutes(8),
          ROW_SIGNATURE.getColumnNames(),
          Arrays.asList("b", "aa", 4L, 4.4, 4.4f)
      ),
      new ListBasedInputRow(
          ROW_SIGNATURE,
          TIMESTAMP.plusMinutes(10),
          ROW_SIGNATURE.getColumnNames(),
          Arrays.asList("b", "bb", 5L, 5.5, 5.5f)
      ),
      new ListBasedInputRow(
          ROW_SIGNATURE,
          TIMESTAMP.plusHours(1),
          ROW_SIGNATURE.getColumnNames(),
          Arrays.asList("a", "aa", 1L, 1.1, 1.1f)
      ),
      new ListBasedInputRow(
          ROW_SIGNATURE,
          TIMESTAMP.plusHours(1).plusMinutes(1),
          ROW_SIGNATURE.getColumnNames(),
          Arrays.asList("a", "dd", 2L, 2.2, 2.2f)
      )
  );

  private static final List<AggregateProjectionSpec> PROJECTIONS = Arrays.asList(
      new AggregateProjectionSpec(
          "ab_hourly_cd_sum",
          VirtualColumns.create(
              Granularities.toVirtualColumn(Granularities.HOUR, "__gran")
          ),
          Arrays.asList(
              new StringDimensionSchema("a"),
              new StringDimensionSchema("b"),
              new LongDimensionSchema("__gran")
          ),
          new AggregatorFactory[]{
              new LongSumAggregatorFactory("_c_sum", "c"),
              new DoubleSumAggregatorFactory("d", "d")
          }
      ),
      new AggregateProjectionSpec(
          "a_hourly_c_sum_with_count_latest",
          VirtualColumns.create(
              Granularities.toVirtualColumn(Granularities.HOUR, "__gran")
          ),
          Arrays.asList(
              new LongDimensionSchema("__gran"),
              new StringDimensionSchema("a")
          ),
          new AggregatorFactory[]{
              new CountAggregatorFactory("chocula"),
              new LongSumAggregatorFactory("_c_sum", "c"),
              new LongLastAggregatorFactory("_c_last", "c", null)
          }
      ),
      new AggregateProjectionSpec(
          "bf_daily_c_sum",
          VirtualColumns.create(
              Granularities.toVirtualColumn(Granularities.DAY, "__gran")
          ),
          Arrays.asList(
              new LongDimensionSchema("__gran"),
              new StringDimensionSchema("b"),
              new FloatDimensionSchema("e")
          ),
          new AggregatorFactory[]{
              new LongSumAggregatorFactory("_c_sum", "c")
          }
      ),
      new AggregateProjectionSpec(
          "ab_daily",
          null,
          Arrays.asList(
              new StringDimensionSchema("a"),
              new StringDimensionSchema("b")
          ),
          null
      ),
      new AggregateProjectionSpec(
          "abfoo_daily",
          VirtualColumns.create(
              new ExpressionVirtualColumn(
                  "bfoo",
                  "concat(b, 'foo')",
                  ColumnType.STRING,
                  TestExprMacroTable.INSTANCE
              )
          ),
          Arrays.asList(
              new StringDimensionSchema("a"),
              new StringDimensionSchema("bfoo")
          ),
          null
      )
  );

  @Parameterized.Parameters(name = "name: {0}, sortByDim: {3}")
  public static Collection<?> constructorFeeder() throws IOException
  {
    final List<Object[]> constructors = new ArrayList<>();
    final DimensionsSpec.Builder dimensionsBuilder =
        DimensionsSpec.builder()
                      .setDimensions(
                          Arrays.asList(
                              new StringDimensionSchema("a"),
                              new StringDimensionSchema("b"),
                              new LongDimensionSchema("c"),
                              new DoubleDimensionSchema("d"),
                              new FloatDimensionSchema("e")
                          )
                      );
    DimensionsSpec dimsTimeOrdered = dimensionsBuilder.build();
    DimensionsSpec dimsOrdered = dimensionsBuilder.setForceSegmentSortByTime(false).build();


    for (boolean incremental : new boolean[]{true, false}) {
      for (boolean sortByDim : new boolean[]{true, false}) {
        final DimensionsSpec dims = sortByDim ? dimsOrdered : dimsTimeOrdered;
        if (incremental) {
          IncrementalIndex index = CLOSER.register(makeBuilder(dims).buildIncrementalIndex());
          constructors.add(new Object[]{
              "incrementalIndex",
              new IncrementalIndexCursorFactory(index),
              new IncrementalIndexTimeBoundaryInspector(index),
              sortByDim
          });
        } else {
          QueryableIndex index = CLOSER.register(makeBuilder(dims).buildMMappedIndex());
          constructors.add(new Object[]{
              "queryableIndex",
              new QueryableIndexCursorFactory(index),
              QueryableIndexTimeBoundaryInspector.create(index),
              sortByDim
          });
        }
      }
    }
    return constructors;
  }

  @AfterClass
  public static void cleanup() throws IOException
  {
    CLOSER.close();
  }


  public final CursorFactory projectionsCursorFactory;
  public final TimeBoundaryInspector projectionsTimeBoundaryInspector;

  private final GroupingEngine groupingEngine;

  private final NonBlockingPool<ByteBuffer> nonBlockingPool;
  public final boolean sortByDim;

  @Rule
  public final CloserRule closer = new CloserRule(false);

  public CursorFactoryProjectionTest(
      String name,
      CursorFactory projectionsCursorFactory,
      TimeBoundaryInspector projectionsTimeBoundaryInspector,
      boolean sortByDim
  )
  {
    this.projectionsCursorFactory = projectionsCursorFactory;
    this.projectionsTimeBoundaryInspector = projectionsTimeBoundaryInspector;
    this.sortByDim = sortByDim;
    this.nonBlockingPool = closer.closeLater(
        new CloseableStupidPool<>(
            "GroupByQueryEngine-bufferPool",
            () -> ByteBuffer.allocate(50000)
        )
    );
    this.groupingEngine = new GroupingEngine(
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
    if (projectionsCursorFactory instanceof QueryableIndexCursorFactory) {
      Assert.assertArrayEquals(new Object[]{"a", "dd"}, results.get(0).getArray());
      Assert.assertArrayEquals(new Object[]{"a", "aa"}, results.get(1).getArray());
      Assert.assertArrayEquals(new Object[]{"b", "aa"}, results.get(2).getArray());
      Assert.assertArrayEquals(new Object[]{"a", "cc"}, results.get(3).getArray());
      Assert.assertArrayEquals(new Object[]{"a", "bb"}, results.get(4).getArray());
      Assert.assertArrayEquals(new Object[]{"b", "bb"}, results.get(5).getArray());
    } else {
      Assert.assertArrayEquals(new Object[]{"a", "aa"}, results.get(0).getArray());
      Assert.assertArrayEquals(new Object[]{"a", "bb"}, results.get(1).getArray());
      Assert.assertArrayEquals(new Object[]{"a", "cc"}, results.get(2).getArray());
      Assert.assertArrayEquals(new Object[]{"a", "dd"}, results.get(3).getArray());
      Assert.assertArrayEquals(new Object[]{"b", "aa"}, results.get(4).getArray());
      Assert.assertArrayEquals(new Object[]{"b", "bb"}, results.get(5).getArray());
    }
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
                    .setLimitSpec(
                        new DefaultLimitSpec(
                            Arrays.asList(
                                new OrderByColumnSpec("a", OrderByColumnSpec.Direction.ASCENDING, StringComparators.LEXICOGRAPHIC),
                                new OrderByColumnSpec("v0", OrderByColumnSpec.Direction.ASCENDING, StringComparators.LEXICOGRAPHIC)
                            ),
                            10
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
    if (projectionsCursorFactory instanceof QueryableIndexCursorFactory) {
      Assert.assertArrayEquals(new Object[]{"a", "ddfoo"}, results.get(0).getArray());
      Assert.assertArrayEquals(new Object[]{"a", "aafoo"}, results.get(1).getArray());
      Assert.assertArrayEquals(new Object[]{"b", "aafoo"}, results.get(2).getArray());
      Assert.assertArrayEquals(new Object[]{"a", "ccfoo"}, results.get(3).getArray());
      Assert.assertArrayEquals(new Object[]{"a", "bbfoo"}, results.get(4).getArray());
      Assert.assertArrayEquals(new Object[]{"b", "bbfoo"}, results.get(5).getArray());
    } else {
      Assert.assertArrayEquals(new Object[]{"a", "aafoo"}, results.get(0).getArray());
      Assert.assertArrayEquals(new Object[]{"a", "bbfoo"}, results.get(1).getArray());
      Assert.assertArrayEquals(new Object[]{"a", "ccfoo"}, results.get(2).getArray());
      Assert.assertArrayEquals(new Object[]{"a", "ddfoo"}, results.get(3).getArray());
      Assert.assertArrayEquals(new Object[]{"b", "aafoo"}, results.get(4).getArray());
      Assert.assertArrayEquals(new Object[]{"b", "bbfoo"}, results.get(5).getArray());
    }
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
    if (projectionsCursorFactory instanceof QueryableIndexCursorFactory) {
      Assert.assertArrayEquals(new Object[]{"a", "dd", 1L}, results.get(0).getArray());
      Assert.assertArrayEquals(new Object[]{"a", "aa", 2L}, results.get(1).getArray());
      Assert.assertArrayEquals(new Object[]{"a", "bb", 1L}, results.get(2).getArray());
      Assert.assertArrayEquals(new Object[]{"b", "aa", 2L}, results.get(3).getArray());
      Assert.assertArrayEquals(new Object[]{"a", "cc", 1L}, results.get(4).getArray());
      Assert.assertArrayEquals(new Object[]{"b", "bb", 1L}, results.get(5).getArray());
    } else {
      Assert.assertArrayEquals(new Object[]{"a", "aa", 2L}, results.get(0).getArray());
      Assert.assertArrayEquals(new Object[]{"a", "bb", 1L}, results.get(1).getArray());
      Assert.assertArrayEquals(new Object[]{"a", "cc", 1L}, results.get(2).getArray());
      Assert.assertArrayEquals(new Object[]{"a", "dd", 1L}, results.get(3).getArray());
      Assert.assertArrayEquals(new Object[]{"b", "aa", 2L}, results.get(4).getArray());
      Assert.assertArrayEquals(new Object[]{"b", "bb", 1L}, results.get(5).getArray());
    }
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
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"))
                    .addAggregator(new LongLastAggregatorFactory("c_last", "c", null))
                    .setContext(ImmutableMap.of(QueryContexts.CTX_NO_PROJECTION, true))
                    .build();
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, null);
    try (final CursorHolder cursorHolder = projectionsCursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      int rowCount = 0;
      while (!cursor.isDone()) {
        rowCount++;
        cursor.advance();
      }
      // has to scan full 8 rows because context ensures projections not used
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
    Assert.assertEquals(2, results.size());
    Assert.assertArrayEquals(
        new Object[]{"a", 7L, Pair.of(TIMESTAMP.plusHours(1).plusMinutes(1).getMillis(), 2L)},
        results.get(0).getArray()
    );
    Assert.assertArrayEquals(
        new Object[]{"b", 12L, Pair.of(TIMESTAMP.plusMinutes(10).getMillis(), 5L)},
        results.get(1).getArray()
    );
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
                    .addAggregator(new LongLastAggregatorFactory("c_last", "c", null))
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
    Assert.assertArrayEquals(
        new Object[]{"a", 7L, Pair.of(TIMESTAMP.plusHours(1).plusMinutes(1).getMillis(), 2L)},
        results.get(0).getArray()
    );
    Assert.assertArrayEquals(
        new Object[]{"b", 12L, Pair.of(TIMESTAMP.plusMinutes(10).getMillis(), 5L)},
        results.get(1).getArray()
    );
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
  public void testProjectionSingleDimMultipleSameAggs()
  {
    // test can use the single dimension projection
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("a")
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"))
                    .addAggregator(new LongSumAggregatorFactory("c_sum_2", "c"))
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
    Assert.assertArrayEquals(new Object[]{"a", 7L, 7L}, results.get(0).getArray());
    Assert.assertArrayEquals(new Object[]{"b", 12L, 12L}, results.get(1).getArray());
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

    if (sortByDim && projectionsCursorFactory instanceof QueryableIndexCursorFactory) {
      Assert.assertArrayEquals(
          new Object[]{ROWS.get(3).getTimestamp().getMillis(), "b", 3L},
          results.get(0).getArray()
      );
      Assert.assertArrayEquals(
          new Object[]{ROWS.get(0).getTimestamp().getMillis(), "a", 1L},
          results.get(1).getArray()
      );
      Assert.assertArrayEquals(
          new Object[]{ROWS.get(7).getTimestamp().getMillis(), "a", 2L},
          results.get(2).getArray()
      );
      Assert.assertArrayEquals(
          new Object[]{ROWS.get(1).getTimestamp().getMillis(), "a", 1L},
          results.get(3).getArray()
      );
      Assert.assertArrayEquals(
          new Object[]{ROWS.get(4).getTimestamp().getMillis(), "b", 4L},
          results.get(4).getArray()
      );
      Assert.assertArrayEquals(
          new Object[]{ROWS.get(2).getTimestamp().getMillis(), "a", 2L},
          results.get(5).getArray()
      );
      Assert.assertArrayEquals(
          new Object[]{ROWS.get(5).getTimestamp().getMillis(), "b", 5L},
          results.get(6).getArray()
      );
      Assert.assertArrayEquals(
          new Object[]{ROWS.get(6).getTimestamp().getMillis(), "a", 1L},
          results.get(7).getArray()
      );
    } else {
      Assert.assertArrayEquals(
          new Object[]{ROWS.get(0).getTimestamp().getMillis(), "a", 1L},
          results.get(0).getArray()
      );
      Assert.assertArrayEquals(
          new Object[]{ROWS.get(1).getTimestamp().getMillis(), "a", 1L},
          results.get(1).getArray()
      );
      Assert.assertArrayEquals(
          new Object[]{ROWS.get(2).getTimestamp().getMillis(), "a", 2L},
          results.get(2).getArray()
      );
      Assert.assertArrayEquals(
          new Object[]{ROWS.get(3).getTimestamp().getMillis(), "b", 3L},
          results.get(3).getArray()
      );
      Assert.assertArrayEquals(
          new Object[]{ROWS.get(4).getTimestamp().getMillis(), "b", 4L},
          results.get(4).getArray()
      );
      Assert.assertArrayEquals(
          new Object[]{ROWS.get(5).getTimestamp().getMillis(), "b", 5L},
          results.get(5).getArray()
      );
      Assert.assertArrayEquals(
          new Object[]{ROWS.get(6).getTimestamp().getMillis(), "a", 1L},
          results.get(6).getArray()
      );
      Assert.assertArrayEquals(
          new Object[]{ROWS.get(7).getTimestamp().getMillis(), "a", 2L},
          results.get(7).getArray()
      );
    }
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
    if (sortByDim && projectionsCursorFactory instanceof QueryableIndexCursorFactory) {
      Assert.assertArrayEquals(new Object[]{TIMESTAMP.plusHours(1).getMillis(), "a", 3L}, results.get(1).getArray());
      Assert.assertArrayEquals(new Object[]{TIMESTAMP.getMillis(), "a", 4L}, results.get(0).getArray());
      Assert.assertArrayEquals(new Object[]{TIMESTAMP.getMillis(), "b", 12L}, results.get(2).getArray());
    } else {
      Assert.assertArrayEquals(new Object[]{TIMESTAMP.getMillis(), "a", 4L}, results.get(0).getArray());
      Assert.assertArrayEquals(new Object[]{TIMESTAMP.getMillis(), "b", 12L}, results.get(1).getArray());
      Assert.assertArrayEquals(new Object[]{TIMESTAMP.plusHours(1).getMillis(), "a", 3L}, results.get(2).getArray());
    }
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
    Assert.assertArrayEquals(new Object[]{"dd", 2L, NullHandling.defaultFloatValue()}, results.get(3).getArray());
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

  private static IndexBuilder makeBuilder(DimensionsSpec dimensionsSpec) throws IOException
  {
    File tmp = FileUtils.createTempDir();
    CLOSER.register(tmp::delete);
    return IndexBuilder.create()
                       .tmpDir(tmp)
                       .schema(
                           IncrementalIndexSchema.builder()
                                                 .withDimensionsSpec(dimensionsSpec)
                                                 .withRollup(false)
                                                 .withMinTimestamp(TIMESTAMP.getMillis())
                                                 .withProjections(PROJECTIONS)
                                                 .build()
                       )
                       .rows(ROWS);
  }
}
