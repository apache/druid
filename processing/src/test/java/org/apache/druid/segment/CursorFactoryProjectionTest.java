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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;
import org.apache.druid.collections.CloseableDefaultBlockingPool;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.ListBasedInputRow;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.firstlast.last.LongLastAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByResourcesReservationPool;
import org.apache.druid.query.groupby.GroupByStatsProvider;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexCursorFactory;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class CursorFactoryProjectionTest extends InitializedNullHandlingTest
{
  private static final Closer CLOSER = Closer.create();
  static final DateTime TIMESTAMP = Granularities.DAY.bucket(DateTimes.nowUtc()).getStart();

  static final RowSignature ROW_SIGNATURE = RowSignature.builder()
                                                        .add("a", ColumnType.STRING)
                                                        .add("b", ColumnType.STRING)
                                                        .add("c", ColumnType.LONG)
                                                        .add("d", ColumnType.DOUBLE)
                                                        .add("e", ColumnType.FLOAT)
                                                        .add("f", ColumnType.NESTED_DATA)
                                                        .build();

  public static List<InputRow> makeRows(List<String> dimensions)
  {
    return Arrays.asList(
        new ListBasedInputRow(
            ROW_SIGNATURE,
            TIMESTAMP,
            dimensions,
            Arrays.asList("a", "aa", 1L, 1.0, null, Map.of("x", "a"))
        ),
        new ListBasedInputRow(
            ROW_SIGNATURE,
            TIMESTAMP.plusMinutes(2),
            dimensions,
            Arrays.asList("a", "bb", 1L, 1.1, 1.1f, Map.of("x", "a"))
        ),
        new ListBasedInputRow(
            ROW_SIGNATURE,
            TIMESTAMP.plusMinutes(4),
            dimensions,
            Arrays.asList("a", "cc", 2L, 2.2, 2.2f, Map.of("x", "a"))
        ),
        new ListBasedInputRow(
            ROW_SIGNATURE,
            TIMESTAMP.plusMinutes(6),
            dimensions,
            Arrays.asList("b", "aa", 3L, 3.3, 3.3f, Map.of("x", "b"))
        ),
        new ListBasedInputRow(
            ROW_SIGNATURE,
            TIMESTAMP.plusMinutes(8),
            dimensions,
            Arrays.asList("b", "aa", 4L, 4.4, 4.4f, Map.of("x", "b"))
        ),
        new ListBasedInputRow(
            ROW_SIGNATURE,
            TIMESTAMP.plusMinutes(10),
            dimensions,
            Arrays.asList("b", "bb", 5L, 5.5, 5.5f, Map.of("x", "b"))
        ),
        new ListBasedInputRow(
            ROW_SIGNATURE,
            TIMESTAMP.plusHours(1),
            dimensions,
            Arrays.asList("a", "aa", 1L, 1.1, 1.1f, Map.of("x", "a"))
        ),
        new ListBasedInputRow(
            ROW_SIGNATURE,
            TIMESTAMP.plusHours(1).plusMinutes(1),
            dimensions,
            Arrays.asList("a", "dd", 2L, 2.2, 2.2f, Map.of("x", "a"))
        )
    );
  }

  static final List<InputRow> ROWS = makeRows(ROW_SIGNATURE.getColumnNames());
  static final List<InputRow> ROLLUP_ROWS = makeRows(ImmutableList.of("a", "b"));

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
          "b_hourly_c_sum_non_time_ordered",
          VirtualColumns.create(
              Granularities.toVirtualColumn(Granularities.HOUR, "__gran")
          ),
          Arrays.asList(
              new StringDimensionSchema("b"),
              new LongDimensionSchema("__gran")
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
          "b_c_sum",
          VirtualColumns.EMPTY,
          List.of(new StringDimensionSchema("b")),
          new AggregatorFactory[]{
              new LongSumAggregatorFactory("_c_sum", "c")
          }
      ),
      new AggregateProjectionSpec(
          "ab",
          null,
          Arrays.asList(
              new StringDimensionSchema("a"),
              new StringDimensionSchema("b")
          ),
          null
      ),
      new AggregateProjectionSpec(
          "abfoo",
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
      ),
      new AggregateProjectionSpec(
          "c_sum_daily",
          VirtualColumns.create(Granularities.toVirtualColumn(Granularities.DAY, "__gran")),
          Collections.singletonList(new LongDimensionSchema("__gran")),
          new AggregatorFactory[]{
              new LongSumAggregatorFactory("_c_sum", "c")
          }
      ),
      new AggregateProjectionSpec(
          "c_sum",
          VirtualColumns.EMPTY,
          Collections.emptyList(),
          new AggregatorFactory[]{
              new LongSumAggregatorFactory("_c_sum", "c")
          }
      ),
      new AggregateProjectionSpec(
          "missing_column",
          VirtualColumns.EMPTY,
          List.of(new StringDimensionSchema("missing")),
          new AggregatorFactory[]{
              new LongSumAggregatorFactory("csum", "c")
          }
      ),
      new  AggregateProjectionSpec(
          "json",
          VirtualColumns.EMPTY,
          List.of(new AutoTypeColumnSchema("f", null)),
          new AggregatorFactory[]{
              new LongSumAggregatorFactory("_c_sum", "c")
          }
      )
  );

  private static final List<AggregateProjectionSpec> ROLLUP_PROJECTIONS = Arrays.asList(
      new AggregateProjectionSpec(
          "a_hourly_c_sum_with_count",
          VirtualColumns.create(
              Granularities.toVirtualColumn(Granularities.HOUR, "__gran")
          ),
          Arrays.asList(
              new LongDimensionSchema("__gran"),
              new StringDimensionSchema("a")
          ),
          new AggregatorFactory[]{
              new CountAggregatorFactory("chocula"),
              new LongSumAggregatorFactory("sum_c", "sum_c")
          }
      ),
      new AggregateProjectionSpec(
          "afoo",
          VirtualColumns.create(
              new ExpressionVirtualColumn(
                  "afoo",
                  "concat(a, 'foo')",
                  ColumnType.STRING,
                  TestExprMacroTable.INSTANCE
              )
          ),
          List.of(
              new StringDimensionSchema("afoo")
          ),
          new AggregatorFactory[]{
              new LongSumAggregatorFactory("sum_c", "sum_c")
          }
      )
  );

  private static final List<AggregateProjectionSpec> AUTO_PROJECTIONS =
      PROJECTIONS.stream()
                 .map(projection -> new AggregateProjectionSpec(
                     projection.getName(),
                     projection.getVirtualColumns(),
                     projection.getGroupingColumns()
                               .stream()
                               .map(x -> new AutoTypeColumnSchema(x.getName(), null))
                               .collect(Collectors.toList()),
                     projection.getAggregators()
                 ))
                 .collect(Collectors.toList());

  private static final List<AggregateProjectionSpec> AUTO_ROLLUP_PROJECTIONS =
      ROLLUP_PROJECTIONS.stream()
                        .map(projection -> new AggregateProjectionSpec(
                            projection.getName(),
                            projection.getVirtualColumns(),
                            projection.getGroupingColumns()
                                      .stream()
                                      .map(x -> new AutoTypeColumnSchema(x.getName(), null))
                                      .collect(Collectors.toList()),
                            projection.getAggregators()
                        ))
                        .collect(Collectors.toList());

  @Parameterized.Parameters(name = "name: {0}, segmentTimeOrdered: {5}, autoSchema: {6}")
  public static Collection<?> constructorFeeder()
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
                              new FloatDimensionSchema("e"),
                              new AutoTypeColumnSchema("f", null),
                              new StringDimensionSchema("missing")
                          )
                      );

    final DimensionsSpec.Builder rollupDimensionsBuilder =
        DimensionsSpec.builder()
                      .setDimensions(
                          Arrays.asList(
                              new StringDimensionSchema("a"),
                              new StringDimensionSchema("b")
                          )
                      );
    final AggregatorFactory[] rollupAggs = new AggregatorFactory[]{
        new LongSumAggregatorFactory("sum_c", "c"),
        new DoubleSumAggregatorFactory("sum_d", "d"),
        new FloatSumAggregatorFactory("sum_e", "e")
    };

    DimensionsSpec dimsTimeOrdered = dimensionsBuilder.build();
    DimensionsSpec dimsOrdered = dimensionsBuilder.setForceSegmentSortByTime(false).build();

    DimensionsSpec rollupDimsTimeOrdered = rollupDimensionsBuilder.build();
    DimensionsSpec rollupDimsOrdered = rollupDimensionsBuilder.setForceSegmentSortByTime(false).build();


    List<DimensionSchema> autoDims = dimsOrdered.getDimensions()
                                                .stream()
                                                .map(x -> new AutoTypeColumnSchema(x.getName(), null))
                                                .collect(Collectors.toList());

    List<DimensionSchema> rollupAutoDims = rollupDimsOrdered.getDimensions()
                                                            .stream()
                                                            .map(x -> new AutoTypeColumnSchema(x.getName(), null))
                                                            .collect(Collectors.toList());

    for (boolean incremental : new boolean[]{true, false}) {
      for (boolean sortByDim : new boolean[]{true, false}) {
        for (boolean autoSchema : new boolean[]{false, true}) {
          for (boolean writeNullColumns : new boolean[]{true, false}) {

            final DimensionsSpec dims;
            final DimensionsSpec rollupDims;
            if (sortByDim) {
              if (autoSchema) {
                dims = dimsOrdered.withDimensions(autoDims);
                rollupDims = rollupDimsOrdered.withDimensions(rollupAutoDims);
              } else {
                dims = dimsOrdered;
                rollupDims = rollupDimsOrdered;
              }
            } else {
              if (autoSchema) {
                dims = dimsTimeOrdered.withDimensions(autoDims);
                rollupDims = rollupDimsTimeOrdered.withDimensions(autoDims);
              } else {
                dims = dimsTimeOrdered;
                rollupDims = rollupDimsTimeOrdered;
              }
            }
            if (incremental) {
              IncrementalIndex index = CLOSER.register(makeBuilder(dims, autoSchema, writeNullColumns).buildIncrementalIndex());
              IncrementalIndex rollupIndex = CLOSER.register(
                  makeRollupBuilder(rollupDims, rollupAggs, autoSchema).buildIncrementalIndex()
              );
              constructors.add(new Object[]{
                  "incrementalIndex",
                  new IncrementalIndexCursorFactory(index),
                  new IncrementalIndexTimeBoundaryInspector(index),
                  new IncrementalIndexCursorFactory(rollupIndex),
                  new IncrementalIndexTimeBoundaryInspector(rollupIndex),
                  !sortByDim,
                  autoSchema
              });
            } else {
              QueryableIndex index = CLOSER.register(makeBuilder(dims, autoSchema, writeNullColumns).buildMMappedIndex());
              QueryableIndex rollupIndex = CLOSER.register(
                  makeRollupBuilder(rollupDims, rollupAggs, autoSchema).buildMMappedIndex()
              );
              constructors.add(new Object[]{
                  "queryableIndex",
                  new QueryableIndexCursorFactory(index),
                  QueryableIndexTimeBoundaryInspector.create(index),
                  new QueryableIndexCursorFactory(rollupIndex),
                  QueryableIndexTimeBoundaryInspector.create(rollupIndex),
                  !sortByDim,
                  autoSchema
              });
            }
          }
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
  public final CursorFactory rollupProjectionsCursorFactory;
  public final TimeBoundaryInspector rollupProjectionsTimeBoundaryInspector;

  private final GroupingEngine groupingEngine;
  private final TimeseriesQueryEngine timeseriesEngine;

  private final NonBlockingPool<ByteBuffer> nonBlockingPool;
  public final boolean segmentSortedByTime;
  public final boolean autoSchema;

  @Rule
  public final CloserRule closer = new CloserRule(false);

  public CursorFactoryProjectionTest(
      String name,
      CursorFactory projectionsCursorFactory,
      TimeBoundaryInspector projectionsTimeBoundaryInspector,
      CursorFactory rollupProjectionsCursorFactory,
      TimeBoundaryInspector rollupProjectionsTimeBoundaryInspector,
      boolean segmentSortedByTime,
      boolean autoSchema
  )
  {
    this.projectionsCursorFactory = projectionsCursorFactory;
    this.projectionsTimeBoundaryInspector = projectionsTimeBoundaryInspector;
    this.rollupProjectionsCursorFactory = rollupProjectionsCursorFactory;
    this.rollupProjectionsTimeBoundaryInspector = rollupProjectionsTimeBoundaryInspector;
    this.segmentSortedByTime = segmentSortedByTime;
    this.autoSchema = autoSchema;
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
        },
        new GroupByStatsProvider()
    );
    this.timeseriesEngine = new TimeseriesQueryEngine(nonBlockingPool);
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
      if (autoSchema) {
        Assert.assertArrayEquals(new Object[]{"b", "bb"}, results.get(0).getArray());
        Assert.assertArrayEquals(new Object[]{"a", "dd"}, results.get(1).getArray());
        Assert.assertArrayEquals(new Object[]{"b", "aa"}, results.get(2).getArray());
        Assert.assertArrayEquals(new Object[]{"a", "cc"}, results.get(3).getArray());
        Assert.assertArrayEquals(new Object[]{"a", "bb"}, results.get(4).getArray());
        Assert.assertArrayEquals(new Object[]{"a", "aa"}, results.get(5).getArray());
      } else {
        Assert.assertArrayEquals(new Object[]{"a", "dd"}, results.get(0).getArray());
        Assert.assertArrayEquals(new Object[]{"a", "aa"}, results.get(1).getArray());
        Assert.assertArrayEquals(new Object[]{"b", "aa"}, results.get(2).getArray());
        Assert.assertArrayEquals(new Object[]{"a", "cc"}, results.get(3).getArray());
        Assert.assertArrayEquals(new Object[]{"a", "bb"}, results.get(4).getArray());
        Assert.assertArrayEquals(new Object[]{"b", "bb"}, results.get(5).getArray());
      }
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
                    .setContext(ImmutableMap.of(QueryContexts.USE_PROJECTION, "abfoo"))
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
      // testing ordering of stuff is kind of tricky at this level...
      if (autoSchema) {
        Assert.assertArrayEquals(new Object[]{"b", "bbfoo"}, results.get(0).getArray());
        Assert.assertArrayEquals(new Object[]{"a", "ddfoo"}, results.get(1).getArray());
        Assert.assertArrayEquals(new Object[]{"b", "aafoo"}, results.get(2).getArray());
        Assert.assertArrayEquals(new Object[]{"a", "ccfoo"}, results.get(3).getArray());
        Assert.assertArrayEquals(new Object[]{"a", "bbfoo"}, results.get(4).getArray());
        Assert.assertArrayEquals(new Object[]{"a", "aafoo"}, results.get(5).getArray());
      } else {
        Assert.assertArrayEquals(new Object[]{"a", "ddfoo"}, results.get(0).getArray());
        Assert.assertArrayEquals(new Object[]{"a", "aafoo"}, results.get(1).getArray());
        Assert.assertArrayEquals(new Object[]{"b", "aafoo"}, results.get(2).getArray());
        Assert.assertArrayEquals(new Object[]{"a", "ccfoo"}, results.get(3).getArray());
        Assert.assertArrayEquals(new Object[]{"a", "bbfoo"}, results.get(4).getArray());
        Assert.assertArrayEquals(new Object[]{"b", "bbfoo"}, results.get(5).getArray());
      }
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
    // cannot use a projection since count is not defined
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
    Assert.assertEquals(6, results.size());
    if (projectionsCursorFactory instanceof QueryableIndexCursorFactory) {
      if (autoSchema) {
        Assert.assertArrayEquals(new Object[]{"b", "aa", 2L}, results.get(0).getArray());
        Assert.assertArrayEquals(new Object[]{"a", "cc", 1L}, results.get(1).getArray());
        Assert.assertArrayEquals(new Object[]{"a", "bb", 1L}, results.get(2).getArray());
        Assert.assertArrayEquals(new Object[]{"b", "bb", 1L}, results.get(3).getArray());
        Assert.assertArrayEquals(new Object[]{"a", "dd", 1L}, results.get(4).getArray());
        Assert.assertArrayEquals(new Object[]{"a", "aa", 2L}, results.get(5).getArray());
      } else {
        Assert.assertArrayEquals(new Object[]{"a", "dd", 1L}, results.get(0).getArray());
        Assert.assertArrayEquals(new Object[]{"a", "aa", 2L}, results.get(1).getArray());
        Assert.assertArrayEquals(new Object[]{"a", "bb", 1L}, results.get(2).getArray());
        Assert.assertArrayEquals(new Object[]{"b", "aa", 2L}, results.get(3).getArray());
        Assert.assertArrayEquals(new Object[]{"a", "cc", 1L}, results.get(4).getArray());
        Assert.assertArrayEquals(new Object[]{"b", "bb", 1L}, results.get(5).getArray());
      }
    } else {
      Assert.assertArrayEquals(new Object[]{"a", "aa", 2L}, results.get(0).getArray());
      Assert.assertArrayEquals(new Object[]{"a", "bb", 1L}, results.get(1).getArray());
      Assert.assertArrayEquals(new Object[]{"a", "cc", 1L}, results.get(2).getArray());
      Assert.assertArrayEquals(new Object[]{"b", "aa", 2L}, results.get(3).getArray());
      Assert.assertArrayEquals(new Object[]{"b", "bb", 1L}, results.get(4).getArray());
      Assert.assertArrayEquals(new Object[]{"a", "dd", 1L}, results.get(5).getArray());
    }
  }

  @Test
  public void testProjectionSelectionTwoDimsCountForce()
  {
    // cannot use a projection since count is not defined
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("a")
                    .addDimension("b")
                    .addAggregator(new CountAggregatorFactory("count"))
                    .setContext(ImmutableMap.of(QueryContexts.FORCE_PROJECTION, true))
                    .build();

    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, null);

    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> projectionsCursorFactory.makeCursorHolder(buildSpec)
    );
    Assert.assertEquals("Force projections specified, but none satisfy query", t.getMessage());
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
                    .setContext(ImmutableMap.of(QueryContexts.NO_PROJECTIONS, true))
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
  public void testProjectionSingleDimMissing()
  {
    // test can use the single dimension projection
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("missing")
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
      Assert.assertEquals(1, rowCount);
    }
    final Sequence<ResultRow> resultRows = groupingEngine.process(
        query,
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        nonBlockingPool,
        null
    );
    final List<ResultRow> results = resultRows.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertArrayEquals(
        new Object[]{null, 19L},
        results.get(0).getArray()
    );
  }

  @Test
  public void testProjectionSingleDimFilter()
  {
    // test can use the single dimension projection
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("a")
                    .setDimFilter(new EqualityFilter("a", ColumnType.STRING, "a", null))
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
      Assert.assertEquals(2, rowCount);
    }
    final Sequence<ResultRow> resultRows = groupingEngine.process(
        query,
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        nonBlockingPool,
        null
    );
    final List<ResultRow> results = resultRows.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertArrayEquals(
        new Object[]{"a", 7L, Pair.of(TIMESTAMP.plusHours(1).plusMinutes(1).getMillis(), 2L)},
        results.get(0).getArray()
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
    if (segmentSortedByTime) {
      queryBuilder.addDimension("a")
                  .setGranularity(Granularities.MINUTE);
    } else {
      queryBuilder.setVirtualColumns(Granularities.toVirtualColumn(Granularities.MINUTE, "__gran"))
                  .setDimensions(
                      DefaultDimensionSpec.of("__gran", ColumnType.LONG),
                      DefaultDimensionSpec.of("a")
                  )
                  .setGranularity(Granularities.ALL);
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

    if (!segmentSortedByTime && projectionsCursorFactory instanceof QueryableIndexCursorFactory) {
      // this sorts funny when not time ordered
      Set<Object[]> resultsInNoParticularOrder = makeArrayResultSet();
      resultsInNoParticularOrder.addAll(
          ROWS.stream()
              .map(x -> new Object[]{x.getTimestamp().getMillis(), x.getRaw("a"), x.getRaw("c")})
              .collect(Collectors.toList())
      );
      for (ResultRow row : results) {
        Assert.assertTrue(resultsInNoParticularOrder.contains(row.getArray()));
      }
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
    if (segmentSortedByTime) {
      queryBuilder.addDimension("a")
                  .setGranularity(Granularities.HOUR);
    } else {
      queryBuilder.setGranularity(Granularities.ALL)
                  .setDimensions(
                      DefaultDimensionSpec.of("__gran", ColumnType.LONG),
                      DefaultDimensionSpec.of("a")
                  )
                  .setVirtualColumns(Granularities.toVirtualColumn(Granularities.HOUR, "__gran"));
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
    if (!segmentSortedByTime && projectionsCursorFactory instanceof QueryableIndexCursorFactory) {
      Set<Object[]> resultsInNoParticularOrder = makeArrayResultSet(
          new Object[]{TIMESTAMP.getMillis(), "a", 4L},
          new Object[]{TIMESTAMP.getMillis(), "b", 12L},
          new Object[]{TIMESTAMP.plusHours(1).getMillis(), "a", 3L}
      );
      for (ResultRow row : results) {
        Assert.assertTrue(resultsInNoParticularOrder.contains(row.getArray()));
      }
    } else {
      Assert.assertArrayEquals(new Object[]{TIMESTAMP.getMillis(), "a", 4L}, results.get(0).getArray());
      Assert.assertArrayEquals(new Object[]{TIMESTAMP.getMillis(), "b", 12L}, results.get(1).getArray());
      Assert.assertArrayEquals(new Object[]{TIMESTAMP.plusHours(1).getMillis(), "a", 3L}, results.get(2).getArray());
    }
  }

  @Test
  public void testQueryGranularityFitsProjectionGranularityNotTimeOrdered()
  {
    final GroupByQuery.Builder queryBuilder =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setInterval(Intervals.ETERNITY)
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"));
    if (segmentSortedByTime) {
      queryBuilder.addDimension("b")
                  .setGranularity(Granularities.HOUR);
    } else {
      queryBuilder.setGranularity(Granularities.ALL)
                  .setDimensions(
                      DefaultDimensionSpec.of("__gran", ColumnType.LONG),
                      DefaultDimensionSpec.of("b")
                  )
                  .setVirtualColumns(Granularities.toVirtualColumn(Granularities.HOUR, "__gran"));
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
      Assert.assertEquals(segmentSortedByTime ? 8 : 5, rowCount);
    }

    final Sequence<ResultRow> resultRows = groupingEngine.process(
        query,
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        nonBlockingPool,
        null
    );

    final List<ResultRow> results = resultRows.toList();
    Assert.assertEquals(5, results.size());
    Set<Object[]> resultsInNoParticularOrder = makeArrayResultSet(
        new Object[]{TIMESTAMP.getMillis(), "aa", 8L},
        new Object[]{TIMESTAMP.getMillis(), "bb", 6L},
        new Object[]{TIMESTAMP.getMillis(), "cc", 2L},
        new Object[]{TIMESTAMP.plusHours(1).getMillis(), "aa", 1L},
        new Object[]{TIMESTAMP.plusHours(1).getMillis(), "dd", 2L}
    );
    for (ResultRow row : results) {
      Assert.assertTrue("missing row" + row.toString(), resultsInNoParticularOrder.contains(row.getArray()));
    }
  }


  @Test
  public void testQueryGranularityLargerProjectionGranularity()
  {
    final GroupByQuery.Builder queryBuilder =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setInterval(Intervals.ETERNITY)
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"));
    if (segmentSortedByTime) {
      queryBuilder.addDimension("a")
                  .setGranularity(Granularities.DAY);
    } else {
      queryBuilder.setGranularity(Granularities.ALL)
                  .setDimensions(
                      DefaultDimensionSpec.of("__gran", ColumnType.LONG),
                      DefaultDimensionSpec.of("a")
                  )
                  .setVirtualColumns(Granularities.toVirtualColumn(Granularities.DAY, "__gran"));
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
    Assert.assertEquals(2, results.size());
    if (!segmentSortedByTime && projectionsCursorFactory instanceof QueryableIndexCursorFactory) {
      Set<Object[]> resultsInNoParticularOrder = makeArrayResultSet(
          new Object[]{TIMESTAMP.getMillis(), "a", 7L},
          new Object[]{TIMESTAMP.getMillis(), "b", 12L}
      );
      for (ResultRow row : results) {
        Assert.assertTrue(resultsInNoParticularOrder.contains(row.getArray()));
      }
    } else {
      Assert.assertArrayEquals(new Object[]{TIMESTAMP.getMillis(), "a", 7L}, results.get(0).getArray());
      Assert.assertArrayEquals(new Object[]{TIMESTAMP.getMillis(), "b", 12L}, results.get(1).getArray());
    }
  }

  @Test
  public void testProjectionSelectionMissingAggregatorButHasAggregatorInput()
  {
    // e is present as a column on the projection, but since its an aggregate projection we cannot use it
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
    Assert.assertArrayEquals(new Object[]{"aa", 9L, 8.8f}, results.get(0).getArray());
    Assert.assertArrayEquals(new Object[]{"bb", 6L, 6.6f}, results.get(1).getArray());
    Assert.assertArrayEquals(new Object[]{"cc", 2L, 2.2f}, results.get(2).getArray());
    Assert.assertArrayEquals(new Object[]{"dd", 2L, 2.2f}, results.get(3).getArray());
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

  @Test
  public void testTimeseriesQueryAllGranularityCanMatchNonTimeDimProjection()
  {
    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource("test")
                                        .intervals(ImmutableList.of(Intervals.ETERNITY))
                                        .granularity(Granularities.ALL)
                                        .aggregators(new LongSumAggregatorFactory("c_sum", "c"))
                                        .context(ImmutableMap.of(QueryContexts.USE_PROJECTION, "b_c_sum"))
                                        .build();

    final CursorBuildSpec buildSpec = TimeseriesQueryEngine.makeCursorBuildSpec(query, null);
    try (final CursorHolder cursorHolder = projectionsCursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      int rowCount = 0;
      while (!cursor.isDone()) {
        rowCount++;
        cursor.advance();
      }
      Assert.assertEquals(4, rowCount);
    }

    final Sequence<Result<TimeseriesResultValue>> resultRows = timeseriesEngine.process(
        query,
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        null
    );

    final List<Result<TimeseriesResultValue>> results = resultRows.toList();
    Assert.assertEquals(1, results.size());
    final RowSignature querySignature = query.getResultRowSignature(RowSignature.Finalization.YES);
    Assert.assertArrayEquals(new Object[]{TIMESTAMP, 19L}, getResultArray(results.get(0), querySignature));
  }

  @Test
  public void testTimeseriesQueryAllGranularitiesAlwaysRuns()
  {
    // Same test as testTimeseriesQueryAllGranularityCanMatchNonTimeDimProjection, but no projection used.
    // Query can run with segment time ordering on/off.
    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource("test")
                                        .intervals(ImmutableList.of(Intervals.ETERNITY))
                                        .granularity(Granularities.ALL)
                                        .aggregators(new LongSumAggregatorFactory("c_sum", "c"))
                                        .context(ImmutableMap.of(QueryContexts.NO_PROJECTIONS, true))
                                        .build();

    final CursorBuildSpec buildSpec = TimeseriesQueryEngine.makeCursorBuildSpec(query, null);
    try (final CursorHolder cursorHolder = projectionsCursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      int rowCount = 0;
      while (!cursor.isDone()) {
        rowCount++;
        cursor.advance();
      }
      Assert.assertEquals(8, rowCount);
    }

    final Sequence<Result<TimeseriesResultValue>> resultRows = timeseriesEngine.process(
        query,
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        null
    );

    final List<Result<TimeseriesResultValue>> results = resultRows.toList();
    Assert.assertEquals(1, results.size());
    final RowSignature querySignature = query.getResultRowSignature(RowSignature.Finalization.YES);
    Assert.assertArrayEquals(new Object[]{TIMESTAMP, 19L}, getResultArray(results.get(0), querySignature));
  }

  @Test
  public void testTimeseriesQueryOrderByNotCompatibleWithProjection()
  {
    // Query has `__time ASC` ordering, but projection has `b ASC` ordering, so no projection can be used.
    // This is not ideal, since we know query has Granularities.DAY, and segment also has Granularities.DAY, so the __time ordering does not matter.
    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource("test")
                                        .intervals(ImmutableList.of(Intervals.ETERNITY))
                                        .granularity(Granularities.DAY)
                                        .aggregators(new LongSumAggregatorFactory("c_sum", "c"))
                                        .context(ImmutableMap.of(QueryContexts.USE_PROJECTION, "b_c_sum"))
                                        .build();

    final CursorBuildSpec buildSpec = TimeseriesQueryEngine.makeCursorBuildSpec(query, null);
    DruidException e = Assert.assertThrows(
        DruidException.class,
        () -> projectionsCursorFactory.makeCursorHolder(buildSpec));
    Assert.assertEquals(DruidException.Category.INVALID_INPUT, e.getCategory());
    Assert.assertEquals("Projection[b_c_sum] specified, but does not satisfy query", e.getMessage());
  }

  @Test
  public void testTimeseriesQueryGranularityFitsProjectionGranularity()
  {
    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource("test")
                                        .intervals(ImmutableList.of(Intervals.ETERNITY))
                                        .granularity(Granularities.HOUR)
                                        .aggregators(new LongSumAggregatorFactory("c_sum", "c"))
                                        .build();

    final CursorBuildSpec buildSpec = TimeseriesQueryEngine.makeCursorBuildSpec(query, null);
    try (final CursorHolder cursorHolder = projectionsCursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      int rowCount = 0;
      while (!cursor.isDone()) {
        rowCount++;
        cursor.advance();
      }
      Assert.assertEquals(3, rowCount);
    }

    final Sequence<Result<TimeseriesResultValue>> resultRows = timeseriesEngine.process(
        query,
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        null
    );

    final List<Result<TimeseriesResultValue>> results = resultRows.toList();
    Assert.assertEquals(2, results.size());
    final RowSignature querySignature = query.getResultRowSignature(RowSignature.Finalization.YES);
    Assert.assertArrayEquals(new Object[]{TIMESTAMP, 16L}, getResultArray(results.get(0), querySignature));
    Assert.assertArrayEquals(new Object[]{TIMESTAMP.plusHours(1), 3L}, getResultArray(results.get(1), querySignature));
  }

  @Test
  public void testTimeseriesQueryGranularityAllFitsProjectionGranularityWithSegmentGranularity()
  {
    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource("test")
                                        .intervals(ImmutableList.of(Intervals.ETERNITY))
                                        .granularity(Granularities.ALL)
                                        .aggregators(new LongSumAggregatorFactory("c_sum", "c"))
                                        .context(ImmutableMap.of(QueryContexts.USE_PROJECTION, "c_sum_daily"))
                                        .build();

    final CursorBuildSpec buildSpec = TimeseriesQueryEngine.makeCursorBuildSpec(query, null);
    try (final CursorHolder cursorHolder = projectionsCursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      int rowCount = 0;
      while (!cursor.isDone()) {
        rowCount++;
        cursor.advance();
      }
      Assert.assertEquals(1, rowCount);
    }

    final Sequence<Result<TimeseriesResultValue>> resultRows = timeseriesEngine.process(
        query,
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        null
    );

    final List<Result<TimeseriesResultValue>> results = resultRows.toList();
    Assert.assertEquals(1, results.size());
    final RowSignature querySignature = query.getResultRowSignature(RowSignature.Finalization.YES);
    Assert.assertArrayEquals(new Object[]{TIMESTAMP, 19L}, getResultArray(results.get(0), querySignature));
  }

  @Test
  public void testTimeseriesQueryGranularityAllFitsProjectionGranularityWithNoGrouping()
  {
    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource("test")
                                        .intervals(ImmutableList.of(Intervals.ETERNITY))
                                        .granularity(Granularities.ALL)
                                        .aggregators(new LongSumAggregatorFactory("c_sum", "c"))
                                        .context(ImmutableMap.of(QueryContexts.USE_PROJECTION, "c_sum"))
                                        .build();

    final CursorBuildSpec buildSpec = TimeseriesQueryEngine.makeCursorBuildSpec(query, null);
    try (final CursorHolder cursorHolder = projectionsCursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      int rowCount = 0;
      while (!cursor.isDone()) {
        rowCount++;
        cursor.advance();
      }
      Assert.assertEquals(1, rowCount);
    }

    final Sequence<Result<TimeseriesResultValue>> resultRows = timeseriesEngine.process(
        query,
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        null
    );

    final List<Result<TimeseriesResultValue>> results = resultRows.toList();
    Assert.assertEquals(1, results.size());
    final RowSignature querySignature = query.getResultRowSignature(RowSignature.Finalization.YES);
    Assert.assertArrayEquals(new Object[]{TIMESTAMP, 19L}, getResultArray(results.get(0), querySignature));
  }

  @Test
  public void testTimeseriesQueryGranularityFinerThanProjectionGranularity()
  {
    Assume.assumeTrue(segmentSortedByTime);
    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource("test")
                                        .intervals(ImmutableList.of(Intervals.ETERNITY))
                                        .granularity(Granularities.MINUTE)
                                        .aggregators(new LongSumAggregatorFactory("c_sum", "c"))
                                        .context(ImmutableMap.of(TimeseriesQuery.SKIP_EMPTY_BUCKETS, true))
                                        .build();

    final CursorBuildSpec buildSpec = TimeseriesQueryEngine.makeCursorBuildSpec(query, null);
    try (final CursorHolder cursorHolder = projectionsCursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      int rowCount = 0;
      while (!cursor.isDone()) {
        rowCount++;
        cursor.advance();
      }
      Assert.assertEquals(8, rowCount);
    }

    final Sequence<Result<TimeseriesResultValue>> resultRows = timeseriesEngine.process(
        query,
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        null
    );

    final List<Result<TimeseriesResultValue>> results = resultRows.toList();
    Assert.assertEquals(8, results.size());
    final RowSignature querySignature = query.getResultRowSignature(RowSignature.Finalization.YES);
    Assert.assertArrayEquals(new Object[]{TIMESTAMP, 1L}, getResultArray(results.get(0), querySignature));
    Assert.assertArrayEquals(new Object[]{TIMESTAMP.plusMinutes(2), 1L}, getResultArray(results.get(1), querySignature));
    Assert.assertArrayEquals(new Object[]{TIMESTAMP.plusMinutes(4), 2L}, getResultArray(results.get(2), querySignature));
    Assert.assertArrayEquals(new Object[]{TIMESTAMP.plusMinutes(6), 3L}, getResultArray(results.get(3), querySignature));
    Assert.assertArrayEquals(new Object[]{TIMESTAMP.plusMinutes(8), 4L}, getResultArray(results.get(4), querySignature));
    Assert.assertArrayEquals(new Object[]{TIMESTAMP.plusMinutes(10), 5L}, getResultArray(results.get(5), querySignature));
    Assert.assertArrayEquals(new Object[]{TIMESTAMP.plusHours(1), 1L}, getResultArray(results.get(6), querySignature));
    Assert.assertArrayEquals(new Object[]{TIMESTAMP.plusHours(1).plusMinutes(1), 2L}, getResultArray(results.get(7), querySignature));
  }

  @Test
  public void testProjectionSingleDimRollupTable()
  {
    // test can use the single dimension projection
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("a")
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "sum_c"))
                    .build();
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, null);
    try (final CursorHolder cursorHolder = rollupProjectionsCursorFactory.makeCursorHolder(buildSpec)) {
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
        rollupProjectionsCursorFactory,
        rollupProjectionsTimeBoundaryInspector,
        nonBlockingPool,
        null
    );
    final List<ResultRow> results = resultRows.toList();
    Assert.assertEquals(2, results.size());
    Assert.assertArrayEquals(
        new Object[]{"a", 7L},
        results.get(0).getArray()
    );
    Assert.assertArrayEquals(
        new Object[]{"b", 12L},
        results.get(1).getArray()
    );
  }

  @Test
  public void testProjectionSingleDimVirtualColumnRollupTable()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("v0")
                    .setVirtualColumns(new ExpressionVirtualColumn("v0", "concat(a, 'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE))
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "sum_c"))
                    .build();
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, null);
    try (final CursorHolder cursorHolder = rollupProjectionsCursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      int rowCount = 0;
      while (!cursor.isDone()) {
        rowCount++;
        cursor.advance();
      }
      Assert.assertEquals(2, rowCount);
    }
    final Sequence<ResultRow> resultRows = groupingEngine.process(
        query,
        rollupProjectionsCursorFactory,
        rollupProjectionsTimeBoundaryInspector,
        nonBlockingPool,
        null
    );
    final List<ResultRow> results = resultRows.toList();
    Assert.assertEquals(2, results.size());
    if (!(projectionsCursorFactory instanceof QueryableIndexCursorFactory) || !autoSchema) {
      Assert.assertArrayEquals(
          new Object[]{"afoo", 7L},
          results.get(0).getArray()
      );
      Assert.assertArrayEquals(
          new Object[]{"bfoo", 12L},
          results.get(1).getArray()
      );
    } else {
      // inconsistent ordering since query isn't ordering
      Assert.assertArrayEquals(
          new Object[]{"bfoo", 12L},
          results.get(0).getArray()
      );
      Assert.assertArrayEquals(
          new Object[]{"afoo", 7L},
          results.get(1).getArray()
      );
    }
  }

  @Test
  public void testProjectionJson()
  {
    // test can use the single dimension projection
    final GroupByQuery.Builder bob =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .setVirtualColumns(
                        new NestedFieldVirtualColumn(
                            "f",
                            "$.x",
                            "v0",
                            ColumnType.STRING

                        )
                    )
                    .addDimension("v0")
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"));

    final GroupByQuery query = bob.build();
    final GroupByQuery queryNoProjection = bob.setContext(Map.of(QueryContexts.NO_PROJECTIONS, true)).build();

    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, null);
    try (final CursorHolder cursorHolder = projectionsCursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      int rowCount = 0;
      while (!cursor.isDone()) {
        rowCount++;
        cursor.advance();
      }
      Assert.assertEquals(2, rowCount);
    }

    final CursorBuildSpec buildSpecNoProjection = GroupingEngine.makeCursorBuildSpec(queryNoProjection, null);
    try (final CursorHolder cursorHolder = projectionsCursorFactory.makeCursorHolder(buildSpecNoProjection)) {
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
    Assert.assertEquals(2, results.size());
    Assert.assertArrayEquals(new Object[]{"a", 7L}, results.get(0).getArray());
    Assert.assertArrayEquals(new Object[]{"b", 12L}, results.get(1).getArray());

    final Sequence<ResultRow> resultRowsNoProjection = groupingEngine.process(
        query,
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        nonBlockingPool,
        null
    );
    final List<ResultRow> resultsNoProjection = resultRowsNoProjection.toList();
    Assert.assertEquals(2, resultsNoProjection.size());
    Assert.assertArrayEquals(new Object[]{"a", 7L}, resultsNoProjection.get(0).getArray());
    Assert.assertArrayEquals(new Object[]{"b", 12L}, resultsNoProjection.get(1).getArray());
  }

  private static IndexBuilder makeBuilder(DimensionsSpec dimensionsSpec, boolean autoSchema, boolean writeNullColumns)
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
                                                 .withProjections(autoSchema ? AUTO_PROJECTIONS : PROJECTIONS)
                                                 .build()
                       )
                       .writeNullColumns(writeNullColumns)
                       .rows(ROWS);
  }

  private static IndexBuilder makeRollupBuilder(DimensionsSpec dimensionsSpec, AggregatorFactory[] aggs, boolean autoSchema)
  {
    File tmp = FileUtils.createTempDir();
    CLOSER.register(tmp::delete);
    return IndexBuilder.create()
                       .tmpDir(tmp)
                       .schema(
                           IncrementalIndexSchema.builder()
                                                 .withDimensionsSpec(dimensionsSpec)
                                                 .withMetrics(aggs)
                                                 .withRollup(true)
                                                 .withMinTimestamp(TIMESTAMP.getMillis())
                                                 .withProjections(autoSchema ? AUTO_ROLLUP_PROJECTIONS : ROLLUP_PROJECTIONS)
                                                 .build()
                       )
                       .writeNullColumns(true)
                       .rows(ROLLUP_ROWS);
  }

  private static Set<Object[]> makeArrayResultSet()
  {
    Set<Object[]> resultsInNoParticularOrder = new ObjectOpenCustomHashSet<>(
        new Hash.Strategy<>()
        {
          @Override
          public int hashCode(Object[] o)
          {
            return Arrays.hashCode(o);
          }

          @Override
          public boolean equals(Object[] a, Object[] b)
          {
            return Arrays.deepEquals(a, b);
          }
        }
    );
    return resultsInNoParticularOrder;
  }

  private static Set<Object[]> makeArrayResultSet(Object[]... values)
  {
    Set<Object[]> resultsInNoParticularOrder = makeArrayResultSet();
    resultsInNoParticularOrder.addAll(Arrays.asList(values));
    return resultsInNoParticularOrder;
  }

  private static Object[] getResultArray(Result<TimeseriesResultValue> result, RowSignature rowSignature)
  {
    final Object[] rowArray = new Object[rowSignature.size()];
    for (int i = 0; i < rowSignature.size(); i++) {
      if (i == 0) {
        rowArray[i] = result.getTimestamp();
      } else {
        rowArray[i] = result.getValue().getMetric(rowSignature.getColumnName(i));
      }
    }
    return rowArray;
  }
}
