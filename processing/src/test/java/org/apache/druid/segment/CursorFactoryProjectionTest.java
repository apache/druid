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
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DefaultQueryMetrics;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.firstlast.last.LongLastAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.expression.TimestampFloorExprMacro;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryMetrics;
import org.apache.druid.query.groupby.GroupByResourcesReservationPool;
import org.apache.druid.query.groupby.GroupByStatsProvider;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec.Direction;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryMetrics;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexCursorFactory;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
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
  // Set a fixed time, when IST is 5 hours 30 minutes ahead of UTC, and PDT is 7 hours behind UTC.
  static final DateTime UTC_MIDNIGHT = Granularities.DAY.bucket(DateTimes.of("2025-08-13")).getStart();
  static final DateTime UTC_01H = UTC_MIDNIGHT.plusHours(1);
  static final DateTime UTC_01H31M = UTC_MIDNIGHT.plusHours(1).plusMinutes(31);

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
            UTC_MIDNIGHT,
            dimensions,
            Arrays.asList("a", "aa", 1L, 1.0, null, Map.of("x", "a", "y", 1L, "z", 1.0))
        ),
        new ListBasedInputRow(
            ROW_SIGNATURE,
            UTC_MIDNIGHT.plusMinutes(2),
            dimensions,
            Arrays.asList("a", "bb", 1L, 1.1, 1.1f, Map.of("x", "a", "y", 1L, "z", 1.1))
        ),
        new ListBasedInputRow(
            ROW_SIGNATURE,
            UTC_MIDNIGHT.plusMinutes(4),
            dimensions,
            Arrays.asList("a", "cc", 2L, 2.2, 2.2f, Map.of("x", "a", "y", 2L, "z", 2.2))
        ),
        new ListBasedInputRow(
            ROW_SIGNATURE,
            UTC_MIDNIGHT.plusMinutes(6),
            dimensions,
            Arrays.asList("b", "aa", 3L, 3.3, 3.3f, Map.of("x", "b", "y", 3L, "z", 3.3))
        ),
        new ListBasedInputRow(
            ROW_SIGNATURE,
            UTC_MIDNIGHT.plusMinutes(8),
            dimensions,
            Arrays.asList("b", "aa", 4L, 4.4, 4.4f, Map.of("x", "b", "y", 4L, "z", 4.4))
        ),
        new ListBasedInputRow(
            ROW_SIGNATURE,
            UTC_MIDNIGHT.plusMinutes(10),
            dimensions,
            Arrays.asList("b", "bb", 5L, 5.5, 5.5f, Map.of("x", "b", "y", 5L, "z", 5.5))
        ),
        new ListBasedInputRow(
            ROW_SIGNATURE,
            UTC_01H,
            dimensions,
            Arrays.asList("a", "aa", 1L, 1.1, 1.1f, Map.of("x", "a", "y", 1L, "z", 1.1))
        ),
        new ListBasedInputRow(
            ROW_SIGNATURE,
            UTC_01H31M,
            dimensions,
            Arrays.asList("a", "dd", 2L, 2.2, 2.2f, Map.of("x", "a", "y", 2L, "z", 2.2))
        )
    );
  }

  static final List<InputRow> ROWS = makeRows(ROW_SIGNATURE.getColumnNames());
  static final List<InputRow> ROLLUP_ROWS = makeRows(ImmutableList.of("a", "b"));

  private static final List<AggregateProjectionSpec> PROJECTIONS = Arrays.asList(
      AggregateProjectionSpec.builder("ab_hourly_cd_sum")
                             .virtualColumns(Granularities.toVirtualColumn(Granularities.HOUR, "__gran"))
                             .groupingColumns(
                                 new StringDimensionSchema("a"),
                                 new StringDimensionSchema("b"),
                                 new LongDimensionSchema("__gran")
                             )
                             .aggregators(
                                 new LongSumAggregatorFactory("_c_sum", "c"),
                                 new DoubleSumAggregatorFactory("d", "d")
                             )
                             .build(),
      AggregateProjectionSpec.builder("a_hourly_c_sum_with_count_latest")
                             .virtualColumns(Granularities.toVirtualColumn(Granularities.HOUR, "__gran"))
                             .groupingColumns(
                                 new LongDimensionSchema("__gran"),
                                 new StringDimensionSchema("a")
                             )
                             .aggregators(
                                 new CountAggregatorFactory("chocula"),
                                 new LongSumAggregatorFactory("_c_sum", "c"),
                                 new LongLastAggregatorFactory("_c_last", "c", null)
                             )
                             .build(),
      AggregateProjectionSpec.builder("b_hourly_c_sum_non_time_ordered")
                             .virtualColumns(Granularities.toVirtualColumn(Granularities.HOUR, "__gran"))
                             .groupingColumns(
                                 new StringDimensionSchema("b"),
                                 new LongDimensionSchema("__gran")
                             )
                             .aggregators(
                                 new CountAggregatorFactory("chocula"),
                                 new LongSumAggregatorFactory("_c_sum", "c"),
                                 new LongLastAggregatorFactory("_c_last", "c", null)
                             )
                             .build(),
      AggregateProjectionSpec.builder("bf_daily_c_sum")
                             .virtualColumns(Granularities.toVirtualColumn(Granularities.DAY, "__gran"))
                             .groupingColumns(
                                 new LongDimensionSchema("__gran"),
                                 new StringDimensionSchema("b"),
                                 new FloatDimensionSchema("e")
                             )
                             .aggregators(new LongSumAggregatorFactory("_c_sum", "c"))
                             .build(),
      AggregateProjectionSpec.builder("b_c_sum")
                             .groupingColumns(new StringDimensionSchema("b"))
                             .aggregators(new LongSumAggregatorFactory("_c_sum", "c"))
                             .build(),
      AggregateProjectionSpec.builder("ab")
                             .groupingColumns(
                                 new StringDimensionSchema("a"),
                                 new StringDimensionSchema("b")
                             )
                             .build(),
      AggregateProjectionSpec.builder("abfoo")
                             .virtualColumns(
                                 new ExpressionVirtualColumn(
                                     "bfoo",
                                     "concat(b, 'foo')",
                                     ColumnType.STRING,
                                     TestExprMacroTable.INSTANCE
                                 )
                             )
                             .groupingColumns(
                                 new StringDimensionSchema("a"),
                                 new StringDimensionSchema("bfoo")
                             )
                             .build(),
      AggregateProjectionSpec.builder("c_sum_daily")
                             .virtualColumns(Granularities.toVirtualColumn(Granularities.DAY, "__gran"))
                             .groupingColumns(new LongDimensionSchema("__gran"))
                             .aggregators(new LongSumAggregatorFactory("_c_sum", "c"))
                             .build(),
      AggregateProjectionSpec.builder("c_sum")
                             .aggregators(new LongSumAggregatorFactory("_c_sum", "c"))
                             .build(),
      AggregateProjectionSpec.builder("missing_column")
                             .groupingColumns(new StringDimensionSchema("missing"))
                             .aggregators(new DoubleSumAggregatorFactory("dsum", "d"))
                             .build(),
      AggregateProjectionSpec.builder("json")
                             .groupingColumns(AutoTypeColumnSchema.of("f"))
                             .aggregators(new LongSumAggregatorFactory("_c_sum", "c"))
                             .build(),
      AggregateProjectionSpec.builder("a_filter_b_aaonly_hourly_cd_sum")
                             .virtualColumns(Granularities.toVirtualColumn(Granularities.HOUR, "__gran"))
                             .filter(new EqualityFilter("b", ColumnType.STRING, "aa", null))
                             .groupingColumns(
                                 new StringDimensionSchema("a"),
                                 new LongDimensionSchema("__gran")
                             )
                             .aggregators(
                                 new LongSumAggregatorFactory("_c_sum", "c"),
                                 new DoubleSumAggregatorFactory("d", "d")
                             )
                             .build(),
      AggregateProjectionSpec.builder("a_concat_b_d_plus_f_sum_c")
                             .virtualColumns(
                                 new ExpressionVirtualColumn(
                                     "__vc2",
                                     "d + e",
                                     ColumnType.LONG,
                                     TestExprMacroTable.INSTANCE
                                 ),
                                 new ExpressionVirtualColumn(
                                     "__vc3",
                                     "concat(a, b)",
                                     ColumnType.STRING,
                                     TestExprMacroTable.INSTANCE
                                 )
                             )
                             .groupingColumns(new LongDimensionSchema("__vc2"), new StringDimensionSchema("__vc3"))
                             .aggregators(new LongSumAggregatorFactory("sum_c", "c"))
                             .build(),
      AggregateProjectionSpec.builder("a_hourly_c_sum_filter_a_to_a")
                             .filter(
                                 new EqualityFilter("a", ColumnType.STRING, "a", null)
                             )
                             .virtualColumns(Granularities.toVirtualColumn(Granularities.HOUR, "__gran"))
                             .groupingColumns(
                                 new StringDimensionSchema("a"),
                                 new LongDimensionSchema("__gran")
                             )
                             .aggregators(
                                 new LongSumAggregatorFactory("_c_sum", "c")
                             )
                             .build(),
      AggregateProjectionSpec.builder("a_hourly_c_sum_filter_a_to_empty")
                             .filter(
                                 new EqualityFilter("a", ColumnType.STRING, "nomatch", null)
                             )
                             .virtualColumns(Granularities.toVirtualColumn(Granularities.HOUR, "__gran"))
                             .groupingColumns(
                                 new StringDimensionSchema("a"),
                                 new LongDimensionSchema("__gran")
                             )
                             .aggregators(
                                 new LongSumAggregatorFactory("_c_sum", "c")
                             )
                             .build(),
      AggregateProjectionSpec.builder("time_and_a")
                             .groupingColumns(
                                 new LongDimensionSchema(ColumnHolder.TIME_COLUMN_NAME),
                                 new StringDimensionSchema("a")
                             )
                             .build()
  );

  private static final List<AggregateProjectionSpec> ROLLUP_PROJECTIONS = Arrays.asList(
      AggregateProjectionSpec.builder("a_hourly_c_sum_with_count")
                             .virtualColumns(Granularities.toVirtualColumn(Granularities.HOUR, "__gran"))
                             .groupingColumns(
                                 new LongDimensionSchema("__gran"),
                                 new StringDimensionSchema("a")
                             )
                             .aggregators(
                                 new CountAggregatorFactory("chocula"),
                                 new LongSumAggregatorFactory("sum_c", "sum_c")
                             )
                             .build(),
      AggregateProjectionSpec.builder("afoo")
                             .virtualColumns(
                                 new ExpressionVirtualColumn(
                                     "afoo",
                                     "concat(a, 'foo')",
                                     ColumnType.STRING,
                                     TestExprMacroTable.INSTANCE
                                 )
                             )
                             .groupingColumns(new StringDimensionSchema("afoo"))
                             .aggregators(
                                 new LongSumAggregatorFactory("sum_c", "sum_c"),
                                 new LongMaxAggregatorFactory("max_c", "max_c")
                             )
                             .build()
  );

  private static final List<AggregateProjectionSpec> AUTO_PROJECTIONS =
      PROJECTIONS.stream()
                 .map(
                     projection ->
                         AggregateProjectionSpec.builder(projection)
                                                .groupingColumns(
                                                    projection.getGroupingColumns()
                                                              .stream()
                                                              .map(x -> AutoTypeColumnSchema.of(x.getName()))
                                                              .collect(Collectors.toList())
                                                )
                                                .build()
                 )
                 .collect(Collectors.toList());

  private static final List<AggregateProjectionSpec> AUTO_ROLLUP_PROJECTIONS =
      ROLLUP_PROJECTIONS.stream()
                        .map(
                            projection ->
                                AggregateProjectionSpec.builder(projection)
                                                       .groupingColumns(
                                                           projection.getGroupingColumns()
                                                                     .stream()
                                                                     .map(x -> AutoTypeColumnSchema.of(x.getName()))
                                                                     .collect(Collectors.toList())
                                                       )
                                                       .build()
                        )
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
                              AutoTypeColumnSchema.of("f"),
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
        new LongMaxAggregatorFactory("max_c", "c"),
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
                                                .map(x -> AutoTypeColumnSchema.of(x.getName()))
                                                .collect(Collectors.toList());

    List<DimensionSchema> rollupAutoDims = rollupDimsOrdered.getDimensions()
                                                            .stream()
                                                            .map(x -> AutoTypeColumnSchema.of(x.getName()))
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
              IncrementalIndex index = CLOSER.register(makeBuilder(
                  dims,
                  autoSchema,
                  writeNullColumns
              ).buildIncrementalIndex());
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
              QueryableIndex index = CLOSER.register(makeBuilder(
                  dims,
                  autoSchema,
                  writeNullColumns
              ).buildMMappedIndex());
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
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("a")
                    .addDimension("b")
                    .build();

    final ExpectedProjectionGroupBy queryMetrics = new ExpectedProjectionGroupBy("ab");

    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 6);

    testGroupBy(
        query,
        queryMetrics,
        makeArrayResultSet(
            new Object[]{"b", "bb"},
            new Object[]{"a", "dd"},
            new Object[]{"b", "aa"},
            new Object[]{"a", "cc"},
            new Object[]{"a", "bb"},
            new Object[]{"a", "aa"}
        )
    );
  }

  @Test
  public void testProjectionSelectionTwoDimsVirtual()
  {
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
                            "concat(\"b\", 'foo')",
                            ColumnType.STRING,
                            TestExprMacroTable.INSTANCE
                        )
                    )
                    .setLimitSpec(
                        new DefaultLimitSpec(
                            Arrays.asList(
                                new OrderByColumnSpec("a", Direction.ASCENDING, StringComparators.LEXICOGRAPHIC),
                                new OrderByColumnSpec("v0", Direction.ASCENDING, StringComparators.LEXICOGRAPHIC)
                            ),
                            10
                        )
                    )
                    .setContext(ImmutableMap.of(QueryContexts.USE_PROJECTION, "abfoo"))
                    .build();

    final ExpectedProjectionGroupBy queryMetrics = new ExpectedProjectionGroupBy("abfoo");

    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);
    assertCursorProjection(buildSpec, queryMetrics, 6);

    testGroupBy(
        query,
        queryMetrics,
        makeArrayResultSet(
            new Object[]{"b", "bbfoo"},
            new Object[]{"a", "ddfoo"},
            new Object[]{"b", "aafoo"},
            new Object[]{"a", "ccfoo"},
            new Object[]{"a", "bbfoo"},
            new Object[]{"a", "aafoo"}
        )
    );
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

    final ExpectedProjectionGroupBy queryMetrics = new ExpectedProjectionGroupBy(null);

    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);
    assertCursorNoProjection(buildSpec, queryMetrics);

    testGroupBy(
        query,
        queryMetrics,
        makeArrayResultSet(
            new Object[]{"b", "aa", 2L},
            new Object[]{"a", "cc", 1L},
            new Object[]{"a", "bb", 1L},
            new Object[]{"b", "bb", 1L},
            new Object[]{"a", "dd", 1L},
            new Object[]{"a", "aa", 2L}
        )
    );
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
    final ExpectedProjectionGroupBy queryMetrics = new ExpectedProjectionGroupBy(null);
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorNoProjection(buildSpec, queryMetrics);

    testGroupBy(
        query,
        queryMetrics,
        List.of(
            new Object[]{"a", 7L, Pair.of(UTC_01H31M.getMillis(), 2L)},
            new Object[]{"b", 12L, Pair.of(UTC_MIDNIGHT.plusMinutes(10).getMillis(), 5L)}
        )
    );
  }

  @Test
  public void testProjectionSingleDim()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("a")
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"))
                    .addAggregator(new LongLastAggregatorFactory("c_last", "c", null))
                    .build();
    final ExpectedProjectionGroupBy queryMetrics =
        new ExpectedProjectionGroupBy("a_hourly_c_sum_with_count_latest");
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 3);

    testGroupBy(
        query,
        queryMetrics,
        List.of(
            new Object[]{"a", 7L, Pair.of(UTC_01H31M.getMillis(), 2L)},
            new Object[]{"b", 12L, Pair.of(UTC_MIDNIGHT.plusMinutes(10).getMillis(), 5L)}
        )
    );
  }

  @Test
  public void testProjectionSingleDimMissing()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("missing")
                    .addAggregator(new DoubleSumAggregatorFactory("d_sum", "d"))
                    .build();
    final ExpectedProjectionGroupBy queryMetrics =
        new ExpectedProjectionGroupBy("missing_column");
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 1);

    testGroupBy(
        query,
        queryMetrics,
        Collections.singletonList(
            new Object[]{null, 20.8}
        )
    );
  }

  @Test
  public void testProjectionSingleDimFilter()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(new Interval(UTC_MIDNIGHT, UTC_MIDNIGHT.plusDays(1)))
                    .addDimension("a")
                    .setDimFilter(new EqualityFilter("a", ColumnType.STRING, "a", null))
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"))
                    .addAggregator(new LongLastAggregatorFactory("c_last", "c", null))
                    .build();
    final ExpectedProjectionGroupBy queryMetrics =
        new ExpectedProjectionGroupBy("a_hourly_c_sum_with_count_latest");
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 2);

    testGroupBy(
        query,
        queryMetrics,
        Collections.singletonList(
            new Object[]{"a", 7L, Pair.of(UTC_01H31M.getMillis(), 2L)}
        )
    );
  }

  @Test
  public void testProjectionSingleDimFilterWithPartialIntervalAligned()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(new Interval(UTC_MIDNIGHT, UTC_MIDNIGHT.plusHours(1)))
                    .addDimension("a")
                    .setDimFilter(new EqualityFilter("a", ColumnType.STRING, "a", null))
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"))
                    .addAggregator(new LongLastAggregatorFactory("c_last", "c", null))
                    .build();
    final ExpectedProjectionGroupBy queryMetrics =
        new ExpectedProjectionGroupBy("a_hourly_c_sum_with_count_latest");
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 1);

    testGroupBy(
        query,
        queryMetrics,
        Collections.singletonList(
            new Object[]{"a", 4L, Pair.of(UTC_MIDNIGHT.plusMinutes(4).getMillis(), 2L)}
        )
    );
  }

  @Test
  public void testProjectionSingleDimFilterWithPartialIntervalUnaligned()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(new Interval(UTC_MIDNIGHT, UTC_MIDNIGHT.plusHours(1).minusMinutes(1)))
                    .addDimension("a")
                    .setDimFilter(new EqualityFilter("a", ColumnType.STRING, "a", null))
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"))
                    .addAggregator(new LongLastAggregatorFactory("c_last", "c", null))
                    .build();
    final ExpectedProjectionGroupBy queryMetrics =
        new ExpectedProjectionGroupBy(null);
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 3);

    testGroupBy(
        query,
        queryMetrics,
        Collections.singletonList(
            new Object[]{"a", 4L, Pair.of(UTC_MIDNIGHT.plusMinutes(4).getMillis(), 2L)}
        )
    );
  }

  @Test
  public void testProjectionSingleDimCount()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("a")
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"))
                    .addAggregator(new CountAggregatorFactory("cnt"))
                    .build();
    final ExpectedProjectionGroupBy queryMetrics =
        new ExpectedProjectionGroupBy("a_hourly_c_sum_with_count_latest");
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 3);

    testGroupBy(
        query,
        queryMetrics,
        List.of(
            new Object[]{"a", 7L, 5L},
            new Object[]{"b", 12L, 3L}
        )
    );
  }

  @Test
  public void testProjectionSingleDimMultipleSameAggs()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("a")
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"))
                    .addAggregator(new LongSumAggregatorFactory("c_sum_2", "c"))
                    .build();
    final ExpectedProjectionGroupBy queryMetrics =
        new ExpectedProjectionGroupBy("a_hourly_c_sum_with_count_latest");
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 3);

    testGroupBy(
        query,
        queryMetrics,
        List.of(
            new Object[]{"a", 7L, 7L},
            new Object[]{"b", 12L, 12L}
        )
    );
  }

  @Test
  public void testQueryGranularityFinerThanProjectionGranularity()
  {
    final GroupByQuery.Builder queryBuilder =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setInterval(Intervals.ETERNITY)
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"));
    final ExpectedProjectionGroupBy queryMetrics = new ExpectedProjectionGroupBy(null);
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
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorNoProjection(buildSpec, queryMetrics);

    Set<Object[]> resultsInNoParticularOrder = makeArrayResultSet();
    resultsInNoParticularOrder.addAll(
        ROWS.stream()
            .map(x -> new Object[]{x.getTimestamp().getMillis(), x.getRaw("a"), x.getRaw("c")})
            .collect(Collectors.toList())
    );
    testGroupBy(
        query,
        queryMetrics,
        resultsInNoParticularOrder
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
    final ExpectedProjectionGroupBy queryMetrics =
        new ExpectedProjectionGroupBy("a_hourly_c_sum_with_count_latest");
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
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 3);

    testGroupBy(
        query,
        queryMetrics,
        makeArrayResultSet(
            new Object[]{UTC_MIDNIGHT.getMillis(), "a", 4L},
            new Object[]{UTC_MIDNIGHT.getMillis(), "b", 12L},
            new Object[]{UTC_01H.getMillis(), "a", 3L}
        )
    );
  }

  @Test
  public void testQueryGranularityFitsProjectionGranularityNotTimeOrdered()
  {
    final GroupByQuery.Builder queryBuilder =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setInterval(Intervals.ETERNITY)
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"));
    final ExpectedProjectionGroupBy queryMetrics =
        new ExpectedProjectionGroupBy(segmentSortedByTime ? null : "b_hourly_c_sum_non_time_ordered");
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
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, segmentSortedByTime ? 8 : 5);

    testGroupBy(
        query,
        queryMetrics,
        makeArrayResultSet(
            new Object[]{UTC_MIDNIGHT.getMillis(), "aa", 8L},
            new Object[]{UTC_MIDNIGHT.getMillis(), "bb", 6L},
            new Object[]{UTC_MIDNIGHT.getMillis(), "cc", 2L},
            new Object[]{UTC_01H.getMillis(), "aa", 1L},
            new Object[]{UTC_01H.getMillis(), "dd", 2L}
        )
    );
  }

  @Test
  public void testQueryGranularityFitsProjectionGranularityWithTimeZone()
  {
    final GroupByQuery.Builder queryBuilder =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setInterval(Intervals.ETERNITY)
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"));
    final ExpectedProjectionGroupBy queryMetrics = new ExpectedProjectionGroupBy("a_hourly_c_sum_with_count_latest");

    if (segmentSortedByTime) {
      queryBuilder.addDimension("a")
                  .setGranularity(new PeriodGranularity(
                      new Period("PT1H"),
                      null,
                      DateTimes.inferTzFromString("America/Los_Angeles")
                  ));
    } else {
      queryBuilder.setGranularity(Granularities.ALL)
                  .setDimensions(
                      DefaultDimensionSpec.of("__gran", ColumnType.LONG),
                      DefaultDimensionSpec.of("a")
                  )
                  .setVirtualColumns(new ExpressionVirtualColumn(
                      "__gran",
                      "timestamp_floor(__time,'PT1H',null,'America/Los_Angeles')",
                      ColumnType.LONG,
                      new ExprMacroTable(List.of(new TimestampFloorExprMacro()))
                  ));
    }
    final GroupByQuery query = queryBuilder.build();
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 3);

    testGroupBy(
        query,
        queryMetrics,
        makeArrayResultSet(
            new Object[]{UTC_MIDNIGHT.getMillis(), "a", 4L},
            new Object[]{UTC_MIDNIGHT.getMillis(), "b", 12L},
            new Object[]{UTC_01H.getMillis(), "a", 3L}
        )
    );
  }

  @Test
  public void testQueryGranularityDoesNotFitProjectionGranularityWithTimeZone()
  {
    final GroupByQuery.Builder queryBuilder =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setInterval(Intervals.ETERNITY)
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"));
    final ExpectedProjectionGroupBy queryMetrics = new ExpectedProjectionGroupBy(null);

    if (segmentSortedByTime) {
      queryBuilder.addDimension("a")
                  .setGranularity(new PeriodGranularity(
                      new Period("PT1H"),
                      null,
                      DateTimes.inferTzFromString("Asia/Kolkata")
                  ));
    } else {
      queryBuilder.setGranularity(Granularities.ALL)
                  .setDimensions(
                      DefaultDimensionSpec.of("__gran", ColumnType.LONG),
                      DefaultDimensionSpec.of("a")
                  )
                  .setVirtualColumns(new ExpressionVirtualColumn(
                      "__gran",
                      "timestamp_floor(__time,'PT1H',null,'Asia/Kolkata')",
                      ColumnType.LONG,
                      new ExprMacroTable(List.of(new TimestampFloorExprMacro()))
                  ));
    }
    final GroupByQuery query = queryBuilder.build();
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 8);
    testGroupBy(
        query,
        queryMetrics,
        makeArrayResultSet(
            new Object[]{UTC_MIDNIGHT.minusMinutes(30).getMillis(), "a", 4L},
            new Object[]{UTC_MIDNIGHT.minusMinutes(30).getMillis(), "b", 12L},
            new Object[]{UTC_01H.minusMinutes(30).getMillis(), "a", 1L},
            new Object[]{UTC_01H31M.minusMinutes(1).getMillis(), "a", 2L}
        )
    );
  }

  @Test
  public void testQueryGranularityLargerProjectionGranularity()
  {
    final GroupByQuery.Builder queryBuilder =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setInterval(Intervals.ETERNITY)
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"));
    final ExpectedProjectionGroupBy queryMetrics =
        new ExpectedProjectionGroupBy("a_hourly_c_sum_with_count_latest");
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
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 3);

    testGroupBy(
        query,
        queryMetrics,
        makeArrayResultSet(
            new Object[]{UTC_MIDNIGHT.getMillis(), "a", 7L},
            new Object[]{UTC_MIDNIGHT.getMillis(), "b", 12L}
        )
    );
  }

  @Test
  public void testProjectionSelectionMissingAggregatorButHasAggregatorInput()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("b")
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"))
                    .addAggregator(new FloatSumAggregatorFactory("e_sum", "e"))
                    .build();
    final ExpectedProjectionGroupBy queryMetrics = new ExpectedProjectionGroupBy(null);
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorNoProjection(buildSpec, queryMetrics);

    testGroupBy(
        query,
        queryMetrics,
        List.of(
            new Object[]{"aa", 9L, 8.8f},
            new Object[]{"bb", 6L, 6.6f},
            new Object[]{"cc", 2L, 2.2f},
            new Object[]{"dd", 2L, 2.2f}
        )
    );
  }

  @Test
  public void testProjectionSelectionMissingAggregatorAndAggregatorInput()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("a")
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"))
                    .addAggregator(new DoubleSumAggregatorFactory("d_sum", "d"))
                    .build();
    final ExpectedProjectionGroupBy queryMetrics =
        new ExpectedProjectionGroupBy("ab_hourly_cd_sum");
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 7);

    testGroupBy(
        query,
        queryMetrics,
        List.of(
            new Object[]{"a", 7L, 7.6000000000000005},
            new Object[]{"b", 12L, 13.2}
        )
    );
  }

  @Test
  public void testProjectionSelectionMissingColumnOnBaseTableToo()
  {
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
    final ExpectedProjectionGroupBy queryMetrics =
        new ExpectedProjectionGroupBy("ab_hourly_cd_sum");
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 7);

    testGroupBy(
        query,
        queryMetrics,
        List.of(
            new Object[]{"a", null, 7L, 7.6000000000000005},
            new Object[]{"b", null, 12L, 13.2}
        )
    );
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

    final ExpectedProjectionTimeseries queryMetrics =
        new ExpectedProjectionTimeseries("b_c_sum");
    final CursorBuildSpec buildSpec = TimeseriesQueryEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 4);

    testTimeseries(
        query,
        queryMetrics,
        Collections.singletonList(
            new Object[]{UTC_MIDNIGHT, 19L}
        )
    );
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

    final ExpectedProjectionTimeseries queryMetrics =
        new ExpectedProjectionTimeseries(null);
    final CursorBuildSpec buildSpec = TimeseriesQueryEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorNoProjection(buildSpec, queryMetrics);

    testTimeseries(
        query,
        queryMetrics,
        Collections.singletonList(
            new Object[]{UTC_MIDNIGHT, 19L}
        )
    );
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
        () -> projectionsCursorFactory.makeCursorHolder(buildSpec)
    );
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

    final ExpectedProjectionTimeseries queryMetrics =
        new ExpectedProjectionTimeseries("a_hourly_c_sum_with_count_latest");
    final CursorBuildSpec buildSpec = TimeseriesQueryEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 3);

    testTimeseries(
        query,
        queryMetrics,
        List.of(
            new Object[]{UTC_MIDNIGHT, 16L},
            new Object[]{UTC_01H, 3L}
        )
    );
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

    final ExpectedProjectionTimeseries queryMetrics =
        new ExpectedProjectionTimeseries("c_sum_daily");
    final CursorBuildSpec buildSpec = TimeseriesQueryEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 1);

    testTimeseries(
        query,
        queryMetrics,
        Collections.singletonList(
            new Object[]{UTC_MIDNIGHT, 19L}
        )
    );
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

    final ExpectedProjectionTimeseries queryMetrics =
        new ExpectedProjectionTimeseries("c_sum");
    final CursorBuildSpec buildSpec = TimeseriesQueryEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 1);

    testTimeseries(
        query,
        queryMetrics,
        Collections.singletonList(
            new Object[]{UTC_MIDNIGHT, 19L}
        )
    );
  }

  @Test
  public void testTimeseriesQueryGranularityFinerThanProjectionGranularity()
  {
    // timeseries query only works on base table if base table is sorted by time
    Assume.assumeTrue(segmentSortedByTime);
    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource("test")
                                        .intervals(ImmutableList.of(Intervals.ETERNITY))
                                        .granularity(Granularities.MINUTE)
                                        .aggregators(new LongSumAggregatorFactory("c_sum", "c"))
                                        .context(ImmutableMap.of(TimeseriesQuery.SKIP_EMPTY_BUCKETS, true))
                                        .build();

    final ExpectedProjectionTimeseries queryMetrics =
        new ExpectedProjectionTimeseries(null);
    final CursorBuildSpec buildSpec = TimeseriesQueryEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorNoProjection(buildSpec, queryMetrics);

    testTimeseries(
        query,
        queryMetrics,
        List.of(
            new Object[]{UTC_MIDNIGHT, 1L},
            new Object[]{UTC_MIDNIGHT.plusMinutes(2), 1L},
            new Object[]{UTC_MIDNIGHT.plusMinutes(4), 2L},
            new Object[]{UTC_MIDNIGHT.plusMinutes(6), 3L},
            new Object[]{UTC_MIDNIGHT.plusMinutes(8), 4L},
            new Object[]{UTC_MIDNIGHT.plusMinutes(10), 5L},
            new Object[]{UTC_01H, 1L},
            new Object[]{UTC_01H31M, 2L}
        )
    );
  }

  @Test
  public void testProjectionSingleDimRollupTable()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("a")
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "sum_c"))
                    .build();

    final ExpectedProjectionGroupBy queryMetrics =
        new ExpectedProjectionGroupBy("a_hourly_c_sum_with_count");
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(rollupProjectionsCursorFactory, buildSpec, queryMetrics, 3);

    testGroupBy(
        rollupProjectionsCursorFactory,
        rollupProjectionsTimeBoundaryInspector,
        query,
        queryMetrics,
        List.of(
            new Object[]{"a", 7L},
            new Object[]{"b", 12L}
        )
    );
  }

  @Test
  public void testProjectionSingleDimVirtualColumnRollupTable()
  {
    final VirtualColumn vc = new ExpressionVirtualColumn(
        "v0",
        "concat(a, 'foo')",
        ColumnType.STRING,
        TestExprMacroTable.INSTANCE
    );
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("v0")
                    .setVirtualColumns(vc)
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "sum_c"))
                    .addAggregator(new LongMaxAggregatorFactory("c_c", "max_c"))
                    .build();

    final ExpectedProjectionGroupBy queryMetrics = new ExpectedProjectionGroupBy("afoo");
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(rollupProjectionsCursorFactory, buildSpec, queryMetrics, 2);

    testGroupBy(
        rollupProjectionsCursorFactory,
        rollupProjectionsTimeBoundaryInspector,
        query,
        queryMetrics,
        makeArrayResultSet(
            new Object[]{"afoo", 7L, 2L},
            new Object[]{"bfoo", 12L, 5L}
        )
    );
  }

  @Test
  public void testProjectionJson()
  {
    final GroupByQuery query =
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
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"))
                    .build();

    final ExpectedProjectionGroupBy queryMetrics =
        new ExpectedProjectionGroupBy("json");
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 6);

    testGroupBy(
        query,
        queryMetrics,
        List.of(
            new Object[]{"a", 7L},
            new Object[]{"b", 12L}
        )
    );
  }


  @Test
  public void testProjectionFilter()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("a")
                    .setDimFilter(new EqualityFilter("b", ColumnType.STRING, "aa", null))
                    .addAggregator(new LongSumAggregatorFactory("c_sum", "c"))
                    .addAggregator(new DoubleSumAggregatorFactory("d_sum", "d"))
                    .setContext(Map.of(QueryContexts.USE_PROJECTION, "a_filter_b_aaonly_hourly_cd_sum"))
                    .build();
    final ExpectedProjectionGroupBy queryMetrics =
        new ExpectedProjectionGroupBy("a_filter_b_aaonly_hourly_cd_sum");
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 3);

    testGroupBy(
        query,
        queryMetrics,
        List.of(
            new Object[]{"a", 2L, 2.1},
            new Object[]{"b", 7L, 7.7}
        )
    );
  }

  @Test
  public void testProjectionSelectionTwoVirtual()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("v0")
                    .addDimension(DefaultDimensionSpec.of("v1", ColumnType.LONG))
                    .setVirtualColumns(
                        new ExpressionVirtualColumn(
                            "v0",
                            "concat(a, \"b\")",
                            ColumnType.STRING,
                            TestExprMacroTable.INSTANCE
                        ),
                        new ExpressionVirtualColumn(
                            "v1",
                            "d + e",
                            ColumnType.LONG,
                            TestExprMacroTable.INSTANCE
                        )
                    )
                    .build();

    final ExpectedProjectionGroupBy queryMetrics =
        new ExpectedProjectionGroupBy("a_concat_b_d_plus_f_sum_c");
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 8);

    testGroupBy(
        query,
        queryMetrics,
        makeArrayResultSet(
            new Object[]{"aaa", null},
            new Object[]{"aaa", 2L},
            new Object[]{"abb", 2L},
            new Object[]{"acc", 4L},
            new Object[]{"add", 4L},
            new Object[]{"baa", 8L},
            new Object[]{"baa", 6L},
            new Object[]{"baa", 8L},
            new Object[]{"bbb", 11L}
        )
    );
  }

  @Test
  public void testProjectionFilteredProjectionMatch()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .setDimFilter(new EqualityFilter("a", ColumnType.STRING, "a", null))
                    .addDimension("a")
                    .build();

    final boolean isRealtime = projectionsCursorFactory instanceof IncrementalIndexCursorFactory;
    // realtime projections don't have row count, so abfoo is chosen because of how projection sorting happens
    final ExpectedProjectionGroupBy queryMetrics = new ExpectedProjectionGroupBy(isRealtime ? "abfoo" : "a_hourly_c_sum_filter_a_to_a");

    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, isRealtime ? 4 : 2);

    testGroupBy(
        query,
        queryMetrics,
        makeArrayResultSet(
            new Object[]{"a"}
        )
    );
  }

  @Test
  public void testProjectionFilteredNoFilteredProjectionMatch()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .setDimFilter(new EqualityFilter("a", ColumnType.STRING, "b", null))
                    .addDimension("a")
                    .build();

    final boolean isRealtime = projectionsCursorFactory instanceof IncrementalIndexCursorFactory;
    final ExpectedProjectionGroupBy queryMetrics = new ExpectedProjectionGroupBy(isRealtime ? "abfoo" : "a_hourly_c_sum_with_count_latest");

    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, isRealtime ? 2 : 1);

    testGroupBy(
        query,
        queryMetrics,
        makeArrayResultSet(
            new Object[]{"b"}
        )
    );
  }

  @Test
  public void testProjectionFilteredToEmpty()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .setDimFilter(new EqualityFilter("a", ColumnType.STRING, "nomatch", null))
                    .setContext(Map.of("useProjection", "a_hourly_c_sum_filter_a_to_empty"))
                    .addDimension("a")
                    .build();

    final ExpectedProjectionGroupBy queryMetrics = new ExpectedProjectionGroupBy("a_hourly_c_sum_filter_a_to_empty");

    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 0);

    testGroupBy(
        query,
        queryMetrics,
        makeArrayResultSet()
    );
  }

  @Test
  public void testProjectionFilteredToEmptyTimeseries()
  {
    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource("test")
                                        .intervals(ImmutableList.of(Intervals.ETERNITY))
                                        .granularity(Granularities.ALL)
                                        .filters(new EqualityFilter("a", ColumnType.STRING, "nomatch", null))
                                        .aggregators(new LongSumAggregatorFactory("c_sum", "c"))
                                        .context(ImmutableMap.of(QueryContexts.USE_PROJECTION, "a_hourly_c_sum_filter_a_to_empty"))
                                        .build();

    final ExpectedProjectionTimeseries queryMetrics =
        new ExpectedProjectionTimeseries("a_hourly_c_sum_filter_a_to_empty");

    final CursorBuildSpec buildSpec = TimeseriesQueryEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 0);

    // realltime results are inconsistent between projection and base table since projection is totally empty, but base
    // table is reduced with filter
    final boolean isRealtime = projectionsCursorFactory instanceof IncrementalIndexCursorFactory;
    final List<Object[]> expectedResults = Collections.singletonList(new Object[]{UTC_MIDNIGHT, null});
    final List<Object[]> expectedRealtimeResults = List.of();

    final Sequence<Result<TimeseriesResultValue>> resultRows = timeseriesEngine.process(
        query,
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        queryMetrics
    );

    queryMetrics.assertProjection();

    final List<Result<TimeseriesResultValue>> results = resultRows.toList();
    assertTimeseriesResults(
        query.getResultRowSignature(RowSignature.Finalization.YES),
        isRealtime ? expectedRealtimeResults : expectedResults,
        results
    );


    Assertions.assertEquals(UTC_MIDNIGHT, projectionsTimeBoundaryInspector.getMinTime());
    if (isRealtime || segmentSortedByTime) {
      Assertions.assertEquals(UTC_01H31M, projectionsTimeBoundaryInspector.getMaxTime());
    } else {
      // min and max time are inexact for non time ordered segments
      Assertions.assertEquals(UTC_01H31M.plusMillis(1), projectionsTimeBoundaryInspector.getMaxTime());
    }

    // timeseries query only works on base table if base table is sorted by time
    Assume.assumeTrue(segmentSortedByTime);
    final Sequence<Result<TimeseriesResultValue>> resultRowsNoProjection = timeseriesEngine.process(
        query.withOverriddenContext(Map.of(QueryContexts.NO_PROJECTIONS, true)),
        projectionsCursorFactory,
        projectionsTimeBoundaryInspector,
        queryMetrics
    );

    final List<Result<TimeseriesResultValue>> resultsNoProjection = resultRowsNoProjection.toList();
    assertTimeseriesResults(
        query.getResultRowSignature(RowSignature.Finalization.YES),
        expectedResults,
        resultsNoProjection
    );
  }

  @Test
  public void testProjectionGroupOnTime()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension(new DefaultDimensionSpec(ColumnHolder.TIME_COLUMN_NAME, "d0", ColumnType.LONG))
                    .addDimension("v0")
                    .setVirtualColumns(
                        new ExpressionVirtualColumn(
                            "v0",
                            "concat(a, 'aaa')",
                            ColumnType.STRING,
                            TestExprMacroTable.INSTANCE
                        )
                    )
                    .build();

    final ExpectedProjectionGroupBy queryMetrics =
        new ExpectedProjectionGroupBy("time_and_a");
    final CursorBuildSpec buildSpec = GroupingEngine.makeCursorBuildSpec(query, queryMetrics);

    assertCursorProjection(buildSpec, queryMetrics, 8);

    testGroupBy(
        query,
        queryMetrics,
        makeArrayResultSet(
            new Object[]{UTC_MIDNIGHT.getMillis(), "aaaa"},
            new Object[]{UTC_MIDNIGHT.plusMinutes(2).getMillis(), "aaaa"},
            new Object[]{UTC_MIDNIGHT.plusMinutes(4).getMillis(), "aaaa"},
            new Object[]{UTC_MIDNIGHT.plusMinutes(6).getMillis(), "baaa"},
            new Object[]{UTC_MIDNIGHT.plusMinutes(8).getMillis(), "baaa"},
            new Object[]{UTC_MIDNIGHT.plusMinutes(10).getMillis(), "baaa"},
            new Object[]{UTC_01H.getMillis(), "aaaa"},
            new Object[]{UTC_01H31M.getMillis(), "aaaa"}
        )
    );
  }

  private void testGroupBy(GroupByQuery query, ExpectedProjectionGroupBy queryMetrics, List<Object[]> expectedResults)
  {
    testGroupBy(projectionsCursorFactory, projectionsTimeBoundaryInspector, query, queryMetrics, expectedResults);
  }

  private void testGroupBy(
      CursorFactory cursorFactory,
      TimeBoundaryInspector timeBoundaryInspector,
      GroupByQuery query,
      ExpectedProjectionGroupBy queryMetrics,
      List<Object[]> expectedResults
  )
  {
    final Sequence<ResultRow> resultRows = groupingEngine.process(
        query,
        cursorFactory,
        timeBoundaryInspector,
        nonBlockingPool,
        queryMetrics
    );

    queryMetrics.assertProjection();

    final List<ResultRow> results = resultRows.toList();
    assertGroupByResults(expectedResults, results);

    final Sequence<ResultRow> resultRowsNoProjection = groupingEngine.process(
        query.withOverriddenContext(Map.of(QueryContexts.NO_PROJECTIONS, true)),
        cursorFactory,
        timeBoundaryInspector,
        nonBlockingPool,
        null
    );

    final List<ResultRow> resultsNoProjection = resultRowsNoProjection.toList();
    assertGroupByResults(expectedResults, resultsNoProjection);
  }

  private void testGroupBy(GroupByQuery query, ExpectedProjectionGroupBy queryMetrics, Set<Object[]> expectedResults)
  {
    testGroupBy(projectionsCursorFactory, projectionsTimeBoundaryInspector, query, queryMetrics, expectedResults);
  }

  private void testGroupBy(
      CursorFactory cursorFactory,
      TimeBoundaryInspector timeBoundaryInspector,
      GroupByQuery query,
      ExpectedProjectionGroupBy queryMetrics,
      Set<Object[]> expectedResults
  )
  {
    final Sequence<ResultRow> resultRows = groupingEngine.process(
        query,
        cursorFactory,
        timeBoundaryInspector,
        nonBlockingPool,
        queryMetrics
    );

    queryMetrics.assertProjection();

    final Object[] results = resultRows.toList().stream().map(ResultRow::getArray).map(Arrays::toString).toArray();
    Arrays.sort(results);

    final Object[] expectedResultsArray = expectedResults.stream().map(Arrays::toString).toArray();
    Arrays.sort(expectedResultsArray);
    // print a full diff of all differing elements.
    Assertions.assertEquals(Arrays.toString(expectedResultsArray), Arrays.toString(results));

    final Sequence<ResultRow> resultRowsNoProjection = groupingEngine.process(
        query.withOverriddenContext(Map.of(QueryContexts.NO_PROJECTIONS, true)),
        cursorFactory,
        timeBoundaryInspector,
        nonBlockingPool,
        null
    );

    final Object[] resultsNoProjection = resultRowsNoProjection.toList().stream().map(ResultRow::getArray).map(Arrays::toString).toArray();
    Arrays.sort(resultsNoProjection);
    // print a full diff of all differing elements.
    Assertions.assertEquals(Arrays.toString(expectedResultsArray), Arrays.toString(resultsNoProjection));

  }

  private void testTimeseries(
      TimeseriesQuery query,
      ExpectedProjectionTimeseries queryMetrics,
      List<Object[]> expectedResults
  )
  {
    testTimeseries(projectionsCursorFactory, projectionsTimeBoundaryInspector, query, queryMetrics, expectedResults);
  }

  private void testTimeseries(
      CursorFactory cursorFactory,
      TimeBoundaryInspector timeBoundaryInspector,
      TimeseriesQuery query,
      ExpectedProjectionTimeseries queryMetrics,
      List<Object[]> expectedResults
  )
  {
    final Sequence<Result<TimeseriesResultValue>> resultRows = timeseriesEngine.process(
        query,
        cursorFactory,
        timeBoundaryInspector,
        queryMetrics
    );

    queryMetrics.assertProjection();

    final List<Result<TimeseriesResultValue>> results = resultRows.toList();
    assertTimeseriesResults(query.getResultRowSignature(RowSignature.Finalization.YES), expectedResults, results);

    // timeseries query only works on base table if base table is sorted by time
    Assume.assumeTrue(segmentSortedByTime);
    final Sequence<Result<TimeseriesResultValue>> resultRowsNoProjection = timeseriesEngine.process(
        query.withOverriddenContext(Map.of(QueryContexts.NO_PROJECTIONS, true)),
        cursorFactory,
        timeBoundaryInspector,
        queryMetrics
    );

    final List<Result<TimeseriesResultValue>> resultsNoProjection = resultRowsNoProjection.toList();
    assertTimeseriesResults(
        query.getResultRowSignature(RowSignature.Finalization.YES),
        expectedResults,
        resultsNoProjection
    );
  }

  private void assertGroupByResults(List<Object[]> expected, List<ResultRow> actual)
  {
    Assertions.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      Assertions.assertArrayEquals(expected.get(i), actual.get(i).getArray());
    }
  }

  private void assertTimeseriesResults(
      RowSignature querySignature,
      List<Object[]> expected,
      List<Result<TimeseriesResultValue>> actual
  )
  {
    Assertions.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      Assertions.assertArrayEquals(expected.get(i), getResultArray(actual.get(i), querySignature));
    }
  }

  private void assertCursorNoProjection(CursorBuildSpec buildSpec, ExpectedProjectionQueryMetrics<?> queryMetrics)
  {
    assertCursorProjection(buildSpec, queryMetrics, 8);
  }

  private void assertCursorProjection(
      CursorBuildSpec buildSpec,
      ExpectedProjectionQueryMetrics<?> queryMetrics,
      int expectedRowCount
  )
  {
    assertCursorProjection(projectionsCursorFactory, buildSpec, queryMetrics, expectedRowCount);
  }

  private void assertCursorProjection(
      CursorFactory cursorFactory,
      CursorBuildSpec buildSpec,
      ExpectedProjectionQueryMetrics<?> queryMetrics,
      int expectedRowCount
  )
  {
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(buildSpec)) {
      queryMetrics.assertProjection();
      final Cursor cursor = cursorHolder.asCursor();
      int rowCount = 0;
      if (cursor != null) {
        while (!cursor.isDone()) {
          rowCount++;
          cursor.advance();
        }
      }
      Assert.assertEquals(expectedRowCount, rowCount);
    }
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
                                                 .withMinTimestamp(UTC_MIDNIGHT.getMillis())
                                                 .withProjections(autoSchema ? AUTO_PROJECTIONS : PROJECTIONS)
                                                 .build()
                       )
                       .writeNullColumns(writeNullColumns)
                       .rows(ROWS);
  }

  private static IndexBuilder makeRollupBuilder(
      DimensionsSpec dimensionsSpec,
      AggregatorFactory[] aggs,
      boolean autoSchema
  )
  {
    File tmp = FileUtils.createTempDir();
    CLOSER.register(tmp::delete);
    return IndexBuilder.create()
                       .tmpDir(tmp)
                       .schema(
                           IncrementalIndexSchema
                               .builder()
                               .withDimensionsSpec(dimensionsSpec)
                               .withMetrics(aggs)
                               .withRollup(true)
                               .withMinTimestamp(UTC_MIDNIGHT.getMillis())
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
            return Arrays.deepHashCode(o);
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

  private static class ExpectedProjectionQueryMetrics<T extends Query<?>> extends DefaultQueryMetrics<T>
  {
    @Nullable
    private final String expectedProjection;
    @Nullable
    private String capturedProjection;

    private ExpectedProjectionQueryMetrics(@Nullable String expectedProjection)
    {
      this.expectedProjection = expectedProjection;
    }

    @Override
    public void projection(String projection)
    {
      capturedProjection = projection;
    }

    void assertProjection()
    {
      Assertions.assertEquals(expectedProjection, capturedProjection);
      capturedProjection = null;
    }
  }

  private static class ExpectedProjectionGroupBy extends ExpectedProjectionQueryMetrics<GroupByQuery>
      implements GroupByQueryMetrics
  {
    private ExpectedProjectionGroupBy(@Nullable String expectedProjection)
    {
      super(expectedProjection);
    }

    @Override
    public void numDimensions(GroupByQuery query)
    {

    }

    @Override
    public void numMetrics(GroupByQuery query)
    {

    }

    @Override
    public void numComplexMetrics(GroupByQuery query)
    {

    }

    @Override
    public void granularity(GroupByQuery query)
    {

    }
  }

  private static class ExpectedProjectionTimeseries extends ExpectedProjectionQueryMetrics<TimeseriesQuery>
      implements TimeseriesQueryMetrics
  {

    private ExpectedProjectionTimeseries(@Nullable String expectedProjection)
    {
      super(expectedProjection);
    }

    @Override
    public void limit(TimeseriesQuery query)
    {

    }

    @Override
    public void numMetrics(TimeseriesQuery query)
    {

    }

    @Override
    public void numComplexMetrics(TimeseriesQuery query)
    {

    }

    @Override
    public void granularity(TimeseriesQuery query)
    {

    }
  }
}
