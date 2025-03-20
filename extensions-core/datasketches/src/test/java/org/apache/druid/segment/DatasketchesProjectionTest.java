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
import org.apache.datasketches.common.Family;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.thetacommon.ThetaUtil;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketchBuilder;
import org.apache.druid.collections.CloseableDefaultBlockingPool;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchBuildAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchHolder;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchModule;
import org.apache.druid.query.aggregation.datasketches.kll.KllDoublesSketchAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.kll.KllSketchModule;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchModule;
import org.apache.druid.query.aggregation.datasketches.theta.SketchHolder;
import org.apache.druid.query.aggregation.datasketches.theta.SketchMergeAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.theta.SketchModule;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchModule;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByResourcesReservationPool;
import org.apache.druid.query.groupby.GroupByStatsProvider;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexCursorFactory;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.testing.InitializedNullHandlingTest;
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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * like {@link CursorFactoryProjectionTest} but for sketch aggs
 */
@RunWith(Parameterized.class)
public class DatasketchesProjectionTest extends InitializedNullHandlingTest
{
  private static final Closer CLOSER = Closer.create();

  private static final List<AggregateProjectionSpec> PROJECTIONS = Collections.singletonList(
      new AggregateProjectionSpec(
          "a_projection",
          VirtualColumns.create(
              Granularities.toVirtualColumn(Granularities.HOUR, "__gran")
          ),
          Arrays.asList(
              new LongDimensionSchema("__gran"),
              new StringDimensionSchema("a")
          ),
          new AggregatorFactory[]{
              new HllSketchBuildAggregatorFactory("_b_hll", "b", null, null, null, null, false),
              new SketchMergeAggregatorFactory("_b_theta", "b", null, null, false, null),
              new DoublesSketchAggregatorFactory("_d_doubles", "d", null),
              new ArrayOfDoublesSketchAggregatorFactory("_bcd_aod", "b", null, Arrays.asList("c", "d"), null),
              new KllDoublesSketchAggregatorFactory("_d_kll", "d", null, null)
          }
      )
  );

  private static final List<AggregateProjectionSpec> AUTO_PROJECTIONS = PROJECTIONS.stream().map(projection -> {
    return new AggregateProjectionSpec(
        projection.getName(),
        projection.getVirtualColumns(),
        projection.getGroupingColumns()
                  .stream()
                  .map(x -> new AutoTypeColumnSchema(x.getName(), null))
                  .collect(Collectors.toList()),
        projection.getAggregators()
    );
  }).collect(Collectors.toList());

  @Parameterized.Parameters(name = "name: {0}, sortByDim: {3}, autoSchema: {4}")
  public static Collection<?> constructorFeeder()
  {
    HllSketchModule.registerSerde();
    TestHelper.JSON_MAPPER.registerModules(new HllSketchModule().getJacksonModules());
    SketchModule.registerSerde();
    TestHelper.JSON_MAPPER.registerModules(new SketchModule().getJacksonModules());
    KllSketchModule.registerSerde();
    TestHelper.JSON_MAPPER.registerModules(new KllSketchModule().getJacksonModules());
    DoublesSketchModule.registerSerde();
    TestHelper.JSON_MAPPER.registerModules(new DoublesSketchModule().getJacksonModules());
    ArrayOfDoublesSketchModule.registerSerde();
    TestHelper.JSON_MAPPER.registerModules(new ArrayOfDoublesSketchModule().getJacksonModules());

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


    List<DimensionSchema> autoDims = dimsOrdered.getDimensions()
                                                .stream()
                                                .map(x -> new AutoTypeColumnSchema(x.getName(), null))
                                                .collect(Collectors.toList());
    for (boolean incremental : new boolean[]{true, false}) {
      for (boolean sortByDim : new boolean[]{true, false}) {
        for (boolean autoSchema : new boolean[]{true, false}) {
          final DimensionsSpec dims;
          if (sortByDim) {
            if (autoSchema) {
              dims = dimsOrdered.withDimensions(autoDims);
            } else {
              dims = dimsOrdered;
            }
          } else {
            if (autoSchema) {
              dims = dimsTimeOrdered.withDimensions(autoDims);
            } else {
              dims = dimsTimeOrdered;
            }
          }
          if (incremental) {
            IncrementalIndex index = CLOSER.register(makeBuilder(dims, autoSchema).buildIncrementalIndex());
            constructors.add(new Object[]{
                "incrementalIndex",
                new IncrementalIndexCursorFactory(index),
                new IncrementalIndexTimeBoundaryInspector(index),
                sortByDim,
                autoSchema
            });
          } else {
            QueryableIndex index = CLOSER.register(makeBuilder(dims, autoSchema).buildMMappedIndex());
            constructors.add(new Object[]{
                "queryableIndex",
                new QueryableIndexCursorFactory(index),
                QueryableIndexTimeBoundaryInspector.create(index),
                sortByDim,
                autoSchema
            });
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

  private static IndexBuilder makeBuilder(DimensionsSpec dimensionsSpec, boolean autoSchema)
  {
    File tmp = FileUtils.createTempDir();
    CLOSER.register(tmp::delete);
    return IndexBuilder.create()
                       .tmpDir(tmp)
                       .schema(
                           IncrementalIndexSchema.builder()
                                                 .withDimensionsSpec(dimensionsSpec)
                                                 .withRollup(false)
                                                 .withMinTimestamp(CursorFactoryProjectionTest.TIMESTAMP.getMillis())
                                                 .withProjections(autoSchema ? AUTO_PROJECTIONS : PROJECTIONS)
                                                 .build()
                       )
                       .rows(CursorFactoryProjectionTest.ROWS);
  }

  public final CursorFactory projectionsCursorFactory;
  public final TimeBoundaryInspector projectionsTimeBoundaryInspector;

  private final GroupingEngine groupingEngine;

  private final NonBlockingPool<ByteBuffer> nonBlockingPool;
  public final boolean sortByDim;
  public final boolean autoSchema;

  @Rule
  public final CloserRule closer = new CloserRule(false);

  public DatasketchesProjectionTest(
      String name,
      CursorFactory projectionsCursorFactory,
      TimeBoundaryInspector projectionsTimeBoundaryInspector,
      boolean sortByDim,
      boolean autoSchema
  )
  {
    this.projectionsCursorFactory = projectionsCursorFactory;
    this.projectionsTimeBoundaryInspector = projectionsTimeBoundaryInspector;
    this.sortByDim = sortByDim;
    this.autoSchema = autoSchema;
    this.nonBlockingPool = closer.closeLater(
        new CloseableStupidPool<>(
            "GroupByQueryEngine-bufferPool",
            () -> ByteBuffer.allocate(1 << 24)
        )
    );
    this.groupingEngine = new GroupingEngine(
        new DruidProcessingConfig(),
        GroupByQueryConfig::new,
        new GroupByResourcesReservationPool(
            closer.closeLater(
                new CloseableDefaultBlockingPool<>(
                    () -> ByteBuffer.allocate(1 << 24),
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
                    .setAggregatorSpecs(
                        new HllSketchBuildAggregatorFactory("b_distinct", "b", null, null, null, true, true),
                        new SketchMergeAggregatorFactory("b_distinct_theta", "b", null, null, null, null),
                        new DoublesSketchAggregatorFactory("d_doubles", "d", null, null, null),
                        new ArrayOfDoublesSketchAggregatorFactory("b_doubles", "b", null, Arrays.asList("c", "d"), null),
                        new KllDoublesSketchAggregatorFactory("d", "d", null, null)
                    )
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
    List<Object[]> expectedResults = getSingleDimExpected();
    final RowSignature querySignature = query.getResultRowSignature(RowSignature.Finalization.NO);
    for (int i = 0; i < expectedResults.size(); i++) {
      assertResults(
          expectedResults.get(i),
          results.get(i).getArray(),
          querySignature
      );
    }
  }

  @Test
  public void testProjectionSingleDimNoProjections()
  {
    // test can use the single dimension projection
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("a")
                    .setAggregatorSpecs(
                        new HllSketchBuildAggregatorFactory("b_distinct", "b", null, null, null, true, true),
                        new SketchMergeAggregatorFactory("b_distinct_theta", "b", null, null, null, null),
                        new DoublesSketchAggregatorFactory("d_doubles", "d", null, null, null),
                        new ArrayOfDoublesSketchAggregatorFactory("b_doubles", "b", null, Arrays.asList("c", "d"), null),
                        new KllDoublesSketchAggregatorFactory("d", "d", null, null)
                    )
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
    List<Object[]> expectedResults = getSingleDimExpected();
    final RowSignature querySignature = query.getResultRowSignature(RowSignature.Finalization.NO);
    for (int i = 0; i < expectedResults.size(); i++) {
      assertResults(
          expectedResults.get(i),
          results.get(i).getArray(),
          querySignature
      );
    }
  }

  private List<Object[]> getSingleDimExpected()
  {
    HllSketch hll1 = new HllSketch(HllSketch.DEFAULT_LG_K);
    Union theta1 = (Union) SetOperation.builder().build(Family.UNION);
    DoublesSketch d1 = DoublesSketch.builder().setK(DoublesSketchAggregatorFactory.DEFAULT_K).build();
    ArrayOfDoublesUpdatableSketch ad1 = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(ThetaUtil.DEFAULT_NOMINAL_ENTRIES)
                                                                                  .setNumberOfValues(2)
                                                                                  .build();
    KllDoublesSketch kll1 = KllDoublesSketch.newHeapInstance();
    hll1.update("aa");
    hll1.update("bb");
    hll1.update("cc");
    hll1.update("dd");
    theta1.update("aa");
    theta1.update("bb");
    theta1.update("cc");
    theta1.update("dd");
    d1.update(1.0);
    d1.update(1.1);
    d1.update(2.2);
    d1.update(1.1);
    d1.update(2.2);
    ad1.update("aa", new double[]{1.0, 1.0});
    ad1.update("bb", new double[]{1.0, 1.1});
    ad1.update("cc", new double[]{2.0, 2.2});
    ad1.update("aa", new double[]{1.0, 1.1});
    ad1.update("dd", new double[]{2.0, 2.2});
    kll1.update(1.0);
    kll1.update(1.1);
    kll1.update(2.2);
    kll1.update(1.1);
    kll1.update(2.2);
    HllSketch hll2 = new HllSketch(HllSketch.DEFAULT_LG_K);
    Union theta2 = (Union) SetOperation.builder().build(Family.UNION);
    DoublesSketch d2 = DoublesSketch.builder().setK(DoublesSketchAggregatorFactory.DEFAULT_K).build();
    ArrayOfDoublesUpdatableSketch ad2 = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(ThetaUtil.DEFAULT_NOMINAL_ENTRIES)
                                                                                  .setNumberOfValues(2)
                                                                                  .build();
    KllDoublesSketch kll2 = KllDoublesSketch.newHeapInstance();
    hll2.update("aa");
    hll2.update("bb");
    theta2.update("aa");
    theta2.update("bb");
    d2.update(3.3);
    d2.update(4.4);
    d2.update(5.5);
    ad2.update("aa", new double[]{3.0, 3.3});
    ad2.update("aa", new double[]{4.0, 4.4});
    ad2.update("bb", new double[]{5.0, 5.5});
    kll2.update(3.3);
    kll2.update(4.4);
    kll2.update(5.5);

    return Arrays.asList(
        new Object[]{"a", HllSketchHolder.of(hll1), SketchHolder.of(theta1), d1, ad1, kll1},
        new Object[]{"b", HllSketchHolder.of(hll2), SketchHolder.of(theta2), d2, ad2, kll2}
    );

  }

  private void assertResults(Object[] expected, Object[] actual, RowSignature signature)
  {
    Assert.assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      if (signature.getColumnType(i).get().equals(ColumnType.ofComplex(HllSketchModule.BUILD_TYPE_NAME))) {
        Assert.assertEquals(
            ((HllSketchHolder) expected[i]).getEstimate(),
            ((HllSketchHolder) actual[i]).getEstimate(),
            0.01
        );
      } else if (signature.getColumnType(i).get().equals(DoublesSketchModule.TYPE)) {
        Assert.assertEquals(
            ((DoublesSketch) expected[i]).getMinItem(),
            ((DoublesSketch) actual[i]).getMinItem(),
            0.01
        );
        Assert.assertEquals(
            ((DoublesSketch) expected[i]).getMaxItem(),
            ((DoublesSketch) actual[i]).getMaxItem(),
            0.01
        );
      } else if (signature.getColumnType(i).get().equals(ArrayOfDoublesSketchModule.BUILD_TYPE)) {
        Assert.assertEquals(
            ((ArrayOfDoublesSketch) expected[i]).getEstimate(),
            ((ArrayOfDoublesSketch) actual[i]).getEstimate(),
            0.01
        );
        Assert.assertEquals(
            ((ArrayOfDoublesSketch) expected[i]).getLowerBound(0),
            ((ArrayOfDoublesSketch) actual[i]).getLowerBound(0),
            0.01
        );
        Assert.assertEquals(
            ((ArrayOfDoublesSketch) expected[i]).getUpperBound(0),
            ((ArrayOfDoublesSketch) actual[i]).getUpperBound(0),
            0.01
        );
      } else if (signature.getColumnType(i).get().equals(KllSketchModule.DOUBLES_TYPE)) {
        Assert.assertEquals(
            ((KllDoublesSketch) expected[i]).getMinItem(),
            ((KllDoublesSketch) actual[i]).getMinItem(),
            0.01
        );
        Assert.assertEquals(
            ((KllDoublesSketch) expected[i]).getMaxItem(),
            ((KllDoublesSketch) actual[i]).getMaxItem(),
            0.01
        );
      } else {
        Assert.assertEquals(expected[i], actual[i]);
      }
    }
  }
}
