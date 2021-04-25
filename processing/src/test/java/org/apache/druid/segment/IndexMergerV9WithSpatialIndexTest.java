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
import com.google.common.collect.Lists;
import org.apache.druid.collections.spatial.search.RadiusBound;
import org.apache.druid.collections.spatial.search.RectangularBound;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.SpatialDimensionSchema;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.filter.SpatialDimFilter;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.Interval;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 */
@RunWith(Parameterized.class)
public class IndexMergerV9WithSpatialIndexTest extends InitializedNullHandlingTest
{

  public static final int NUM_POINTS = 5000;
  private static Interval DATA_INTERVAL = Intervals.of("2013-01-01/2013-01-07");

  private static AggregatorFactory[] METRIC_AGGS = new AggregatorFactory[]{
      new CountAggregatorFactory("rows"),
      new LongSumAggregatorFactory("val", "val")
  };

  private static List<String> DIMS = Lists.newArrayList("dim", "lat", "long", "lat2", "long2");

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    List<Object[]> argumentArrays = new ArrayList<>();
    for (SegmentWriteOutMediumFactory segmentWriteOutMediumFactory : SegmentWriteOutMediumFactory.builtInFactories()) {
      IndexMergerV9 indexMergerV9 = TestHelper.getTestIndexMergerV9(segmentWriteOutMediumFactory);
      IndexIO indexIO = TestHelper.getTestIndexIO();

      final IndexSpec indexSpec = new IndexSpec();
      final IncrementalIndex rtIndex = makeIncrementalIndex();
      final QueryableIndex mMappedTestIndex = makeQueryableIndex(indexSpec, indexMergerV9, indexIO);
      final QueryableIndex mergedRealtimeIndex = makeMergedQueryableIndex(indexSpec, indexMergerV9, indexIO);
      argumentArrays.add(new Object[] {new IncrementalIndexSegment(rtIndex, null)});
      argumentArrays.add(new Object[] {new QueryableIndexSegment(mMappedTestIndex, null)});
      argumentArrays.add(new Object[] {new QueryableIndexSegment(mergedRealtimeIndex, null)});
    }
    return argumentArrays;
  }

  private static IncrementalIndex makeIncrementalIndex() throws IOException
  {
    IncrementalIndex theIndex = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withMinTimestamp(DATA_INTERVAL.getStartMillis())
                .withQueryGranularity(Granularities.DAY)
                .withMetrics(METRIC_AGGS)
                .withDimensionsSpec(
                    new DimensionsSpec(
                        null,
                        null,
                        Arrays.asList(
                            new SpatialDimensionSchema(
                                "dim.geo",
                                Arrays.asList("lat", "long")
                            ),
                            new SpatialDimensionSchema(
                                "spatialIsRad",
                                Arrays.asList("lat2", "long2")
                            )

                        )
                    )
                ).build()
        )
        .setMaxRowCount(NUM_POINTS)
        .build();

    theIndex.add(
        new MapBasedInputRow(
            DateTimes.of("2013-01-01").getMillis(),
            DIMS,
            ImmutableMap.of(
                "timestamp", DateTimes.of("2013-01-01").toString(),
                "dim", "foo",
                "lat", 0.0f,
                "long", 0.0f,
                "val", 17L
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            DateTimes.of("2013-01-02").getMillis(),
            DIMS,
            ImmutableMap.of(
                "timestamp", DateTimes.of("2013-01-02").toString(),
                "dim", "foo",
                "lat", 1.0f,
                "long", 3.0f,
                "val", 29L
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            DateTimes.of("2013-01-03").getMillis(),
            DIMS,
            ImmutableMap.of(
                "timestamp", DateTimes.of("2013-01-03").toString(),
                "dim", "foo",
                "lat", 4.0f,
                "long", 2.0f,
                "val", 13L
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            DateTimes.of("2013-01-04").getMillis(),
            DIMS,
            ImmutableMap.of(
                "timestamp", DateTimes.of("2013-01-04").toString(),
                "dim", "foo",
                "lat", 7.0f,
                "long", 3.0f,
                "val", 91L
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            DateTimes.of("2013-01-05").getMillis(),
            DIMS,
            ImmutableMap.of(
                "timestamp", DateTimes.of("2013-01-05").toString(),
                "dim", "foo",
                "lat", 8.0f,
                "long", 6.0f,
                "val", 47L
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            DateTimes.of("2013-01-05").getMillis(),
            DIMS,
            ImmutableMap.of(
                "timestamp", DateTimes.of("2013-01-05").toString(),
                "dim", "foo",
                "lat", "_mmx.unknown",
                "long", "_mmx.unknown",
                "val", 101L
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            DateTimes.of("2013-01-05").getMillis(),
            DIMS,
            ImmutableMap.of(
                "timestamp", DateTimes.of("2013-01-05").toString(),
                "dim", "foo",
                "dim.geo", "_mmx.unknown",
                "val", 501L
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            DateTimes.of("2013-01-05").getMillis(),
            DIMS,
            ImmutableMap.of(
                "timestamp", DateTimes.of("2013-01-05").toString(),
                "lat2", 0.0f,
                "long2", 0.0f,
                "val", 13L
            )
        )
    );

    // Add a bunch of random points
    Random rand = ThreadLocalRandom.current();
    for (int i = 8; i < NUM_POINTS; i++) {
      theIndex.add(
          new MapBasedInputRow(
              DateTimes.of("2013-01-01").getMillis(),
              DIMS,
              ImmutableMap.of(
                  "timestamp", DateTimes.of("2013-01-01").toString(),
                  "dim", "boo",
                  "lat", (float) (rand.nextFloat() * 10 + 10.0),
                  "long", (float) (rand.nextFloat() * 10 + 10.0),
                  "val", i
              )
          )
      );
    }

    return theIndex;
  }

  private static QueryableIndex makeQueryableIndex(IndexSpec indexSpec, IndexMergerV9 indexMergerV9, IndexIO indexIO)
      throws IOException
  {
    IncrementalIndex theIndex = makeIncrementalIndex();
    File tmpFile = File.createTempFile("billy", "yay");
    tmpFile.delete();
    tmpFile.mkdirs();

    try {
      indexMergerV9.persist(theIndex, tmpFile, indexSpec, null);
      return indexIO.loadIndex(tmpFile);
    }
    finally {
      FileUtils.deleteDirectory(tmpFile);
    }
  }

  private static QueryableIndex makeMergedQueryableIndex(
      IndexSpec indexSpec,
      IndexMergerV9 indexMergerV9,
      IndexIO indexIO
  )
  {
    try {
      IncrementalIndex first = new OnheapIncrementalIndex.Builder()
          .setIndexSchema(
              new IncrementalIndexSchema.Builder()
                  .withMinTimestamp(DATA_INTERVAL.getStartMillis())
                  .withQueryGranularity(Granularities.DAY)
                  .withMetrics(METRIC_AGGS)
                  .withDimensionsSpec(
                      new DimensionsSpec(
                          null,
                          null,
                          Arrays.asList(
                              new SpatialDimensionSchema(
                                  "dim.geo",
                                  Arrays.asList("lat", "long")
                              ),
                              new SpatialDimensionSchema(
                                  "spatialIsRad",
                                  Arrays.asList("lat2", "long2")
                              )

                          )
                      )
                  ).build()
          )
          .setMaxRowCount(1000)
          .build();

      IncrementalIndex second = new OnheapIncrementalIndex.Builder()
          .setIndexSchema(
              new IncrementalIndexSchema.Builder()
                  .withMinTimestamp(DATA_INTERVAL.getStartMillis())
                  .withQueryGranularity(Granularities.DAY)
                  .withMetrics(METRIC_AGGS)
                  .withDimensionsSpec(
                      new DimensionsSpec(
                          null,
                          null,
                          Arrays.asList(
                              new SpatialDimensionSchema(
                                  "dim.geo",
                                  Arrays.asList("lat", "long")
                              ),
                              new SpatialDimensionSchema(
                                  "spatialIsRad",
                                  Arrays.asList("lat2", "long2")
                              )

                          )
                      )
                  ).build()
          )
          .setMaxRowCount(1000)
          .build();

      IncrementalIndex third = new OnheapIncrementalIndex.Builder()
          .setIndexSchema(
              new IncrementalIndexSchema.Builder()
                  .withMinTimestamp(DATA_INTERVAL.getStartMillis())
                  .withQueryGranularity(Granularities.DAY)
                  .withMetrics(METRIC_AGGS)
                  .withDimensionsSpec(
                      new DimensionsSpec(
                          null,
                          null,
                          Arrays.asList(
                              new SpatialDimensionSchema(
                                  "dim.geo",
                                  Arrays.asList("lat", "long")
                              ),
                              new SpatialDimensionSchema(
                                  "spatialIsRad",
                                  Arrays.asList("lat2", "long2")
                              )

                          )
                      )
                  ).build()
          )
          .setMaxRowCount(NUM_POINTS)
          .build();

      first.add(
          new MapBasedInputRow(
              DateTimes.of("2013-01-01").getMillis(),
              DIMS,
              ImmutableMap.of(
                  "timestamp", DateTimes.of("2013-01-01").toString(),
                  "dim", "foo",
                  "lat", 0.0f,
                  "long", 0.0f,
                  "val", 17L
              )
          )
      );
      first.add(
          new MapBasedInputRow(
              DateTimes.of("2013-01-02").getMillis(),
              DIMS,
              ImmutableMap.of(
                  "timestamp", DateTimes.of("2013-01-02").toString(),
                  "dim", "foo",
                  "lat", 1.0f,
                  "long", 3.0f,
                  "val", 29L
              )
          )
      );
      first.add(
          new MapBasedInputRow(
              DateTimes.of("2013-01-03").getMillis(),
              DIMS,
              ImmutableMap.of(
                  "timestamp", DateTimes.of("2013-01-03").toString(),
                  "dim", "foo",
                  "lat", 4.0f,
                  "long", 2.0f,
                  "val", 13L
              )
          )
      );
      first.add(
          new MapBasedInputRow(
              DateTimes.of("2013-01-05").getMillis(),
              DIMS,
              ImmutableMap.of(
                  "timestamp", DateTimes.of("2013-01-05").toString(),
                  "dim", "foo",
                  "lat", "_mmx.unknown",
                  "long", "_mmx.unknown",
                  "val", 101L
              )
          )
      );
      first.add(
          new MapBasedInputRow(
              DateTimes.of("2013-01-05").getMillis(),
              DIMS,
              ImmutableMap.of(
                  "timestamp", DateTimes.of("2013-01-05").toString(),
                  "dim", "foo",
                  "dim.geo", "_mmx.unknown",
                  "val", 501L
              )
          )
      );
      second.add(
          new MapBasedInputRow(
              DateTimes.of("2013-01-04").getMillis(),
              DIMS,
              ImmutableMap.of(
                  "timestamp", DateTimes.of("2013-01-04").toString(),
                  "dim", "foo",
                  "lat", 7.0f,
                  "long", 3.0f,
                  "val", 91L
              )
          )
      );
      second.add(
          new MapBasedInputRow(
              DateTimes.of("2013-01-05").getMillis(),
              DIMS,
              ImmutableMap.of(
                  "timestamp", DateTimes.of("2013-01-05").toString(),
                  "dim", "foo",
                  "lat", 8.0f,
                  "long", 6.0f,
                  "val", 47L
              )
          )
      );
      second.add(
          new MapBasedInputRow(
              DateTimes.of("2013-01-05").getMillis(),
              DIMS,
              ImmutableMap.of(
                  "timestamp", DateTimes.of("2013-01-05").toString(),
                  "lat2", 0.0f,
                  "long2", 0.0f,
                  "val", 13L
              )
          )
      );

      // Add a bunch of random points
      Random rand = ThreadLocalRandom.current();
      for (int i = 8; i < NUM_POINTS; i++) {
        third.add(
            new MapBasedInputRow(
                DateTimes.of("2013-01-01").getMillis(),
                DIMS,
                ImmutableMap.of(
                    "timestamp", DateTimes.of("2013-01-01").toString(),
                    "dim", "boo",
                    "lat", (float) (rand.nextFloat() * 10 + 10.0),
                    "long", (float) (rand.nextFloat() * 10 + 10.0),
                    "val", i
                )
            )
        );
      }


      File tmpFile = File.createTempFile("yay", "who");
      tmpFile.delete();

      File firstFile = new File(tmpFile, "first");
      File secondFile = new File(tmpFile, "second");
      File thirdFile = new File(tmpFile, "third");
      File mergedFile = new File(tmpFile, "merged");

      firstFile.mkdirs();
      secondFile.mkdirs();
      thirdFile.mkdirs();
      mergedFile.mkdirs();

      indexMergerV9.persist(first, DATA_INTERVAL, firstFile, indexSpec, null);
      indexMergerV9.persist(second, DATA_INTERVAL, secondFile, indexSpec, null);
      indexMergerV9.persist(third, DATA_INTERVAL, thirdFile, indexSpec, null);

      try {
        QueryableIndex mergedRealtime = indexIO.loadIndex(
            indexMergerV9.mergeQueryableIndex(
                Arrays.asList(
                    indexIO.loadIndex(firstFile),
                    indexIO.loadIndex(secondFile),
                    indexIO.loadIndex(thirdFile)
                ),
                true,
                METRIC_AGGS,
                mergedFile,
                indexSpec,
                null,
                -1
            )
        );
        return mergedRealtime;

      }
      finally {
        FileUtils.deleteDirectory(firstFile);
        FileUtils.deleteDirectory(secondFile);
        FileUtils.deleteDirectory(thirdFile);
        FileUtils.deleteDirectory(mergedFile);
      }

    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private final Segment segment;

  public IndexMergerV9WithSpatialIndexTest(Segment segment)
  {
    this.segment = segment;
  }

  @Test
  public void testSpatialQuery()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test")
                                  .granularity(Granularities.ALL)
                                  .intervals(Collections.singletonList(Intervals.of("2013-01-01/2013-01-07")))
                                  .filters(
                                      new SpatialDimFilter(
                                          "dim.geo",
                                          new RadiusBound(new float[]{0.0f, 0.0f}, 5)
                                      )
                                  )
                                  .aggregators(
                                      Arrays.asList(
                                          new CountAggregatorFactory("rows"),
                                          new LongSumAggregatorFactory("val", "val")
                                      )
                                  )
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2013-01-01T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("rows", 3L)
                    .put("val", 59L)
                    .build()
            )
        )
    );
    try {
      TimeseriesQueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
          new TimeseriesQueryQueryToolChest(),
          new TimeseriesQueryEngine(),
          QueryRunnerTestHelper.NOOP_QUERYWATCHER
      );

      QueryRunner runner = new FinalizeResultsQueryRunner(
          factory.createRunner(segment),
          factory.getToolchest()
      );

      TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  @Test
  public void testSpatialQueryWithOtherSpatialDim()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test")
                                  .granularity(Granularities.ALL)
                                  .intervals(Collections.singletonList(Intervals.of("2013-01-01/2013-01-07")))
                                  .filters(
                                      new SpatialDimFilter(
                                          "spatialIsRad",
                                          new RadiusBound(new float[]{0.0f, 0.0f}, 5)
                                      )
                                  )
                                  .aggregators(
                                      Arrays.asList(
                                          new CountAggregatorFactory("rows"),
                                          new LongSumAggregatorFactory("val", "val")
                                      )
                                  )
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2013-01-01T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("rows", 1L)
                    .put("val", 13L)
                    .build()
            )
        )
    );
    try {
      TimeseriesQueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
          new TimeseriesQueryQueryToolChest(),
          new TimeseriesQueryEngine(),
          QueryRunnerTestHelper.NOOP_QUERYWATCHER
      );

      QueryRunner runner = new FinalizeResultsQueryRunner(
          factory.createRunner(segment),
          factory.getToolchest()
      );

      TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testSpatialQueryMorePoints()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test")
                                  .granularity(Granularities.DAY)
                                  .intervals(Collections.singletonList(Intervals.of("2013-01-01/2013-01-07")))
                                  .filters(
                                      new SpatialDimFilter(
                                          "dim.geo",
                                          new RectangularBound(new float[]{0.0f, 0.0f}, new float[]{9.0f, 9.0f})
                                      )
                                  )
                                  .aggregators(
                                      Arrays.asList(
                                          new CountAggregatorFactory("rows"),
                                          new LongSumAggregatorFactory("val", "val")
                                      )
                                  )
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2013-01-01T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("val", 17L)
                            .build()
            )
        ),
        new Result<>(
            DateTimes.of("2013-01-02T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("val", 29L)
                            .build()
            )
        ),
        new Result<>(
            DateTimes.of("2013-01-03T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("val", 13L)
                            .build()
            )
        ),
        new Result<>(
            DateTimes.of("2013-01-04T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("val", 91L)
                            .build()
            )
        ),
        new Result<>(
            DateTimes.of("2013-01-05T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("val", 47L)
                            .build()
            )
        )
    );
    try {
      TimeseriesQueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
          new TimeseriesQueryQueryToolChest(),
          new TimeseriesQueryEngine(),
          QueryRunnerTestHelper.NOOP_QUERYWATCHER
      );

      QueryRunner runner = new FinalizeResultsQueryRunner(
          factory.createRunner(segment),
          factory.getToolchest()
      );

      TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
