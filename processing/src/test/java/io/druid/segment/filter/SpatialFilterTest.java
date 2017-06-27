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

package io.druid.segment.filter;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.collections.spatial.search.RadiusBound;
import io.druid.collections.spatial.search.RectangularBound;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.Druids;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.filter.SpatialDimFilter;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

/**
 */
@RunWith(Parameterized.class)
public class SpatialFilterTest
{
  private static IndexMerger INDEX_MERGER = TestHelper.getTestIndexMergerV9();
  private static IndexIO INDEX_IO = TestHelper.getTestIndexIO();

  public static final int NUM_POINTS = 5000;
  private static Interval DATA_INTERVAL = new Interval("2013-01-01/2013-01-07");

  private static AggregatorFactory[] METRIC_AGGS = new AggregatorFactory[]{
      new CountAggregatorFactory("rows"),
      new LongSumAggregatorFactory("val", "val")
  };

  private static List<String> DIMS = Lists.newArrayList("dim", "lat", "long", "lat2", "long2");

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    final IndexSpec indexSpec = new IndexSpec();
    final IncrementalIndex rtIndex = makeIncrementalIndex();
    final QueryableIndex mMappedTestIndex = makeQueryableIndex(indexSpec);
    final QueryableIndex mergedRealtimeIndex = makeMergedQueryableIndex(indexSpec);
    return Arrays.asList(
        new Object[][]{
            {
                new IncrementalIndexSegment(rtIndex, null)
            },
            {
                new QueryableIndexSegment(null, mMappedTestIndex)
            },
            {
                new QueryableIndexSegment(null, mergedRealtimeIndex)
            }
        }
    );
  }

  private static IncrementalIndex makeIncrementalIndex() throws IOException
  {
    IncrementalIndex theIndex = new IncrementalIndex.Builder()
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
        .setReportParseExceptions(false)
        .setMaxRowCount(NUM_POINTS)
        .buildOnheap();

    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-01").getMillis(),
            DIMS,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-01").toString(),
                "dim", "foo",
                "lat", 0.0f,
                "long", 0.0f,
                "val", 17L
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-02").getMillis(),
            DIMS,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-02").toString(),
                "dim", "foo",
                "lat", 1.0f,
                "long", 3.0f,
                "val", 29L
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-03").getMillis(),
            DIMS,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-03").toString(),
                "dim", "foo",
                "lat", 4.0f,
                "long", 2.0f,
                "val", 13L
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-04").getMillis(),
            DIMS,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-04").toString(),
                "dim", "foo",
                "lat", 7.0f,
                "long", 3.0f,
                "val", 91L
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-05").getMillis(),
            DIMS,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-05").toString(),
                "dim", "foo",
                "lat", 8.0f,
                "long", 6.0f,
                "val", 47L
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-05").getMillis(),
            DIMS,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-05").toString(),
                "dim", "foo",
                "lat", "_mmx.unknown",
                "long", "_mmx.unknown",
                "val", 101L
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-05").getMillis(),
            DIMS,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-05").toString(),
                "dim", "foo",
                "dim.geo", "_mmx.unknown",
                "val", 501L
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-05").getMillis(),
            DIMS,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-05").toString(),
                "lat2", 0.0f,
                "long2", 0.0f,
                "val", 13L
            )
        )
    );

    // Add a bunch of random points
    Random rand = new Random();
    for (int i = 8; i < NUM_POINTS; i++) {
      theIndex.add(
          new MapBasedInputRow(
              new DateTime("2013-01-01").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-01").toString(),
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

  private static QueryableIndex makeQueryableIndex(IndexSpec indexSpec) throws IOException
  {
    IncrementalIndex theIndex = makeIncrementalIndex();
    File tmpFile = File.createTempFile("billy", "yay");
    tmpFile.delete();
    tmpFile.mkdirs();
    tmpFile.deleteOnExit();

    INDEX_MERGER.persist(theIndex, tmpFile, indexSpec);
    return INDEX_IO.loadIndex(tmpFile);
  }

  private static QueryableIndex makeMergedQueryableIndex(IndexSpec indexSpec)
  {
    try {
      IncrementalIndex first = new IncrementalIndex.Builder()
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
          .setReportParseExceptions(false)
          .setMaxRowCount(1000)
          .buildOnheap();

      IncrementalIndex second = new IncrementalIndex.Builder()
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
          .setReportParseExceptions(false)
          .setMaxRowCount(1000)
          .buildOnheap();

      IncrementalIndex third = new IncrementalIndex.Builder()
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
          .setReportParseExceptions(false)
          .setMaxRowCount(NUM_POINTS)
          .buildOnheap();

      first.add(
          new MapBasedInputRow(
              new DateTime("2013-01-01").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-01").toString(),
                  "dim", "foo",
                  "lat", 0.0f,
                  "long", 0.0f,
                  "val", 17L
              )
          )
      );
      first.add(
          new MapBasedInputRow(
              new DateTime("2013-01-02").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-02").toString(),
                  "dim", "foo",
                  "lat", 1.0f,
                  "long", 3.0f,
                  "val", 29L
              )
          )
      );
      first.add(
          new MapBasedInputRow(
              new DateTime("2013-01-03").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-03").toString(),
                  "dim", "foo",
                  "lat", 4.0f,
                  "long", 2.0f,
                  "val", 13L
              )
          )
      );
      first.add(
          new MapBasedInputRow(
              new DateTime("2013-01-05").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-05").toString(),
                  "dim", "foo",
                  "lat", "_mmx.unknown",
                  "long", "_mmx.unknown",
                  "val", 101L
              )
          )
      );
      first.add(
          new MapBasedInputRow(
              new DateTime("2013-01-05").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-05").toString(),
                  "dim", "foo",
                  "dim.geo", "_mmx.unknown",
                  "val", 501L
              )
          )
      );
      second.add(
          new MapBasedInputRow(
              new DateTime("2013-01-04").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-04").toString(),
                  "dim", "foo",
                  "lat", 7.0f,
                  "long", 3.0f,
                  "val", 91L
              )
          )
      );
      second.add(
          new MapBasedInputRow(
              new DateTime("2013-01-05").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-05").toString(),
                  "dim", "foo",
                  "lat", 8.0f,
                  "long", 6.0f,
                  "val", 47L
              )
          )
      );
      second.add(
          new MapBasedInputRow(
              new DateTime("2013-01-05").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-05").toString(),
                  "lat2", 0.0f,
                  "long2", 0.0f,
                  "val", 13L
              )
          )
      );

      // Add a bunch of random points
      Random rand = new Random();
      for (int i = 8; i < NUM_POINTS; i++) {
        third.add(
            new MapBasedInputRow(
                new DateTime("2013-01-01").getMillis(),
                DIMS,
                ImmutableMap.<String, Object>of(
                    "timestamp", new DateTime("2013-01-01").toString(),
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
      firstFile.deleteOnExit();
      secondFile.mkdirs();
      secondFile.deleteOnExit();
      thirdFile.mkdirs();
      thirdFile.deleteOnExit();
      mergedFile.mkdirs();
      mergedFile.deleteOnExit();

      INDEX_MERGER.persist(first, DATA_INTERVAL, firstFile, indexSpec);
      INDEX_MERGER.persist(second, DATA_INTERVAL, secondFile, indexSpec);
      INDEX_MERGER.persist(third, DATA_INTERVAL, thirdFile, indexSpec);

      QueryableIndex mergedRealtime = INDEX_IO.loadIndex(
          INDEX_MERGER.mergeQueryableIndex(
              Arrays.asList(INDEX_IO.loadIndex(firstFile), INDEX_IO.loadIndex(secondFile), INDEX_IO.loadIndex(thirdFile)),
              true,
              METRIC_AGGS,
              mergedFile,
              indexSpec
          )
      );

      return mergedRealtime;
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private final Segment segment;

  public SpatialFilterTest(Segment segment)
  {
    this.segment = segment;
  }

  @Test
  public void testSpatialQuery()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test")
                                  .granularity(Granularities.ALL)
                                  .intervals(Arrays.asList(new Interval("2013-01-01/2013-01-07")))
                                  .filters(
                                      new SpatialDimFilter(
                                          "dim.geo",
                                          new RadiusBound(new float[]{0.0f, 0.0f}, 5)
                                      )
                                  )
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          new CountAggregatorFactory("rows"),
                                          new LongSumAggregatorFactory("val", "val")
                                      )
                                  )
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2013-01-01T00:00:00.000Z"),
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
          new TimeseriesQueryQueryToolChest(
              QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()),
          new TimeseriesQueryEngine(),
          QueryRunnerTestHelper.NOOP_QUERYWATCHER
      );

      QueryRunner runner = new FinalizeResultsQueryRunner(
          factory.createRunner(segment),
          factory.getToolchest()
      );

      TestHelper.assertExpectedResults(expectedResults, runner.run(query, Maps.newHashMap()));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }


  @Test
  public void testSpatialQueryWithOtherSpatialDim()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test")
                                  .granularity(Granularities.ALL)
                                  .intervals(Arrays.asList(new Interval("2013-01-01/2013-01-07")))
                                  .filters(
                                      new SpatialDimFilter(
                                          "spatialIsRad",
                                          new RadiusBound(new float[]{0.0f, 0.0f}, 5)
                                      )
                                  )
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          new CountAggregatorFactory("rows"),
                                          new LongSumAggregatorFactory("val", "val")
                                      )
                                  )
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2013-01-01T00:00:00.000Z"),
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
          new TimeseriesQueryQueryToolChest(
              QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()),
          new TimeseriesQueryEngine(),
          QueryRunnerTestHelper.NOOP_QUERYWATCHER
      );

      QueryRunner runner = new FinalizeResultsQueryRunner(
          factory.createRunner(segment),
          factory.getToolchest()
      );

      TestHelper.assertExpectedResults(expectedResults, runner.run(query, Maps.newHashMap()));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Test
  public void testSpatialQueryMorePoints()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test")
                                  .granularity(Granularities.DAY)
                                  .intervals(Arrays.asList(new Interval("2013-01-01/2013-01-07")))
                                  .filters(
                                      new SpatialDimFilter(
                                          "dim.geo",
                                          new RectangularBound(new float[]{0.0f, 0.0f}, new float[]{9.0f, 9.0f})
                                      )
                                  )
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          new CountAggregatorFactory("rows"),
                                          new LongSumAggregatorFactory("val", "val")
                                      )
                                  )
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2013-01-01T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("val", 17L)
                            .build()
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2013-01-02T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("val", 29L)
                            .build()
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2013-01-03T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("val", 13L)
                            .build()
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2013-01-04T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("val", 91L)
                            .build()
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2013-01-05T00:00:00.000Z"),
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
          new TimeseriesQueryQueryToolChest(
              QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()),
          new TimeseriesQueryEngine(),
          QueryRunnerTestHelper.NOOP_QUERYWATCHER
      );

      QueryRunner runner = new FinalizeResultsQueryRunner(
          factory.createRunner(segment),
          factory.getToolchest()
      );

      TestHelper.assertExpectedResults(expectedResults, runner.run(query, Maps.newHashMap()));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
