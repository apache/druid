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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.collections.spatial.search.RadiusBound;
import org.apache.druid.collections.spatial.search.RectangularBound;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.SpatialDimensionSchema;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.filter.SpatialDimFilter;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 */
@RunWith(Parameterized.class)
public class SpatialFilterBonusTest
{
  public static final int NUM_POINTS = 5000;
  private static Interval DATA_INTERVAL = Intervals.of("2013-01-01/2013-01-07");
  private static AggregatorFactory[] METRIC_AGGS = new AggregatorFactory[]{
      new CountAggregatorFactory("rows"),
      new LongSumAggregatorFactory("val", "val")
  };
  private static List<String> DIMS = Lists.newArrayList("dim", "dim.geo");

  private final Segment segment;

  public SpatialFilterBonusTest(Segment segment)
  {
    this.segment = segment;
  }

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    List<Object[]> argumentArrays = new ArrayList<>();
    for (SegmentWriteOutMediumFactory segmentWriteOutMediumFactory : SegmentWriteOutMediumFactory.builtInFactories()) {
      IndexMerger indexMerger = TestHelper.getTestIndexMergerV9(segmentWriteOutMediumFactory);
      IndexIO indexIO = TestHelper.getTestIndexIO();
      final IndexSpec indexSpec = new IndexSpec();
      final IncrementalIndex rtIndex = makeIncrementalIndex();
      final QueryableIndex mMappedTestIndex = makeQueryableIndex(indexSpec, indexMerger, indexIO);
      final QueryableIndex mergedRealtimeIndex = makeMergedQueryableIndex(indexSpec, indexMerger, indexIO);
      argumentArrays.add(new Object[] {new IncrementalIndexSegment(rtIndex, null)});
      argumentArrays.add(new Object[] {new QueryableIndexSegment(mMappedTestIndex, null)});
      argumentArrays.add(new Object[] {new QueryableIndexSegment(mergedRealtimeIndex, null)});
    }
    return argumentArrays;
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
                        Collections.singletonList(
                            new SpatialDimensionSchema(
                                "dim.geo",
                                new ArrayList<>()
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
            DateTimes.of("2013-01-01").getMillis(),
            DIMS,
            ImmutableMap.of(
                "timestamp", DateTimes.of("2013-01-01").toString(),
                "dim", "foo",
                "dim.geo", "0.0,0.0",
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
                "dim.geo", "1.0,3.0",
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
                "dim.geo", "4.0,2.0",
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
                "dim.geo", "7.0,3.0",
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
                "dim.geo", "8.0,6.0",
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
                "dim.geo", "_mmx.unknown",
                "val", 501L
            )
        )
    );

    // Add a bunch of random points, without replacement
    Set<String> alreadyChosen = new HashSet<>();
    Random rand = ThreadLocalRandom.current();
    for (int i = 6; i < NUM_POINTS; i++) {
      String coord = null;
      while (coord == null) {
        coord = StringUtils.format(
            "%s,%s",
            (float) (rand.nextFloat() * 10 + 10.0),
            (float) (rand.nextFloat() * 10 + 10.0)
        );
        if (!alreadyChosen.add(coord)) {
          coord = null;
        }
      }
      theIndex.add(
          new MapBasedInputRow(
              DateTimes.of("2013-01-01").getMillis(),
              DIMS,
              ImmutableMap.of(
                  "timestamp", DateTimes.of("2013-01-01").toString(),
                  "dim", "boo",
                  "dim.geo", coord,
                  "val", i
              )
          )
      );
    }

    return theIndex;
  }

  private static QueryableIndex makeQueryableIndex(IndexSpec indexSpec, IndexMerger indexMerger, IndexIO indexIO)
      throws IOException
  {
    IncrementalIndex theIndex = makeIncrementalIndex();
    File tmpFile = File.createTempFile("billy", "yay");
    tmpFile.delete();
    tmpFile.mkdirs();
    tmpFile.deleteOnExit();

    indexMerger.persist(theIndex, tmpFile, indexSpec, null);
    return indexIO.loadIndex(tmpFile);
  }

  private static QueryableIndex makeMergedQueryableIndex(
      final IndexSpec indexSpec,
      final IndexMerger indexMerger,
      final IndexIO indexIO
  )
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
                          Collections.singletonList(
                              new SpatialDimensionSchema(
                                  "dim.geo",
                                  new ArrayList<>()
                              )
                          )
                      )

                  ).build()
          )
          .setReportParseExceptions(false)
          .setMaxRowCount(NUM_POINTS)
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
                          Collections.singletonList(
                              new SpatialDimensionSchema(
                                  "dim.geo",
                                  new ArrayList<>()
                              )
                          )
                      )
                  ).build()
          )
          .setReportParseExceptions(false)
          .setMaxRowCount(NUM_POINTS)
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
                          Collections.singletonList(
                              new SpatialDimensionSchema(
                                  "dim.geo",
                                  new ArrayList<>()
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
              DateTimes.of("2013-01-01").getMillis(),
              DIMS,
              ImmutableMap.of(
                  "timestamp", DateTimes.of("2013-01-01").toString(),
                  "dim", "foo",
                  "dim.geo", "0.0,0.0",
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
                  "dim.geo", "1.0,3.0",
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
                  "dim.geo", "4.0,2.0",
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
                  "dim.geo", "7.0,3.0",
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
                  "dim.geo", "8.0,6.0",
                  "val", 47L
              )
          )
      );

      // Add a bunch of random points
      Random rand = ThreadLocalRandom.current();
      for (int i = 6; i < NUM_POINTS; i++) {
        third.add(
            new MapBasedInputRow(
                DateTimes.of("2013-01-01").getMillis(),
                DIMS,
                ImmutableMap.of(
                    "timestamp", DateTimes.of("2013-01-01").toString(),
                    "dim", "boo",
                    "dim.geo", StringUtils.format(
                        "%s,%s",
                        (float) (rand.nextFloat() * 10 + 10.0),
                        (float) (rand.nextFloat() * 10 + 10.0)
                    ),
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

      indexMerger.persist(first, DATA_INTERVAL, firstFile, indexSpec, null);
      indexMerger.persist(second, DATA_INTERVAL, secondFile, indexSpec, null);
      indexMerger.persist(third, DATA_INTERVAL, thirdFile, indexSpec, null);

      QueryableIndex mergedRealtime = indexIO.loadIndex(
          indexMerger.mergeQueryableIndex(
              Arrays.asList(
                  indexIO.loadIndex(firstFile),
                  indexIO.loadIndex(secondFile),
                  indexIO.loadIndex(thirdFile)
              ),
              true,
              METRIC_AGGS,
              mergedFile,
              indexSpec,
              null
          )
      );

      return mergedRealtime;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
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
        new Result<TimeseriesResultValue>(
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
        new Result<TimeseriesResultValue>(
            DateTimes.of("2013-01-01T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("rows", 1L)
                    .put("val", 17L)
                    .build()
            )
        ),
        new Result<TimeseriesResultValue>(
            DateTimes.of("2013-01-02T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("rows", 1L)
                    .put("val", 29L)
                    .build()
            )
        ),
        new Result<TimeseriesResultValue>(
            DateTimes.of("2013-01-03T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("rows", 1L)
                    .put("val", 13L)
                    .build()
            )
        ),
        new Result<TimeseriesResultValue>(
            DateTimes.of("2013-01-04T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("rows", 1L)
                    .put("val", 91L)
                    .build()
            )
        ),
        new Result<TimeseriesResultValue>(
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

  @Test
  public void testSpatialQueryFilteredAggregator()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test")
                                  .granularity(Granularities.DAY)
                                  .intervals(Collections.singletonList(Intervals.of("2013-01-01/2013-01-07")))
                                  .aggregators(
                                      Arrays.asList(
                                          new CountAggregatorFactory("rows"),
                                          new FilteredAggregatorFactory(
                                              new LongSumAggregatorFactory("valFiltered", "val"),
                                              new SpatialDimFilter(
                                                  "dim.geo",
                                                  new RectangularBound(new float[]{0.0f, 0.0f}, new float[]{9.0f, 9.0f})
                                              )
                                          ),
                                          new LongSumAggregatorFactory("val", "val")
                                      )
                                  )
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2013-01-01T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("rows", 4995L)
                    .put("val", 12497502L)
                    .put("valFiltered", 17L)
                    .build()
            )
        ),
        new Result<>(
            DateTimes.of("2013-01-02T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("rows", 1L)
                    .put("val", 29L)
                    .put("valFiltered", 29L)
                    .build()
            )
        ),
        new Result<>(
            DateTimes.of("2013-01-03T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("rows", 1L)
                    .put("val", 13L)
                    .put("valFiltered", 13L)
                    .build()
            )
        ),
        new Result<>(
            DateTimes.of("2013-01-04T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("rows", 1L)
                    .put("val", 91L)
                    .put("valFiltered", 91L)
                    .build()
            )
        ),
        new Result<>(
            DateTimes.of("2013-01-05T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("rows", 2L)
                    .put("val", 548L)
                    .put("valFiltered", 47L)
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
