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

package org.apache.druid.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.segment.AbstractSegment;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.server.SegmentManager.DataSourceState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class SegmentManagerTest
{
  private static final SegmentLoader segmentLoader = new SegmentLoader()
  {
    @Override
    public boolean isSegmentLoaded(DataSegment segment)
    {
      return false;
    }

    @Override
    public Segment getSegment(final DataSegment segment)
    {
      return new SegmentForTesting(
          MapUtils.getString(segment.getLoadSpec(), "version"),
          (Interval) segment.getLoadSpec().get("interval")
      );
    }

    @Override
    public File getSegmentFiles(DataSegment segment)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void cleanup(DataSegment segment)
    {

    }
  };

  private static class SegmentForTesting extends AbstractSegment
  {
    private final String version;
    private final Interval interval;

    SegmentForTesting(String version, Interval interval)
    {
      this.version = version;
      this.interval = interval;
    }

    public String getVersion()
    {
      return version;
    }

    public Interval getInterval()
    {
      return interval;
    }

    @Override
    public SegmentId getId()
    {
      return SegmentId.dummy(version);
    }

    @Override
    public Interval getDataInterval()
    {
      return interval;
    }

    @Override
    public QueryableIndex asQueryableIndex()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public StorageAdapter asStorageAdapter()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
    }
  }

  private static final List<DataSegment> segments = ImmutableList.of(
      new DataSegment(
          "small_source",
          Intervals.of("0/1000"),
          "0",
          ImmutableMap.of("interval", Intervals.of("0/1000"), "version", 0),
          new ArrayList<>(),
          new ArrayList<>(),
          NoneShardSpec.instance(),
          0,
          10
      ),
      new DataSegment(
          "small_source",
          Intervals.of("1000/2000"),
          "0",
          ImmutableMap.of("interval", Intervals.of("1000/2000"), "version", 0),
          new ArrayList<>(),
          new ArrayList<>(),
          NoneShardSpec.instance(),
          0,
          10
      ),
      new DataSegment(
          "large_source",
          Intervals.of("0/1000"),
          "0",
          ImmutableMap.of("interval", Intervals.of("0/1000"), "version", 0),
          new ArrayList<>(),
          new ArrayList<>(),
          NoneShardSpec.instance(),
          0,
          100
      ),
      new DataSegment(
          "large_source",
          Intervals.of("1000/2000"),
          "0",
          ImmutableMap.of("interval", Intervals.of("1000/2000"), "version", 0),
          new ArrayList<>(),
          new ArrayList<>(),
          NoneShardSpec.instance(),
          0,
          100
      ),
      // overshadowing the ahead segment
      new DataSegment(
          "large_source",
          Intervals.of("1000/2000"),
          "1",
          ImmutableMap.of("interval", Intervals.of("1000/2000"), "version", 1),
          new ArrayList<>(),
          new ArrayList<>(),
          NoneShardSpec.instance(),
          1,
          100
      )
  );

  private ExecutorService executor;
  private SegmentManager segmentManager;

  @Before
  public void setup()
  {
    segmentManager = new SegmentManager(segmentLoader);
    executor = Executors.newFixedThreadPool(segments.size());
  }

  @After
  public void tearDown()
  {
    executor.shutdownNow();
  }

  @Test
  public void testLoadSegment() throws ExecutionException, InterruptedException, SegmentLoadingException
  {
    final List<Future<Boolean>> futures = segments.stream()
                                                  .map(
                                                      segment -> executor.submit(
                                                          () -> segmentManager.loadSegment(segment)
                                                      )
                                                  )
                                                  .collect(Collectors.toList());

    for (Future<Boolean> eachFuture : futures) {
      Assert.assertTrue(eachFuture.get());
    }

    assertResult(segments);
  }

  @Test
  public void testDropSegment() throws SegmentLoadingException, ExecutionException, InterruptedException
  {
    for (DataSegment eachSegment : segments) {
      Assert.assertTrue(segmentManager.loadSegment(eachSegment));
    }

    final List<Future<Void>> futures = ImmutableList.of(segments.get(0), segments.get(2)).stream()
                                                    .map(
                                                        segment -> executor.submit(
                                                            () -> {
                                                              segmentManager.dropSegment(segment);
                                                              return (Void) null;
                                                            }
                                                        )
                                                    )
                                                    .collect(Collectors.toList());

    for (Future<Void> eachFuture : futures) {
      eachFuture.get();
    }

    assertResult(
        ImmutableList.of(segments.get(1), segments.get(3), segments.get(4))
    );
  }

  @Test
  public void testLoadDropSegment() throws SegmentLoadingException, ExecutionException, InterruptedException
  {
    Assert.assertTrue(segmentManager.loadSegment(segments.get(0)));
    Assert.assertTrue(segmentManager.loadSegment(segments.get(2)));

    final List<Future<Boolean>> loadFutures = ImmutableList.of(segments.get(1), segments.get(3), segments.get(4))
                                                           .stream()
                                                           .map(
                                                               segment -> executor.submit(
                                                                   () -> segmentManager.loadSegment(segment)
                                                               )
                                                           )
                                                           .collect(Collectors.toList());
    final List<Future<Void>> dropFutures = ImmutableList.of(segments.get(0), segments.get(2)).stream()
                                                        .map(
                                                            segment -> executor.submit(
                                                                () -> {
                                                                  segmentManager.dropSegment(segment);
                                                                  return (Void) null;
                                                                }
                                                            )
                                                        )
                                                        .collect(Collectors.toList());

    for (Future<Boolean> eachFuture : loadFutures) {
      Assert.assertTrue(eachFuture.get());
    }
    for (Future<Void> eachFuture : dropFutures) {
      eachFuture.get();
    }

    assertResult(
        ImmutableList.of(segments.get(1), segments.get(3), segments.get(4))
    );
  }

  @Test
  public void testLoadDuplicatedSegmentsSequentially() throws SegmentLoadingException
  {
    for (DataSegment segment : segments) {
      Assert.assertTrue(segmentManager.loadSegment(segment));
    }
    // try to load an existing segment
    Assert.assertFalse(segmentManager.loadSegment(segments.get(0)));

    assertResult(segments);
  }

  @Test
  public void testLoadDuplicatedSegmentsInParallel()
      throws ExecutionException, InterruptedException, SegmentLoadingException
  {
    final List<Future<Boolean>> futures = ImmutableList.of(segments.get(0), segments.get(0), segments.get(0)).stream()
                                                       .map(
                                                           segment -> executor.submit(
                                                               () -> segmentManager.loadSegment(segment)
                                                           )
                                                       )
                                                       .collect(Collectors.toList());

    int numSucceededFutures = 0;
    int numFailedFutures = 0;
    for (Future<Boolean> future : futures) {
      numSucceededFutures += future.get() ? 1 : 0;
      numFailedFutures += future.get() ? 0 : 1;
    }

    Assert.assertEquals(1, numSucceededFutures);
    Assert.assertEquals(2, numFailedFutures);

    assertResult(ImmutableList.of(segments.get(0)));
  }

  @Test
  public void testNonExistingSegmentsSequentially() throws SegmentLoadingException
  {
    Assert.assertTrue(segmentManager.loadSegment(segments.get(0)));

    // try to drop a non-existing segment of different data source
    segmentManager.dropSegment(segments.get(2));
    assertResult(
        ImmutableList.of(segments.get(0))
    );
  }

  @Test
  public void testNonExistingSegmentsInParallel()
      throws SegmentLoadingException, ExecutionException, InterruptedException
  {
    segmentManager.loadSegment(segments.get(0));
    final List<Future<Void>> futures = ImmutableList.of(segments.get(1), segments.get(2)).stream()
                                                       .map(
                                                           segment -> executor.submit(
                                                               () -> {
                                                                 segmentManager.dropSegment(segment);
                                                                 return (Void) null;
                                                               }
                                                           )
                                                       )
                                                       .collect(Collectors.toList());

    for (Future<Void> future : futures) {
      future.get();
    }

    assertResult(ImmutableList.of(segments.get(0)));
  }

  @Test
  public void testRemoveEmptyTimeline() throws SegmentLoadingException
  {
    segmentManager.loadSegment(segments.get(0));
    assertResult(ImmutableList.of(segments.get(0)));
    Assert.assertEquals(1, segmentManager.getDataSources().size());
    segmentManager.dropSegment(segments.get(0));
    Assert.assertEquals(0, segmentManager.getDataSources().size());
  }

  @Test
  public void testGetNonExistingTimeline()
  {
    Assert.assertNull(segmentManager.getTimeline("nonExisting"));
  }

  @SuppressWarnings("RedundantThrows") // TODO remove when the bug in intelliJ is fixed.
  private void assertResult(List<DataSegment> expectedExistingSegments) throws SegmentLoadingException
  {
    final Map<String, Long> expectedDataSourceSizes = expectedExistingSegments
        .stream()
        .collect(Collectors.toMap(DataSegment::getDataSource, DataSegment::getSize, Long::sum));
    final Map<String, Long> expectedDataSourceCounts = expectedExistingSegments
        .stream()
        .collect(Collectors.toMap(DataSegment::getDataSource, segment -> 1L, Long::sum));
    final Map<String, VersionedIntervalTimeline<String, ReferenceCountingSegment>> expectedDataSources
        = new HashMap<>();
    for (DataSegment segment : expectedExistingSegments) {
      final VersionedIntervalTimeline<String, ReferenceCountingSegment> expectedTimeline =
          expectedDataSources.computeIfAbsent(
              segment.getDataSource(),
              k -> new VersionedIntervalTimeline<>(Ordering.natural())
          );
      expectedTimeline.add(
          segment.getInterval(),
          segment.getVersion(),
          segment.getShardSpec().createChunk(new ReferenceCountingSegment(segmentLoader.getSegment(segment)))
      );
    }

    Assert.assertEquals(expectedDataSourceCounts, segmentManager.getDataSourceCounts());
    Assert.assertEquals(expectedDataSourceSizes, segmentManager.getDataSourceSizes());

    final Map<String, DataSourceState> dataSources = segmentManager.getDataSources();
    Assert.assertEquals(expectedDataSources.size(), dataSources.size());

    dataSources.forEach(
        (sourceName, dataSourceState) -> {
          Assert.assertEquals(expectedDataSourceCounts.get(sourceName).longValue(), dataSourceState.getNumSegments());
          Assert.assertEquals(expectedDataSourceSizes.get(sourceName).longValue(), dataSourceState.getTotalSegmentSize());
          Assert.assertEquals(
              expectedDataSources.get(sourceName).getAllTimelineEntries(),
              dataSourceState.getTimeline().getAllTimelineEntries()
          );
        }
    );
  }
}
