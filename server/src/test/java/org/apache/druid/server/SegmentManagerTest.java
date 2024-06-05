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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.loading.LeastBytesUsedStorageLocationSelectorStrategy;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.SegmentLocalCacheManager;
import org.apache.druid.segment.loading.StorageLocation;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.server.SegmentManager.DataSourceState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.NumberedOverwriteShardSpec;
import org.apache.druid.timeline.partition.PartitionIds;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class SegmentManagerTest
{
  private static final List<DataSegment> SEGMENTS = ImmutableList.of(
      TestSegmentUtils.makeSegment("small_source", "0", Intervals.of("0/1000")),
      TestSegmentUtils.makeSegment("small_source", "0", Intervals.of("1000/2000")),
      TestSegmentUtils.makeSegment("large_source", "0", Intervals.of("0/1000")),
      TestSegmentUtils.makeSegment("large_source", "0", Intervals.of("1000/2000")),
      TestSegmentUtils.makeSegment("large_source", "1", Intervals.of("1000/2000"))
  );

  private ExecutorService executor;
  private SegmentManager segmentManager;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setup() throws IOException
  {
    final File segmentCacheDir = temporaryFolder.newFolder();
    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public File getInfoDir()
      {
        return segmentCacheDir;
      }

      @Override
      public List<StorageLocationConfig> getLocations()
      {
        return Collections.singletonList(
            new StorageLocationConfig(segmentCacheDir, null, null)
        );
      }
    };

    final ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    objectMapper.registerSubtypes(TestSegmentUtils.TestLoadSpec.class);
    objectMapper.registerSubtypes(TestSegmentUtils.TestSegmentizerFactory.class);

    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
    final SegmentLocalCacheManager cacheManager = new SegmentLocalCacheManager(
        storageLocations,
        loaderConfig,
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestIndex.INDEX_IO,
        objectMapper
    );

    segmentManager = new SegmentManager(cacheManager);
    executor = Execs.multiThreaded(SEGMENTS.size(), "SegmentManagerTest-%d");
  }

  @After
  public void tearDown()
  {
    executor.shutdownNow();
  }

  @Test
  public void testLoadSegment() throws ExecutionException, InterruptedException
  {
    final List<Future<Void>> loadFutures = SEGMENTS.stream()
                                                  .map(
                                                      segment -> executor.submit(
                                                          () -> loadSegmentOrFail(segment)
                                                      )
                                                  )
                                                  .collect(Collectors.toList());

    for (Future<Void> loadFuture : loadFutures) {
      loadFuture.get();
    }

    assertResult(SEGMENTS);
  }

  @Test
  public void testLoadBootstrapSegment() throws ExecutionException, InterruptedException
  {
    final List<Future<Void>> loadFutures = SEGMENTS.stream()
                                                   .map(
                                                       segment -> executor.submit(
                                                           () -> {
                                                             try {
                                                               segmentManager.loadSegmentOnBootstrap(segment, SegmentLazyLoadFailCallback.NOOP);
                                                             }
                                                             catch (IOException | SegmentLoadingException e) {
                                                               throw new RuntimeException(e);
                                                             }
                                                             return (Void) null;
                                                           }
                                                       )
                                                   )
                                                   .collect(Collectors.toList());

    for (Future<Void> loadFuture : loadFutures) {
      loadFuture.get();
    }

    assertResult(SEGMENTS);
  }

  @Test
  public void testDropSegment() throws SegmentLoadingException, ExecutionException, InterruptedException, IOException
  {
    for (DataSegment eachSegment : SEGMENTS) {
      segmentManager.loadSegment(eachSegment);
    }

    final List<Future<Void>> futures = ImmutableList.of(SEGMENTS.get(0), SEGMENTS.get(2)).stream()
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
        ImmutableList.of(SEGMENTS.get(1), SEGMENTS.get(3), SEGMENTS.get(4))
    );
  }

  private Void loadSegmentOrFail(DataSegment segment)
  {
    try {
      segmentManager.loadSegment(segment);
    }
    catch (IOException | SegmentLoadingException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  @Test
  public void testLoadDropSegment()
      throws SegmentLoadingException, ExecutionException, InterruptedException, IOException
  {
    segmentManager.loadSegment(SEGMENTS.get(0));
    segmentManager.loadSegment(SEGMENTS.get(2));

    final List<Future<Void>> loadFutures = ImmutableList.of(SEGMENTS.get(1), SEGMENTS.get(3), SEGMENTS.get(4))
                                                     .stream()
                                                     .map(
                                                         segment -> executor.submit(() -> loadSegmentOrFail(segment))
                                                     )
                                                     .collect(Collectors.toList());
    final List<Future<Void>> dropFutures = ImmutableList.of(SEGMENTS.get(0), SEGMENTS.get(2)).stream()
                                                        .map(
                                                            segment -> executor.submit(
                                                                () -> {
                                                                  segmentManager.dropSegment(segment);
                                                                  return (Void) null;
                                                                }
                                                            )
                                                        )
                                                        .collect(Collectors.toList());

    for (Future<Void> loadFuture : loadFutures) {
      loadFuture.get();
    }
    for (Future<Void> dropFuture : dropFutures) {
      dropFuture.get();
    }

    assertResult(
        ImmutableList.of(SEGMENTS.get(1), SEGMENTS.get(3), SEGMENTS.get(4))
    );
  }

  @Test
  public void testLoadDuplicatedSegmentsSequentially() throws SegmentLoadingException, IOException
  {
    for (DataSegment segment : SEGMENTS) {
      segmentManager.loadSegment(segment);
    }
    // try to load an existing segment
    segmentManager.loadSegment(SEGMENTS.get(0));

    assertResult(SEGMENTS);
  }

  @Test
  public void testLoadDuplicatedSegmentsInParallel()
      throws ExecutionException, InterruptedException
  {
    final List<Future<Void>> loadFutures = ImmutableList.of(SEGMENTS.get(0), SEGMENTS.get(0), SEGMENTS.get(0))
                                                       .stream()
                                                       .map(
                                                           segment -> executor.submit(
                                                               () -> loadSegmentOrFail(segment)
                                                           )
                                                       )
                                                       .collect(Collectors.toList());

    for (Future<Void> loadFuture : loadFutures) {
      loadFuture.get();
    }

    assertResult(ImmutableList.of(SEGMENTS.get(0)));
  }

  @Test
  public void testNonExistingSegmentsSequentially() throws SegmentLoadingException, IOException
  {
    segmentManager.loadSegment(SEGMENTS.get(0));

    // try to drop a non-existing segment of different data source
    segmentManager.dropSegment(SEGMENTS.get(2));
    assertResult(ImmutableList.of(SEGMENTS.get(0)));
  }

  @Test
  public void testNonExistingSegmentsInParallel()
      throws SegmentLoadingException, ExecutionException, InterruptedException, IOException
  {
    segmentManager.loadSegment(SEGMENTS.get(0));
    final List<Future<Void>> futures = ImmutableList.of(SEGMENTS.get(1), SEGMENTS.get(2))
                                                    .stream()
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

    assertResult(ImmutableList.of(SEGMENTS.get(0)));
  }

  @Test
  public void testRemoveEmptyTimeline() throws SegmentLoadingException, IOException
  {
    segmentManager.loadSegment(SEGMENTS.get(0));
    assertResult(ImmutableList.of(SEGMENTS.get(0)));
    Assert.assertEquals(1, segmentManager.getDataSources().size());
    segmentManager.dropSegment(SEGMENTS.get(0));
    Assert.assertEquals(0, segmentManager.getDataSources().size());
  }

  @Test
  public void testGetNonExistingTimeline()
  {
    Assert.assertEquals(
        Optional.empty(),
        segmentManager.getTimeline((new TableDataSource("nonExisting")).getAnalysis())
    );
  }

  @Test
  public void testLoadAndDropNonRootGenerationSegment() throws SegmentLoadingException, IOException
  {
    final DataSegment segment = new DataSegment(
        "small_source",
        Intervals.of("0/1000"),
        "0",
        ImmutableMap.of("type", "test", "interval", Intervals.of("0/1000"), "version", 0),
        new ArrayList<>(),
        new ArrayList<>(),
        new NumberedOverwriteShardSpec(
            PartitionIds.NON_ROOT_GEN_START_PARTITION_ID + 10,
            10,
            20,
            (short) 1,
            (short) 1
        ),
        0,
        10
    );

    segmentManager.loadSegment(segment);
    assertResult(ImmutableList.of(segment));

    segmentManager.dropSegment(segment);
    assertResult(ImmutableList.of());
  }

  private void assertResult(List<DataSegment> expectedExistingSegments)
  {
    final Map<String, Long> expectedDataSourceSizes =
        expectedExistingSegments.stream()
                                .collect(Collectors.toMap(DataSegment::getDataSource, DataSegment::getSize, Long::sum));
    final Map<String, Long> expectedDataSourceCounts =
        expectedExistingSegments.stream()
                                .collect(Collectors.toMap(DataSegment::getDataSource, segment -> 1L, Long::sum));
    final Set<String> expectedDataSourceNames = expectedExistingSegments.stream()
                                                                        .map(DataSegment::getDataSource)
                                                                        .collect(Collectors.toSet());
    final Map<String, VersionedIntervalTimeline<String, ReferenceCountingSegment>> expectedTimelines = new HashMap<>();
    for (DataSegment segment : expectedExistingSegments) {
      final VersionedIntervalTimeline<String, ReferenceCountingSegment> expectedTimeline =
          expectedTimelines.computeIfAbsent(
              segment.getDataSource(),
              k -> new VersionedIntervalTimeline<>(Ordering.natural())
          );
      expectedTimeline.add(
          segment.getInterval(),
          segment.getVersion(),
          segment.getShardSpec().createChunk(
              ReferenceCountingSegment.wrapSegment(
                  ReferenceCountingSegment.wrapSegment(
                      new TestSegmentUtils.SegmentForTesting(
                          segment.getDataSource(),
                          (Interval) segment.getLoadSpec().get("interval"),
                          MapUtils.getString(segment.getLoadSpec(), "version")
                  ), segment.getShardSpec()),
                  segment.getShardSpec()
              )
          )
      );
    }

    Assert.assertEquals(expectedDataSourceNames, segmentManager.getDataSourceNames());
    Assert.assertEquals(expectedDataSourceCounts, segmentManager.getDataSourceCounts());
    Assert.assertEquals(expectedDataSourceSizes, segmentManager.getDataSourceSizes());

    final Map<String, DataSourceState> dataSources = segmentManager.getDataSources();
    Assert.assertEquals(expectedTimelines.size(), dataSources.size());

    dataSources.forEach(
        (sourceName, dataSourceState) -> {
          Assert.assertEquals(expectedDataSourceCounts.get(sourceName).longValue(), dataSourceState.getNumSegments());
          Assert.assertEquals(
              expectedDataSourceSizes.get(sourceName).longValue(),
              dataSourceState.getTotalSegmentSize()
          );
          Assert.assertEquals(
              expectedTimelines.get(sourceName).getAllTimelineEntries(),
              dataSourceState.getTimeline().getAllTimelineEntries()
          );
        }
    );
  }
}
