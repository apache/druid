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

package org.apache.druid.server.coordination;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.guice.ServerTypeConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.loading.NoopSegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.segment.loading.TombstoneSegmentizerFactory;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.TestSegmentUtils;
import org.apache.druid.server.coordination.SegmentChangeStatus.State;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SegmentLoadDropHandlerTest
{
  private static final int COUNT = 50;

  private TestDataSegmentAnnouncer segmentAnnouncer;
  private TestDataServerAnnouncer serverAnnouncer;
  private List<Runnable> scheduledRunnable;
  private SegmentLoaderConfig segmentLoaderConfig;
  private ScheduledExecutorFactory scheduledExecutorFactory;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException
  {
    final File segmentCacheDir = temporaryFolder.newFolder();

    scheduledRunnable = new ArrayList<>();
    segmentAnnouncer = new TestDataSegmentAnnouncer();
    serverAnnouncer = new TestDataServerAnnouncer();
    segmentLoaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public File getInfoDir()
      {
        return segmentCacheDir;
      }

      @Override
      public int getNumLoadingThreads()
      {
        return 5;
      }

      @Override
      public int getAnnounceIntervalMillis()
      {
        return 50;
      }

      @Override
      public List<StorageLocationConfig> getLocations()
      {
        return Collections.singletonList(
            new StorageLocationConfig(segmentCacheDir, null, null)
        );
      }

      @Override
      public int getDropSegmentDelayMillis()
      {
        return 0;
      }
    };

    scheduledExecutorFactory = (corePoolSize, nameFormat) -> {
      // Override normal behavior by adding the runnable to a list so that you can make sure
      // all the shceduled runnables are executed by explicitly calling run() on each item in the list
      return new ScheduledThreadPoolExecutor(corePoolSize, Execs.makeThreadFactory(nameFormat))
      {
        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
        {
          scheduledRunnable.add(command);
          return null;
        }
      };
    };

    EmittingLogger.registerEmitter(new NoopServiceEmitter());
  }

  /**
   * Steps:
   * <ul>
   * <li> {@code removeSegment()} schedules a delete runnable to deletes segment files. </li>
   * <li> {@code addSegment()} succesfully loads the segment and announces it. </li>
   * <li> scheduled delete task executes and realizes it should not delete the segment files. </li>
   * </ul>
   */
  @Test
  public void testSegmentLoading1() throws Exception
  {
    final TestSegmentCacheManager cacheManager = new TestSegmentCacheManager();
    final SegmentManager segmentManager = new SegmentManager(cacheManager);
    final SegmentLoadDropHandler handler = initSegmentLoadDropHandler(segmentManager);

    handler.start();

    Assert.assertEquals(1, serverAnnouncer.getObservedCount());

    final DataSegment segment = makeSegment("test", "1", Intervals.of("P1d/2011-04-01"));

    handler.removeSegment(segment, DataSegmentChangeCallback.NOOP);

    Assert.assertFalse(segmentAnnouncer.getObservedSegments().contains(segment));

    handler.addSegment(segment, DataSegmentChangeCallback.NOOP);

    // Make sure the scheduled runnable that "deletes" segment files has been executed.
    // Because another addSegment() call is executed, which removes the segment from segmentsToDelete field in
    // ZkCoordinator, the scheduled runnable will not actually delete segment files.
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }
    Assert.assertEquals(ImmutableList.of(segment), cacheManager.observedSegments);
    Assert.assertEquals(ImmutableList.of(segment), cacheManager.observedSegmentsLoadedIntoPageCache);
    Assert.assertEquals(ImmutableList.of(), cacheManager.observedBootstrapSegments);
    Assert.assertEquals(ImmutableList.of(), cacheManager.observedBootstrapSegmentsLoadedIntoPageCache);

    Assert.assertEquals(ImmutableList.of(segment), segmentAnnouncer.getObservedSegments());
    Assert.assertFalse(
        "segment files shouldn't be deleted",
        cacheManager.observedSegmentsRemovedFromCache.contains(segment)
    );

    handler.stop();
    Assert.assertEquals(0, serverAnnouncer.getObservedCount());
  }

  /**
   * Steps:
   * <ul>
   * <li> {@code addSegment()} succesfully loads the segment and announces it. </li>
   * <li> {@code removeSegment()} unannounces the segment and schedules a delete runnable to delete segment files. </li>
   * <li> {@code addSegment()} calls {@code loadSegment()} and announces it again. </li>
   * <li> scheduled delete task executes and realizes it should not delete the segment files. </li>
   * </ul>
   */
  @Test
  public void testSegmentLoading2() throws Exception
  {
    final TestSegmentCacheManager cacheManager = new TestSegmentCacheManager();
    final SegmentManager segmentManager = new SegmentManager(cacheManager);
    final SegmentLoadDropHandler handler = initSegmentLoadDropHandler(segmentManager);

    handler.start();

    Assert.assertEquals(1, serverAnnouncer.getObservedCount());

    final DataSegment segment = makeSegment("test", "1", Intervals.of("P1d/2011-04-01"));

    handler.addSegment(segment, DataSegmentChangeCallback.NOOP);

    Assert.assertTrue(segmentAnnouncer.getObservedSegments().contains(segment));

    handler.removeSegment(segment, DataSegmentChangeCallback.NOOP);

    Assert.assertFalse(segmentAnnouncer.getObservedSegments().contains(segment));

    handler.addSegment(segment, DataSegmentChangeCallback.NOOP);

    // Make sure the scheduled runnable that "deletes" segment files has been executed.
    // Because another addSegment() call is executed, which removes the segment from segmentsToDelete field in
    // ZkCoordinator, the scheduled runnable will not actually delete segment files.
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }

    // The same segment reference will be fetched more than once in the above sequence, but the segment should
    // be loaded only once onto the page cache.
    Assert.assertEquals(ImmutableList.of(segment, segment), cacheManager.observedSegments);
    Assert.assertEquals(ImmutableList.of(segment), cacheManager.observedSegmentsLoadedIntoPageCache);
    Assert.assertEquals(ImmutableList.of(), cacheManager.observedBootstrapSegments);
    Assert.assertEquals(ImmutableList.of(), cacheManager.observedBootstrapSegmentsLoadedIntoPageCache);

    Assert.assertTrue(segmentAnnouncer.getObservedSegments().contains(segment));
    Assert.assertFalse(
        "segment files shouldn't be deleted",
        cacheManager.observedSegmentsRemovedFromCache.contains(segment)
    );

    handler.stop();
    Assert.assertEquals(0, serverAnnouncer.getObservedCount());
  }

  @Test
  public void testLoadCache() throws Exception
  {
    Set<DataSegment> segments = new HashSet<>();
    for (int i = 0; i < COUNT; ++i) {
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-02")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("P1d/2011-04-02")));
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-03")));
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-04")));
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-05")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("PT1h/2011-04-04T01")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("PT1h/2011-04-04T02")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("PT1h/2011-04-04T03")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("PT1h/2011-04-04T05")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("PT1h/2011-04-04T06")));
      segments.add(makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-02")));
    }

    final TestSegmentCacheManager cacheManager = new TestSegmentCacheManager(segments);
    final SegmentManager segmentManager = new SegmentManager(cacheManager);
    final SegmentLoadDropHandler handler = initSegmentLoadDropHandler(segmentManager);

    Assert.assertTrue(segmentManager.getDataSourceCounts().isEmpty());

    handler.start();

    Assert.assertEquals(1, serverAnnouncer.getObservedCount());
    Assert.assertFalse(segmentManager.getDataSourceCounts().isEmpty());

    for (int i = 0; i < COUNT; ++i) {
      Assert.assertEquals(11L, segmentManager.getDataSourceCounts().get("test" + i).longValue());
      Assert.assertEquals(2L, segmentManager.getDataSourceCounts().get("test_two" + i).longValue());
    }

    Assert.assertEquals(ImmutableList.copyOf(segments), segmentAnnouncer.getObservedSegments());

    final ImmutableList<DataSegment> expectedBootstrapSegments = ImmutableList.copyOf(segments);
    Assert.assertEquals(expectedBootstrapSegments, cacheManager.observedBootstrapSegments);
    Assert.assertEquals(expectedBootstrapSegments, cacheManager.observedBootstrapSegmentsLoadedIntoPageCache);
    Assert.assertEquals(ImmutableList.of(), cacheManager.observedSegments);
    Assert.assertEquals(ImmutableList.of(), cacheManager.observedSegmentsLoadedIntoPageCache);

    handler.stop();

    Assert.assertEquals(0, serverAnnouncer.getObservedCount());
    Assert.assertEquals(1, cacheManager.observedShutdownBootstrapCount.get());
  }

  @Test
  public void testStartStop() throws Exception
  {
    final Set<DataSegment> segments = new HashSet<>();
    for (int i = 0; i < COUNT; ++i) {
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-02")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("P1d/2011-04-02")));
      segments.add(makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-02")));
    }

    final TestSegmentCacheManager cacheManager = new TestSegmentCacheManager(segments);
    final SegmentManager segmentManager = new SegmentManager(cacheManager);
    final SegmentLoadDropHandler handler = initSegmentLoadDropHandler(segmentManager);

    Assert.assertTrue(segmentManager.getDataSourceCounts().isEmpty());

    handler.start();

    Assert.assertEquals(1, serverAnnouncer.getObservedCount());
    Assert.assertFalse(segmentManager.getDataSourceCounts().isEmpty());

    for (int i = 0; i < COUNT; ++i) {
      Assert.assertEquals(3L, segmentManager.getDataSourceCounts().get("test" + i).longValue());
      Assert.assertEquals(2L, segmentManager.getDataSourceCounts().get("test_two" + i).longValue());
    }

    Assert.assertEquals(ImmutableList.copyOf(segments), segmentAnnouncer.getObservedSegments());

    final ImmutableList<DataSegment> expectedBootstrapSegments = ImmutableList.copyOf(segments);
    Assert.assertEquals(expectedBootstrapSegments, cacheManager.observedBootstrapSegments);
    Assert.assertEquals(expectedBootstrapSegments, cacheManager.observedBootstrapSegmentsLoadedIntoPageCache);
    Assert.assertEquals(ImmutableList.of(), cacheManager.observedSegments);
    Assert.assertEquals(ImmutableList.of(), cacheManager.observedSegmentsLoadedIntoPageCache);

    handler.stop();

    Assert.assertEquals(0, serverAnnouncer.getObservedCount());
    Assert.assertEquals(1, cacheManager.observedShutdownBootstrapCount.get());
  }

  @Test(timeout = 60_000L)
  public void testProcessBatch() throws Exception
  {
    final TestSegmentCacheManager cacheManager = new TestSegmentCacheManager();
    final SegmentManager segmentManager = new SegmentManager(cacheManager);
    final SegmentLoadDropHandler handler = initSegmentLoadDropHandler(segmentManager);

    handler.start();

    Assert.assertEquals(1, serverAnnouncer.getObservedCount());

    DataSegment segment1 = makeSegment("batchtest1", "1", Intervals.of("P1d/2011-04-01"));
    DataSegment segment2 = makeSegment("batchtest2", "1", Intervals.of("P1d/2011-04-01"));

    List<DataSegmentChangeRequest> batch = ImmutableList.of(
        new SegmentChangeRequestLoad(segment1),
        new SegmentChangeRequestDrop(segment2)
    );

    ListenableFuture<List<DataSegmentChangeResponse>> future = handler.processBatch(batch);

    Map<DataSegmentChangeRequest, SegmentChangeStatus> expectedStatusMap = new HashMap<>();
    expectedStatusMap.put(batch.get(0), SegmentChangeStatus.PENDING);
    expectedStatusMap.put(batch.get(1), SegmentChangeStatus.SUCCESS);
    List<DataSegmentChangeResponse> result = future.get();
    for (DataSegmentChangeResponse requestAndStatus : result) {
      Assert.assertEquals(expectedStatusMap.get(requestAndStatus.getRequest()), requestAndStatus.getStatus());
    }

    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }

    result = handler.processBatch(ImmutableList.of(new SegmentChangeRequestLoad(segment1))).get();
    Assert.assertEquals(SegmentChangeStatus.SUCCESS, result.get(0).getStatus());

    Assert.assertEquals(ImmutableList.of(segment1), segmentAnnouncer.getObservedSegments());

    final ImmutableList<DataSegment> expectedSegments = ImmutableList.of(segment1);
    Assert.assertEquals(expectedSegments, cacheManager.observedSegments);
    Assert.assertEquals(expectedSegments, cacheManager.observedSegmentsLoadedIntoPageCache);
    Assert.assertEquals(ImmutableList.of(), cacheManager.observedBootstrapSegments);
    Assert.assertEquals(ImmutableList.of(), cacheManager.observedBootstrapSegmentsLoadedIntoPageCache);

    handler.stop();
    Assert.assertEquals(0, serverAnnouncer.getObservedCount());
  }

  @Test(timeout = 60_000L)
  public void testProcessBatchDuplicateLoadRequestsWhenFirstRequestFailsSecondRequestShouldSucceed() throws Exception
  {
    final SegmentManager segmentManager = Mockito.mock(SegmentManager.class);
    Mockito.doThrow(new RuntimeException("segment loading failure test"))
           .doNothing()
           .when(segmentManager)
           .loadSegment(ArgumentMatchers.any());

    final SegmentLoadDropHandler handler = initSegmentLoadDropHandler(segmentManager);

    handler.start();

    Assert.assertEquals(1, serverAnnouncer.getObservedCount());

    DataSegment segment1 = makeSegment("batchtest1", "1", Intervals.of("P1d/2011-04-01"));
    List<DataSegmentChangeRequest> batch = ImmutableList.of(new SegmentChangeRequestLoad(segment1));

    ListenableFuture<List<DataSegmentChangeResponse>> future = handler.processBatch(batch);

    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }
    List<DataSegmentChangeResponse> result = future.get();
    Assert.assertEquals(State.FAILED, result.get(0).getStatus().getState());
    Assert.assertEquals(ImmutableList.of(), segmentAnnouncer.getObservedSegments());

    future = handler.processBatch(batch);
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }
    result = future.get();
    Assert.assertEquals(State.SUCCESS, result.get(0).getStatus().getState());
    Assert.assertEquals(ImmutableList.of(segment1, segment1), segmentAnnouncer.getObservedSegments());

    handler.stop();
    Assert.assertEquals(0, serverAnnouncer.getObservedCount());
  }

  @Test(timeout = 60_000L)
  public void testProcessBatchLoadDropLoadSequenceForSameSegment() throws Exception
  {
    final SegmentManager segmentManager = Mockito.mock(SegmentManager.class);
    Mockito.doNothing().when(segmentManager).loadSegment(ArgumentMatchers.any());
    Mockito.doNothing().when(segmentManager).dropSegment(ArgumentMatchers.any());

    final File storageDir = temporaryFolder.newFolder();
    final SegmentLoaderConfig noAnnouncerSegmentLoaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public File getInfoDir()
      {
        return storageDir;
      }

      @Override
      public int getNumLoadingThreads()
      {
        return 5;
      }

      @Override
      public int getAnnounceIntervalMillis()
      {
        return 0;
      }

      @Override
      public List<StorageLocationConfig> getLocations()
      {
        return Collections.singletonList(
            new StorageLocationConfig(storageDir, null, null)
        );
      }

      @Override
      public int getDropSegmentDelayMillis()
      {
        return 0;
      }
    };

    final SegmentLoadDropHandler handler = initSegmentLoadDropHandler(
        noAnnouncerSegmentLoaderConfig,
        segmentManager
    );

    handler.start();

    Assert.assertEquals(1, serverAnnouncer.getObservedCount());

    final DataSegment segment1 = makeSegment("batchtest1", "1", Intervals.of("P1d/2011-04-01"));
    List<DataSegmentChangeRequest> batch = ImmutableList.of(new SegmentChangeRequestLoad(segment1));

    // Request 1: Load the segment
    ListenableFuture<List<DataSegmentChangeResponse>> future = handler.processBatch(batch);
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }
    List<DataSegmentChangeResponse> result = future.get();
    Assert.assertEquals(State.SUCCESS, result.get(0).getStatus().getState());
    Assert.assertEquals(ImmutableList.of(segment1), segmentAnnouncer.getObservedSegments());
    scheduledRunnable.clear();

    // Request 2: Drop the segment
    batch = ImmutableList.of(new SegmentChangeRequestDrop(segment1));
    future = handler.processBatch(batch);
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }
    result = future.get();
    Assert.assertEquals(State.SUCCESS, result.get(0).getStatus().getState());
    Assert.assertEquals(ImmutableList.of(), segmentAnnouncer.getObservedSegments());
    Assert.assertFalse(segmentAnnouncer.getObservedSegments().contains(segment1)); //
    scheduledRunnable.clear();

    // check invocations after a load-drop sequence
    Mockito.verify(segmentManager, Mockito.times(1))
           .loadSegment(ArgumentMatchers.any());
    Mockito.verify(segmentManager, Mockito.times(1))
           .dropSegment(ArgumentMatchers.any());

    // Request 3: Reload the segment
    batch = ImmutableList.of(new SegmentChangeRequestLoad(segment1));
    future = handler.processBatch(batch);
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }
    result = future.get();
    Assert.assertEquals(State.SUCCESS, result.get(0).getStatus().getState());
    Assert.assertEquals(ImmutableList.of(segment1), segmentAnnouncer.getObservedSegments());
    scheduledRunnable.clear();

    // check invocations - 1 more load has happened
    Mockito.verify(segmentManager, Mockito.times(2))
           .loadSegment(ArgumentMatchers.any());
    Mockito.verify(segmentManager, Mockito.times(1))
           .dropSegment(ArgumentMatchers.any());

    // Request 4: Try to reload the segment - segment is loaded and announced again
    batch = ImmutableList.of(new SegmentChangeRequestLoad(segment1));
    future = handler.processBatch(batch);
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }
    result = future.get();
    Assert.assertEquals(State.SUCCESS, result.get(0).getStatus().getState());
    Assert.assertEquals(ImmutableList.of(segment1, segment1), segmentAnnouncer.getObservedSegments());
    scheduledRunnable.clear();

    // check invocations - the load segment counter should bump up
    Mockito.verify(segmentManager, Mockito.times(3))
           .loadSegment(ArgumentMatchers.any());
    Mockito.verify(segmentManager, Mockito.times(1))
           .dropSegment(ArgumentMatchers.any());

    handler.stop();
    Assert.assertEquals(0, serverAnnouncer.getObservedCount());
  }

  private SegmentLoadDropHandler initSegmentLoadDropHandler(SegmentManager segmentManager)
  {
    return initSegmentLoadDropHandler(segmentLoaderConfig, segmentManager);
  }

  private SegmentLoadDropHandler initSegmentLoadDropHandler(SegmentLoaderConfig config, SegmentManager segmentManager)
  {
    return new SegmentLoadDropHandler(
        config,
        segmentAnnouncer,
        serverAnnouncer,
        segmentManager,
        scheduledExecutorFactory.create(5, "SegmentLoadDropHandlerTest-[%d]"),
        new ServerTypeConfig(ServerType.HISTORICAL)
    );
  }

  private DataSegment makeSegment(String dataSource, String version, Interval interval)
  {
    return TestSegmentUtils.makeSegment(dataSource, version, interval);
  }

  /**
   * A local cache manager to test the bootstrapping and segment add/remove operations. It stubs only the necessary
   * methods to support these operations; any other method invoked will throw an exception from the base class,
   * {@link NoopSegmentCacheManager}.
   */
  private static class TestSegmentCacheManager extends NoopSegmentCacheManager
  {
    private final List<DataSegment> cachedSegments;

    private final List<DataSegment> observedBootstrapSegments;
    private final List<DataSegment> observedBootstrapSegmentsLoadedIntoPageCache;
    private final List<DataSegment> observedSegments;
    private final List<DataSegment> observedSegmentsLoadedIntoPageCache;
    private final List<DataSegment> observedSegmentsRemovedFromCache;
    private final AtomicInteger observedShutdownBootstrapCount;

    TestSegmentCacheManager()
    {
      this(ImmutableSet.of());
    }

    TestSegmentCacheManager(final Set<DataSegment> segmentsToCache)
    {
      this.cachedSegments = ImmutableList.copyOf(segmentsToCache);
      this.observedBootstrapSegments = new ArrayList<>();
      this.observedBootstrapSegmentsLoadedIntoPageCache = new ArrayList<>();
      this.observedSegments = new ArrayList<>();
      this.observedSegmentsLoadedIntoPageCache = new ArrayList<>();
      this.observedSegmentsRemovedFromCache = new ArrayList<>();
      this.observedShutdownBootstrapCount = new AtomicInteger(0);
    }

    @Override
    public boolean canHandleSegments()
    {
      return true;
    }

    @Override
    public List<DataSegment> getCachedSegments()
    {
      return cachedSegments;
    }

    @Override
    public ReferenceCountingSegment getBootstrapSegment(DataSegment segment, SegmentLazyLoadFailCallback loadFailed)
    {
      observedBootstrapSegments.add(segment);
      return getSegmentInternal(segment);
    }

    @Override
    public ReferenceCountingSegment getSegment(final DataSegment segment)
    {
      observedSegments.add(segment);
      return getSegmentInternal(segment);
    }

    private ReferenceCountingSegment getSegmentInternal(final DataSegment segment)
    {
      if (segment.isTombstone()) {
        return ReferenceCountingSegment
            .wrapSegment(TombstoneSegmentizerFactory.segmentForTombstone(segment), segment.getShardSpec());
      } else {
        return ReferenceCountingSegment.wrapSegment(
            new TestSegmentUtils.SegmentForTesting(
                segment.getDataSource(),
                (Interval) segment.getLoadSpec().get("interval"),
                MapUtils.getString(segment.getLoadSpec(), "version")
            ), segment.getShardSpec()
        );
      }
    }

    @Override
    public void loadSegmentIntoPageCache(DataSegment segment)
    {
      observedSegmentsLoadedIntoPageCache.add(segment);
    }

    @Override
    public void loadSegmentIntoPageCacheOnBootstrap(DataSegment segment)
    {
      observedBootstrapSegmentsLoadedIntoPageCache.add(segment);
    }

    @Override
    public void shutdownBootstrap()
    {
      observedShutdownBootstrapCount.incrementAndGet();
    }

    @Override
    public void storeInfoFile(DataSegment segment)
    {
    }

    @Override
    public void removeInfoFile(DataSegment segment)
    {
    }

    @Override
    public void cleanup(DataSegment segment)
    {
      observedSegmentsRemovedFromCache.add(segment);
    }
  }
}
