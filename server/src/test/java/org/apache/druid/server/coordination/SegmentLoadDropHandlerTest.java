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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.guice.ServerTypeConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.loading.CacheTestSegmentLoader;
import org.apache.druid.segment.loading.NoopSegmentCacheManager;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class SegmentLoadDropHandlerTest
{
  private static final int COUNT = 50;
  private static final String EXECUTOR_NAME_FORMAT = "SegmentLoadDropHandlerTest-[%d]";

  private final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

  private SegmentLoadDropHandler segmentLoadDropHandler;

  private TestDataSegmentAnnouncer announcer;
  private CacheTestSegmentLoader segmentLoader;

  private File infoDir;
  private TestStorageLocation testStorageLocation;
  private SegmentManager segmentManager;
  private SegmentLoaderConfig segmentLoaderConfig;
  private BlockingExecutorService loadingExecutor;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  public SegmentLoadDropHandlerTest()
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
  }

  @Before
  public void setUp() throws IOException
  {
    try {
      testStorageLocation = new TestStorageLocation(temporaryFolder);
      infoDir = testStorageLocation.getInfoDir();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    final List<StorageLocationConfig> locations = Collections.singletonList(
        testStorageLocation.toStorageLocationConfig()
    );

    segmentLoader = new CacheTestSegmentLoader();
    segmentManager = new SegmentManager(segmentLoader);
    announcer = new TestDataSegmentAnnouncer();

    segmentLoaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public File getInfoDir()
      {
        return testStorageLocation.getInfoDir();
      }

      @Override
      public List<StorageLocationConfig> getLocations()
      {
        return locations;
      }
    };

    loadingExecutor = new BlockingExecutorService(EXECUTOR_NAME_FORMAT);
    segmentLoadDropHandler = initHandler(segmentManager);
  }

  @Test
  public void testLoadCancelsPendingDropOfMissingSegment() throws Exception
  {
    segmentLoadDropHandler.start();

    final DataSegment segment = makeSegment("test", "1", Intervals.of("P1d/2011-04-01"));

    // Queue a drop even though the segment is not loaded yet
    segmentLoadDropHandler.submitRequestBatch(dropRequest(segment));
    Assert.assertFalse(announcer.isSegmentAnnounced(segment));
    Assert.assertEquals(1, loadingExecutor.numPendingTasks());

    // Queue a load of the segment
    segmentLoadDropHandler.submitRequestBatch(loadRequest(segment));
    Assert.assertFalse(announcer.isSegmentAnnounced(segment));
    Assert.assertEquals(2, loadingExecutor.numPendingTasks());

    // Try to complete both the pending drop and load
    loadingExecutor.finishAllPendingTasks();

    // Verify that the segment is loaded and the drop never happens
    Assert.assertTrue(announcer.isSegmentAnnounced(segment));
    Assert.assertTrue(segmentLoader.getLoadedSegments().contains(segment));
    Assert.assertFalse(segmentLoader.getRemovedSegments().contains(segment));

    segmentLoadDropHandler.stop();
  }

  @Test
  public void testLoadCancelsPendingDrop() throws Exception
  {
    segmentLoadDropHandler.start();

    final String datasource = "test";
    final DataSegment segment = makeSegment(datasource, "1", Intervals.of("P1d/2011-04-01"));

    // Load the segment
    segmentLoadDropHandler.submitRequestBatch(loadRequest(segment));
    loadingExecutor.finishNextPendingTask();
    Assert.assertTrue(announcer.isSegmentAnnounced(segment));
    Assert.assertEquals(1, segmentManager.getDataSourceCounts().get(datasource).intValue());

    // Queue a drop of the segment but do not process it
    segmentLoadDropHandler.submitRequestBatch(dropRequest(segment));
    Assert.assertTrue(announcer.isSegmentAnnounced(segment));
    Assert.assertEquals(1, loadingExecutor.numPendingTasks());

    // Queue a load of the segment, but drop is still pending
    segmentLoadDropHandler.submitRequestBatch(loadRequest(segment));
    Assert.assertEquals(1, loadingExecutor.numPendingTasks());

    // Try to complete both the pending drop and load
    loadingExecutor.finishAllPendingTasks();

    // Verify that the segment is loaded and the drop never happens
    Assert.assertTrue(announcer.isSegmentAnnounced(segment));
    Assert.assertTrue(segmentLoader.getLoadedSegments().contains(segment));
    Assert.assertFalse(segmentLoader.getRemovedSegments().contains(segment));

    segmentLoadDropHandler.stop();
  }

  @Test
  public void testDropCancelsPendingLoad() throws IOException
  {
    segmentLoadDropHandler.start();

    final String datasource = "test";
    final DataSegment segment = makeSegment(datasource, "1", Intervals.of("P1d/2011-04-01"));

    // Queue a load of the segment but do not process it
    segmentLoadDropHandler.submitRequestBatch(loadRequest(segment));
    Assert.assertFalse(announcer.isSegmentAnnounced(segment));
    Assert.assertEquals(1, loadingExecutor.numPendingTasks());

    // Queue a drop of the segment
    segmentLoadDropHandler.submitRequestBatch(dropRequest(segment));
    Assert.assertFalse(announcer.isSegmentAnnounced(segment));
    Assert.assertEquals(2, loadingExecutor.numPendingTasks());

    // Try to complete the drop first and then the load
    loadingExecutor.finishAllPendingTasks();

    // Verify that the segment is unannounced and the load never happens
    Assert.assertFalse(announcer.isSegmentAnnounced(segment));
    Assert.assertFalse(segmentLoader.getLoadedSegments().contains(segment));
    Assert.assertTrue(segmentLoader.getRemovedSegments().contains(segment));

    segmentLoadDropHandler.stop();
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

    for (DataSegment segment : segments) {
      testStorageLocation.writeSegmentInfoToCache(segment);
    }

    testStorageLocation.checkInfoCache(segments);
    Assert.assertTrue(segmentManager.getDataSourceCounts().isEmpty());
    segmentLoadDropHandler.start();
    Assert.assertFalse(segmentManager.getDataSourceCounts().isEmpty());
    for (int i = 0; i < COUNT; ++i) {
      Assert.assertEquals(11L, segmentManager.getDataSourceCounts().get("test" + i).longValue());
      Assert.assertEquals(2L, segmentManager.getDataSourceCounts().get("test_two" + i).longValue());
    }
    Assert.assertEquals(13 * COUNT, announcer.getNumAnnouncedSegments());
    segmentLoadDropHandler.stop();

    for (DataSegment segment : segments) {
      testStorageLocation.deleteSegmentInfoFromCache(segment);
    }

    Assert.assertEquals(0, infoDir.listFiles().length);
    Assert.assertTrue(infoDir.delete());
  }

  private DataSegment makeSegment(String dataSource, String version, Interval interval)
  {
    return new DataSegment(
        dataSource,
        interval,
        version,
        ImmutableMap.of("version", version, "interval", interval, "cacheDir", infoDir),
        Arrays.asList("dim1", "dim2", "dim3"),
        Arrays.asList("metric1", "metric2"),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        123L
    );
  }

  @Test
  public void testStartStop() throws Exception
  {
    Set<DataSegment> segments = new HashSet<>();
    for (int i = 0; i < COUNT; ++i) {
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-02")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("P1d/2011-04-02")));
      segments.add(makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-02")));
    }

    for (DataSegment segment : segments) {
      testStorageLocation.writeSegmentInfoToCache(segment);
    }

    testStorageLocation.checkInfoCache(segments);
    Assert.assertTrue(segmentManager.getDataSourceCounts().isEmpty());

    segmentLoadDropHandler.start();
    Assert.assertFalse(segmentManager.getDataSourceCounts().isEmpty());
    for (int i = 0; i < COUNT; ++i) {
      Assert.assertEquals(3L, segmentManager.getDataSourceCounts().get("test" + i).longValue());
      Assert.assertEquals(2L, segmentManager.getDataSourceCounts().get("test_two" + i).longValue());
    }
    Assert.assertEquals(5 * COUNT, announcer.getNumAnnouncedSegments());
    segmentLoadDropHandler.stop();

    for (DataSegment segment : segments) {
      testStorageLocation.deleteSegmentInfoFromCache(segment);
    }

    Assert.assertEquals(0, infoDir.listFiles().length);
    Assert.assertTrue(infoDir.delete());
  }

  @Test(timeout = 60_000L)
  public void testProcessBatch() throws Exception
  {
    segmentLoadDropHandler.start();

    DataSegment segment1 = makeSegment("batchtest1", "1", Intervals.of("P1d/2011-04-01"));
    DataSegment segment2 = makeSegment("batchtest2", "1", Intervals.of("P1d/2011-04-01"));

    final List<DataSegmentChangeRequest> batch = ImmutableList.of(
        new SegmentChangeRequestLoad(segment1),
        new SegmentChangeRequestDrop(segment2)
    );

    ListenableFuture<List<DataSegmentChangeResponse>> future
        = segmentLoadDropHandler.submitRequestBatch(batch);
    List<DataSegmentChangeResponse> result = future.get();
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(SegmentChangeStatus.PENDING, result.get(0).getStatus());
    Assert.assertEquals(SegmentChangeStatus.SUCCESS, result.get(1).getStatus());

    loadingExecutor.finishAllPendingTasks();

    // Resubmit the batch and verify that both the requests are now complete
    result = segmentLoadDropHandler.submitRequestBatch(batch).get();
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(SegmentChangeStatus.SUCCESS, result.get(0).getStatus());
    Assert.assertEquals(SegmentChangeStatus.SUCCESS, result.get(1).getStatus());
    Assert.assertTrue(segmentLoader.getLoadedSegments().contains(segment1));

    // Finish clean up of segment files
    loadingExecutor.finishNextPendingTask();
    Assert.assertTrue(segmentLoader.getRemovedSegments().contains(segment2));

    segmentLoadDropHandler.stop();
  }

  @Test(timeout = 60_000L)
  public void testProcessBatchDuplicateLoadRequestsWhenFirstRequestFailsSecondRequestShouldSucceed() throws Exception
  {
    final SegmentManager segmentManager = Mockito.mock(SegmentManager.class);
    whenLoadSegment(segmentManager)
        .thenThrow(new RuntimeException("segment loading failure test"))
        .thenReturn(true);

    final SegmentLoadDropHandler segmentLoadDropHandler = initHandler(segmentManager);
    segmentLoadDropHandler.start();

    final DataSegment segment1 = makeSegment("batchtest1", "1", Intervals.of("P1d/2011-04-01"));

    ListenableFuture<List<DataSegmentChangeResponse>> future
        = segmentLoadDropHandler.submitRequestBatch(loadRequest(segment1));

    loadingExecutor.finishAllPendingTasks();
    List<DataSegmentChangeResponse> result = future.get();
    Assert.assertEquals(SegmentChangeStatus.State.FAILED, result.get(0).getStatus().getState());

    future = segmentLoadDropHandler.submitRequestBatch(loadRequest(segment1));
    loadingExecutor.finishAllPendingTasks();
    result = future.get();
    Assert.assertEquals(SegmentChangeStatus.State.SUCCESS, result.get(0).getStatus().getState());

    segmentLoadDropHandler.stop();
  }

  @Test(timeout = 60_000L)
  public void testProcessBatchLoadDropLoadSequenceForSameSegment() throws Exception
  {
    final SegmentManager segmentManager = Mockito.mock(SegmentManager.class);
    whenLoadSegment(segmentManager).thenReturn(true);
    Mockito.doNothing().when(segmentManager).dropSegment(ArgumentMatchers.any());
    final SegmentLoadDropHandler segmentLoadDropHandler = initHandler(segmentManager);

    segmentLoadDropHandler.start();

    final DataSegment segment1 = makeSegment("batchtest1", "1", Intervals.of("P1d/2011-04-01"));

    // Request 1: Load the segment
    ListenableFuture<List<DataSegmentChangeResponse>> future
        = segmentLoadDropHandler.submitRequestBatch(loadRequest(segment1));
    loadingExecutor.finishAllPendingTasks();
    List<DataSegmentChangeResponse> result = future.get();
    Assert.assertEquals(SegmentChangeStatus.State.SUCCESS, result.get(0).getStatus().getState());

    // Request 2: Drop the segment
    future = segmentLoadDropHandler.submitRequestBatch(dropRequest(segment1));
    loadingExecutor.finishAllPendingTasks();
    result = future.get();
    Assert.assertEquals(SegmentChangeStatus.State.SUCCESS, result.get(0).getStatus().getState());

    // Verify that 1 load and 1 drop has happened
    verifyLoadCalled(segmentManager, 1);
    verifyDropCalled(segmentManager, 1);

    // Request 3: Reload the segment
    future = segmentLoadDropHandler.submitRequestBatch(loadRequest(segment1));
    loadingExecutor.finishAllPendingTasks();
    result = future.get();
    Assert.assertEquals(SegmentChangeStatus.State.SUCCESS, result.get(0).getStatus().getState());

    // Verify that 1 more load has happened
    verifyLoadCalled(segmentManager, 2);
    verifyDropCalled(segmentManager, 1);

    // Request 4: Try to reload the segment - segment is loaded again
    future = segmentLoadDropHandler.submitRequestBatch(loadRequest(segment1));
    loadingExecutor.finishAllPendingTasks();
    result = future.get();
    Assert.assertEquals(SegmentChangeStatus.State.SUCCESS, result.get(0).getStatus().getState());

    // Verify that 1 more load has happened
    verifyLoadCalled(segmentManager, 2);
    verifyDropCalled(segmentManager, 1);

    segmentLoadDropHandler.stop();
  }

  private SegmentLoadDropHandler initHandler(SegmentManager manager)
  {
    SegmentCacheManager segmentCacheManager = new NoopSegmentCacheManager()
    {
      @Override
      public boolean isSegmentCached(DataSegment segment)
      {
        Map<String, Object> loadSpec = segment.getLoadSpec();
        return new File(MapUtils.getString(loadSpec, "cacheDir")).exists();
      }
    };
    return new SegmentLoadDropHandler(
        jsonMapper,
        segmentLoaderConfig,
        announcer,
        Mockito.mock(DataSegmentServerAnnouncer.class),
        manager,
        segmentCacheManager,
        new ServerTypeConfig(ServerType.HISTORICAL),
        (corePoolSize, nameFormat) -> new WrappingScheduledExecutorService(nameFormat, loadingExecutor, false)
    );
  }

  private OngoingStubbing<Boolean> whenLoadSegment(SegmentManager manager) throws SegmentLoadingException
  {
    return Mockito.when(
        manager.loadSegment(
            ArgumentMatchers.any(),
            ArgumentMatchers.anyBoolean(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )
    );
  }

  private void verifyLoadCalled(SegmentManager manager, int times) throws SegmentLoadingException
  {
    Mockito.verify(manager, Mockito.times(times)).loadSegment(
        ArgumentMatchers.any(),
        ArgumentMatchers.anyBoolean(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
    );
  }

  private void verifyDropCalled(SegmentManager manager, int times)
  {
    Mockito.verify(manager, Mockito.times(times)).dropSegment(ArgumentMatchers.any());
  }

  private static List<DataSegmentChangeRequest> loadRequest(DataSegment segment)
  {
    return Collections.singletonList(new SegmentChangeRequestLoad(segment));
  }

  private static List<DataSegmentChangeRequest> dropRequest(DataSegment segment)
  {
    return Collections.singletonList(new SegmentChangeRequestDrop(segment));
  }
}
