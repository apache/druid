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
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.guice.ServerTypeConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.loading.CacheTestSegmentLoader;
import org.apache.druid.segment.loading.NoopSegmentCacheManager;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordination.SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus;
import org.apache.druid.server.coordination.SegmentLoadDropHandler.Status.STATE;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class SegmentLoadDropHandlerTest
{
  public static final int COUNT = 50;

  private final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

  private SegmentLoadDropHandler segmentLoadDropHandler;

  private DataSegmentAnnouncer announcer;
  private File infoDir;
  private TestStorageLocation testStorageLocation;
  private AtomicInteger announceCount;
  private ConcurrentSkipListSet<DataSegment> segmentsAnnouncedByMe;
  private SegmentCacheManager segmentCacheManager;
  private Set<DataSegment> segmentsRemovedFromCache;
  private SegmentManager segmentManager;
  private List<Runnable> scheduledRunnable;
  private SegmentLoaderConfig segmentLoaderConfig;
  private SegmentLoaderConfig noAnnouncerSegmentLoaderConfig;
  private ScheduledExecutorFactory scheduledExecutorFactory;
  private List<StorageLocationConfig> locations;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

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

    locations = Collections.singletonList(
        testStorageLocation.toStorageLocationConfig()
    );

    scheduledRunnable = new ArrayList<>();

    segmentsRemovedFromCache = new HashSet<>();
    segmentCacheManager = new NoopSegmentCacheManager()
    {
      @Override
      public boolean isSegmentCached(DataSegment segment)
      {
        Map<String, Object> loadSpec = segment.getLoadSpec();
        return new File(MapUtils.getString(loadSpec, "cacheDir")).exists();
      }

      @Override
      public void cleanup(DataSegment segment)
      {
        segmentsRemovedFromCache.add(segment);
      }
    };

    segmentManager = new SegmentManager(new CacheTestSegmentLoader());
    segmentsAnnouncedByMe = new ConcurrentSkipListSet<>();
    announceCount = new AtomicInteger(0);

    announcer = new DataSegmentAnnouncer()
    {
      @Override
      public void announceSegment(DataSegment segment)
      {
        segmentsAnnouncedByMe.add(segment);
        announceCount.incrementAndGet();
      }

      @Override
      public void unannounceSegment(DataSegment segment)
      {
        segmentsAnnouncedByMe.remove(segment);
        announceCount.decrementAndGet();
      }

      @Override
      public void announceSegments(Iterable<DataSegment> segments)
      {
        for (DataSegment segment : segments) {
          segmentsAnnouncedByMe.add(segment);
        }
        announceCount.addAndGet(Iterables.size(segments));
      }

      @Override
      public void unannounceSegments(Iterable<DataSegment> segments)
      {
        for (DataSegment segment : segments) {
          segmentsAnnouncedByMe.remove(segment);
        }
        announceCount.addAndGet(-Iterables.size(segments));
      }

      @Override
      public void announceSegmentSchemas(String taskId, SegmentSchemas segmentSchemas, SegmentSchemas segmentSchemasChange)
      {
      }

      @Override
      public void removeSegmentSchemasForTask(String taskId)
      {
      }
    };

    segmentLoaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public File getInfoDir()
      {
        return testStorageLocation.getInfoDir();
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
        return locations;
      }

      @Override
      public int getDropSegmentDelayMillis()
      {
        return 0;
      }
    };

    noAnnouncerSegmentLoaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public File getInfoDir()
      {
        return testStorageLocation.getInfoDir();
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
        return locations;
      }

      @Override
      public int getDropSegmentDelayMillis()
      {
        return 0;
      }
    };

    scheduledExecutorFactory = new ScheduledExecutorFactory()
    {
      @Override
      public ScheduledExecutorService create(int corePoolSize, String nameFormat)
      {
            /*
               Override normal behavoir by adding the runnable to a list so that you can make sure
               all the shceduled runnables are executed by explicitly calling run() on each item in the list
             */
        return new ScheduledThreadPoolExecutor(corePoolSize, Execs.makeThreadFactory(nameFormat))
        {
          @Override
          public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
          {
            scheduledRunnable.add(command);
            return null;
          }
        };
      }
    };

    segmentLoadDropHandler = new SegmentLoadDropHandler(
        jsonMapper,
        segmentLoaderConfig,
        announcer,
        Mockito.mock(DataSegmentServerAnnouncer.class),
        segmentManager,
        segmentCacheManager,
        scheduledExecutorFactory.create(5, "SegmentLoadDropHandlerTest-[%d]"),
        new ServerTypeConfig(ServerType.HISTORICAL)
    );
  }

  /**
   * Steps:
   * 1. removeSegment() schedules a delete runnable that deletes segment files,
   * 2. addSegment() succesfully loads the segment and annouces it
   * 3. scheduled delete task executes and realizes it should not delete the segment files.
   */
  @Test
  public void testSegmentLoading1() throws Exception
  {
    segmentLoadDropHandler.start();

    final DataSegment segment = makeSegment("test", "1", Intervals.of("P1d/2011-04-01"));

    segmentLoadDropHandler.removeSegment(segment, DataSegmentChangeCallback.NOOP);

    Assert.assertFalse(segmentsAnnouncedByMe.contains(segment));

    segmentLoadDropHandler.addSegment(segment, DataSegmentChangeCallback.NOOP);

    /*
       make sure the scheduled runnable that "deletes" segment files has been executed.
       Because another addSegment() call is executed, which removes the segment from segmentsToDelete field in
       ZkCoordinator, the scheduled runnable will not actually delete segment files.
     */
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }

    Assert.assertTrue(segmentsAnnouncedByMe.contains(segment));
    Assert.assertFalse("segment files shouldn't be deleted", segmentsRemovedFromCache.contains(segment));

    segmentLoadDropHandler.stop();
  }

  /**
   * Steps:
   * 1. addSegment() succesfully loads the segment and annouces it
   * 2. removeSegment() unannounces the segment and schedules a delete runnable that deletes segment files
   * 3. addSegment() calls loadSegment() and annouces it again
   * 4. scheduled delete task executes and realizes it should not delete the segment files.
   */
  @Test
  public void testSegmentLoading2() throws Exception
  {
    segmentLoadDropHandler.start();

    final DataSegment segment = makeSegment("test", "1", Intervals.of("P1d/2011-04-01"));

    segmentLoadDropHandler.addSegment(segment, DataSegmentChangeCallback.NOOP);

    Assert.assertTrue(segmentsAnnouncedByMe.contains(segment));

    segmentLoadDropHandler.removeSegment(segment, DataSegmentChangeCallback.NOOP);

    Assert.assertFalse(segmentsAnnouncedByMe.contains(segment));

    segmentLoadDropHandler.addSegment(segment, DataSegmentChangeCallback.NOOP);

    /*
       make sure the scheduled runnable that "deletes" segment files has been executed.
       Because another addSegment() call is executed, which removes the segment from segmentsToDelete field in
       ZkCoordinator, the scheduled runnable will not actually delete segment files.
     */
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }

    Assert.assertTrue(segmentsAnnouncedByMe.contains(segment));
    Assert.assertFalse("segment files shouldn't be deleted", segmentsRemovedFromCache.contains(segment));

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
    Assert.assertTrue(!segmentManager.getDataSourceCounts().isEmpty());
    for (int i = 0; i < COUNT; ++i) {
      Assert.assertEquals(11L, segmentManager.getDataSourceCounts().get("test" + i).longValue());
      Assert.assertEquals(2L, segmentManager.getDataSourceCounts().get("test_two" + i).longValue());
    }
    Assert.assertEquals(13 * COUNT, announceCount.get());
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
    SegmentLoadDropHandler handler = new SegmentLoadDropHandler(
        jsonMapper,
        new SegmentLoaderConfig()
        {
          @Override
          public File getInfoDir()
          {
            return infoDir;
          }

          @Override
          public int getNumLoadingThreads()
          {
            return 5;
          }

          @Override
          public List<StorageLocationConfig> getLocations()
          {
            return locations;
          }

          @Override
          public int getAnnounceIntervalMillis()
          {
            return 50;
          }
        },
        announcer,
        Mockito.mock(DataSegmentServerAnnouncer.class),
        segmentManager,
        segmentCacheManager,
        new ServerTypeConfig(ServerType.HISTORICAL)
    );

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

    handler.start();
    Assert.assertTrue(!segmentManager.getDataSourceCounts().isEmpty());
    for (int i = 0; i < COUNT; ++i) {
      Assert.assertEquals(3L, segmentManager.getDataSourceCounts().get("test" + i).longValue());
      Assert.assertEquals(2L, segmentManager.getDataSourceCounts().get("test_two" + i).longValue());
    }
    Assert.assertEquals(5 * COUNT, announceCount.get());
    handler.stop();

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

    List<DataSegmentChangeRequest> batch = ImmutableList.of(
        new SegmentChangeRequestLoad(segment1),
        new SegmentChangeRequestDrop(segment2)
    );

    ListenableFuture<List<SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus>> future = segmentLoadDropHandler
        .processBatch(batch);

    Map<DataSegmentChangeRequest, SegmentLoadDropHandler.Status> expectedStatusMap = new HashMap<>();
    expectedStatusMap.put(batch.get(0), SegmentLoadDropHandler.Status.PENDING);
    expectedStatusMap.put(batch.get(1), SegmentLoadDropHandler.Status.SUCCESS);
    List<SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus> result = future.get();
    for (SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus requestAndStatus : result) {
      Assert.assertEquals(expectedStatusMap.get(requestAndStatus.getRequest()), requestAndStatus.getStatus());
    }

    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }

    result = segmentLoadDropHandler.processBatch(ImmutableList.of(new SegmentChangeRequestLoad(segment1))).get();
    Assert.assertEquals(SegmentLoadDropHandler.Status.SUCCESS, result.get(0).getStatus());

    segmentLoadDropHandler.stop();
  }

  @Test(timeout = 60_000L)
  public void testProcessBatchDuplicateLoadRequestsWhenFirstRequestFailsSecondRequestShouldSucceed() throws Exception
  {
    final SegmentManager segmentManager = Mockito.mock(SegmentManager.class);
    Mockito.when(segmentManager.loadSegment(ArgumentMatchers.any(), ArgumentMatchers.anyBoolean(),
                                            ArgumentMatchers.any(), ArgumentMatchers.any()))
           .thenThrow(new RuntimeException("segment loading failure test"))
           .thenReturn(true);
    final SegmentLoadDropHandler segmentLoadDropHandler = new SegmentLoadDropHandler(
        jsonMapper,
        segmentLoaderConfig,
        announcer,
        Mockito.mock(DataSegmentServerAnnouncer.class),
        segmentManager,
        segmentCacheManager,
        scheduledExecutorFactory.create(5, "SegmentLoadDropHandlerTest-[%d]"),
        new ServerTypeConfig(ServerType.HISTORICAL)
    );

    segmentLoadDropHandler.start();

    DataSegment segment1 = makeSegment("batchtest1", "1", Intervals.of("P1d/2011-04-01"));

    List<DataSegmentChangeRequest> batch = ImmutableList.of(new SegmentChangeRequestLoad(segment1));

    ListenableFuture<List<DataSegmentChangeRequestAndStatus>> future = segmentLoadDropHandler
        .processBatch(batch);

    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }
    List<DataSegmentChangeRequestAndStatus> result = future.get();
    Assert.assertEquals(STATE.FAILED, result.get(0).getStatus().getState());

    future = segmentLoadDropHandler.processBatch(batch);
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }
    result = future.get();
    Assert.assertEquals(STATE.SUCCESS, result.get(0).getStatus().getState());

    segmentLoadDropHandler.stop();
  }

  @Test(timeout = 60_000L)
  public void testProcessBatchLoadDropLoadSequenceForSameSegment() throws Exception
  {
    final SegmentManager segmentManager = Mockito.mock(SegmentManager.class);
    Mockito.doReturn(true).when(segmentManager).loadSegment(
        ArgumentMatchers.any(),
        ArgumentMatchers.anyBoolean(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
    );
    Mockito.doNothing().when(segmentManager).dropSegment(ArgumentMatchers.any());
    final SegmentLoadDropHandler segmentLoadDropHandler = new SegmentLoadDropHandler(
        jsonMapper,
        noAnnouncerSegmentLoaderConfig,
        announcer,
        Mockito.mock(DataSegmentServerAnnouncer.class),
        segmentManager,
        segmentCacheManager,
        scheduledExecutorFactory.create(5, "SegmentLoadDropHandlerTest-[%d]"),
        new ServerTypeConfig(ServerType.HISTORICAL)
    );

    segmentLoadDropHandler.start();

    final DataSegment segment1 = makeSegment("batchtest1", "1", Intervals.of("P1d/2011-04-01"));

    List<DataSegmentChangeRequest> batch = ImmutableList.of(new SegmentChangeRequestLoad(segment1));

    // Request 1: Load the segment
    ListenableFuture<List<DataSegmentChangeRequestAndStatus>> future = segmentLoadDropHandler
        .processBatch(batch);
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }
    List<DataSegmentChangeRequestAndStatus> result = future.get();
    Assert.assertEquals(STATE.SUCCESS, result.get(0).getStatus().getState());
    scheduledRunnable.clear();

    // Request 2: Drop the segment
    batch = ImmutableList.of(new SegmentChangeRequestDrop(segment1));
    future = segmentLoadDropHandler.processBatch(batch);
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }
    result = future.get();
    Assert.assertEquals(STATE.SUCCESS, result.get(0).getStatus().getState());
    scheduledRunnable.clear();

    // check invocations after a load-drop sequence
    Mockito.verify(segmentManager, Mockito.times(1)).loadSegment(
        ArgumentMatchers.any(),
        ArgumentMatchers.anyBoolean(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
    );
    Mockito.verify(segmentManager, Mockito.times(1))
           .dropSegment(ArgumentMatchers.any());

    // Request 3: Reload the segment
    batch = ImmutableList.of(new SegmentChangeRequestLoad(segment1));
    future = segmentLoadDropHandler.processBatch(batch);
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }
    result = future.get();
    Assert.assertEquals(STATE.SUCCESS, result.get(0).getStatus().getState());
    scheduledRunnable.clear();

    // check invocations - 1 more load has happened
    Mockito.verify(segmentManager, Mockito.times(2)).loadSegment(
        ArgumentMatchers.any(),
        ArgumentMatchers.anyBoolean(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
    );
    Mockito.verify(segmentManager, Mockito.times(1))
           .dropSegment(ArgumentMatchers.any());

    // Request 4: Try to reload the segment - segment is loaded again
    batch = ImmutableList.of(new SegmentChangeRequestLoad(segment1));
    future = segmentLoadDropHandler.processBatch(batch);
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }
    result = future.get();
    Assert.assertEquals(STATE.SUCCESS, result.get(0).getStatus().getState());
    scheduledRunnable.clear();

    // check invocations - the load segment counter should bump up
    Mockito.verify(segmentManager, Mockito.times(3)).loadSegment(
        ArgumentMatchers.any(),
        ArgumentMatchers.anyBoolean(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
    );
    Mockito.verify(segmentManager, Mockito.times(1))
           .dropSegment(ArgumentMatchers.any());

    segmentLoadDropHandler.stop();
  }
}
