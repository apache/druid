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
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordination.SegmentChangeStatus.State;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.druid.server.TestSegmentUtils.makeSegment;

public class SegmentLoadDropHandlerTest
{
  private TestDataSegmentAnnouncer segmentAnnouncer;
  private List<Runnable> scheduledRunnable;
  private SegmentLoaderConfig segmentLoaderConfig;
  private ScheduledExecutorFactory scheduledExecutorFactory;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException
  {
    final File segmentCacheDir = temporaryFolder.newFolder();

    scheduledRunnable = new ArrayList<>();
    segmentAnnouncer = new TestDataSegmentAnnouncer();
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

    EmittingLogger.registerEmitter(new StubServiceEmitter());
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
  public void testSegmentLoading1()
  {
    final TestSegmentCacheManager cacheManager = new TestSegmentCacheManager();
    final SegmentManager segmentManager = new SegmentManager(cacheManager);
    final SegmentLoadDropHandler handler = initSegmentLoadDropHandler(segmentManager);

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
    Assert.assertEquals(ImmutableList.of(segment), cacheManager.getObservedSegments());
    Assert.assertEquals(ImmutableList.of(segment), cacheManager.getObservedSegmentsLoadedIntoPageCache());
    Assert.assertEquals(ImmutableList.of(), cacheManager.getObservedBootstrapSegments());
    Assert.assertEquals(ImmutableList.of(), cacheManager.getObservedBootstrapSegmentsLoadedIntoPageCache());

    Assert.assertEquals(ImmutableList.of(segment), segmentAnnouncer.getObservedSegments());
    Assert.assertFalse(
        "segment files shouldn't be deleted",
        cacheManager.getObservedSegmentsRemovedFromCache().contains(segment)
    );
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
  public void testSegmentLoading2()
  {
    final TestSegmentCacheManager cacheManager = new TestSegmentCacheManager();
    final SegmentManager segmentManager = new SegmentManager(cacheManager);
    final SegmentLoadDropHandler handler = initSegmentLoadDropHandler(segmentManager);

    // handler.start();

    // Assert.assertEquals(1, serverAnnouncer.getObservedCount());

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
    Assert.assertEquals(ImmutableList.of(segment, segment), cacheManager.getObservedSegments());
    Assert.assertEquals(ImmutableList.of(segment), cacheManager.getObservedSegmentsLoadedIntoPageCache());
    Assert.assertEquals(ImmutableList.of(), cacheManager.getObservedBootstrapSegments());
    Assert.assertEquals(ImmutableList.of(), cacheManager.getObservedBootstrapSegmentsLoadedIntoPageCache());

    Assert.assertTrue(segmentAnnouncer.getObservedSegments().contains(segment));
    Assert.assertFalse(
        "segment files shouldn't be deleted",
        cacheManager.getObservedSegmentsRemovedFromCache().contains(segment)
    );
  }

  @Test(timeout = 60_000L)
  public void testProcessBatch() throws Exception
  {
    final TestSegmentCacheManager cacheManager = new TestSegmentCacheManager();
    final SegmentManager segmentManager = new SegmentManager(cacheManager);
    final SegmentLoadDropHandler handler = initSegmentLoadDropHandler(segmentManager);

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
    Assert.assertEquals(expectedSegments, cacheManager.getObservedSegments());
    Assert.assertEquals(expectedSegments, cacheManager.getObservedSegmentsLoadedIntoPageCache());
    Assert.assertEquals(ImmutableList.of(), cacheManager.getObservedBootstrapSegments());
    Assert.assertEquals(ImmutableList.of(), cacheManager.getObservedBootstrapSegmentsLoadedIntoPageCache());
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

  }

  private SegmentLoadDropHandler initSegmentLoadDropHandler(SegmentManager segmentManager)
  {
    return initSegmentLoadDropHandler(segmentLoaderConfig, segmentManager);
  }

  private SegmentLoadDropHandler initSegmentLoadDropHandler(
      SegmentLoaderConfig config,
      SegmentManager segmentManager
  )
  {
    return new SegmentLoadDropHandler(
        config,
        segmentAnnouncer,
        segmentManager,
        scheduledExecutorFactory.create(5, "SegmentLoadDropHandlerTest-[%d]")
    );
  }
}
