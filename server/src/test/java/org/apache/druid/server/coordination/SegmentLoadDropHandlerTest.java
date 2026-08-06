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
import org.apache.druid.server.http.SegmentLoadingMode;
import org.apache.druid.test.utils.TestSegmentCacheManager;
import org.apache.druid.timeline.DataSegment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.druid.segment.TestSegmentUtils.makeSegment;

public class SegmentLoadDropHandlerTest
{
  private TestDataSegmentAnnouncer segmentAnnouncer;
  private List<Runnable> scheduledRunnable;
  private SegmentLoaderConfig segmentLoaderConfig;
  private ScheduledExecutorFactory scheduledExecutorFactory;

  @TempDir
  public File temporaryFolder;

  @BeforeEach
  public void setUp() throws IOException
  {
    final File segmentCacheDir = newFolder(temporaryFolder, "junit");

    scheduledRunnable = new ArrayList<>();
    segmentAnnouncer = new TestDataSegmentAnnouncer();
    segmentLoaderConfig = SegmentLoaderConfig.builder()
        .infoDir(segmentCacheDir)
        .numLoadingThreads(5)
        .announceIntervalMillis(50)
        .locations(new StorageLocationConfig(segmentCacheDir, null, null))
        .dropSegmentDelayMillis(0)
        .build();

    scheduledExecutorFactory = (corePoolSize, nameFormat) ->
      // Override normal behavior by adding the runnable to a list so that you can make sure
      // all the scheduled runnables are executed by explicitly calling run() on each item in the list
      new ScheduledThreadPoolExecutor(corePoolSize, Execs.makeThreadFactory(nameFormat))
      {
        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
        {
          scheduledRunnable.add(command);
          return null;
        }
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

    Assertions.assertFalse(segmentAnnouncer.getObservedSegments().contains(segment));

    handler.addSegment(segment, DataSegmentChangeCallback.NOOP, null);

    // Make sure the scheduled runnable that "deletes" segment files has been executed.
    // Because another addSegment() call is executed, which removes the segment from segmentsToDelete field in
    // SegmentLoadDropHandler, the scheduled runnable will not actually delete segment files.
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }
    Assertions.assertEquals(ImmutableList.of(segment), cacheManager.getObservedSegments());
    Assertions.assertEquals(ImmutableList.of(), cacheManager.getObservedBootstrapSegments());

    Assertions.assertEquals(ImmutableList.of(segment), segmentAnnouncer.getObservedSegments());
    Assertions.assertFalse(
        cacheManager.getObservedSegmentsRemovedFromCache().contains(segment.getId()),
        "segment files shouldn't be deleted"
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

    handler.addSegment(segment, DataSegmentChangeCallback.NOOP, null);

    Assertions.assertTrue(segmentAnnouncer.getObservedSegments().contains(segment));

    handler.removeSegment(segment, DataSegmentChangeCallback.NOOP);

    Assertions.assertFalse(segmentAnnouncer.getObservedSegments().contains(segment));

    handler.addSegment(segment, DataSegmentChangeCallback.NOOP, null);

    // Make sure the scheduled runnable that "deletes" segment files has been executed.
    // Because another addSegment() call is executed, which removes the segment from segmentsToDelete field in
    // SegmentLoadDropHandler, the scheduled runnable will not actually delete segment files.
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }

    // The same segment reference will be fetched more than once in the above sequence, but the segment should
    // be loaded only once onto the page cache.
    Assertions.assertEquals(ImmutableList.of(segment, segment), cacheManager.getObservedSegments());
    Assertions.assertEquals(ImmutableList.of(), cacheManager.getObservedBootstrapSegments());

    Assertions.assertTrue(segmentAnnouncer.getObservedSegments().contains(segment));
    Assertions.assertFalse(
        cacheManager.getObservedSegmentsRemovedFromCache().contains(segment.getId()),
        "segment files shouldn't be deleted"
    );
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
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

    ListenableFuture<List<DataSegmentChangeResponse>> future = handler.processBatch(batch, SegmentLoadingMode.TURBO);

    Map<DataSegmentChangeRequest, SegmentChangeStatus> expectedStatusMap = new HashMap<>();
    expectedStatusMap.put(batch.get(0), SegmentChangeStatus.pending(SegmentLoadingMode.TURBO));
    expectedStatusMap.put(batch.get(1), SegmentChangeStatus.success());
    List<DataSegmentChangeResponse> result = future.get();
    for (DataSegmentChangeResponse requestAndStatus : result) {
      Assertions.assertEquals(expectedStatusMap.get(requestAndStatus.getRequest()), requestAndStatus.getStatus());
    }

    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }

    result = handler.processBatch(ImmutableList.of(new SegmentChangeRequestLoad(segment1)), SegmentLoadingMode.TURBO).get();
    Assertions.assertEquals(SegmentChangeStatus.success(SegmentLoadingMode.TURBO), result.get(0).getStatus());

    Assertions.assertEquals(ImmutableList.of(segment1), segmentAnnouncer.getObservedSegments());

    final ImmutableList<DataSegment> expectedSegments = ImmutableList.of(segment1);
    Assertions.assertEquals(expectedSegments, cacheManager.getObservedSegments());
    Assertions.assertEquals(ImmutableList.of(), cacheManager.getObservedBootstrapSegments());
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testProcessBatchDuplicateLoadRequestsWhenFirstRequestFailsSecondRequestShouldSucceed() throws Exception
  {
    final SegmentManager segmentManager = Mockito.mock(SegmentManager.class);
    // loadSegment returns DataSegment, so doNothing() would be rejected by Mockito. Throw on the first call,
    // then return the input segment on subsequent calls (the announcement path uses the returned segment).
    Mockito.when(segmentManager.loadSegment(ArgumentMatchers.any()))
           .thenThrow(new RuntimeException("segment loading failure test"))
           .thenAnswer(invocation -> invocation.getArgument(0));

    final SegmentLoadDropHandler handler = initSegmentLoadDropHandler(segmentManager);


    DataSegment segment1 = makeSegment("batchtest1", "1", Intervals.of("P1d/2011-04-01"));
    List<DataSegmentChangeRequest> batch = ImmutableList.of(new SegmentChangeRequestLoad(segment1));

    ListenableFuture<List<DataSegmentChangeResponse>> future = handler.processBatch(batch, SegmentLoadingMode.NORMAL);

    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }
    List<DataSegmentChangeResponse> result = future.get();
    Assertions.assertEquals(State.FAILED, result.get(0).getStatus().getState());
    Assertions.assertEquals(ImmutableList.of(), segmentAnnouncer.getObservedSegments());

    future = handler.processBatch(batch, SegmentLoadingMode.NORMAL);
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }
    result = future.get();
    Assertions.assertEquals(SegmentChangeStatus.success(SegmentLoadingMode.NORMAL), result.get(0).getStatus());
    Assertions.assertEquals(ImmutableList.of(segment1, segment1), segmentAnnouncer.getObservedSegments());

  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testProcessBatchLoadDropLoadSequenceForSameSegment() throws Exception
  {
    final SegmentManager segmentManager = Mockito.mock(SegmentManager.class);
    // loadSegment returns DataSegment; return the input so the announcement path sees a plain DataSegment.
    Mockito.when(segmentManager.loadSegment(ArgumentMatchers.any()))
           .thenAnswer(invocation -> invocation.getArgument(0));
    Mockito.doNothing().when(segmentManager).dropSegment(ArgumentMatchers.any());

    final File storageDir = newFolder(temporaryFolder, "junit");
    final SegmentLoaderConfig noAnnouncerSegmentLoaderConfig = SegmentLoaderConfig.builder()
        .infoDir(storageDir)
        .numLoadingThreads(5)
        .announceIntervalMillis(0)
        .locations(new StorageLocationConfig(storageDir, null, null))
        .dropSegmentDelayMillis(0)
        .build();

    final SegmentLoadDropHandler handler = initSegmentLoadDropHandler(
        noAnnouncerSegmentLoaderConfig,
        segmentManager
    );


    final DataSegment segment1 = makeSegment("batchtest1", "1", Intervals.of("P1d/2011-04-01"));
    List<DataSegmentChangeRequest> batch = ImmutableList.of(new SegmentChangeRequestLoad(segment1));

    // Request 1: Load the segment
    ListenableFuture<List<DataSegmentChangeResponse>> future = handler.processBatch(batch, SegmentLoadingMode.NORMAL);
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }
    List<DataSegmentChangeResponse> result = future.get();
    Assertions.assertEquals(State.SUCCESS, result.get(0).getStatus().getState());
    Assertions.assertEquals(ImmutableList.of(segment1), segmentAnnouncer.getObservedSegments());
    scheduledRunnable.clear();

    // Request 2: Drop the segment
    batch = ImmutableList.of(new SegmentChangeRequestDrop(segment1));
    future = handler.processBatch(batch, SegmentLoadingMode.NORMAL);
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }
    result = future.get();
    Assertions.assertEquals(State.SUCCESS, result.get(0).getStatus().getState());
    Assertions.assertEquals(ImmutableList.of(), segmentAnnouncer.getObservedSegments());
    Assertions.assertFalse(segmentAnnouncer.getObservedSegments().contains(segment1)); //
    scheduledRunnable.clear();

    // check invocations after a load-drop sequence
    Mockito.verify(segmentManager, Mockito.times(1))
           .loadSegment(ArgumentMatchers.any());
    Mockito.verify(segmentManager, Mockito.times(1))
           .dropSegment(ArgumentMatchers.any());

    // Request 3: Reload the segment
    batch = ImmutableList.of(new SegmentChangeRequestLoad(segment1));
    future = handler.processBatch(batch, SegmentLoadingMode.NORMAL);
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }
    result = future.get();
    Assertions.assertEquals(State.SUCCESS, result.get(0).getStatus().getState());
    Assertions.assertEquals(ImmutableList.of(segment1), segmentAnnouncer.getObservedSegments());
    scheduledRunnable.clear();

    // check invocations - 1 more load has happened
    Mockito.verify(segmentManager, Mockito.times(2))
           .loadSegment(ArgumentMatchers.any());
    Mockito.verify(segmentManager, Mockito.times(1))
           .dropSegment(ArgumentMatchers.any());

    // Request 4: Try to reload the segment - segment is loaded and announced again
    batch = ImmutableList.of(new SegmentChangeRequestLoad(segment1));
    future = handler.processBatch(batch, SegmentLoadingMode.NORMAL);
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }
    result = future.get();
    Assertions.assertEquals(State.SUCCESS, result.get(0).getStatus().getState());
    Assertions.assertEquals(ImmutableList.of(segment1, segment1), segmentAnnouncer.getObservedSegments());
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
        scheduledExecutorFactory.create(5, "LoadDropHandlerTest-[%d]"),
        (ThreadPoolExecutor) scheduledExecutorFactory.create(5, "TurboSegmentLoadDropHandlerTest-[%d]")
    );
  }

  private static File newFolder(File root, String... subDirs) throws IOException
  {
    if (subDirs.length == 0 || (subDirs.length == 1 && "junit".equals(subDirs[0]))) {
      return java.nio.file.Files.createTempDirectory(root.toPath(), "junit").toFile();
    }
    String subFolder = String.join("/", subDirs);
    File result = new File(root, subFolder);
    if (!result.mkdirs()) {
      throw new IOException("Couldn't create folders " + root);
    }
    return result;
  }
}
